using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// Concatenates the results from multiple queries
    /// </summary>
    class ConcatenateNode : BaseDataNode
    {
        /// <summary>
        /// The data sources to concatenate
        /// </summary>
        [Browsable(false)]
        public List<IDataExecutionPlanNodeInternal> Sources { get; } = new List<IDataExecutionPlanNodeInternal>();

        /// <summary>
        /// The columns to produce in the result and the source columns from each data source
        /// </summary>
        [Category("Concatenate")]
        [Description("The columns to produce in the result and the source columns from each data source")]
        [DisplayName("Column Set")]
        public List<ConcatenateColumn> ColumnSet { get; } = new List<ConcatenateColumn>();

        protected override IEnumerable<Entity> ExecuteInternal(NodeExecutionContext context)
        {
            for (var i = 0; i < Sources.Count; i++)
            {
                var source = Sources[i];

                foreach (var entity in source.Execute(context))
                {
                    var result = new Entity(entity.LogicalName, entity.Id);

                    foreach (var col in ColumnSet)
                        result[col.OutputColumn] = entity[col.SourceColumns[i]];

                    yield return result;
                }
            }
        }

        public override INodeSchema GetSchema(NodeCompilationContext context)
        {
            var schema = new ColumnList();

            var sourceSchema = Sources[0].GetSchema(context);

            foreach (var col in ColumnSet)
                schema[col.OutputColumn] = sourceSchema.Schema[col.SourceColumns[0]];

            return new NodeSchema(
                primaryKey: null,
                schema: schema,
                aliases: null,
                sortOrder: null);
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            return Sources;
        }

        public override IDataExecutionPlanNodeInternal FoldQuery(NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            for (var i = 0; i < Sources.Count; i++)
            {
                Sources[i] = Sources[i].FoldQuery(context, hints);
                Sources[i].Parent = this;
            }

            // Work out the column types
            var sourceColumnTypes = Sources.Select((source, index) => GetColumnTypes(index, context)).ToArray();
            var types = (DataTypeReference[]) sourceColumnTypes[0].Clone();

            for (var i = 1; i < Sources.Count; i++)
            {
                var nextTypes = GetColumnTypes(i, context);

                for (var colIndex = 0; colIndex < types.Length; colIndex++)
                {
                    if (!SqlTypeConverter.CanMakeConsistentTypes(types[colIndex], nextTypes[colIndex], context.PrimaryDataSource, null, "union", out var colType))
                        throw new NotSupportedQueryFragmentException(Sql4CdsError.TypeClash(ColumnSet[colIndex].SourceExpressions[i], types[colIndex], nextTypes[colIndex]));

                    types[colIndex] = colType;
                }
            }

            // Apply any necessary conversions
            for (var i = 0; i < Sources.Count; i++)
            {
                var constant = Sources[i] as ConstantScanNode;
                var conversion = new ComputeScalarNode { Source = Sources[i] };

                for (var col = 0; col < ColumnSet.Count; col++)
                {
                    if (types[col].IsSameAs(sourceColumnTypes[i][col]))
                        continue;

                    var sourceCol = ColumnSet[col].SourceColumns[i];

                    if (constant != null)
                    {
                        foreach (var row in constant.Values)
                            row[sourceCol] = new ConvertCall { Parameter = row[sourceCol], DataType = types[col] };

                        constant.Schema[sourceCol] = new ColumnDefinition(types[col], constant.Schema[sourceCol].IsNullable, constant.Schema[sourceCol].IsCalculated);
                    }
                    else
                    {
                        conversion.Columns[sourceCol + "_converted"] = new ConvertCall { Parameter = sourceCol.ToColumnReference(), DataType = types[col] };
                        ColumnSet[col].SourceColumns[i] = sourceCol + "_converted";
                    }
                }

                if (conversion.Columns.Count > 0)
                {
                    Sources[i] = conversion.FoldQuery(context, hints);
                    Sources[i].Parent = this;

                    if (Sources[i] is ComputeScalarNode foldedConversion)
                    {
                        // Might be producing intermediate values that aren't necessary
                        var valuesToRemove = foldedConversion.Columns.Keys
                            .Where(calc => !ColumnSet.Any(col => col.SourceColumns[i] == calc))
                            .ToList();

                        foreach (var calc in valuesToRemove)
                            foldedConversion.Columns.Remove(calc);
                    }
                }
            }

            // If all the sources are constants, combine them
            if (Sources.All(s => s is ConstantScanNode))
            {
                var constants = Sources.Cast<ConstantScanNode>().ToArray();
                var originalRows = constants[0].Values.Count;

                for (var i = 0; i < constants.Length; i++)
                {
                    foreach (var row in constants[i].Values.ToList())
                    {
                        var newRow = new Dictionary<string, ScalarExpression>(StringComparer.OrdinalIgnoreCase);

                        foreach (var col in ColumnSet)
                            newRow[col.OutputColumn] = row[col.SourceColumns[i]];

                        constants[0].Values.Add(newRow);
                    }
                }

                var newSchema = ColumnSet.ToDictionary(col => col.OutputColumn, col => constants[0].Schema[col.SourceColumns[0]]);
                constants[0].Schema.Clear();

                foreach (var col in newSchema)
                    constants[0].Schema[col.Key] = col.Value;

                constants[0].Values.RemoveRange(0, originalRows);

                return constants[0];
            }

            return this;
        }

        private DataTypeReference[] GetColumnTypes(int sourceIndex, NodeCompilationContext context)
        {
            var schema = Sources[sourceIndex].GetSchema(context);
            var types = new DataTypeReference[ColumnSet.Count];

            for (var i = 0; i < ColumnSet.Count; i++)
                types[i] = schema.Schema[ColumnSet[i].SourceColumns[sourceIndex]].Type;

            return types;
        }

        public override void AddRequiredColumns(NodeCompilationContext context, IList<string> requiredColumns)
        {
            for (var i = 0; i < Sources.Count; i++)
            {
                var sourceRequiredColumns = ColumnSet
                    .Select(c => c.SourceColumns[i])
                    .Distinct()
                    .ToList();

                Sources[i].AddRequiredColumns(context, sourceRequiredColumns);
            }
        }

        protected override RowCountEstimate EstimateRowsOutInternal(NodeCompilationContext context)
        {
            var estimate = 0;
            var minimum = 0;
            var maximum = 0;
            var exact = true;

            foreach (var source in Sources)
            {
                var sourceEstimate = source.EstimateRowsOut(context);

                estimate = AddUpToMaxValue(estimate, sourceEstimate.Value);

                if (exact && sourceEstimate is RowCountEstimateDefiniteRange range)
                {
                    minimum = AddUpToMaxValue(minimum, range.Minimum);
                    maximum = AddUpToMaxValue(maximum, range.Maximum);
                }
                else
                {
                    exact = false;
                }
            }

            if (exact)
                return new RowCountEstimateDefiniteRange(minimum, maximum);

            return new RowCountEstimate(estimate);
        }

        private int AddUpToMaxValue(int x, int y)
        {
            // Avoid overflow error when adding large row count estimates
            var maxAddition = Int32.MaxValue - x;

            if (y > maxAddition)
                return Int32.MaxValue;

            return x + y;
        }

        public override object Clone()
        {
            var clone = new ConcatenateNode();

            foreach (var source in Sources)
            {
                var sourceClone = (IDataExecutionPlanNodeInternal)source.Clone();
                sourceClone.Parent = clone;
                clone.Sources.Add(sourceClone);
            }

            foreach (var col in ColumnSet)
            {
                var colClone = new ConcatenateColumn();
                colClone.OutputColumn = col.OutputColumn;
                colClone.SourceColumns.AddRange(col.SourceColumns);
                colClone.SourceExpressions.AddRange(col.SourceExpressions);
                clone.ColumnSet.Add(colClone);
            }

            return clone;
        }
    }

    class ConcatenateColumn
    {
        /// <summary>
        /// The name of the column that is generated in the output
        /// </summary>
        [Description("The name of the column that is generated in the output")]
        [DictionaryKey]
        public string OutputColumn { get; set; }

        /// <summary>
        /// The names of the column in each source node that generates the data for this column
        /// </summary>
        [Description("The names of the column in each source node that generates the data for this column")]
        public List<string> SourceColumns { get; } = new List<string>();

        /// <summary>
        /// The expressions in the source queries that provide the column values.
        /// </summary>
        /// <remarks>
        /// Used for reporting errors only, not for calculations
        /// </remarks>
        [Browsable(false)]
        public List<TSqlFragment> SourceExpressions { get; } = new List<TSqlFragment>();
    }
}
