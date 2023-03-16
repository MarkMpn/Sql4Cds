using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.Engine.Visitors;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// Calculates the value of scalar expressions
    /// </summary>
    class ComputeScalarNode : BaseDataNode, ISingleSourceExecutionPlanNode
    {
        /// <summary>
        /// The names and associated expressions for the columns to calculate
        /// </summary>
        [Category("Compute Scalar")]
        [Description("The names and associated expressions for the columns to calculate")]
        public Dictionary<string, ScalarExpression> Columns { get; } = new Dictionary<string, ScalarExpression>();

        /// <summary>
        /// The data source to use for the calculations
        /// </summary>
        [Browsable(false)]
        public IDataExecutionPlanNodeInternal Source { get; set; }

        protected override IEnumerable<Entity> ExecuteInternal(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, DataTypeReference> parameterTypes, IDictionary<string, object> parameterValues)
        {
            var schema = Source.GetSchema(dataSources, parameterTypes);
            var columns = Columns
                .Select(kvp => new { Name = kvp.Key, Expression = kvp.Value.Compile(dataSources[options.PrimaryDataSource], schema, parameterTypes) })
                .ToList();

            foreach (var entity in Source.Execute(dataSources, options, parameterTypes, parameterValues))
            {
                foreach (var col in columns)
                    entity[col.Name] = col.Expression(entity, parameterValues, options);

                yield return entity;
            }
        }

        public override INodeSchema GetSchema(IDictionary<string, DataSource> dataSources, IDictionary<string, DataTypeReference> parameterTypes)
        {
            // Copy the source schema and add in the additional computed columns
            var sourceSchema = Source.GetSchema(dataSources, parameterTypes);
            var schema = new Dictionary<string, DataTypeReference>(sourceSchema.Schema.Count, StringComparer.OrdinalIgnoreCase);

            foreach (var col in sourceSchema.Schema)
                schema[col.Key] = col.Value;

            foreach (var calc in Columns)
            {
                calc.Value.GetType(dataSources[options.PrimaryDataSource], sourceSchema, null, parameterTypes, out var calcType);
                schema[calc.Key] = calcType;
            }

            return new NodeSchema(
                primaryKey: sourceSchema.PrimaryKey,
                schema: schema,
                aliases: sourceSchema.Aliases,
                notNullColumns: sourceSchema.NotNullColumns,
                sortOrder: sourceSchema.SortOrder);
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            yield return Source;
        }

        public override IDataExecutionPlanNodeInternal FoldQuery(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, DataTypeReference> parameterTypes, IList<OptimizerHint> hints)
        {
            Source = Source.FoldQuery(dataSources, options, parameterTypes, hints);

            // Combine multiple ComputeScalar nodes. Calculations in this node might be dependent on those in the previous node, so rewrite any references
            // to the earlier computed columns
            if (Source is ComputeScalarNode computeScalar)
            {
                var rewrites = new Dictionary<ScalarExpression, ScalarExpression>();

                foreach (var prevCalc in computeScalar.Columns)
                    rewrites[prevCalc.Key.ToColumnReference()] = prevCalc.Value;

                var rewrite = new RewriteVisitor(rewrites);

                foreach (var calc in Columns)
                    computeScalar.Columns.Add(calc.Key, rewrite.ReplaceExpression(calc.Value));

                return computeScalar;
            }
            
            if (Source is ConstantScanNode constant && String.IsNullOrEmpty(constant.Alias))
            {
                var folded = new List<string>();

                foreach (var calc in Columns)
                {
                    if (calc.Value is ConvertCall c1 && c1.Parameter is Literal ||
                        calc.Value is CastCall c2 && c2.Parameter is Literal ||
                        calc.Value is Literal)
                    {
                        foreach (var row in constant.Values)
                            row[calc.Key] = calc.Value;

                        folded.Add(calc.Key);

                        if (calc.Value is ConvertCall convert)
                        {
                            constant.Schema[calc.Key] = convert.DataType;
                        }
                        else if (calc.Value is CastCall cast)
                        {
                            constant.Schema[calc.Key] = cast.DataType;
                        }
                        else
                        {
                            calc.Value.GetType(dataSources[options.PrimaryDataSource], null, null, parameterTypes, out var calcType);
                            constant.Schema[calc.Key] = calcType;
                        }
                    }
                }

                if (folded.Count == Columns.Count)
                    return constant;

                foreach (var col in folded)
                    Columns.Remove(col);
            }
            
            Source.Parent = this;
            return this;
        }

        public override void AddRequiredColumns(IDictionary<string, DataSource> dataSources, IDictionary<string, DataTypeReference> parameterTypes, IList<string> requiredColumns)
        {
            var schema = Source.GetSchema(dataSources, parameterTypes);

            var calcSourceColumns = Columns.Values
                .SelectMany(expr => expr.GetColumns());

            foreach (var col in calcSourceColumns)
            {
                if (!schema.ContainsColumn(col, out var normalized))
                    continue;

                if (!requiredColumns.Contains(normalized, StringComparer.OrdinalIgnoreCase))
                    requiredColumns.Add(normalized);
            }

            Source.AddRequiredColumns(dataSources, parameterTypes, requiredColumns);
        }

        protected override RowCountEstimate EstimateRowsOutInternal(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, DataTypeReference> parameterTypes)
        {
            return Source.EstimateRowsOut(dataSources, options, parameterTypes);
        }

        protected override IEnumerable<string> GetVariablesInternal()
        {
            return Columns
                .Values
                .SelectMany(expr => expr.GetVariables())
                .Distinct();
        }

        public override object Clone()
        {
            var clone = new ComputeScalarNode
            {
                Source = (IDataExecutionPlanNodeInternal)Source.Clone()
            };

            clone.Source.Parent = clone;

            foreach (var kvp in Columns)
                clone.Columns.Add(kvp.Key, kvp.Value);

            return clone;
        }
    }
}
