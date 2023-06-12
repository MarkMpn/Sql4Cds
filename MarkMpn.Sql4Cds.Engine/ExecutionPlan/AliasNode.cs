using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// Applies a different alias to the results of a query to keep names unique throughout a query plan
    /// </summary>
    class AliasNode : BaseDataNode, ISingleSourceExecutionPlanNode
    {
        /// <summary>
        /// Creates a new <see cref="AliasNode"/> to wrap the results of a subquery with a different alias
        /// </summary>
        /// <param name="select">The subquery to wrap the results of</param>
        /// <param name="identifier">The alias to use for the subquery</param>
        public AliasNode(SelectNode select, Identifier identifier)
        {
            ColumnSet.AddRange(select.ColumnSet);
            Source = select.Source;
            Alias = identifier.Value;
            LogicalSourceSchema = select.LogicalSourceSchema;

            // Check for duplicate columns
            var duplicateColumn = select.ColumnSet
                .GroupBy(col => col.OutputColumn, StringComparer.OrdinalIgnoreCase)
                .Where(g => g.Count() > 1)
                .FirstOrDefault();

            if (duplicateColumn != null)
                throw new NotSupportedQueryFragmentException($"The column '{duplicateColumn.Key}' was specified multiple times", identifier);
        }

        private AliasNode()
        {
        }

        /// <summary>
        /// The alias to apply to the results of the subquery
        /// </summary>
        [Category("Alias")]
        [Description("The alias to apply to the results of the subquery")]
        public string Alias { get; set; }

        /// <summary>
        /// The columns that are produced by the subquery
        /// </summary>
        [Category("Alias")]
        [Description("The columns that are produced by the subquery")]
        [DisplayName("Column Set")]
        public List<SelectColumn> ColumnSet { get; } = new List<SelectColumn>();

        /// <summary>
        /// The data source of the query
        /// </summary>
        [Browsable(false)]
        public IDataExecutionPlanNodeInternal Source { get; set; }

        /// <summary>
        /// The schema that shold be used for expanding "*" columns
        /// </summary>
        [Browsable(false)]
        public INodeSchema LogicalSourceSchema { get; set; }

        public override void AddRequiredColumns(NodeCompilationContext context, IList<string> requiredColumns)
        {
            var mappings = ColumnSet.Where(col => !col.AllColumns).ToDictionary(col => col.OutputColumn, col => col.SourceColumn);
            ColumnSet.Clear();

            // Map the aliased names to the base names
            for (var i = 0; i < requiredColumns.Count; i++)
            {
                if (requiredColumns[i].StartsWith(Alias + "."))
                {
                    requiredColumns[i] = requiredColumns[i].Substring(Alias.Length + 1);

                    if (!mappings.TryGetValue(requiredColumns[i], out var sourceCol))
                        sourceCol = requiredColumns[i];

                    ColumnSet.Add(new SelectColumn
                    {
                        SourceColumn = sourceCol,
                        OutputColumn = requiredColumns[i]
                    });

                    requiredColumns[i] = sourceCol;
                }
            }

            Source.AddRequiredColumns(context, requiredColumns);
        }

        public override IDataExecutionPlanNodeInternal FoldQuery(NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            Source = Source.FoldQuery(context, hints);
            Source.Parent = this;

            SelectNode.FoldFetchXmlColumns(Source, ColumnSet, context);
            SelectNode.ExpandWildcardColumns(Source, LogicalSourceSchema, ColumnSet, context);

            if (Source is FetchXmlScan fetchXml)
            {
                // Check if all the source and output column names match. If so, just change the alias of the source FetchXML
                if (ColumnSet.All(col => col.SourceColumn == $"{fetchXml.Alias}.{col.OutputColumn}"))
                {
                    fetchXml.Alias = Alias;
                    return fetchXml;
                }
            }

            if (Source is ConstantScanNode constant)
            {
                // Remove any unused columns
                var unusedColumns = constant.Schema.Keys
                    .Where(sourceCol => !ColumnSet.Any(col => col.SourceColumn.Split('.').Last() == sourceCol))
                    .ToList();

                foreach (var col in unusedColumns)
                {
                    constant.Schema.Remove(col);

                    foreach (var row in constant.Values)
                        row.Remove(col);
                }

                // Copy/rename any columns using the new aliases
                foreach (var col in ColumnSet)
                {
                    var sourceColumn = col.SourceColumn.Split('.').Last();

                    if (String.IsNullOrEmpty(constant.Alias) && col.OutputColumn != col.SourceColumn ||
                        !String.IsNullOrEmpty(constant.Alias) && col.OutputColumn != constant.Alias + "." + col.SourceColumn)
                    {
                        constant.Schema[col.OutputColumn] = constant.Schema[sourceColumn];

                        foreach (var row in constant.Values)
                            row[col.OutputColumn] = row[sourceColumn];
                    }
                }

                // Change the alias of the whole constant scan
                constant.Alias = Alias;
                return constant;
            }

            return this;
        }

        public override INodeSchema GetSchema(NodeCompilationContext context)
        {
            // Map the base names to the alias names
            var sourceSchema = Source.GetSchema(context);
            var schema = new ColumnList();
            var aliases = new Dictionary<string, IReadOnlyList<string>>();
            var primaryKey = (string)null;
            var mappings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            foreach (var col in ColumnSet)
            {
                if (col.AllColumns)
                {
                    foreach (var sourceCol in sourceSchema.Schema)
                    {
                        if (col.SourceColumn != null && !sourceCol.Key.StartsWith(col.SourceColumn + "."))
                            continue;

                        var simpleName = sourceCol.Key.Split('.').Last();
                        var outputName = $"{Alias}.{simpleName}";

                        AddSchemaColumn(simpleName, sourceCol.Key, schema, aliases, ref primaryKey, mappings, sourceSchema);
                    }
                }
                else
                {
                    AddSchemaColumn(col.OutputColumn, col.SourceColumn, schema, aliases, ref primaryKey, mappings, sourceSchema);
                }
            }

            var notNullColumns = sourceSchema.NotNullColumns
                .Select(col =>
                {
                    sourceSchema.ContainsColumn(col, out col);
                    return col;
                })
                .Where(col => col != null)
                .Select(col =>
                {
                    mappings.TryGetValue(col, out col);
                    return col;
                })
                .Where(col => col != null)
                .ToArray();
            var sortOrder = sourceSchema.SortOrder
                .Select(col =>
                {
                    sourceSchema.ContainsColumn(col, out col);
                    return col;
                })
                .Where(col => col != null)
                .Select(col =>
                {
                    mappings.TryGetValue(col, out col);
                    return col;
                })
                .Where(col => col != null)
                .ToArray();

            return new NodeSchema(
                primaryKey: primaryKey,
                schema: schema,
                aliases: aliases,
                notNullColumns: notNullColumns,
                sortOrder: sortOrder);
        }

        private void AddSchemaColumn(string outputColumn, string sourceColumn, ColumnList schema, Dictionary<string, IReadOnlyList<string>> aliases, ref string primaryKey, Dictionary<string, string> mappings, INodeSchema sourceSchema)
        {
            if (!sourceSchema.ContainsColumn(sourceColumn, out var normalized))
                return;

            var mapped = $"{Alias}.{outputColumn}";
            schema[mapped] = sourceSchema.Schema[normalized];
            mappings[normalized] = mapped;

            if (normalized == sourceSchema.PrimaryKey)
                primaryKey = mapped;

            if (!aliases.TryGetValue(outputColumn, out var a))
            {
                a = new List<string>();
                aliases[outputColumn] = a;
            }

            ((List<string>)a).Add(mapped);
        }

        protected override IEnumerable<Entity> ExecuteInternal(NodeExecutionContext context)
        {
            foreach (var entity in Source.Execute(context))
            {
                foreach (var col in ColumnSet)
                {
                    var mapped = $"{Alias}.{col.OutputColumn}";
                    entity[mapped] = entity[col.SourceColumn];
                }

                yield return entity;
            }
        }

        public override string ToString()
        {
            return "Subquery Alias";
        }

        protected override RowCountEstimate EstimateRowsOutInternal(NodeCompilationContext context)
        {
            return Source.EstimateRowsOut(context);
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            yield return Source;
        }

        public override object Clone()
        {
            var clone = new AliasNode
            {
                Alias = Alias,
                Source = (IDataExecutionPlanNodeInternal)Source.Clone(),
                LogicalSourceSchema = LogicalSourceSchema,
            };

            clone.Source.Parent = clone;
            clone.ColumnSet.AddRange(ColumnSet);

            return clone;
        }
    }
}
