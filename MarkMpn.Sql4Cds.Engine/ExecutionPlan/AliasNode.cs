using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;
using Newtonsoft.Json;

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
        public AliasNode(SelectNode select, Identifier identifier, NodeCompilationContext context)
        {
            if (select == null)
                return;

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
                throw new NotSupportedQueryFragmentException(Sql4CdsError.DuplicateColumn(identifier, duplicateColumn.Key));

            // Ensure each column has an output name
            foreach (var col in ColumnSet)
            {
                if (string.IsNullOrEmpty(col.OutputColumn))
                    col.OutputColumn = context.GetExpressionName();
                else
                    col.OutputColumn = col.OutputColumn.EscapeIdentifier();
            }
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
        [JsonIgnore]
        public INodeSchema LogicalSourceSchema { get; set; }

        public override void AddRequiredColumns(NodeCompilationContext context, IList<string> requiredColumns)
        {
            var mappings = ColumnSet.Where(col => !col.AllColumns).ToDictionary(col => col.OutputColumn, col => col.SourceColumn);
            var required = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            var escapedAlias = Alias.EscapeIdentifier();

            // Map the aliased names to the base names
            for (var i = 0; i < requiredColumns.Count; i++)
            {
                if (requiredColumns[i].StartsWith(escapedAlias + "."))
                {
                    requiredColumns[i] = requiredColumns[i].Substring(escapedAlias.Length + 1);

                    if (mappings.TryGetValue(requiredColumns[i], out var sourceCol))
                    {
                        required.Add(requiredColumns[i]);
                        requiredColumns[i] = sourceCol;
                    }
                }
            }

            // Remove any unsued column mappings
            for (var i = ColumnSet.Count - 1; i >= 0; i--)
            {
                if (!ColumnSet[i].AllColumns && !required.Contains(ColumnSet[i].OutputColumn))
                    ColumnSet.RemoveAt(i);
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
                FoldToFetchXML(fetchXml);
                return fetchXml;
            }

            if (Source is ConstantScanNode constant)
            {
                // Remove any unused columns
                var unusedColumns = constant.Schema.Keys
                    .Where(sourceCol => !ColumnSet.Any(col => (String.IsNullOrEmpty(constant.Alias) && col.SourceColumn == sourceCol) || (!String.IsNullOrEmpty(constant.Alias) && col.SourceColumn == constant.Alias.EscapeIdentifier() + "." + sourceCol)))
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
                    var sourceColumn = constant.Alias == null ? col.SourceColumn : col.SourceColumn.SplitMultiPartIdentifier().Last();

                    if (String.IsNullOrEmpty(constant.Alias) && col.OutputColumn != col.SourceColumn ||
                        !String.IsNullOrEmpty(constant.Alias) && col.OutputColumn != constant.Alias.EscapeIdentifier() + "." + col.SourceColumn)
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

        internal void FoldToFetchXML(FetchXmlScan fetchXml)
        {
            // Add the mappings to the FetchXML to produce the columns with the expected names, and hide all other possible columns
            var originalAlias = fetchXml.Alias.EscapeIdentifier();
            fetchXml.Alias = Alias;
            fetchXml.ColumnMappings.Clear();

            var escapedAlias = Alias.EscapeIdentifier();

            foreach (var col in ColumnSet)
            {
                if (col.SourceColumn != null && col.SourceColumn.StartsWith(originalAlias + "."))
                    col.SourceColumn = escapedAlias + col.SourceColumn.Substring(originalAlias.Length);

                if (col.AllColumns)
                    col.OutputColumn = escapedAlias;
                else if (col.OutputColumn != null)
                    col.OutputColumn = escapedAlias + "." + col.OutputColumn;

                fetchXml.ColumnMappings.Add(col);
            }

            fetchXml.HiddenAliases.Add(Alias);

            foreach (var link in fetchXml.Entity.GetLinkEntities())
                fetchXml.HiddenAliases.Add(link.alias);
        }

        public override INodeSchema GetSchema(NodeCompilationContext context)
        {
            // Map the base names to the alias names
            var sourceSchema = Source.GetSchema(context);
            var schema = new ColumnList();
            var aliases = new Dictionary<string, IReadOnlyList<string>>(StringComparer.OrdinalIgnoreCase);
            var primaryKey = (string)null;
            var mappings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            var escapedAlias = Alias.EscapeIdentifier();

            foreach (var col in ColumnSet)
            {
                if (col.AllColumns)
                {
                    foreach (var sourceCol in sourceSchema.Schema)
                    {
                        if (col.SourceColumn != null && !sourceCol.Key.StartsWith(col.SourceColumn + "."))
                            continue;

                        var simpleName = sourceCol.Key.SplitMultiPartIdentifier().Last();
                        AddSchemaColumn(escapedAlias, simpleName, sourceCol.Key, schema, aliases, ref primaryKey, mappings, sourceSchema);
                    }
                }
                else
                {
                    AddSchemaColumn(escapedAlias, col.OutputColumn, col.SourceColumn, schema, aliases, ref primaryKey, mappings, sourceSchema);
                }
            }

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
                sortOrder: sortOrder);
        }

        private void AddSchemaColumn(string escapedAlias, string outputColumn, string sourceColumn, ColumnList schema, Dictionary<string, IReadOnlyList<string>> aliases, ref string primaryKey, Dictionary<string, string> mappings, INodeSchema sourceSchema)
        {
            if (!sourceSchema.ContainsColumn(sourceColumn, out var normalized))
                return;

            var mapped = $"{escapedAlias}.{outputColumn}";
            schema[mapped] = new ColumnDefinition(sourceSchema.Schema[normalized].Type, sourceSchema.Schema[normalized].IsNullable, false);
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
            var escapedAlias = Alias.EscapeIdentifier();

            foreach (var entity in Source.Execute(context))
            {
                foreach (var col in ColumnSet)
                {
                    var mapped = $"{escapedAlias}.{col.OutputColumn.EscapeIdentifier()}";
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

            foreach (var col in ColumnSet)
            {
                clone.ColumnSet.Add(new SelectColumn
                {
                    AllColumns = col.AllColumns,
                    OutputColumn = col.OutputColumn,
                    SourceColumn = col.SourceColumn,
                    SourceExpression = col.SourceExpression,
                });
            }

            return clone;
        }
    }
}
