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

            // Check for duplicate columns
            var duplicateColumn = select.ColumnSet
                .GroupBy(col => col.OutputColumn, StringComparer.OrdinalIgnoreCase)
                .Where(g => g.Count() > 1)
                .FirstOrDefault();

            if (duplicateColumn != null)
                throw new NotSupportedQueryFragmentException($"The column '{duplicateColumn.Key}' was specified multiple times", identifier);
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
        public IDataExecutionPlanNode Source { get; set; }

        public override void AddRequiredColumns(IDictionary<string, DataSource> dataSources, IDictionary<string, Type> parameterTypes, IList<string> requiredColumns)
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

            Source.AddRequiredColumns(dataSources, parameterTypes, requiredColumns);
        }

        public override IDataExecutionPlanNode FoldQuery(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes)
        {
            Source = Source.FoldQuery(dataSources, options, parameterTypes);
            Source.Parent = this;

            SelectNode.FoldFetchXmlColumns(Source, ColumnSet, dataSources, parameterTypes);
            SelectNode.ExpandWildcardColumns(Source, ColumnSet, dataSources, parameterTypes);

            if (Source is FetchXmlScan fetchXml)
            {
                // Check if all the source and output column names match. If so, just change the alias of the source FetchXML
                if (ColumnSet.All(col => col.SourceColumn == $"{fetchXml.Alias}.{col.OutputColumn}"))
                {
                    fetchXml.Alias = Alias;
                    return fetchXml;
                }
            }

            return this;
        }

        public override NodeSchema GetSchema(IDictionary<string, DataSource> dataSources, IDictionary<string, Type> parameterTypes)
        {
            // Map the base names to the alias names
            var sourceSchema = Source.GetSchema(dataSources, parameterTypes);
            var schema = new NodeSchema();

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

                        AddSchemaColumn(simpleName, sourceCol.Key, schema, sourceSchema);
                    }
                }
                else
                {
                    AddSchemaColumn(col.OutputColumn, col.SourceColumn, schema, sourceSchema);
                }
            }

            return schema;
        }

        private void AddSchemaColumn(string outputColumn, string sourceColumn, NodeSchema schema, NodeSchema sourceSchema)
        {
            if (!sourceSchema.ContainsColumn(sourceColumn, out var normalized))
                return;

            var mapped = $"{Alias}.{outputColumn}";
            schema.Schema[mapped] = sourceSchema.Schema[normalized];

            if (normalized == sourceSchema.PrimaryKey)
                schema.PrimaryKey = mapped;

            if (!schema.Aliases.TryGetValue(outputColumn, out var aliases))
            {
                aliases = new List<string>();
                schema.Aliases[outputColumn] = aliases;
            }

            aliases.Add(mapped);
        }

        protected override IEnumerable<Entity> ExecuteInternal(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues)
        {
            foreach (var entity in Source.Execute(dataSources, options, parameterTypes, parameterValues))
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

        public override int EstimateRowsOut(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes)
        {
            return Source.EstimateRowsOut(dataSources, options, parameterTypes);
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            yield return Source;
        }
    }
}
