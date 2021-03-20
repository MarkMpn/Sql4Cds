using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    public class AliasNode : BaseDataNode, ISingleSourceExecutionPlanNode
    {
        public AliasNode(SelectNode select)
        {
            ColumnSet.AddRange(select.ColumnSet);
            Source = select.Source;
        }

        public string Alias { get; set; }

        public List<SelectColumn> ColumnSet { get; } = new List<SelectColumn>();

        public IDataExecutionPlanNode Source { get; set; }

        public override void AddRequiredColumns(IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes, IList<string> requiredColumns)
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

            Source.AddRequiredColumns(metadata, parameterTypes, requiredColumns);
        }

        public override IDataExecutionPlanNode FoldQuery(IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes)
        {
            Source = Source.FoldQuery(metadata, options, parameterTypes);
            Source.Parent = this;

            SelectNode.FoldFetchXmlColumns(Source, ColumnSet, metadata, parameterTypes);
            SelectNode.ExpandWildcardColumns(Source, ColumnSet, metadata, parameterTypes);

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

        public override NodeSchema GetSchema(IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes)
        {
            // Map the base names to the alias names
            var sourceSchema = Source.GetSchema(metadata, parameterTypes);
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

        protected override IEnumerable<Entity> ExecuteInternal(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues)
        {
            foreach (var entity in Source.Execute(org, metadata, options, parameterTypes, parameterValues))
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

        public override int EstimateRowsOut(IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes, ITableSizeCache tableSize)
        {
            return Source.EstimateRowsOut(metadata, parameterTypes, tableSize);
        }

        public override IEnumerable<IDataExecutionPlanNode> GetSources()
        {
            yield return Source;
        }
    }
}
