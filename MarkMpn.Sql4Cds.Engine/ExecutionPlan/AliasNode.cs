using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    public class AliasNode : SelectNode
    {
        public AliasNode(SelectNode select)
        {
            ColumnSet.AddRange(select.ColumnSet);
            Source = select.Source;
        }

        public string Alias { get; set; }

        public override void AddRequiredColumns(IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes, IList<string> requiredColumns)
        {
            // Map the aliased names to the base names
            for (var i = 0; i < requiredColumns.Count; i++)
            {
                if (requiredColumns[i].StartsWith(Alias + "."))
                    requiredColumns[i] = requiredColumns[i].Substring(Alias.Length + 1);
            }

            base.AddRequiredColumns(metadata, parameterTypes, requiredColumns);
        }

        public override IExecutionPlanNode FoldQuery(IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes)
        {
            base.FoldQuery(metadata, options, parameterTypes);

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
            // TODO: Handle wildcards
            var sourceSchema = Source.GetSchema(metadata, parameterTypes);
            var schema = new NodeSchema();

            foreach (var col in ColumnSet)
            {
                if (!sourceSchema.ContainsColumn(col.SourceColumn, out var normalized))
                    continue;

                var mapped = $"{Alias}.{col.OutputColumn}";
                schema.Schema[mapped] = sourceSchema.Schema[normalized];

                if (normalized == sourceSchema.PrimaryKey)
                    schema.PrimaryKey = mapped;

                if (!schema.Aliases.TryGetValue(col.OutputColumn, out var aliases))
                {
                    aliases = new List<string>();
                    schema.Aliases[col.OutputColumn] = aliases;
                }

                aliases.Add(mapped);
            }

            return schema;
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
    }
}
