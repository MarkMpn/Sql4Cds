using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Metadata;
using Microsoft.Xrm.Sdk.Metadata.Query;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    public class MetadataQueryNode : BaseNode
    {
        private IDictionary<string, string> _entityProps;

        public bool IncludeEntity { get; set; }

        public string EntityAlias { get; set; }

        public EntityQueryExpression Query { get; } = new EntityQueryExpression();

        public override void AddRequiredColumns(IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes, IList<string> requiredColumns)
        {
            _entityProps = new Dictionary<string, string>();

            foreach (var col in requiredColumns)
            {
                var parts = col.Split('.');

                if (parts.Length != 2)
                    continue;

                if (parts[0].Equals(EntityAlias, StringComparison.OrdinalIgnoreCase))
                {
                    if (Query.Properties == null)
                        Query.Properties = new MetadataPropertiesExpression();

                    var prop = typeof(EntityMetadata).GetProperty(parts[1], BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase);
                    Query.Properties.PropertyNames.Add(prop.Name);

                    _entityProps[col] = prop.Name;
                }
            }
        }

        public override int EstimateRowsOut(IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes, ITableSizeCache tableSize)
        {
            return 100;
        }

        public override IExecutionPlanNode FoldQuery(IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes)
        {
            return this;
        }

        public override NodeSchema GetSchema(IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes)
        {
            var schema = new NodeSchema();

            if (IncludeEntity)
            {
                var excludedProps = new[]
                {
                    nameof(EntityMetadata.ExtensionData),
                    nameof(EntityMetadata.Attributes),
                    nameof(EntityMetadata.Keys),
                    nameof(EntityMetadata.ManyToManyRelationships),
                    nameof(EntityMetadata.ManyToOneRelationships),
                    nameof(EntityMetadata.OneToManyRelationships),
                    nameof(EntityMetadata.Privileges)
                };

                var entityProps = typeof(EntityMetadata).GetProperties().Where(p => !excludedProps.Contains(p.Name));

                if (Query.Properties != null)
                    entityProps = entityProps.Where(p => Query.Properties.PropertyNames.Contains(p.Name, StringComparer.OrdinalIgnoreCase));

                foreach (var prop in entityProps)
                {
                    var propType = prop.PropertyType;

                    if (propType == typeof(Label) || propType.IsEnum || propType.IsGenericType && propType.GetGenericTypeDefinition() == typeof(Nullable<>) && propType.GetGenericArguments()[0].IsEnum)
                        propType = typeof(string);

                    schema.Schema[$"{EntityAlias}.{prop.Name}"] = propType;

                    if (!schema.Aliases.TryGetValue(prop.Name, out var aliases))
                    {
                        aliases = new List<string>();
                        schema.Aliases[prop.Name] = aliases;
                    }

                    aliases.Add($"{EntityAlias}.{prop.Name}");
                }

                schema.PrimaryKey = $"{EntityAlias}.{nameof(EntityMetadata.MetadataId)}";
            }

            return schema;
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            return Array.Empty<IExecutionPlanNode>();
        }

        protected override IEnumerable<Entity> ExecuteInternal(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues)
        {
            var resp = (RetrieveMetadataChangesResponse) org.Execute(new RetrieveMetadataChangesRequest { Query = Query });
            var entityProps = typeof(EntityMetadata).GetProperties().ToDictionary(p => p.Name);

            foreach (var entity in resp.EntityMetadata)
            {
                if (IncludeEntity)
                {
                    var converted = new Entity("entity", entity.MetadataId ?? Guid.Empty);

                    foreach (var prop in _entityProps)
                    {
                        var value = entityProps[prop.Value].GetValue(entity);

                        if (value is Label l)
                            value = l.UserLocalizedLabel?.Label;
                        else if (value != null && value.GetType().IsEnum)
                            value = value.ToString();

                        converted[prop.Key] = value;
                    }

                    yield return converted;
                }
            }
        }
    }
}
