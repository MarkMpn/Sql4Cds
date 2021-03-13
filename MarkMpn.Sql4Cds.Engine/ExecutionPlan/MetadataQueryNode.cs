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
        private IDictionary<string, string> _attributeProps;

        private static Type[] _attributeTypes = typeof(AttributeMetadata).Assembly.GetTypes().Where(t => typeof(AttributeMetadata).IsAssignableFrom(t) && !t.IsAbstract).ToArray();

        public bool IncludeEntity { get; set; }

        public string EntityAlias { get; set; }

        public bool IncludeAttribute { get; set; }

        public string AttributeAlias { get; set; }

        public EntityQueryExpression Query { get; } = new EntityQueryExpression();

        public override void AddRequiredColumns(IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes, IList<string> requiredColumns)
        {
            _entityProps = new Dictionary<string, string>();
            _attributeProps = new Dictionary<string, string>();

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

                    if (!Query.Properties.PropertyNames.Contains(prop.Name))
                        Query.Properties.PropertyNames.Add(prop.Name);

                    _entityProps[col] = prop.Name;
                }
                else if (parts[0].Equals(AttributeAlias, StringComparison.OrdinalIgnoreCase))
                {
                    if (Query.AttributeQuery == null)
                        Query.AttributeQuery = new AttributeQueryExpression();

                    if (Query.AttributeQuery.Properties == null)
                        Query.AttributeQuery.Properties = new MetadataPropertiesExpression();

                    foreach (var type in _attributeTypes)
                    {
                        var prop = type.GetProperty(parts[1], BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase);

                        if (prop == null)
                            continue;

                        if (!Query.AttributeQuery.Properties.PropertyNames.Contains(prop.Name))
                            Query.AttributeQuery.Properties.PropertyNames.Add(prop.Name);

                        _attributeProps[col] = prop.Name;
                    }
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

                var entityProps = typeof(EntityMetadata).GetProperties(BindingFlags.Public | BindingFlags.Instance).Where(p => !excludedProps.Contains(p.Name));

                if (Query.Properties != null)
                    entityProps = entityProps.Where(p => Query.Properties.PropertyNames.Contains(p.Name, StringComparer.OrdinalIgnoreCase));

                foreach (var prop in entityProps)
                {
                    schema.Schema[$"{EntityAlias}.{prop.Name}"] = GetPropertyType(prop.PropertyType);

                    if (!schema.Aliases.TryGetValue(prop.Name, out var aliases))
                    {
                        aliases = new List<string>();
                        schema.Aliases[prop.Name] = aliases;
                    }

                    aliases.Add($"{EntityAlias}.{prop.Name}");
                }

                schema.PrimaryKey = $"{EntityAlias}.{nameof(EntityMetadata.MetadataId)}";
            }

            if (IncludeAttribute)
            {
                var excludedProps = new[]
                {
                    nameof(AttributeMetadata.ExtensionData)
                };

                var attributeProps = _attributeTypes.SelectMany(t => t.GetProperties(BindingFlags.Public | BindingFlags.Instance).Where(p => !excludedProps.Contains(p.Name)));

                if (Query.AttributeQuery?.Properties != null)
                    attributeProps = attributeProps.Where(p => Query.AttributeQuery.Properties.PropertyNames.Contains(p.Name, StringComparer.OrdinalIgnoreCase));

                foreach (var prop in attributeProps.GroupBy(p => p.Name).Select(g => g.First()))
                {
                    schema.Schema[$"{AttributeAlias}.{prop.Name}"] = GetPropertyType(prop.PropertyType);

                    if (!schema.Aliases.TryGetValue(prop.Name, out var aliases))
                    {
                        aliases = new List<string>();
                        schema.Aliases[prop.Name] = aliases;
                    }

                    aliases.Add($"{AttributeAlias}.{prop.Name}");
                }

                schema.PrimaryKey = $"{AttributeAlias}.{nameof(AttributeMetadata.MetadataId)}";
            }

            return schema;
        }

        private Type GetPropertyType(Type propType)
        {
            if (propType.BaseType != null && propType.BaseType.IsGenericType && propType.BaseType.GetGenericTypeDefinition() == typeof(ManagedProperty<>))
                propType = propType.BaseType.GetGenericArguments()[0];

            if (propType.BaseType != null && propType.BaseType.IsGenericType && propType.BaseType.GetGenericTypeDefinition() == typeof(ConstantsBase<>))
                propType = propType.BaseType.GetGenericArguments()[0];

            if (propType.IsGenericType && propType.GetGenericTypeDefinition() == typeof(Nullable<>))
                propType = propType.GetGenericArguments()[0];

            if (propType == typeof(Label) || propType.IsEnum || propType.IsArray)
                propType = typeof(string);
            else if (typeof(MetadataBase).IsAssignableFrom(propType))
                propType = typeof(Guid?);

            return propType;
        }

        private object GetPropertyValue(PropertyInfo prop, object target)
        {
            var value = prop.GetValue(target);

            if (value == null)
                return null;
            
            var valueType = value.GetType();

            if (valueType.BaseType != null && valueType.BaseType.IsGenericType && valueType.BaseType.GetGenericTypeDefinition() == typeof(ManagedProperty<>))
            {
                value = valueType.GetProperty("Value").GetValue(value);
                valueType = value.GetType();
            }

            if (valueType.BaseType != null && valueType.BaseType.IsGenericType && valueType.BaseType.GetGenericTypeDefinition() == typeof(ConstantsBase<>))
            {
                value = valueType.GetProperty("Value").GetValue(value);
                valueType = value.GetType();
            }

            if (valueType.IsArray)
            {
                value = String.Join(",", (object[])value);
                valueType = typeof(string);
            }

            if (value is Label l)
                value = l.UserLocalizedLabel?.Label;
            else if (value != null && value.GetType().IsEnum)
                value = value.ToString();
            else if (value is MetadataBase meta)
                value = meta.MetadataId;

            return value;
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            return Array.Empty<IExecutionPlanNode>();
        }

        protected override IEnumerable<Entity> ExecuteInternal(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues)
        {
            if (IncludeAttribute)
            {
                if (Query.Properties == null)
                    Query.Properties = new MetadataPropertiesExpression();

                // Ensure the entity metadata contains the attributes
                if (!Query.Properties.PropertyNames.Contains(nameof(EntityMetadata.Attributes)))
                    Query.Properties.PropertyNames.Add(nameof(EntityMetadata.Attributes));
            }

            var resp = (RetrieveMetadataChangesResponse) org.Execute(new RetrieveMetadataChangesRequest { Query = Query });
            var entityProps = typeof(EntityMetadata).GetProperties().ToDictionary(p => p.Name);
            var attributeProps = _attributeTypes.ToDictionary(t => t, t => t.GetProperties().ToDictionary(p => p.Name));

            var results = resp.EntityMetadata.Select(e => new { Entity = e, Attribute = (AttributeMetadata)null });

            if (IncludeAttribute)
                results = results.SelectMany(r => r.Entity.Attributes.Select(a => new { Entity = r.Entity, Attribute = a }));

            foreach (var result in results)
            {
                var converted = new Entity();

                if (IncludeEntity)
                {
                    converted.LogicalName = "entity";
                    converted.Id = result.Entity.MetadataId ?? Guid.Empty;

                    foreach (var prop in _entityProps)
                        converted[prop.Key] = GetPropertyValue(entityProps[prop.Value], result.Entity);
                }

                if (IncludeAttribute)
                {
                    converted.LogicalName = "attribute";
                    converted.Id = result.Attribute.MetadataId ?? Guid.Empty;

                    var availableProps = attributeProps[result.Attribute.GetType()];

                    foreach (var prop in _attributeProps)
                    {
                        if (!availableProps.TryGetValue(prop.Value, out var attributeProp))
                        {
                            converted[prop.Key] = null;
                            continue;
                        }

                        converted[prop.Key] = GetPropertyValue(attributeProp, result.Attribute);
                    }
                }

                yield return converted;
            }
        }
    }
}
