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
    public class MetadataQueryNode : BaseDataNode
    {
        private IDictionary<string, string> _entityCols;
        private IDictionary<string, string> _attributeCols;
        private IDictionary<string, string> _oneToManyRelationshipCols;
        private IDictionary<string, string> _manyToOneRelationshipCols;
        private IDictionary<string, string> _manyToManyRelationshipCols;

        private static Type[] _attributeTypes = typeof(AttributeMetadata).Assembly.GetTypes().Where(t => typeof(AttributeMetadata).IsAssignableFrom(t) && !t.IsAbstract).ToArray();
        private static readonly Dictionary<Type, Dictionary<string, PropertyInfo>> _attributeProps = _attributeTypes.ToDictionary(t => t, t => t.GetProperties(BindingFlags.Public | BindingFlags.Instance).Where(p => p.Name != nameof(AttributeMetadata.ExtensionData)).ToDictionary(p => p.Name));
        private static readonly Dictionary<string, PropertyInfo> _flattenedAttributeProps = _attributeProps.SelectMany(kvp => kvp.Value).Select(kvp => kvp.Value).GroupBy(p => p.Name).ToDictionary(g => g.Key, g => g.First(), StringComparer.OrdinalIgnoreCase);

        public MetadataSource MetadataSource { get; set; }

        public string EntityAlias { get; set; }

        public string AttributeAlias { get; set; }

        public string OneToManyRelationshipAlias { get; set; }

        public string ManyToOneRelationshipAlias { get; set; }

        public string ManyToManyRelationshipAlias { get; set; }

        public string ManyToManyRelationshipJoin { get; set; }

        public EntityQueryExpression Query { get; } = new EntityQueryExpression();

        public override void AddRequiredColumns(IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes, IList<string> requiredColumns)
        {
            _entityCols = new Dictionary<string, string>();
            _attributeCols = new Dictionary<string, string>();
            _oneToManyRelationshipCols = new Dictionary<string, string>();
            _manyToOneRelationshipCols = new Dictionary<string, string>();
            _manyToManyRelationshipCols = new Dictionary<string, string>();

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

                    _entityCols[col] = prop.Name;
                }
                else if (parts[0].Equals(AttributeAlias, StringComparison.OrdinalIgnoreCase))
                {
                    if (Query.AttributeQuery == null)
                        Query.AttributeQuery = new AttributeQueryExpression();

                    if (Query.AttributeQuery.Properties == null)
                        Query.AttributeQuery.Properties = new MetadataPropertiesExpression();

                    var prop = _flattenedAttributeProps[parts[1]];

                    if (!Query.AttributeQuery.Properties.PropertyNames.Contains(prop.Name))
                        Query.AttributeQuery.Properties.PropertyNames.Add(prop.Name);

                    _attributeCols[col] = prop.Name;
                }
                else if (parts[0].Equals(OneToManyRelationshipAlias, StringComparison.OrdinalIgnoreCase))
                {
                    if (Query.RelationshipQuery == null)
                        Query.RelationshipQuery = new RelationshipQueryExpression();

                    if (Query.RelationshipQuery.Properties == null)
                        Query.RelationshipQuery.Properties = new MetadataPropertiesExpression();

                    var prop = typeof(OneToManyRelationshipMetadata).GetProperty(parts[1], BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase);

                    if (!Query.RelationshipQuery.Properties.PropertyNames.Contains(prop.Name))
                        Query.RelationshipQuery.Properties.PropertyNames.Add(prop.Name);

                    _oneToManyRelationshipCols[col] = prop.Name;
                }
                else if (parts[0].Equals(ManyToOneRelationshipAlias, StringComparison.OrdinalIgnoreCase))
                {
                    if (Query.RelationshipQuery == null)
                        Query.RelationshipQuery = new RelationshipQueryExpression();

                    if (Query.RelationshipQuery.Properties == null)
                        Query.RelationshipQuery.Properties = new MetadataPropertiesExpression();

                    var prop = typeof(OneToManyRelationshipMetadata).GetProperty(parts[1], BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase);

                    if (!Query.RelationshipQuery.Properties.PropertyNames.Contains(prop.Name))
                        Query.RelationshipQuery.Properties.PropertyNames.Add(prop.Name);

                    _manyToOneRelationshipCols[col] = prop.Name;
                }
                else if (parts[0].Equals(ManyToManyRelationshipAlias, StringComparison.OrdinalIgnoreCase))
                {
                    if (Query.RelationshipQuery == null)
                        Query.RelationshipQuery = new RelationshipQueryExpression();

                    if (Query.RelationshipQuery.Properties == null)
                        Query.RelationshipQuery.Properties = new MetadataPropertiesExpression();

                    var prop = typeof(ManyToManyRelationshipMetadata).GetProperty(parts[1], BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase);

                    if (!Query.RelationshipQuery.Properties.PropertyNames.Contains(prop.Name))
                        Query.RelationshipQuery.Properties.PropertyNames.Add(prop.Name);

                    _manyToManyRelationshipCols[col] = prop.Name;
                }
            }
        }

        public override int EstimateRowsOut(IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes, ITableSizeCache tableSize)
        {
            return 100;
        }

        public override IDataExecutionPlanNode FoldQuery(IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes)
        {
            return this;
        }

        public override NodeSchema GetSchema(IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes)
        {
            var schema = new NodeSchema();
            var childCount = 0;

            if (MetadataSource.HasFlag(MetadataSource.Entity))
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

            if (MetadataSource.HasFlag(MetadataSource.Attribute))
            {
                var attributeProps = (IEnumerable<PropertyInfo>) _flattenedAttributeProps.Values;

                if (Query.AttributeQuery?.Properties != null)
                    attributeProps = attributeProps.Where(p => Query.AttributeQuery.Properties.PropertyNames.Contains(p.Name, StringComparer.OrdinalIgnoreCase));

                foreach (var prop in attributeProps)
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
                childCount++;
            }

            if (MetadataSource.HasFlag(MetadataSource.OneToManyRelationship))
            {
                var relationshipProps = typeof(OneToManyRelationshipMetadata).GetProperties(BindingFlags.Public | BindingFlags.Instance).Where(p => p.Name != nameof(OneToManyRelationshipMetadata.ExtensionData));

                if (Query.RelationshipQuery?.Properties != null)
                    relationshipProps = relationshipProps.Where(p => Query.RelationshipQuery.Properties.PropertyNames.Contains(p.Name, StringComparer.OrdinalIgnoreCase));

                foreach (var prop in relationshipProps)
                {
                    schema.Schema[$"{OneToManyRelationshipAlias}.{prop.Name}"] = GetPropertyType(prop.PropertyType);

                    if (!schema.Aliases.TryGetValue(prop.Name, out var aliases))
                    {
                        aliases = new List<string>();
                        schema.Aliases[prop.Name] = aliases;
                    }

                    aliases.Add($"{OneToManyRelationshipAlias}.{prop.Name}");
                }

                schema.PrimaryKey = $"{OneToManyRelationshipAlias}.{nameof(RelationshipMetadataBase.MetadataId)}";
                childCount++;
            }

            if (MetadataSource.HasFlag(MetadataSource.ManyToOneRelationship))
            {
                var relationshipProps = typeof(OneToManyRelationshipMetadata).GetProperties(BindingFlags.Public | BindingFlags.Instance).Where(p => p.Name != nameof(OneToManyRelationshipMetadata.ExtensionData));

                if (Query.RelationshipQuery?.Properties != null)
                    relationshipProps = relationshipProps.Where(p => Query.RelationshipQuery.Properties.PropertyNames.Contains(p.Name, StringComparer.OrdinalIgnoreCase));

                foreach (var prop in relationshipProps)
                {
                    schema.Schema[$"{ManyToOneRelationshipAlias}.{prop.Name}"] = GetPropertyType(prop.PropertyType);

                    if (!schema.Aliases.TryGetValue(prop.Name, out var aliases))
                    {
                        aliases = new List<string>();
                        schema.Aliases[prop.Name] = aliases;
                    }

                    aliases.Add($"{ManyToOneRelationshipAlias}.{prop.Name}");
                }

                schema.PrimaryKey = $"{ManyToOneRelationshipAlias}.{nameof(RelationshipMetadataBase.MetadataId)}";
                childCount++;
            }

            if (MetadataSource.HasFlag(MetadataSource.ManyToManyRelationship))
            {
                var relationshipProps = typeof(ManyToManyRelationshipMetadata).GetProperties(BindingFlags.Public | BindingFlags.Instance).Where(p => p.Name != nameof(OneToManyRelationshipMetadata.ExtensionData));

                if (Query.RelationshipQuery?.Properties != null)
                    relationshipProps = relationshipProps.Where(p => Query.RelationshipQuery.Properties.PropertyNames.Contains(p.Name, StringComparer.OrdinalIgnoreCase));

                foreach (var prop in relationshipProps)
                {
                    schema.Schema[$"{ManyToManyRelationshipAlias}.{prop.Name}"] = GetPropertyType(prop.PropertyType);

                    if (!schema.Aliases.TryGetValue(prop.Name, out var aliases))
                    {
                        aliases = new List<string>();
                        schema.Aliases[prop.Name] = aliases;
                    }

                    aliases.Add($"{ManyToManyRelationshipAlias}.{prop.Name}");
                }

                schema.PrimaryKey = $"{ManyToManyRelationshipAlias}.{nameof(RelationshipMetadataBase.MetadataId)}";
                childCount++;
            }

            if (childCount > 1)
                schema.PrimaryKey = null;

            return schema;
        }

        internal static Type GetPropertyType(Type propType)
        {
            if (propType == typeof(OptionMetadata))
                propType = typeof(int?);

            if (propType.BaseType != null && propType.BaseType.IsGenericType && propType.BaseType.GetGenericTypeDefinition() == typeof(ManagedProperty<>))
                propType = propType.BaseType.GetGenericArguments()[0];

            if (propType.BaseType != null && propType.BaseType.IsGenericType && propType.BaseType.GetGenericTypeDefinition() == typeof(ConstantsBase<>))
                propType = propType.BaseType.GetGenericArguments()[0];

            if (propType == typeof(Label) || propType.IsEnum || propType.IsArray)
                propType = typeof(string);
            else if (typeof(MetadataBase).IsAssignableFrom(propType))
                propType = typeof(Guid?);

            return propType;
        }

        internal static object GetPropertyValue(PropertyInfo prop, object target)
        {
            var value = prop.GetValue(target);

            if (value == null)
                return null;

            if (value is OptionMetadata option)
                value = option.Value;
            
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

        public override IEnumerable<IDataExecutionPlanNode> GetSources()
        {
            return Array.Empty<IDataExecutionPlanNode>();
        }

        protected override IEnumerable<Entity> ExecuteInternal(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues)
        {
            if (MetadataSource.HasFlag(MetadataSource.Attribute))
            {
                if (Query.Properties == null)
                    Query.Properties = new MetadataPropertiesExpression();

                // Ensure the entity metadata contains the attributes
                if (!Query.Properties.PropertyNames.Contains(nameof(EntityMetadata.Attributes)))
                    Query.Properties.PropertyNames.Add(nameof(EntityMetadata.Attributes));
            }

            if (MetadataSource.HasFlag(MetadataSource.OneToManyRelationship))
            {
                if (Query.Properties == null)
                    Query.Properties = new MetadataPropertiesExpression();

                // Ensure the entity metadata contains the relationships
                if (!Query.Properties.PropertyNames.Contains(nameof(EntityMetadata.OneToManyRelationships)))
                    Query.Properties.PropertyNames.Add(nameof(EntityMetadata.OneToManyRelationships));
            }

            if (MetadataSource.HasFlag(MetadataSource.ManyToOneRelationship))
            {
                if (Query.Properties == null)
                    Query.Properties = new MetadataPropertiesExpression();

                // Ensure the entity metadata contains the relationships
                if (!Query.Properties.PropertyNames.Contains(nameof(EntityMetadata.ManyToOneRelationships)))
                    Query.Properties.PropertyNames.Add(nameof(EntityMetadata.ManyToOneRelationships));
            }

            if (MetadataSource.HasFlag(MetadataSource.ManyToManyRelationship))
            {
                if (Query.Properties == null)
                    Query.Properties = new MetadataPropertiesExpression();

                // Ensure the entity metadata contains the relationships
                if (!Query.Properties.PropertyNames.Contains(nameof(EntityMetadata.ManyToManyRelationships)))
                    Query.Properties.PropertyNames.Add(nameof(EntityMetadata.ManyToManyRelationships));
            }

            var resp = (RetrieveMetadataChangesResponse) org.Execute(new RetrieveMetadataChangesRequest { Query = Query });
            var entityProps = typeof(EntityMetadata).GetProperties().ToDictionary(p => p.Name);
            var oneToManyRelationshipProps = typeof(OneToManyRelationshipMetadata).GetProperties().ToDictionary(p => p.Name);
            var manyToManyRelationshipProps = typeof(ManyToManyRelationshipMetadata).GetProperties().ToDictionary(p => p.Name);

            var results = resp.EntityMetadata.Select(e => new { Entity = e, Attribute = (AttributeMetadata)null, Relationship = (RelationshipMetadataBase)null });

            if (MetadataSource.HasFlag(MetadataSource.Attribute))
                results = results.SelectMany(r => r.Entity.Attributes.Select(a => new { Entity = r.Entity, Attribute = a, Relationship = r.Relationship }));

            if (MetadataSource.HasFlag(MetadataSource.OneToManyRelationship))
                results = results.SelectMany(r => r.Entity.OneToManyRelationships.Select(om => new { Entity = r.Entity, Attribute = r.Attribute, Relationship = (RelationshipMetadataBase)om }));

            if (MetadataSource.HasFlag(MetadataSource.ManyToOneRelationship))
                results = results.SelectMany(r => r.Entity.ManyToOneRelationships.Select(mo => new { Entity = r.Entity, Attribute = r.Attribute, Relationship = (RelationshipMetadataBase)mo }));

            if (MetadataSource.HasFlag(MetadataSource.ManyToManyRelationship))
                results = results.SelectMany(r => r.Entity.ManyToManyRelationships.Where(mm => ((string) typeof(ManyToManyRelationshipMetadata).GetProperty(ManyToManyRelationshipJoin).GetValue(mm)) == r.Entity.LogicalName).Select(mm => new { Entity = r.Entity, Attribute = r.Attribute, Relationship = (RelationshipMetadataBase)mm }));

            foreach (var result in results)
            {
                var converted = new Entity();

                if (MetadataSource.HasFlag(MetadataSource.Entity))
                {
                    converted.LogicalName = "entity";
                    converted.Id = result.Entity.MetadataId ?? Guid.Empty;

                    foreach (var prop in _entityCols)
                        converted[prop.Key] = GetPropertyValue(entityProps[prop.Value], result.Entity);
                }

                if (MetadataSource.HasFlag(MetadataSource.Attribute))
                {
                    converted.LogicalName = "attribute";
                    converted.Id = result.Attribute.MetadataId ?? Guid.Empty;

                    var availableProps = _attributeProps[result.Attribute.GetType()];

                    foreach (var prop in _attributeCols)
                    {
                        if (!availableProps.TryGetValue(prop.Value, out var attributeProp))
                        {
                            converted[prop.Key] = null;
                            continue;
                        }

                        converted[prop.Key] = GetPropertyValue(attributeProp, result.Attribute);
                    }
                }

                if (MetadataSource.HasFlag(MetadataSource.OneToManyRelationship))
                {
                    converted.LogicalName = "relationship_1_n";
                    converted.Id = result.Relationship.MetadataId ?? Guid.Empty;

                    foreach (var prop in _oneToManyRelationshipCols)
                        converted[prop.Key] = GetPropertyValue(oneToManyRelationshipProps[prop.Value], result.Relationship);
                }

                if (MetadataSource.HasFlag(MetadataSource.ManyToOneRelationship))
                {
                    converted.LogicalName = "relationship_n_1";
                    converted.Id = result.Relationship.MetadataId ?? Guid.Empty;

                    foreach (var prop in _manyToOneRelationshipCols)
                        converted[prop.Key] = GetPropertyValue(oneToManyRelationshipProps[prop.Value], result.Relationship);
                }

                if (MetadataSource.HasFlag(MetadataSource.ManyToManyRelationship))
                {
                    converted.LogicalName = "relationship_n_n";
                    converted.Id = result.Relationship.MetadataId ?? Guid.Empty;

                    foreach (var prop in _manyToManyRelationshipCols)
                        converted[prop.Key] = GetPropertyValue(manyToManyRelationshipProps[prop.Value], result.Relationship);
                }

                yield return converted;
            }
        }
    }

    [Flags]
    public enum MetadataSource
    {
        Entity = 1,
        Attribute = 2,
        OneToManyRelationship = 4,
        ManyToOneRelationship = 8,
        ManyToManyRelationship = 16,
    }
}
