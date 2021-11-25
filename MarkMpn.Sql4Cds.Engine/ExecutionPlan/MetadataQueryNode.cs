using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlTypes;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Metadata;
using Microsoft.Xrm.Sdk.Metadata.Query;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// Returns data from a metadata query
    /// </summary>
    class MetadataQueryNode : BaseDataNode
    {
        class MetadataProperty
        {
            public string SqlName { get; set; }
            public string PropertyName { get; set; }
            public Func<object,object> Accessor { get; set; }
            public Type Type { get; set; }
        }

        class AttributeProperty
        {
            public string SqlName { get; set; }
            public string PropertyName { get; set; }
            public IDictionary<Type, Func<object,object>> Accessors { get; set; }
            public Type Type { get; set; }
        }

        private IDictionary<string, MetadataProperty> _entityCols;
        private IDictionary<string, AttributeProperty> _attributeCols;
        private IDictionary<string, MetadataProperty> _oneToManyRelationshipCols;
        private IDictionary<string, MetadataProperty> _manyToOneRelationshipCols;
        private IDictionary<string, MetadataProperty> _manyToManyRelationshipCols;

        private static readonly Dictionary<string, MetadataProperty> _entityProps;
        private static readonly Dictionary<string, MetadataProperty> _oneToManyRelationshipProps;
        private static readonly Dictionary<string, MetadataProperty> _manyToManyRelationshipProps;
        private static readonly Type[] _attributeTypes;
        private static readonly Dictionary<string, AttributeProperty> _attributeProps;

        static MetadataQueryNode()
        {
            var excludedEntityProps = new[]
            {
                nameof(EntityMetadata.ExtensionData),
                nameof(EntityMetadata.Attributes),
                nameof(EntityMetadata.Keys),
                nameof(EntityMetadata.ManyToManyRelationships),
                nameof(EntityMetadata.ManyToOneRelationships),
                nameof(EntityMetadata.OneToManyRelationships),
                nameof(EntityMetadata.Privileges)
            };

            _entityProps = typeof(EntityMetadata)
                .GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Where(p => !excludedEntityProps.Contains(p.Name))
                .ToDictionary(p => p.Name, p => new MetadataProperty { SqlName = p.Name.ToLowerInvariant(), PropertyName = p.Name, Type = GetPropertyType(p.PropertyType), Accessor = GetPropertyAccessor(p, GetPropertyType(p.PropertyType)) }, StringComparer.OrdinalIgnoreCase);

            var excludedOneToManyRelationshipProps = new[]
            {
                nameof(OneToManyRelationshipMetadata.ExtensionData),
                nameof(OneToManyRelationshipMetadata.AssociatedMenuConfiguration),
                nameof(OneToManyRelationshipMetadata.CascadeConfiguration),
                nameof(OneToManyRelationshipMetadata.RelationshipAttributes)
            };

            _oneToManyRelationshipProps = typeof(OneToManyRelationshipMetadata)
                .GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Where(p => !excludedOneToManyRelationshipProps.Contains(p.Name))
                .ToDictionary(p => p.Name, p => new MetadataProperty { SqlName = p.Name.ToLowerInvariant(), PropertyName = p.Name, Type = GetPropertyType(p.PropertyType), Accessor = GetPropertyAccessor(p, GetPropertyType(p.PropertyType)) }, StringComparer.OrdinalIgnoreCase);

            var excludedManyToManyRelationshipProps = new[]
            {
                nameof(ManyToManyRelationshipMetadata.ExtensionData),
                nameof(ManyToManyRelationshipMetadata.Entity1AssociatedMenuConfiguration),
                nameof(ManyToManyRelationshipMetadata.Entity2AssociatedMenuConfiguration)
            };

            _manyToManyRelationshipProps = typeof(ManyToManyRelationshipMetadata)
                .GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Where(p => !excludedManyToManyRelationshipProps.Contains(p.Name))
                .ToDictionary(p => p.Name, p => new MetadataProperty { SqlName = p.Name.ToLowerInvariant(), PropertyName = p.Name, Type = GetPropertyType(p.PropertyType), Accessor = GetPropertyAccessor(p, GetPropertyType(p.PropertyType)) }, StringComparer.OrdinalIgnoreCase);

            // Get a list of all attribute types
            _attributeTypes = typeof(AttributeMetadata).Assembly
                .GetTypes()
                .Where(t => typeof(AttributeMetadata).IsAssignableFrom(t) && !t.IsAbstract)
                .ToArray();

            // Combine the properties available from each attribute type
            _attributeProps = _attributeTypes
                .SelectMany(t => t.GetProperties(BindingFlags.Public | BindingFlags.Instance).Select(p => new { Type = t, Property = p }))
                .Where(p => p.Property.Name != nameof(AttributeMetadata.ExtensionData))
                .GroupBy(p => p.Property.Name, p => p, StringComparer.OrdinalIgnoreCase)
                .Select(g =>
                {
                    // Work out the consistent type for each property
                    Type type = null;

                    foreach (var prop in g)
                    {
                        if (type == null)
                        {
                            type = GetPropertyType(prop.Property.PropertyType);
                        }
                        else if (!SqlTypeConverter.CanMakeConsistentTypes(type, GetPropertyType(prop.Property.PropertyType), out type))
                        {
                            // Can't make a consistent type for this property, so we can't use it
                            type = null;
                            break;
                        }
                    }

                    if (type == null)
                        return null;

                    return new AttributeProperty
                    {
                        SqlName = g.Key.ToLowerInvariant(),
                        PropertyName = g.Key,
                        Type = type,
                        Accessors = g.ToDictionary(p => p.Type, p => GetPropertyAccessor(p.Property, type))
                    };
                })
                .Where(p => p != null)
                .ToDictionary(p => p.SqlName, StringComparer.OrdinalIgnoreCase);
        }

        /// <summary>
        /// The instance that this node will be executed against
        /// </summary>
        [Category("Data Source")]
        [Description("The data source this query is executed against")]
        public string DataSource { get; set; }

        /// <summary>
        /// The types of metadata to include in the result
        /// </summary>
        [Category("Metadata Query")]
        [Description("The types of metadata to include in the result")]
        [DisplayName("Metadata Source")]
        public MetadataSource MetadataSource { get; set; }

        /// <summary>
        /// The alias for entity data
        /// </summary>
        [Category("Metadata Query")]
        [Description("The alias for entity data")]
        [DisplayName("Entity Alias")]
        public string EntityAlias { get; set; }

        /// <summary>
        /// The alias for attribute data
        /// </summary>
        [Category("Metadata Query")]
        [Description("The alias for attribute data")]
        [DisplayName("Attribute Alias")]
        public string AttributeAlias { get; set; }

        /// <summary>
        /// The alias for one-to-many relationship data
        /// </summary>
        [Category("Metadata Query")]
        [Description("The alias for one-to-many relationship data")]
        [DisplayName("One-to-Many Relationship Alias")]
        public string OneToManyRelationshipAlias { get; set; }

        /// <summary>
        /// The alias for many-to-one relationship data
        /// </summary>
        [Category("Metadata Query")]
        [Description("The alias for many-to-one relationship data")]
        [DisplayNameAttribute("Many-to-One Relationship Alias")]
        public string ManyToOneRelationshipAlias { get; set; }

        /// <summary>
        /// The alias for many-to-many relationship data
        /// </summary>
        [Category("Metadata Query")]
        [Description("The alias for many-to-many relationship data")]
        [DisplayName("Many-to-Many Relationship Alias")]
        public string ManyToManyRelationshipAlias { get; set; }

        /// <summary>
        /// The property used to access many-to-many relationship data
        /// </summary>
        [Category("Metadata Query")]
        [Description("The property used to access many-to-many relationship data")]
        [DisplayName("Many-to-Many Relationship Join")]
        public string ManyToManyRelationshipJoin { get; set; }

        /// <summary>
        /// The metadata query to be executed
        /// </summary>
        [Category("Metadata Query")]
        [Description("The metadata query to be executed")]
        public EntityQueryExpression Query { get; } = new EntityQueryExpression();

        public override void AddRequiredColumns(IDictionary<string, DataSource> dataSources, IDictionary<string, Type> parameterTypes, IList<string> requiredColumns)
        {
            _entityCols = new Dictionary<string, MetadataProperty>();
            _attributeCols = new Dictionary<string, AttributeProperty>();
            _oneToManyRelationshipCols = new Dictionary<string, MetadataProperty>();
            _manyToOneRelationshipCols = new Dictionary<string, MetadataProperty>();
            _manyToManyRelationshipCols = new Dictionary<string, MetadataProperty>();

            foreach (var col in requiredColumns)
            {
                var parts = col.Split('.');

                if (parts.Length != 2)
                    continue;

                if (parts[0].Equals(EntityAlias, StringComparison.OrdinalIgnoreCase))
                {
                    if (Query.Properties == null)
                        Query.Properties = new MetadataPropertiesExpression();

                    var prop = _entityProps[parts[1]];

                    if (!Query.Properties.AllProperties && !Query.Properties.PropertyNames.Contains(prop.PropertyName))
                        Query.Properties.PropertyNames.Add(prop.PropertyName);

                    _entityCols[col] = prop;
                }
                else if (parts[0].Equals(AttributeAlias, StringComparison.OrdinalIgnoreCase))
                {
                    if (Query.AttributeQuery == null)
                        Query.AttributeQuery = new AttributeQueryExpression();

                    if (Query.AttributeQuery.Properties == null)
                        Query.AttributeQuery.Properties = new MetadataPropertiesExpression();

                    var prop = _attributeProps[parts[1]];

                    if (!Query.AttributeQuery.Properties.AllProperties && !Query.AttributeQuery.Properties.PropertyNames.Contains(prop.PropertyName))
                        Query.AttributeQuery.Properties.PropertyNames.Add(prop.PropertyName);

                    _attributeCols[col] = prop;
                }
                else if (parts[0].Equals(OneToManyRelationshipAlias, StringComparison.OrdinalIgnoreCase))
                {
                    if (Query.RelationshipQuery == null)
                        Query.RelationshipQuery = new RelationshipQueryExpression();

                    if (Query.RelationshipQuery.Properties == null)
                        Query.RelationshipQuery.Properties = new MetadataPropertiesExpression();

                    var prop = _oneToManyRelationshipProps[parts[1]];

                    if (!Query.RelationshipQuery.Properties.AllProperties && !Query.RelationshipQuery.Properties.PropertyNames.Contains(prop.PropertyName))
                        Query.RelationshipQuery.Properties.PropertyNames.Add(prop.PropertyName);

                    _oneToManyRelationshipCols[col] = prop;
                }
                else if (parts[0].Equals(ManyToOneRelationshipAlias, StringComparison.OrdinalIgnoreCase))
                {
                    if (Query.RelationshipQuery == null)
                        Query.RelationshipQuery = new RelationshipQueryExpression();

                    if (Query.RelationshipQuery.Properties == null)
                        Query.RelationshipQuery.Properties = new MetadataPropertiesExpression();

                    var prop = _oneToManyRelationshipProps[parts[1]];

                    if (!Query.RelationshipQuery.Properties.AllProperties && !Query.RelationshipQuery.Properties.PropertyNames.Contains(prop.PropertyName))
                        Query.RelationshipQuery.Properties.PropertyNames.Add(prop.PropertyName);

                    _manyToOneRelationshipCols[col] = prop;
                }
                else if (parts[0].Equals(ManyToManyRelationshipAlias, StringComparison.OrdinalIgnoreCase))
                {
                    if (Query.RelationshipQuery == null)
                        Query.RelationshipQuery = new RelationshipQueryExpression();

                    if (Query.RelationshipQuery.Properties == null)
                        Query.RelationshipQuery.Properties = new MetadataPropertiesExpression();

                    var prop = _manyToManyRelationshipProps[parts[1]];

                    if (!Query.RelationshipQuery.Properties.AllProperties && !Query.RelationshipQuery.Properties.PropertyNames.Contains(prop.PropertyName))
                        Query.RelationshipQuery.Properties.PropertyNames.Add(prop.PropertyName);

                    _manyToManyRelationshipCols[col] = prop;
                }
            }
        }

        public override int EstimateRowsOut(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes)
        {
            return 100;
        }

        public override IDataExecutionPlanNode FoldQuery(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes)
        {
            return this;
        }

        public override NodeSchema GetSchema(IDictionary<string, DataSource> dataSources, IDictionary<string, Type> parameterTypes)
        {
            var schema = new NodeSchema();
            var childCount = 0;

            if (MetadataSource.HasFlag(MetadataSource.Entity))
            {
                var entityProps = (IEnumerable<MetadataProperty>) _entityProps.Values;

                if (Query.Properties != null)
                    entityProps = entityProps.Where(p => Query.Properties.AllProperties || Query.Properties.PropertyNames.Contains(p.PropertyName, StringComparer.OrdinalIgnoreCase));

                foreach (var prop in entityProps)
                {
                    schema.Schema[$"{EntityAlias}.{prop.SqlName}"] = prop.Type;

                    if (!schema.Aliases.TryGetValue(prop.SqlName, out var aliases))
                    {
                        aliases = new List<string>();
                        schema.Aliases[prop.SqlName] = aliases;
                    }

                    aliases.Add($"{EntityAlias}.{prop.SqlName}");
                }

                schema.PrimaryKey = $"{EntityAlias}.{nameof(EntityMetadata.MetadataId)}";
            }

            if (MetadataSource.HasFlag(MetadataSource.Attribute))
            {
                var attributeProps = (IEnumerable<AttributeProperty>) _attributeProps.Values;

                if (Query.AttributeQuery?.Properties != null)
                    attributeProps = attributeProps.Where(p => Query.AttributeQuery.Properties.AllProperties || Query.AttributeQuery.Properties.PropertyNames.Contains(p.PropertyName, StringComparer.OrdinalIgnoreCase));

                foreach (var prop in attributeProps)
                {
                    schema.Schema[$"{AttributeAlias}.{prop.SqlName}"] = prop.Type;

                    if (!schema.Aliases.TryGetValue(prop.SqlName, out var aliases))
                    {
                        aliases = new List<string>();
                        schema.Aliases[prop.SqlName] = aliases;
                    }

                    aliases.Add($"{AttributeAlias}.{prop.SqlName}");
                }

                schema.PrimaryKey = $"{AttributeAlias}.{nameof(AttributeMetadata.MetadataId)}";
                childCount++;
            }

            if (MetadataSource.HasFlag(MetadataSource.OneToManyRelationship))
            {
                var relationshipProps = (IEnumerable<MetadataProperty>)_oneToManyRelationshipProps.Values;

                if (Query.RelationshipQuery?.Properties != null)
                    relationshipProps = relationshipProps.Where(p => Query.RelationshipQuery.Properties.AllProperties || Query.RelationshipQuery.Properties.PropertyNames.Contains(p.PropertyName, StringComparer.OrdinalIgnoreCase));

                foreach (var prop in relationshipProps)
                {
                    schema.Schema[$"{OneToManyRelationshipAlias}.{prop.SqlName}"] = prop.Type;

                    if (!schema.Aliases.TryGetValue(prop.SqlName, out var aliases))
                    {
                        aliases = new List<string>();
                        schema.Aliases[prop.SqlName] = aliases;
                    }

                    aliases.Add($"{OneToManyRelationshipAlias}.{prop.SqlName}");
                }

                schema.PrimaryKey = $"{OneToManyRelationshipAlias}.{nameof(RelationshipMetadataBase.MetadataId)}";
                childCount++;
            }

            if (MetadataSource.HasFlag(MetadataSource.ManyToOneRelationship))
            {
                var relationshipProps = (IEnumerable<MetadataProperty>)_oneToManyRelationshipProps.Values;

                if (Query.RelationshipQuery?.Properties != null)
                    relationshipProps = relationshipProps.Where(p => Query.RelationshipQuery.Properties.AllProperties || Query.RelationshipQuery.Properties.PropertyNames.Contains(p.PropertyName, StringComparer.OrdinalIgnoreCase));

                foreach (var prop in relationshipProps)
                {
                    schema.Schema[$"{ManyToOneRelationshipAlias}.{prop.SqlName}"] = prop.Type;

                    if (!schema.Aliases.TryGetValue(prop.SqlName, out var aliases))
                    {
                        aliases = new List<string>();
                        schema.Aliases[prop.SqlName] = aliases;
                    }

                    aliases.Add($"{ManyToOneRelationshipAlias}.{prop.SqlName}");
                }

                schema.PrimaryKey = $"{ManyToOneRelationshipAlias}.{nameof(RelationshipMetadataBase.MetadataId)}";
                childCount++;
            }

            if (MetadataSource.HasFlag(MetadataSource.ManyToManyRelationship))
            {
                var relationshipProps = (IEnumerable<MetadataProperty>)_manyToManyRelationshipProps.Values;

                if (Query.RelationshipQuery?.Properties != null)
                    relationshipProps = relationshipProps.Where(p => Query.RelationshipQuery.Properties.AllProperties || Query.RelationshipQuery.Properties.PropertyNames.Contains(p.PropertyName, StringComparer.OrdinalIgnoreCase));

                foreach (var prop in relationshipProps)
                {
                    schema.Schema[$"{ManyToManyRelationshipAlias}.{prop.SqlName}"] = prop.Type;

                    if (!schema.Aliases.TryGetValue(prop.SqlName, out var aliases))
                    {
                        aliases = new List<string>();
                        schema.Aliases[prop.SqlName] = aliases;
                    }

                    aliases.Add($"{ManyToManyRelationshipAlias}.{prop.SqlName}");
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
                propType = typeof(SqlInt32);

            if (propType.BaseType != null && propType.BaseType.IsGenericType && propType.BaseType.GetGenericTypeDefinition() == typeof(ManagedProperty<>))
                propType = propType.BaseType.GetGenericArguments()[0];

            if (propType.BaseType != null && propType.BaseType.IsGenericType && propType.BaseType.GetGenericTypeDefinition() == typeof(ConstantsBase<>))
                propType = propType.BaseType.GetGenericArguments()[0];

            if (propType == typeof(Label) || propType.IsEnum || propType.IsArray)
                propType = typeof(string);
            else if (typeof(MetadataBase).IsAssignableFrom(propType))
                propType = typeof(Guid);

            return SqlTypeConverter.NetToSqlType(propType);
        }

        internal static Func<object,object> GetPropertyAccessor(PropertyInfo prop, Type targetType)
        {
            var rawParam = Expression.Parameter(typeof(object));
            var param = SqlTypeConverter.Convert(rawParam, prop.DeclaringType);
            var value = (Expression)Expression.Property(param, prop);

            // Extract base value from complex types
            if (value.Type.IsGenericType && value.Type.GetGenericTypeDefinition() == typeof(Nullable<>))
                value = Expression.Property(value, nameof(Nullable<bool>.Value));

            if (typeof(OptionMetadata).IsAssignableFrom(value.Type))
                value = Expression.Property(value, nameof(OptionMetadata.Value));

            if (value.Type.BaseType != null && value.Type.BaseType.IsGenericType && value.Type.BaseType.GetGenericTypeDefinition() == typeof(ManagedProperty<>))
                value = Expression.Property(value, nameof(ManagedProperty<bool>.Value));

            if (value.Type.BaseType != null && value.Type.BaseType.IsGenericType && value.Type.BaseType.GetGenericTypeDefinition() == typeof(ConstantsBase<>))
                value = Expression.Property(value, nameof(ConstantsBase<bool>.Value));

            if (value.Type.IsArray)
                value = Expr.Call(() => String.Join(Expr.Arg<string>(), Expr.Arg<object[]>()), Expression.Constant(","), value);

            if (value.Type == typeof(Label))
                value = Expression.Property(value, nameof(Label.UserLocalizedLabel));

            if (value.Type == typeof(LocalizedLabel))
                value = Expression.Condition(Expression.Equal(value, Expression.Constant(null)), SqlTypeConverter.Convert(Expression.Constant(null), typeof(string)), Expression.Property(value, nameof(LocalizedLabel.Label)));

            if (value.Type.IsEnum)
                value = Expression.Call(value, nameof(Enum.ToString), Array.Empty<Type>());

            if (typeof(MetadataBase).IsAssignableFrom(value.Type))
                value = Expression.Condition(Expression.Equal(value, Expression.Constant(null)), SqlTypeConverter.Convert(Expression.Constant(null), typeof(Guid?)), Expression.Property(value, nameof(MetadataBase.MetadataId)));

            var directConversionType = SqlTypeConverter.NetToSqlType(value.Type);

            if (directConversionType == typeof(SqlString) && value.Type != typeof(string))
                value = Expression.Call(value, nameof(Object.ToString), Array.Empty<Type>());

            var converted = SqlTypeConverter.Convert(value, directConversionType);
            if (targetType != directConversionType)
                converted = SqlTypeConverter.Convert(converted, targetType);

            // Return null literal if final value is null
            if (!value.Type.IsValueType)
                value = Expression.Condition(Expression.Equal(value, Expression.Constant(null)), Expression.Constant(SqlTypeConverter.GetNullValue(targetType)), converted);
            else
                value = converted;

            // Return null literal if original value is null
            if (!prop.PropertyType.IsValueType || prop.PropertyType.IsGenericType && prop.PropertyType.GetGenericTypeDefinition() == typeof(Nullable<>))
                value = Expression.Condition(Expression.Equal(Expression.Property(param, prop), Expression.Constant(null)), Expression.Constant(SqlTypeConverter.GetNullValue(targetType)), value);

            // Compile the function
            value = Expr.Box(value);
            var func = (Func<object,object>) Expression.Lambda(value, rawParam).Compile();
            return func;
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            return Array.Empty<IDataExecutionPlanNode>();
        }

        protected override IEnumerable<Entity> ExecuteInternal(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues)
        {
            if (MetadataSource.HasFlag(MetadataSource.Attribute))
            {
                if (Query.Properties == null)
                    Query.Properties = new MetadataPropertiesExpression();

                // Ensure the entity metadata contains the attributes
                if (!Query.Properties.AllProperties && !Query.Properties.PropertyNames.Contains(nameof(EntityMetadata.Attributes)))
                    Query.Properties.PropertyNames.Add(nameof(EntityMetadata.Attributes));
            }

            if (MetadataSource.HasFlag(MetadataSource.OneToManyRelationship))
            {
                if (Query.Properties == null)
                    Query.Properties = new MetadataPropertiesExpression();

                // Ensure the entity metadata contains the relationships
                if (!Query.Properties.AllProperties && !Query.Properties.PropertyNames.Contains(nameof(EntityMetadata.OneToManyRelationships)))
                    Query.Properties.PropertyNames.Add(nameof(EntityMetadata.OneToManyRelationships));
            }

            if (MetadataSource.HasFlag(MetadataSource.ManyToOneRelationship))
            {
                if (Query.Properties == null)
                    Query.Properties = new MetadataPropertiesExpression();

                // Ensure the entity metadata contains the relationships
                if (!Query.Properties.AllProperties && !Query.Properties.PropertyNames.Contains(nameof(EntityMetadata.ManyToOneRelationships)))
                    Query.Properties.PropertyNames.Add(nameof(EntityMetadata.ManyToOneRelationships));
            }

            if (MetadataSource.HasFlag(MetadataSource.ManyToManyRelationship))
            {
                if (Query.Properties == null)
                    Query.Properties = new MetadataPropertiesExpression();

                // Ensure the entity metadata contains the relationships
                if (!Query.Properties.AllProperties && !Query.Properties.PropertyNames.Contains(nameof(EntityMetadata.ManyToManyRelationships)))
                    Query.Properties.PropertyNames.Add(nameof(EntityMetadata.ManyToManyRelationships));
            }

            if (!dataSources.TryGetValue(DataSource, out var dataSource))
                throw new NotSupportedQueryFragmentException("Missing datasource " + DataSource);

            var resp = (RetrieveMetadataChangesResponse) dataSource.Connection.Execute(new RetrieveMetadataChangesRequest { Query = Query });
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
                results = results.SelectMany(r => r.Entity.ManyToManyRelationships.Where(mm => ManyToManyRelationshipJoin == null || ((string) typeof(ManyToManyRelationshipMetadata).GetProperty(ManyToManyRelationshipJoin).GetValue(mm)) == r.Entity.LogicalName).Select(mm => new { Entity = r.Entity, Attribute = r.Attribute, Relationship = (RelationshipMetadataBase)mm }));


            foreach (var result in results)
            {
                var converted = new Entity();

                if (MetadataSource.HasFlag(MetadataSource.Entity))
                {
                    converted.LogicalName = "entity";
                    converted.Id = result.Entity.MetadataId ?? Guid.Empty;

                    foreach (var prop in _entityCols)
                        converted[prop.Key] = prop.Value.Accessor(result.Entity);
                }

                if (MetadataSource.HasFlag(MetadataSource.Attribute))
                {
                    converted.LogicalName = "attribute";
                    converted.Id = result.Attribute.MetadataId ?? Guid.Empty;

                    foreach (var prop in _attributeCols)
                    {
                        if (!prop.Value.Accessors.TryGetValue(result.Attribute.GetType(), out var accessor))
                        {
                            converted[prop.Key] = SqlTypeConverter.GetNullValue(prop.Value.Type);
                            continue;
                        }

                        converted[prop.Key] = accessor(result.Attribute);
                    }
                }

                if (MetadataSource.HasFlag(MetadataSource.OneToManyRelationship))
                {
                    converted.LogicalName = "relationship_1_n";
                    converted.Id = result.Relationship.MetadataId ?? Guid.Empty;

                    foreach (var prop in _oneToManyRelationshipCols)
                        converted[prop.Key] = prop.Value.Accessor(result.Relationship);
                }

                if (MetadataSource.HasFlag(MetadataSource.ManyToOneRelationship))
                {
                    converted.LogicalName = "relationship_n_1";
                    converted.Id = result.Relationship.MetadataId ?? Guid.Empty;

                    foreach (var prop in _manyToOneRelationshipCols)
                        converted[prop.Key] = prop.Value.Accessor(result.Relationship);
                }

                if (MetadataSource.HasFlag(MetadataSource.ManyToManyRelationship))
                {
                    converted.LogicalName = "relationship_n_n";
                    converted.Id = result.Relationship.MetadataId ?? Guid.Empty;

                    foreach (var prop in _manyToManyRelationshipCols)
                        converted[prop.Key] = prop.Value.Accessor(result.Relationship);
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
