using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlTypes;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Metadata;
using Microsoft.Xrm.Sdk.Metadata.Query;
using Microsoft.Xrm.Sdk.Query;

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
            public DataTypeReference SqlType { get; set; }
            public Type Type { get; set; }
        }

        class AttributeProperty
        {
            public string SqlName { get; set; }
            public string PropertyName { get; set; }
            public IDictionary<Type, Func<object,object>> Accessors { get; set; }
            public DataTypeReference SqlType { get; set; }
            public Type Type { get; set; }
            public bool IsNullable { get; set; }
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
        private static readonly string[] _entityNotNullProps;
        private static readonly string[] _attributeNotNullProps;
        private static readonly string[] _oneToManyRelationshipNotNullProps;
        private static readonly string[] _manyToManyRelationshipNotNullProps;

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
                .ToDictionary(p => p.Name, p => new MetadataProperty { SqlName = p.Name.ToLowerInvariant(), PropertyName = p.Name, Type = p.PropertyType, SqlType = GetPropertyType(p.PropertyType), Accessor = GetPropertyAccessor(p, GetPropertyType(p.PropertyType).ToNetType(out _)) }, StringComparer.OrdinalIgnoreCase);

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
                .ToDictionary(p => p.Name, p => new MetadataProperty { SqlName = p.Name.ToLowerInvariant(), PropertyName = p.Name, Type = p.PropertyType, SqlType = GetPropertyType(p.PropertyType), Accessor = GetPropertyAccessor(p, GetPropertyType(p.PropertyType).ToNetType(out _)) }, StringComparer.OrdinalIgnoreCase);

            var excludedManyToManyRelationshipProps = new[]
            {
                nameof(ManyToManyRelationshipMetadata.ExtensionData),
                nameof(ManyToManyRelationshipMetadata.Entity1AssociatedMenuConfiguration),
                nameof(ManyToManyRelationshipMetadata.Entity2AssociatedMenuConfiguration)
            };

            _manyToManyRelationshipProps = typeof(ManyToManyRelationshipMetadata)
                .GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Where(p => !excludedManyToManyRelationshipProps.Contains(p.Name))
                .ToDictionary(p => p.Name, p => new MetadataProperty { SqlName = p.Name.ToLowerInvariant(), PropertyName = p.Name, Type = p.PropertyType, SqlType = GetPropertyType(p.PropertyType), Accessor = GetPropertyAccessor(p, GetPropertyType(p.PropertyType).ToNetType(out _)) }, StringComparer.OrdinalIgnoreCase);

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
                    DataTypeReference type = null;
                    var isNullable = false;

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

                        if (!isNullable && IsNullable(prop.Property.PropertyType))
                            isNullable = false;
                    }

                    if (type == null)
                        return null;

                    var netType = type.ToNetType(out _);

                    return new AttributeProperty
                    {
                        SqlName = g.Key.ToLowerInvariant(),
                        PropertyName = g.Key,
                        SqlType = type,
                        Type = netType,
                        Accessors = g.ToDictionary(p => p.Type, p => GetPropertyAccessor(p.Property, netType)),
                        IsNullable = isNullable
                    };
                })
                .Where(p => p != null)
                .ToDictionary(p => p.SqlName, StringComparer.OrdinalIgnoreCase);

            _entityNotNullProps = _entityProps.Values
                .Where(p => !IsNullable(p.Type))
                .Select(p => p.PropertyName)
                .Union(new[] { nameof(EntityMetadata.MetadataId), nameof(EntityMetadata.LogicalName), nameof(EntityMetadata.SchemaName), nameof(EntityMetadata.PrimaryIdAttribute) })
                .ToArray();

            _attributeNotNullProps = _attributeProps.Values
                .Where(p => !p.IsNullable)
                .Select(p => p.PropertyName)
                .Union(new[] { nameof(AttributeMetadata.MetadataId), nameof(AttributeMetadata.LogicalName), nameof(AttributeMetadata.SchemaName), nameof(AttributeMetadata.EntityLogicalName) })
                .ToArray();

            _oneToManyRelationshipNotNullProps = _oneToManyRelationshipProps.Values
                .Where(p => !IsNullable(p.Type))
                .Select(p => p.PropertyName)
                .Union(new[] { nameof(OneToManyRelationshipMetadata.MetadataId), nameof(OneToManyRelationshipMetadata.SchemaName), nameof(OneToManyRelationshipMetadata.ReferencedEntity), nameof(OneToManyRelationshipMetadata.ReferencedAttribute), nameof(OneToManyRelationshipMetadata.ReferencingEntity), nameof(OneToManyRelationshipMetadata.ReferencingAttribute) })
                .ToArray();

            _manyToManyRelationshipNotNullProps = _manyToManyRelationshipProps.Values
                .Where(p => !IsNullable(p.Type))
                .Select(p => p.PropertyName)
                .Union(new[] { nameof(ManyToManyRelationshipMetadata.MetadataId), nameof(ManyToManyRelationshipMetadata.Entity1LogicalName), nameof(ManyToManyRelationshipMetadata.Entity1IntersectAttribute), nameof(ManyToManyRelationshipMetadata.Entity2LogicalName), nameof(ManyToManyRelationshipMetadata.Entity2IntersectAttribute), nameof(ManyToManyRelationshipMetadata.IntersectEntityName) })
                .ToArray();
        }

        private static bool IsNullable(Type type)
        {
            if (!type.IsValueType)
                return true;

            if (type.IsGenericType)
                type = type.GetGenericTypeDefinition();

            if (type == typeof(Nullable<>))
                return true;

            return false;
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
        public EntityQueryExpression Query { get; private set; } = new EntityQueryExpression();

        public override void AddRequiredColumns(IDictionary<string, DataSource> dataSources, IDictionary<string, DataTypeReference> parameterTypes, IList<string> requiredColumns)
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

        protected override int EstimateRowsOutInternal(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, DataTypeReference> parameterTypes)
        {
            var entityCount = 100;
            var attributesPerEntity = 1;
            var relationshipsPerEntity = 1;

            if (HasEqualityFilter(Query.Criteria, nameof(EntityMetadata.LogicalName)))
            {
                entityCount = 1;
            }

            if (MetadataSource.HasFlag(MetadataSource.Attribute))
            {
                attributesPerEntity = 100;

                if (HasEqualityFilter(Query.AttributeQuery?.Criteria, nameof(AttributeMetadata.LogicalName)))
                {
                    attributesPerEntity = 1;
                }
            }

            if (MetadataSource.HasFlag(MetadataSource.OneToManyRelationship) ||
                MetadataSource.HasFlag(MetadataSource.ManyToOneRelationship) ||
                MetadataSource.HasFlag(MetadataSource.ManyToManyRelationship))
            {
                relationshipsPerEntity = 5;

                if (HasEqualityFilter(Query.RelationshipQuery?.Criteria, nameof(RelationshipMetadataBase.SchemaName)))
                {
                    relationshipsPerEntity = 1;
                }
            }

            return entityCount * attributesPerEntity * relationshipsPerEntity;
        }

        private bool HasEqualityFilter(MetadataFilterExpression filter, string propName)
        {
            if (filter == null)
                return false;

            if (filter.FilterOperator == LogicalOperator.Or && (filter.Conditions.Count + filter.Filters.Count) > 1)
                return false;

            foreach (var cond in filter.Conditions)
            {
                if (cond.PropertyName == propName && cond.ConditionOperator == MetadataConditionOperator.Equals)
                    return true;
            }

            if (filter.Filters.Any(subFilter => HasEqualityFilter(subFilter, propName)))
                return true;

            return false;
        }

        public override IDataExecutionPlanNodeInternal FoldQuery(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, DataTypeReference> parameterTypes, IList<OptimizerHint> hints)
        {
            return this;
        }

        public override INodeSchema GetSchema(IDictionary<string, DataSource> dataSources, IDictionary<string, DataTypeReference> parameterTypes)
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
                    var fullName = $"{EntityAlias}.{prop.SqlName}";
                    schema.Schema[fullName] = prop.SqlType;

                    if (!schema.Aliases.TryGetValue(prop.SqlName, out var aliases))
                    {
                        aliases = new List<string>();
                        schema.Aliases[prop.SqlName] = aliases;
                    }

                    aliases.Add(fullName);

                    if (_entityNotNullProps.Contains(prop.PropertyName) || HasNotNullFilter(Query.Criteria, prop.PropertyName))
                        schema.NotNullColumns.Add(fullName);
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
                    var fullName = $"{AttributeAlias}.{prop.SqlName}";
                    schema.Schema[fullName] = prop.SqlType;

                    if (!schema.Aliases.TryGetValue(prop.SqlName, out var aliases))
                    {
                        aliases = new List<string>();
                        schema.Aliases[prop.SqlName] = aliases;
                    }

                    aliases.Add(fullName);

                    if (_attributeNotNullProps.Contains(prop.PropertyName) || HasNotNullFilter(Query.AttributeQuery?.Criteria, prop.PropertyName))
                        schema.NotNullColumns.Add(fullName);
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
                    var fullName = $"{OneToManyRelationshipAlias}.{prop.SqlName}";
                    schema.Schema[fullName] = prop.SqlType;

                    if (!schema.Aliases.TryGetValue(prop.SqlName, out var aliases))
                    {
                        aliases = new List<string>();
                        schema.Aliases[prop.SqlName] = aliases;
                    }

                    aliases.Add(fullName);

                    if (_oneToManyRelationshipNotNullProps.Contains(prop.PropertyName) || HasNotNullFilter(Query.RelationshipQuery?.Criteria, prop.PropertyName))
                        schema.NotNullColumns.Add(fullName);
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
                    var fullName = $"{ManyToOneRelationshipAlias}.{prop.SqlName}";
                    schema.Schema[fullName] = prop.SqlType;

                    if (!schema.Aliases.TryGetValue(prop.SqlName, out var aliases))
                    {
                        aliases = new List<string>();
                        schema.Aliases[prop.SqlName] = aliases;
                    }

                    aliases.Add(fullName);

                    if (_oneToManyRelationshipNotNullProps.Contains(prop.PropertyName) || HasNotNullFilter(Query.RelationshipQuery?.Criteria, prop.PropertyName))
                        schema.NotNullColumns.Add(fullName);
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
                    var fullName = $"{ManyToManyRelationshipAlias}.{prop.SqlName}";
                    schema.Schema[fullName] = prop.SqlType;

                    if (!schema.Aliases.TryGetValue(prop.SqlName, out var aliases))
                    {
                        aliases = new List<string>();
                        schema.Aliases[prop.SqlName] = aliases;
                    }

                    aliases.Add(fullName);

                    if (_manyToManyRelationshipNotNullProps.Contains(prop.PropertyName) || HasNotNullFilter(Query.RelationshipQuery?.Criteria, prop.PropertyName))
                        schema.NotNullColumns.Add(fullName);
                }

                schema.PrimaryKey = $"{ManyToManyRelationshipAlias}.{nameof(RelationshipMetadataBase.MetadataId)}";
                childCount++;
            }

            if (childCount > 1)
                schema.PrimaryKey = null;

            return schema;
        }

        private bool HasNotNullFilter(MetadataFilterExpression filter, string prop)
        {
            if (filter == null)
                return false;

            if (filter.FilterOperator == LogicalOperator.Or && (filter.Conditions.Count + filter.Filters.Count) > 1)
                return false;

            foreach (var cond in filter.Conditions)
            {
                if (cond.PropertyName != prop)
                    continue;

                if (cond.ConditionOperator == MetadataConditionOperator.Equals)
                    return cond.Value != null;

                if (cond.ConditionOperator == MetadataConditionOperator.NotEquals)
                    return cond.Value == null;

                if (cond.ConditionOperator == MetadataConditionOperator.GreaterThan || cond.ConditionOperator == MetadataConditionOperator.LessThan)
                    return true;

                if (cond.ConditionOperator == MetadataConditionOperator.In)
                    return Array.IndexOf((Array)cond.Value, null) == -1;

                if (cond.ConditionOperator == MetadataConditionOperator.NotIn)
                    return Array.IndexOf((Array)cond.Value, null) != -1;
            }

            return false;
        }

        internal static DataTypeReference GetPropertyType(Type propType)
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

            return SqlTypeConverter.NetToSqlType(propType).ToSqlType();
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

        protected override IEnumerable<Entity> ExecuteInternal(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, DataTypeReference> parameterTypes, IDictionary<string, object> parameterValues)
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

        public override object Clone()
        {
            return new MetadataQueryNode
            {
                AttributeAlias = AttributeAlias,
                DataSource = DataSource,
                EntityAlias= EntityAlias,
                ManyToManyRelationshipAlias = ManyToManyRelationshipAlias,
                ManyToManyRelationshipJoin = ManyToManyRelationshipJoin,
                ManyToOneRelationshipAlias = ManyToOneRelationshipAlias,
                MetadataSource = MetadataSource,
                OneToManyRelationshipAlias = OneToManyRelationshipAlias,
                Query = Query,
                _attributeCols = _attributeCols,
                _entityCols = _entityCols,
                _manyToManyRelationshipCols = _manyToManyRelationshipCols,
                _manyToOneRelationshipCols = _manyToOneRelationshipCols,
                _oneToManyRelationshipCols = _oneToManyRelationshipCols
            };
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
