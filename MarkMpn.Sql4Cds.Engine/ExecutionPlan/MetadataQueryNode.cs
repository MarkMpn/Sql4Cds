﻿using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlTypes;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.Serialization;
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
            public IComparable[] DataMemberOrder { get; set; }
        }

        class AttributeProperty
        {
            public string SqlName { get; set; }
            public string PropertyName { get; set; }
            public IDictionary<Type, Func<object,object>> Accessors { get; set; }
            public DataTypeReference SqlType { get; set; }
            public Type Type { get; set; }
            public bool IsNullable { get; set; }
            public IComparable[] DataMemberOrder { get; set; }
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
                .ToDictionary(p => p.Name, p => new MetadataProperty { SqlName = p.Name.ToLowerInvariant(), PropertyName = p.Name, Type = p.PropertyType, SqlType = GetPropertyType(p.PropertyType), Accessor = GetPropertyAccessor(p, GetPropertyType(p.PropertyType).ToNetType(out _)), DataMemberOrder = GetDataMemberOrder(p) }, StringComparer.OrdinalIgnoreCase);

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
                .ToDictionary(p => p.Name, p => new MetadataProperty { SqlName = p.Name.ToLowerInvariant(), PropertyName = p.Name, Type = p.PropertyType, SqlType = GetPropertyType(p.PropertyType), Accessor = GetPropertyAccessor(p, GetPropertyType(p.PropertyType).ToNetType(out _)), DataMemberOrder = GetDataMemberOrder(p) }, StringComparer.OrdinalIgnoreCase);

            var excludedManyToManyRelationshipProps = new[]
            {
                nameof(ManyToManyRelationshipMetadata.ExtensionData),
                nameof(ManyToManyRelationshipMetadata.Entity1AssociatedMenuConfiguration),
                nameof(ManyToManyRelationshipMetadata.Entity2AssociatedMenuConfiguration)
            };

            _manyToManyRelationshipProps = typeof(ManyToManyRelationshipMetadata)
                .GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Where(p => !excludedManyToManyRelationshipProps.Contains(p.Name))
                .ToDictionary(p => p.Name, p => new MetadataProperty { SqlName = p.Name.ToLowerInvariant(), PropertyName = p.Name, Type = p.PropertyType, SqlType = GetPropertyType(p.PropertyType), Accessor = GetPropertyAccessor(p, GetPropertyType(p.PropertyType).ToNetType(out _)), DataMemberOrder = GetDataMemberOrder(p) }, StringComparer.OrdinalIgnoreCase);

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
                    var isNullable = g.Count() < _attributeTypes.Length;

                    foreach (var prop in g)
                    {
                        if (type == null)
                        {
                            type = GetPropertyType(prop.Property.PropertyType);
                        }
                        else if (!SqlTypeConverter.CanMakeConsistentTypes(type, GetPropertyType(prop.Property.PropertyType), null, null, null, out type))
                        {
                            // Can't make a consistent type for this property, so we can't use it
                            type = null;
                            break;
                        }

                        if (!isNullable && IsNullable(prop.Property.PropertyType))
                            isNullable = true;
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
                        IsNullable = isNullable,
                        DataMemberOrder = GetDataMemberOrder(g.First().Property)
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

        public override void AddRequiredColumns(NodeCompilationContext context, IList<string> requiredColumns)
        {
            _entityCols = new Dictionary<string, MetadataProperty>();
            _attributeCols = new Dictionary<string, AttributeProperty>();
            _oneToManyRelationshipCols = new Dictionary<string, MetadataProperty>();
            _manyToOneRelationshipCols = new Dictionary<string, MetadataProperty>();
            _manyToManyRelationshipCols = new Dictionary<string, MetadataProperty>();

            foreach (var col in requiredColumns)
            {
                var parts = col.SplitMultiPartIdentifier();

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

            NormalizeProperties();
        }

        private void NormalizeProperties()
        {
            NormalizeProperties(Query, _entityProps.Values.Select(p => p.PropertyName));
            NormalizeProperties(Query.AttributeQuery, _attributeProps.Values.Select(p => p.PropertyName));
            NormalizeProperties(Query.RelationshipQuery, _oneToManyRelationshipProps.Values.Select(p => p.PropertyName).Union(_manyToManyRelationshipProps.Values.Select(p => p.PropertyName)));
        }

        private void NormalizeProperties(MetadataQueryExpression query, IEnumerable<string> allProperties)
        {
            if (query == null || query.Properties == null || query.Properties.AllProperties)
                return;

            var missingProperties = allProperties.Except(query.Properties.PropertyNames).Any();

            if (!missingProperties)
            {
                query.Properties.PropertyNames.Clear();
                query.Properties.AllProperties = true;
            }
        }

        protected override RowCountEstimate EstimateRowsOutInternal(NodeCompilationContext context)
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

            var count = entityCount * attributesPerEntity * relationshipsPerEntity;

            if (count == 1)
                return RowCountEstimateDefiniteRange.ZeroOrOne;

            return new RowCountEstimate(count);
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

        public override IDataExecutionPlanNodeInternal FoldQuery(NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            CompileFilters(context);
            return this;
        }

        private void CompileFilters(NodeCompilationContext context)
        {
            var ecc = new ExpressionCompilationContext(context, null, null);

            CompileFilters(Query.Criteria, ecc, typeof(EntityMetadata));
            CompileFilters(Query.AttributeQuery?.Criteria, ecc, typeof(AttributeMetadata));
            CompileFilters(Query.RelationshipQuery?.Criteria, ecc, typeof(RelationshipMetadataBase));
        }

        private void CompileFilters(MetadataFilterExpression criteria, ExpressionCompilationContext ecc, Type targetType)
        {
            if (criteria == null)
                return;

            foreach (var filter in criteria.Filters)
                CompileFilters(filter, ecc, targetType);

            foreach (var condition in criteria.Conditions)
            {
                // Convert the SQL value to the .NET value
                var prop = targetType.GetProperty(condition.PropertyName);
                var targetValueType = prop.PropertyType;

                // Managed properties and nullable types are handled through their Value property
                if (targetValueType.BaseType != null &&
                    targetValueType.BaseType.IsGenericType &&
                    targetValueType.BaseType.GetGenericTypeDefinition() == typeof(ManagedProperty<>))
                    targetValueType = targetValueType.BaseType.GetGenericArguments()[0];

                if (targetValueType.IsGenericType &&
                    targetValueType.GetGenericTypeDefinition() == typeof(Nullable<>))
                    targetValueType = targetValueType.GetGenericArguments()[0];

                if (condition.Value is ScalarExpression expression)
                    condition.Value = CompileExpression(expression, ecc, targetValueType);
                else if (condition.Value is IList<ScalarExpression> expressions)
                    condition.Value = new CompiledExpressionList(expressions.Select(e => CompileExpression(e, ecc, targetValueType))) { ElementType = targetValueType };
            }
        }

        private CompiledExpression CompileExpression(ScalarExpression expression, ExpressionCompilationContext ecc, Type targetValueType)
        {
            // Compile the expression to return the SQL value
            var expr = expression.Compile(ecc);
            expression.GetType(ecc, out var exprSqlType);

            var targetSqlType = GetPropertyType(targetValueType);
            var sqlConverter = SqlTypeConverter.GetConversion(exprSqlType, targetSqlType);
            
            var targetNetType = targetSqlType.ToNetType(out _);
            var netConverter = SqlTypeConverter.GetConversion(targetNetType, targetValueType);

            return new CompiledExpression(expression, context => netConverter(sqlConverter((INullable)expr(context))));
        }

        public override INodeSchema GetSchema(NodeCompilationContext context)
        {
            var schema = new ColumnList();
            var aliases = new Dictionary<string, IReadOnlyList<string>>();
            var primaryKey = (string)null;
            var sortOrder = new List<string>();

            var childCount = 0;

            if (MetadataSource.HasFlag(MetadataSource.Entity))
            {
                var entityProps = (IEnumerable<MetadataProperty>) _entityProps.Values;

                if (Query.Properties != null)
                    entityProps = entityProps.Where(p => Query.Properties.AllProperties || Query.Properties.PropertyNames.Contains(p.PropertyName, StringComparer.OrdinalIgnoreCase));

                if (context.Options.ColumnOrdering == ColumnOrdering.Alphabetical)
                    entityProps = entityProps.OrderBy(p => p.SqlName);
                else
                    entityProps = entityProps.OrderBy(p => p.DataMemberOrder[0]).ThenBy(p => p.DataMemberOrder[1]).ThenBy(p => p.DataMemberOrder[2]);

                var escapedEntityAlias = EntityAlias.EscapeIdentifier();

                foreach (var prop in entityProps)
                {
                    var fullName = $"{escapedEntityAlias}.{prop.SqlName}";
                    var nullable = true;

                    if (_entityNotNullProps.Contains(prop.PropertyName) || HasNotNullFilter(Query.Criteria, prop.PropertyName))
                        nullable = false;

                    schema[fullName] = new ColumnDefinition(prop.SqlType, nullable, false);

                    if (!aliases.TryGetValue(prop.SqlName, out var a))
                    {
                        a = new List<string>();
                        aliases[prop.SqlName] = a;
                    }

                    ((List<string>)a).Add(fullName);
                }

                primaryKey = $"{EntityAlias}.{nameof(EntityMetadata.MetadataId)}";
            }

            if (MetadataSource.HasFlag(MetadataSource.Attribute))
            {
                var attributeProps = (IEnumerable<AttributeProperty>) _attributeProps.Values;

                if (Query.AttributeQuery?.Properties != null)
                    attributeProps = attributeProps.Where(p => Query.AttributeQuery.Properties.AllProperties || Query.AttributeQuery.Properties.PropertyNames.Contains(p.PropertyName, StringComparer.OrdinalIgnoreCase));

                if (context.Options.ColumnOrdering == ColumnOrdering.Alphabetical)
                    attributeProps = attributeProps.OrderBy(p => p.SqlName);
                else
                    attributeProps = attributeProps.OrderBy(p => p.DataMemberOrder[0]).ThenBy(p => p.DataMemberOrder[1]).ThenBy(p => p.DataMemberOrder[2]);

                var escapedAttributeAlias = AttributeAlias.EscapeIdentifier();

                foreach (var prop in attributeProps)
                {
                    var fullName = $"{escapedAttributeAlias}.{prop.SqlName}";
                    var nullable = true;

                    if (_attributeNotNullProps.Contains(prop.PropertyName) || HasNotNullFilter(Query.AttributeQuery?.Criteria, prop.PropertyName))
                        nullable = false;

                    schema[fullName] = new ColumnDefinition(prop.SqlType, nullable, false);

                    if (!aliases.TryGetValue(prop.SqlName, out var a))
                    {
                        a = new List<string>();
                        aliases[prop.SqlName] = a;
                    }

                    ((List<string>)a).Add(fullName);
                }

                primaryKey = $"{AttributeAlias}.{nameof(AttributeMetadata.MetadataId)}";
                childCount++;
            }

            if (MetadataSource.HasFlag(MetadataSource.OneToManyRelationship))
            {
                var relationshipProps = (IEnumerable<MetadataProperty>)_oneToManyRelationshipProps.Values;

                if (Query.RelationshipQuery?.Properties != null)
                    relationshipProps = relationshipProps.Where(p => Query.RelationshipQuery.Properties.AllProperties || Query.RelationshipQuery.Properties.PropertyNames.Contains(p.PropertyName, StringComparer.OrdinalIgnoreCase));

                if (context.Options.ColumnOrdering == ColumnOrdering.Alphabetical)
                    relationshipProps = relationshipProps.OrderBy(p => p.SqlName);
                else
                    relationshipProps = relationshipProps.OrderBy(p => p.DataMemberOrder[0]).ThenBy(p => p.DataMemberOrder[1]).ThenBy(p => p.DataMemberOrder[2]);

                var escapedOneToManyRelationshipAlias = OneToManyRelationshipAlias.EscapeIdentifier();

                foreach (var prop in relationshipProps)
                {
                    var fullName = $"{escapedOneToManyRelationshipAlias}.{prop.SqlName}";
                    var nullable = true;

                    if (_oneToManyRelationshipNotNullProps.Contains(prop.PropertyName) || HasNotNullFilter(Query.RelationshipQuery?.Criteria, prop.PropertyName))
                        nullable = false;

                    schema[fullName] = new ColumnDefinition(prop.SqlType, nullable, false);

                    if (!aliases.TryGetValue(prop.SqlName, out var a))
                    {
                        a = new List<string>();
                        aliases[prop.SqlName] = a;
                    }

                    ((List<string>)a).Add(fullName);
                }

                primaryKey = $"{OneToManyRelationshipAlias}.{nameof(RelationshipMetadataBase.MetadataId)}";
                childCount++;
            }

            if (MetadataSource.HasFlag(MetadataSource.ManyToOneRelationship))
            {
                var relationshipProps = (IEnumerable<MetadataProperty>)_oneToManyRelationshipProps.Values;

                if (Query.RelationshipQuery?.Properties != null)
                    relationshipProps = relationshipProps.Where(p => Query.RelationshipQuery.Properties.AllProperties || Query.RelationshipQuery.Properties.PropertyNames.Contains(p.PropertyName, StringComparer.OrdinalIgnoreCase));

                if (context.Options.ColumnOrdering == ColumnOrdering.Alphabetical)
                    relationshipProps = relationshipProps.OrderBy(p => p.SqlName);
                else
                    relationshipProps = relationshipProps.OrderBy(p => p.DataMemberOrder[0]).ThenBy(p => p.DataMemberOrder[1]).ThenBy(p => p.DataMemberOrder[2]);

                var escapedManyToOneRelationshipAlias = ManyToOneRelationshipAlias.EscapeIdentifier();

                foreach (var prop in relationshipProps)
                {
                    var fullName = $"{escapedManyToOneRelationshipAlias}.{prop.SqlName}";
                    var nullable = true;

                    if (_oneToManyRelationshipNotNullProps.Contains(prop.PropertyName) || HasNotNullFilter(Query.RelationshipQuery?.Criteria, prop.PropertyName))
                        nullable = false;

                    schema[fullName] = new ColumnDefinition(prop.SqlType, nullable, false);

                    if (!aliases.TryGetValue(prop.SqlName, out var a))
                    {
                        a = new List<string>();
                        aliases[prop.SqlName] = a;
                    }

                    ((List<string>)a).Add(fullName);
                }

                primaryKey = $"{ManyToOneRelationshipAlias}.{nameof(RelationshipMetadataBase.MetadataId)}";
                childCount++;
            }

            if (MetadataSource.HasFlag(MetadataSource.ManyToManyRelationship))
            {
                var relationshipProps = (IEnumerable<MetadataProperty>)_manyToManyRelationshipProps.Values;

                if (Query.RelationshipQuery?.Properties != null)
                    relationshipProps = relationshipProps.Where(p => Query.RelationshipQuery.Properties.AllProperties || Query.RelationshipQuery.Properties.PropertyNames.Contains(p.PropertyName, StringComparer.OrdinalIgnoreCase));

                if (context.Options.ColumnOrdering == ColumnOrdering.Alphabetical)
                    relationshipProps = relationshipProps.OrderBy(p => p.SqlName);
                else
                    relationshipProps = relationshipProps.OrderBy(p => p.DataMemberOrder[0]).ThenBy(p => p.DataMemberOrder[1]).ThenBy(p => p.DataMemberOrder[2]);

                var escapedManyToManyRelationshipAlias = ManyToManyRelationshipAlias.EscapeIdentifier();

                foreach (var prop in relationshipProps)
                {
                    var fullName = $"{escapedManyToManyRelationshipAlias}.{prop.SqlName}";
                    var nullable = true;

                    if (_manyToManyRelationshipNotNullProps.Contains(prop.PropertyName) || HasNotNullFilter(Query.RelationshipQuery?.Criteria, prop.PropertyName))
                        nullable = false;

                    schema[fullName] = new ColumnDefinition(prop.SqlType, nullable, false);

                    if (!aliases.TryGetValue(prop.SqlName, out var a))
                    {
                        a = new List<string>();
                        aliases[prop.SqlName] = a;
                    }

                    ((List<string>)a).Add(fullName);
                }

                primaryKey = $"{ManyToManyRelationshipAlias}.{nameof(RelationshipMetadataBase.MetadataId)}";
                childCount++;
            }

            if (childCount > 1)
                primaryKey = null;

            return new NodeSchema(
                primaryKey: primaryKey,
                schema: schema,
                aliases: aliases,
                sortOrder: sortOrder);
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

                if (cond.ConditionOperator == MetadataConditionOperator.In || cond.ConditionOperator == MetadataConditionOperator.NotIn)
                    return true;
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

            return SqlTypeConverter.NetToSqlType(propType).ToSqlType(null);
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

            Expression converted;

            if (value.Type == typeof(string) && directConversionType == typeof(SqlString) && targetType == typeof(SqlString))
                converted = Expr.Call(() => ApplyCollation(Expr.Arg<string>()), value);
            else
                converted = SqlTypeConverter.Convert(value, directConversionType);

            if (targetType != directConversionType)
                converted = SqlTypeConverter.Convert(converted, targetType);

            // Return null literal if final value is null
            if (!value.Type.IsValueType)
                value = Expression.Condition(Expression.Equal(value, Expression.Constant(null)), Expression.Constant(SqlTypeConverter.GetNullValue(targetType), targetType), converted);
            else
                value = converted;

            // Return null literal if original value is null
            if (!prop.PropertyType.IsValueType || prop.PropertyType.IsGenericType && prop.PropertyType.GetGenericTypeDefinition() == typeof(Nullable<>))
                value = Expression.Condition(Expression.Equal(Expression.Property(param, prop), Expression.Constant(null)), Expression.Constant(SqlTypeConverter.GetNullValue(targetType), targetType), value);

            // Compile the function
            value = Expr.Box(value);
            var func = (Func<object,object>) Expression.Lambda(value, rawParam).Compile();
            return func;
        }

        internal static IComparable[] GetDataMemberOrder(PropertyInfo prop)
        {
            // https://learn.microsoft.com/en-us/dotnet/framework/wcf/feature-details/data-member-order
            var inheritanceDepth = 0;
            var type = prop.DeclaringType;
            while (type.BaseType != null)
            {
                inheritanceDepth++;
                type = type.BaseType;
            }

            var attr = prop.GetCustomAttribute<DataMemberAttribute>();

            return new IComparable[]
            {
                inheritanceDepth,
                attr?.Order ?? Int32.MinValue,
                prop.Name
            };
        }

        private static SqlString ApplyCollation(string value)
        {
            if (value == null)
                return SqlString.Null;

            // Assume all metadata values should use standard collation rather than datasource specific?
            return Collation.USEnglish.ToSqlString(value);
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            return Array.Empty<IDataExecutionPlanNode>();
        }

        protected override IEnumerable<Entity> ExecuteInternal(NodeExecutionContext context)
        {
            // TODO: Execute expressions to get filter conditions, but preserve the original expressions for later executions
            ApplyFilterValues(new ExpressionExecutionContext(context));

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

            if (!context.DataSources.TryGetValue(DataSource, out var dataSource))
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

        private void ApplyFilterValues(ExpressionExecutionContext context)
        {
            ApplyFilterValues(Query.Criteria, context);
            ApplyFilterValues(Query.AttributeQuery?.Criteria, context);
            ApplyFilterValues(Query.RelationshipQuery?.Criteria, context);
        }

        private void ApplyFilterValues(MetadataFilterExpression criteria, ExpressionExecutionContext context)
        {
            if (criteria == null)
                return;

            foreach (var filter in criteria.Filters)
                ApplyFilterValues(filter, context);

            foreach (var condition in criteria.Conditions.ToArray())
            {
                MetadataFilterExpression replacementFilter = null;

                if (condition.Value is CompiledExpression expression)
                {
                    var value = expression.Compiled(context);

                    if (value == null)
                    {
                        // Comparing any value to null should always return false in SQL, but the metadata query endpoint allows them to match
                        // using .NET semantics. Replace the condition with an impossible filter
                        replacementFilter = new MetadataFilterExpression
                        {
                            Conditions =
                            {
                                new MetadataConditionExpression(condition.PropertyName, MetadataConditionOperator.Equals, null),
                                new MetadataConditionExpression(condition.PropertyName, MetadataConditionOperator.NotEquals, null)
                            }
                        };
                    }
                    else if (value != null && value.GetType().IsEnum && !Enum.IsDefined(value.GetType(), value))
                    {
                        // Converting an unknown string to a enum will produce a -1 value. Replace the condition with a filter that produces
                        // the correct results, e.g. AttributeTypeCode = -1 is impossible but AttributeTypeCode <> -1 is true for all non-null values
                        if (condition.ConditionOperator == MetadataConditionOperator.Equals)
                        {
                            replacementFilter = new MetadataFilterExpression
                            {
                                Conditions =
                                {
                                    new MetadataConditionExpression(condition.PropertyName, MetadataConditionOperator.Equals, Enum.GetValues(value.GetType()).GetValue(0)),
                                    new MetadataConditionExpression(condition.PropertyName, MetadataConditionOperator.NotEquals, Enum.GetValues(value.GetType()).GetValue(0))
                                }
                            };
                        }
                        else if (condition.ConditionOperator == MetadataConditionOperator.NotEquals)
                        {
                            replacementFilter = new MetadataFilterExpression
                            {
                                Conditions =
                                {
                                    new MetadataConditionExpression(condition.PropertyName, MetadataConditionOperator.NotEquals, null)
                                }
                            };
                        }
                    }
                    else
                    {
                        condition.Value = value;
                    }
                }
                else if (condition.Value is CompiledExpressionList expressions)
                {
                    var values = new List<object>();

                    foreach (var expr in expressions)
                    {
                        var value = expr.Compiled(context);

                        if (value == null || !value.GetType().IsEnum || Enum.IsDefined(value.GetType(), value))
                            values.Add(value);
                    }

                    var array = Array.CreateInstance(expressions.ElementType, values.Count);

                    for (var i = 0; i < values.Count; i++)
                        array.SetValue(values[i], i);

                    condition.Value = array;
                }

                if (replacementFilter != null)
                {
                    criteria.Conditions.Remove(condition);
                    criteria.Filters.Add(replacementFilter);
                }
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
                Query = Query.Clone(),
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
