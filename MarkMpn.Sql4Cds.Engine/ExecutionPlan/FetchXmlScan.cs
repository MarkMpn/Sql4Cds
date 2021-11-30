using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlTypes;
using System.Globalization;
using System.IO;
using System.Linq;
using System.ServiceModel;
using System.Text.RegularExpressions;
using System.Xml.Serialization;
using MarkMpn.Sql4Cds.Engine.FetchXml;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Metadata;
using Microsoft.Xrm.Sdk.Query;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    class FetchXmlScan : BaseDataNode, IFetchXmlExecutionPlanNode
    {
        class ParameterizedCondition
        {
            private readonly filter _filter;
            private readonly condition _condition;
            private readonly filter _contradiction;

            public ParameterizedCondition(filter filter, condition condition)
            {
                _filter = filter;
                _condition = condition;
                _contradiction = new filter
                {
                    Items = new object[]
                    {
                        new condition { attribute = condition.attribute, @operator = @operator.@null },
                        new condition { attribute = condition.attribute, @operator = @operator.notnull },
                    }
                };
            }

            public void SetValue(object value)
            {
                if (value == null)
                {
                    if (_filter.Items.Contains(_contradiction))
                        return;

                    _filter.Items = _filter.Items.Except(new[] { _condition }).Concat(new[] { _contradiction }).ToArray();
                }
                else
                {
                    if (!_filter.Items.Contains(_condition))
                        _filter.Items = _filter.Items.Except(new[] { _contradiction }).Concat(new[] { _condition }).ToArray();

                    _condition.value = value.ToString();
                }
            }
        }

        private Dictionary<string, ParameterizedCondition> _parameterizedConditions;
        private HashSet<string> _entityNameGroupings;
        private Dictionary<string, string> _primaryKeyColumns;

        public FetchXmlScan()
        {
            AllPages = true;
        }

        /// <summary>
        /// The instance that this node will be executed against
        /// </summary>
        [Category("Data Source")]
        [Description("The data source this query is executed against")]
        public string DataSource { get; set; }

        /// <summary>
        /// The FetchXML query
        /// </summary>
        [Browsable(false)]
        public FetchType FetchXml { get; set; }

        /// <summary>
        /// The main &lt;entity&gt; node in the <see cref="FetchXml"/>
        /// </summary>
        [Browsable(false)]
        public FetchEntityType Entity => FetchXml.Items.OfType<FetchEntityType>().Single();

        /// <summary>
        /// The alias to apply to the primary entity in the query
        /// </summary>
        [Category("FetchXML Scan")]
        [Description("The alias to apply to the primary entity in the query")]
        public string Alias { get; set; }

        /// <summary>
        /// The string representation of the <see cref="FetchXml"/>
        /// </summary>
        [Category("FetchXML Scan")]
        [Description("The FetchXML query to execute")]
        [DisplayName("FetchXML")]
        public string FetchXmlString => Serialize(FetchXml);

        /// <summary>
        /// Indicates if the query will page across all the available data
        /// </summary>
        [Category("FetchXML Scan")]
        [Description("Indicates if all subsequent pages of results should be returned")]
        [DisplayName("All Pages")]
        public bool AllPages { get; set; }

        /// <summary>
        /// Indicates if all available attributes should be returned as part of the schema, used while the execution plan is being built
        /// </summary>
        [Browsable(false)]
        public bool ReturnFullSchema { get; set; }

        protected override IEnumerable<Entity> ExecuteInternal(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues)
        {
            if (!dataSources.TryGetValue(DataSource, out var dataSource))
                throw new NotSupportedQueryFragmentException("Missing datasource " + DataSource);

            ReturnFullSchema = false;
            var schema = GetSchema(dataSources, parameterTypes);

            // Apply any variable conditions
            if (parameterValues != null)
            {
                if (_parameterizedConditions == null)
                    FindParameterizedConditions();

                foreach (var param in parameterValues)
                {
                    if (_parameterizedConditions.TryGetValue(param.Key, out var condition))
                        condition.SetValue(param.Value);
                }
            }

            FindEntityNameGroupings(dataSource.Metadata);

            var mainEntity = FetchXml.Items.OfType<FetchEntityType>().Single();
            var name = mainEntity.name;
            var meta = dataSource.Metadata[name];
            options.Progress(0, $"Retrieving {GetDisplayName(0, meta)}...");

            // Get the first page of results
            options.RetrievingNextPage();
            var res = dataSource.Connection.RetrieveMultiple(new FetchExpression(Serialize(FetchXml)));

            var count = res.Entities.Count;

            // Aggregate queries return up to 5000 records and don't provide a method to move on to the next page
            // Throw an exception to indicate the error to the caller
            if (AllPages && FetchXml.aggregateSpecified && FetchXml.aggregate && count == 5000 && FetchXml.top != "5000" && !res.MoreRecords)
                throw new FaultException<OrganizationServiceFault>(new OrganizationServiceFault { ErrorCode = -2147164125, Message = "AggregateQueryRecordLimitExceeded" });

            foreach (var entity in res.Entities)
            {
                OnRetrievedEntity(entity, schema, options, dataSource.Metadata);
                yield return entity;
            }

            // Move on to subsequent pages
            while (AllPages && res.MoreRecords && options.ContinueRetrieve(count))
            {
                options.Progress(0, $"Retrieved {count:N0} {GetDisplayName(count, meta)}...");

                if (FetchXml.page == null)
                    FetchXml.page = "2";
                else
                    FetchXml.page = (Int32.Parse(FetchXml.page, CultureInfo.InvariantCulture) + 1).ToString();

                FetchXml.pagingcookie = res.PagingCookie;

                options.RetrievingNextPage();
                var nextPage = dataSource.Connection.RetrieveMultiple(new FetchExpression(Serialize(FetchXml)));

                foreach (var entity in nextPage.Entities)
                {
                    OnRetrievedEntity(entity, schema, options, dataSource.Metadata);
                    yield return entity;
                }

                count += nextPage.Entities.Count;
                res = nextPage;
            }
        }

        private void OnRetrievedEntity(Entity entity, NodeSchema schema, IQueryExecutionOptions options, IAttributeMetadataCache metadata)
        {
            // Expose any formatted values for OptionSetValue and EntityReference values
            foreach (var formatted in entity.FormattedValues)
            {
                if (!entity.Contains(formatted.Key + "name"))
                    entity[formatted.Key + "name"] = formatted.Value;
            }

            if (options.UseLocalTimeZone)
            {
                // For any datetime values, check the metadata to see if they are affected by timezones and convert them
                foreach (var attribute in entity.Attributes.ToList())
                {
                    var entityName = entity.LogicalName;
                    var attributeName = attribute.Key;
                    var value = attribute.Value;

                    if (value is AliasedValue alias)
                    {
                        entityName = alias.EntityLogicalName;
                        attributeName = alias.AttributeLogicalName;
                        value = alias.Value;
                    }

                    if (value is DateTime dt)
                    {
                        var meta = metadata[entityName];
                        var attrMeta = (DateTimeAttributeMetadata) meta.Attributes.Single(a => a.LogicalName == attributeName);

                        if (attrMeta.DateTimeBehavior == DateTimeBehavior.UserLocal)
                        {
                            dt = dt.ToLocalTime();
                            entity[attribute.Key] = dt;
                        }
                    }
                }
            }

            // Prefix all attributes of the main entity with the expected alias
            foreach (var attribute in entity.Attributes.Where(attr => !attr.Key.Contains('.') && !(attr.Value is AliasedValue)).ToList())
                entity[$"{Alias}.{attribute.Key}"] = attribute.Value;

            // Only prefix aliased values if they're not aggregates
            PrefixAliasedScalarAttributes(entity, Entity.Items, Alias);

            // Convert aliased values to the underlying value
            foreach (var attribute in entity.Attributes.Where(attr => attr.Value is AliasedValue).ToList())
            {
                var aliasedValue = (AliasedValue)attribute.Value;

                // When grouping by EntityName attributes the value is converted from the normal string value to an OptionSetValue
                // Convert it back now for consistency
                if (_entityNameGroupings.Contains(attribute.Key))
                {
                    var otc = ((OptionSetValue)aliasedValue.Value).Value;
                    var meta = metadata[otc];
                    entity[attribute.Key] = meta.LogicalName;
                }
                else
                {
                    entity[attribute.Key] = aliasedValue.Value;
                }
            }

            // Copy any grouped values to their full names
            if (FetchXml.aggregateSpecified && FetchXml.aggregate)
            {
                if (Entity.Items != null)
                {
                    foreach (var attr in Entity.Items.OfType<FetchAttributeType>().Where(a => a.groupbySpecified && a.groupby == FetchBoolType.@true))
                    {
                        if (entity.Attributes.TryGetValue(attr.alias, out var value))
                            entity[$"{Alias}.{attr.alias}"] = value;
                    }
                }

                foreach (var linkEntity in Entity.GetLinkEntities().Where(le => le.Items != null))
                {
                    foreach (var attr in linkEntity.Items.OfType<FetchAttributeType>().Where(a => a.groupbySpecified && a.groupby == FetchBoolType.@true))
                    {
                        if (entity.Attributes.TryGetValue(attr.alias, out var value))
                            entity[$"{linkEntity.alias}.{attr.alias}"] = value;
                    }
                }
            }

            // Expose the type of lookup values
            foreach (var attribute in entity.Attributes.Where(attr => attr.Value is EntityReference).ToList())
            {
                if (!entity.Contains(attribute.Key + "type"))
                    entity[attribute.Key + "type"] = ((EntityReference)attribute.Value).LogicalName;

                //entity[attribute.Key] = ((EntityReference)attribute.Value).Id;
            }

            // Convert values to SQL types
            foreach (var col in schema.Schema)
            {
                object sqlValue;

                if (entity.Attributes.TryGetValue(col.Key, out var value) && value != null)
                    sqlValue = SqlTypeConverter.NetToSqlType(DataSource, value);
                else
                    sqlValue = SqlTypeConverter.GetNullValue(col.Value);

                if (_primaryKeyColumns.TryGetValue(col.Key, out var logicalName) && sqlValue is SqlGuid guid)
                    sqlValue = new SqlEntityReference(DataSource, logicalName, guid);

                entity[col.Key] = sqlValue;
            }
        }

        private void PrefixAliasedScalarAttributes(Entity entity, object[] items, string alias)
        {
            if (items == null)
                return;

            foreach (var attr in items.OfType<FetchAttributeType>().Where(a => !String.IsNullOrEmpty(a.alias) && a.aggregateSpecified == false))
            {
                var value = entity.GetAttributeValue<AliasedValue>(attr.alias);

                if (value != null)
                    entity[$"{alias}.{attr.alias}"] = value;
            }

            foreach (var linkEntity in items.OfType<FetchLinkEntityType>())
                PrefixAliasedScalarAttributes(entity, linkEntity.Items, linkEntity.alias);
        }

        private void FindParameterizedConditions()
        {
            _parameterizedConditions = new Dictionary<string, ParameterizedCondition>();

            FindParameterizedConditions(null, Entity.Items);
        }

        private void FindParameterizedConditions(filter filter, object[] items)
        {
            foreach (var condition in items.OfType<condition>().Where(c => c.value != null && c.value.StartsWith("@")))
                _parameterizedConditions[condition.value] = new ParameterizedCondition(filter, condition);

            foreach (var subFilter in items.OfType<filter>())
                FindParameterizedConditions(subFilter, subFilter.Items);

            foreach (var link in items.OfType<FetchLinkEntityType>())
                FindParameterizedConditions(null, link.Items);
        }

        private void FindEntityNameGroupings(IAttributeMetadataCache metadata)
        {
            _entityNameGroupings = new HashSet<string>();

            if (FetchXml.aggregateSpecified && FetchXml.aggregate)
                FindEntityNameGroupings(metadata, Entity.name, Entity.Items);
        }

        private void FindEntityNameGroupings(IAttributeMetadataCache metadata, string logicalName, object[] items)
        {
            if (items == null)
                return;

            foreach (var attr in items.OfType<FetchAttributeType>().Where(a => a.groupbySpecified && a.groupby == FetchBoolType.@true))
            {
                var attributeMetadata = metadata[logicalName].Attributes.Single(a => a.LogicalName == attr.name);

                if (attributeMetadata.AttributeType == AttributeTypeCode.EntityName)
                    _entityNameGroupings.Add(attr.alias);
            }

            foreach (var linkEntity in items.OfType<FetchLinkEntityType>())
                FindEntityNameGroupings(metadata, linkEntity.name, linkEntity.Items);
        }

        private bool ContainsSort(object[] items)
        {
            if (items == null)
                return false;

            if (items.OfType<FetchOrderType>().Any())
                return true;

            if (items.OfType<FetchEntityType>().Any(entity => ContainsSort(entity.Items)))
                return true;

            if (items.OfType<FetchLinkEntityType>().Any(linkEntity => ContainsSort(linkEntity.Items)))
                return true;

            return false;
        }

        /// <summary>
        /// Convert the FetchXML query object to a string
        /// </summary>
        /// <param name="fetch">The FetchXML query object to convert</param>
        /// <returns>The string representation of the query</returns>
        internal static string Serialize(FetchType fetch)
        {
            var serializer = new XmlSerializer(typeof(FetchType));

            using (var writer = new StringWriter())
            using (var xmlWriter = System.Xml.XmlWriter.Create(writer, new System.Xml.XmlWriterSettings
            {
                OmitXmlDeclaration = true,
                Indent = true
            }))
            {
                // Add in a separate namespace to remove the xsi and xsd namespaces added by default
                var xsn = new XmlSerializerNamespaces();
                xsn.Add("generator", "MarkMpn.SQL4CDS");

                serializer.Serialize(xmlWriter, fetch, xsn);
                return writer.ToString();
            }
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            return Array.Empty<IDataExecutionPlanNode>();
        }

        public override NodeSchema GetSchema(IDictionary<string, DataSource> dataSources, IDictionary<string, Type> parameterTypes)
        {
            if (!dataSources.TryGetValue(DataSource, out var dataSource))
                throw new NotSupportedQueryFragmentException("Missing datasource " + DataSource);

            _primaryKeyColumns = new Dictionary<string, string>();
            var schema = new NodeSchema();

            // Add each attribute from the main entity and recurse into link entities
            var entity = FetchXml.Items.OfType<FetchEntityType>().Single();
            var meta = dataSource.Metadata[entity.name];

            if (!FetchXml.aggregate)
                schema.PrimaryKey = $"{Alias}.{meta.PrimaryIdAttribute}";

            AddSchemaAttributes(schema, dataSource.Metadata, entity.name, Alias, entity.Items);
            
            return schema;
        }

        internal FetchAttributeType AddAttribute(string colName, Func<FetchAttributeType, bool> predicate, IAttributeMetadataCache metadata, out bool added, out FetchLinkEntityType linkEntity)
        {
            var parts = colName.Split('.');

            if (parts.Length == 1)
            {
                added = false;
                return Entity.FindAliasedAttribute(colName, predicate, out linkEntity);
            }

            var entityName = parts[0];
            var attr = new FetchAttributeType { name = parts[1].ToLowerInvariant() };

            if (Alias == entityName)
            {
                linkEntity = null;

                var meta = metadata[Entity.name].Attributes.SingleOrDefault(a => a.LogicalName == attr.name && a.AttributeOf == null);
                if (meta == null && (attr.name.EndsWith("name") || attr.name.EndsWith("type")))
                {
                    var logicalName = attr.name.Substring(0, attr.name.Length - 4);
                    meta = metadata[Entity.name].Attributes.SingleOrDefault(a => a.LogicalName == logicalName && a.AttributeOf == null);

                    if (meta != null)
                        attr.name = logicalName;
                }

                if (Entity.Items != null)
                {
                    var existing = Entity.Items.OfType<FetchAttributeType>().FirstOrDefault(a => a.name == attr.name || a.alias == attr.name);
                    if (existing != null && (predicate == null || predicate(existing)))
                    {
                        added = false;
                        return existing;
                    }
                }

                Entity.AddItem(attr);
            }
            else
            {
                linkEntity = Entity.FindLinkEntity(entityName);

                var meta = metadata[linkEntity.name].Attributes.SingleOrDefault(a => a.LogicalName == attr.name && a.AttributeOf == null);
                if (meta == null && (attr.name.EndsWith("name") || attr.name.EndsWith("type")))
                {
                    var logicalName = attr.name.Substring(0, attr.name.Length - 4);
                    meta = metadata[linkEntity.name].Attributes.SingleOrDefault(a => a.LogicalName == logicalName && a.AttributeOf == null);

                    if (meta != null)
                        attr.name = logicalName;
                }

                if (linkEntity.Items != null)
                {
                    var existing = linkEntity.Items.OfType<FetchAttributeType>().FirstOrDefault(a => a.name == attr.name || a.alias == attr.name);
                    if (existing != null && (predicate == null || predicate(existing)))
                    {
                        added = false;
                        return existing;
                    }
                }

                linkEntity.AddItem(attr);
            }

            added = true;
            return attr;
        }

        internal static bool IsValidAlias(string alias)
        {
            // FetchXML only supports aliases for attributes and link-entities matching a simple pattern
            return Regex.IsMatch(alias, "^[A-Za-z_][A-Za-z0-9_]*$");
        }

        internal bool IsAliasReferenced(string alias)
        {
            var entity = FetchXml.Items.OfType<FetchEntityType>().Single();
            return IsAliasReferenced(entity.Items, alias);
        }

        private bool IsAliasReferenced(object[] items, string alias)
        {
            if (items == null)
                return false;

            var hasSort = items.OfType<FetchOrderType>().Any(sort => sort.alias != null && sort.alias.Equals(alias, StringComparison.OrdinalIgnoreCase));

            if (hasSort)
                return true;

            var hasCondition = items.OfType<condition>().Any(condition => condition.alias != null && condition.alias.Equals(alias, StringComparison.OrdinalIgnoreCase));

            if (hasCondition)
                return true;

            foreach (var filter in items.OfType<filter>())
            {
                if (IsAliasReferenced(filter.Items, alias))
                    return true;
            }

            foreach (var linkEntity in items.OfType<FetchLinkEntityType>())
            {
                if (IsAliasReferenced(linkEntity.Items, alias))
                    return true;
            }

            return false;
        }

        private void AddSchemaAttributes(NodeSchema schema, IAttributeMetadataCache metadata, string entityName, string alias, object[] items)
        {
            if (items == null && !ReturnFullSchema)
                return;

            var meta = metadata[entityName];

            if (ReturnFullSchema)
            {
                foreach (var attrMetadata in meta.Attributes)
                {
                    if (attrMetadata.IsValidForRead == false)
                        continue;

                    if (attrMetadata.AttributeOf != null)
                        continue;

                    var fullName = $"{alias}.{attrMetadata.LogicalName}";
                    var attrType = attrMetadata.GetAttributeSqlType();
                    AddSchemaAttribute(schema, fullName, attrMetadata.LogicalName, attrType, attrMetadata);
                }
            }

            if (items != null)
            {
                foreach (var attribute in items.OfType<FetchAttributeType>())
                {
                    var attrMetadata = meta.Attributes.Single(a => a.LogicalName == attribute.name);
                    var attrType = attrMetadata.GetAttributeSqlType();

                    if (attribute.aggregateSpecified && (attribute.aggregate == Engine.FetchXml.AggregateType.count || attribute.aggregate == Engine.FetchXml.AggregateType.countcolumn) ||
                        attribute.dategroupingSpecified)
                        attrType = typeof(SqlInt32);

                    string fullName;
                    string attrAlias;

                    if (!String.IsNullOrEmpty(attribute.alias))
                    {
                        if (!FetchXml.aggregate || attribute.groupbySpecified && attribute.groupby == FetchBoolType.@true)
                        {
                            fullName = $"{alias}.{attribute.alias}";
                            attrAlias = attribute.alias;
                        }
                        else
                        {
                            fullName = attribute.alias;
                            attrAlias = null;
                        }
                    }
                    else
                    {
                        fullName = $"{alias}.{attribute.name}";
                        attrAlias = attribute.name;
                    }

                    AddSchemaAttribute(schema, fullName, attrAlias, attrType, attrMetadata);
                }

                if (items.OfType<allattributes>().Any())
                {
                    foreach (var attrMetadata in meta.Attributes)
                    {
                        if (attrMetadata.IsValidForRead == false)
                            continue;

                        if (attrMetadata.AttributeOf != null)
                            continue;

                        var attrType = attrMetadata.GetAttributeSqlType();
                        var attrName = attrMetadata.LogicalName;
                        var fullName = $"{alias}.{attrName}";

                        AddSchemaAttribute(schema, fullName, attrName, attrType, attrMetadata);
                    }
                }

                foreach (var linkEntity in items.OfType<FetchLinkEntityType>())
                {
                    if (linkEntity.SemiJoin)
                        continue;

                    if (schema.PrimaryKey != null)
                    {
                        var childMeta = metadata[linkEntity.name];

                        if (linkEntity.from != childMeta.PrimaryIdAttribute)
                        {
                            if (linkEntity.linktype == "inner")
                                schema.PrimaryKey = $"{linkEntity.alias}.{childMeta.PrimaryIdAttribute}";
                            else
                                schema.PrimaryKey = null;
                        }
                    }

                    AddSchemaAttributes(schema, metadata, linkEntity.name, linkEntity.alias, linkEntity.Items);
                }
            }
        }

        private void AddSchemaAttribute(NodeSchema schema, string fullName, string simpleName, Type type, AttributeMetadata attrMetadata)
        {
            // Add the logical attribute
            AddSchemaAttribute(schema, fullName, simpleName, type);

            if (attrMetadata.IsPrimaryId == true)
                _primaryKeyColumns[fullName] = attrMetadata.EntityLogicalName;

            if (FetchXml.aggregate)
                return;

            // Add standard virtual attributes
            if (attrMetadata is EnumAttributeMetadata || attrMetadata is BooleanAttributeMetadata)
                AddSchemaAttribute(schema, fullName + "name", attrMetadata.LogicalName + "name", typeof(SqlString));

            if (attrMetadata is LookupAttributeMetadata lookup)
            {
                AddSchemaAttribute(schema, fullName + "name", attrMetadata.LogicalName + "name", typeof(SqlString));

                if (lookup.Targets?.Length > 1 && lookup.AttributeType != AttributeTypeCode.PartyList)
                    AddSchemaAttribute(schema, fullName + "type", attrMetadata.LogicalName + "type", typeof(SqlString));
            }
        }

        private void AddSchemaAttribute(NodeSchema schema, string fullName, string simpleName, Type type)
        {
            schema.Schema[fullName] = type;

            if (simpleName == null)
                return;

            if (!schema.Aliases.TryGetValue(simpleName, out var simpleColumnNameAliases))
            {
                simpleColumnNameAliases = new List<string>();
                schema.Aliases[simpleName] = simpleColumnNameAliases;
            }

            if (!simpleColumnNameAliases.Contains(fullName))
                simpleColumnNameAliases.Add(fullName);
        }

        public override IDataExecutionPlanNode FoldQuery(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes)
        {
            return this;
        }

        public override void AddRequiredColumns(IDictionary<string, DataSource> dataSources, IDictionary<string, Type> parameterTypes, IList<string> requiredColumns)
        {
            if (!dataSources.TryGetValue(DataSource, out var dataSource))
                throw new NotSupportedQueryFragmentException("Missing datasource " + DataSource);

            var schema = GetSchema(dataSources, parameterTypes);

            // Add columns to FetchXml
            foreach (var col in requiredColumns)
            {
                if (!schema.ContainsColumn(col, out var normalizedCol))
                    continue;

                var parts = normalizedCol.Split('.');

                if (parts.Length != 2)
                    continue;

                var attr = parts[1] == "*" ? (object)new allattributes() : new FetchAttributeType { name = parts[1].ToLowerInvariant() };

                if (Alias.Equals(parts[0], StringComparison.OrdinalIgnoreCase))
                {
                    if (attr is allattributes)
                    {
                        Entity.Items = new object[] { attr };
                    }
                    else
                    {
                        var attrName = ((FetchAttributeType)attr).name;
                        var attrMeta = dataSource.Metadata[Entity.name].Attributes.SingleOrDefault(a => a.LogicalName == attrName && a.AttributeOf == null);

                        if (attrMeta == null && (attrName.EndsWith("name") || attrName.EndsWith("type")))
                            attrMeta = dataSource.Metadata[Entity.name].Attributes.SingleOrDefault(a => a.LogicalName == attrName.Substring(0, attrName.Length - 4));

                        if (attrMeta == null)
                            continue;

                        ((FetchAttributeType)attr).name = attrMeta.LogicalName;

                        if (Entity.Items == null || (!Entity.Items.OfType<allattributes>().Any() && !Entity.Items.OfType<FetchAttributeType>().Any(a => (a.alias ?? a.name) == ((FetchAttributeType)attr).name)))
                            Entity.AddItem(attr);
                    }
                }
                else
                {
                    var linkEntity = Entity.FindLinkEntity(parts[0]);

                    if (linkEntity != null)
                    {
                        if (attr is allattributes)
                        {
                            linkEntity.Items = new object[] { attr };
                        }
                        else
                        {
                            var attrName = ((FetchAttributeType)attr).name;
                            var attrMeta = dataSource.Metadata[linkEntity.name].Attributes.SingleOrDefault(a => a.LogicalName == attrName && a.AttributeOf == null);

                            if (attrMeta == null && (attrName.EndsWith("name") || attrName.EndsWith("type")))
                                attrMeta = dataSource.Metadata[linkEntity.name].Attributes.SingleOrDefault(a => a.LogicalName == attrName.Substring(0, attrName.Length - 4));

                            if (attrMeta == null)
                                continue;

                            ((FetchAttributeType)attr).name = attrMeta.LogicalName;

                            if (linkEntity.Items == null || (!linkEntity.Items.OfType<allattributes>().Any() && !linkEntity.Items.OfType<FetchAttributeType>().Any(a => (a.alias ?? a.name) == ((FetchAttributeType)attr).name)))
                                linkEntity.AddItem(attr);
                        }
                    }
                }
            }
        }

        public override int EstimateRowsOut(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes)
        {
            return EstimateRowsOut(Entity.name, Entity.Items, dataSources);
        }

        private int EstimateRowsOut(string name, object[] items, IDictionary<string, DataSource> dataSources)
        {
            if (!String.IsNullOrEmpty(FetchXml.top))
                return Int32.Parse(FetchXml.top, CultureInfo.InvariantCulture);

            if (!dataSources.TryGetValue(DataSource, out var dataSource))
                throw new NotSupportedQueryFragmentException("Missing datasource " + DataSource);

            // Start with the total number of records
            var rowCount = dataSource.TableSizeCache[name];

            // If there's any 1:N joins, use the larger number
            if (items == null)
                return rowCount;

            var entityMetadata = dataSource.Metadata[name];
            var joins = items.OfType<FetchLinkEntityType>();

            foreach (var join in joins)
            {
                if (join.to != entityMetadata.PrimaryIdAttribute)
                    continue;

                var childCount = EstimateRowsOut(join.name, join.Items, dataSources);

                if (join.linktype == "outer")
                    rowCount = Math.Max(rowCount, childCount);
                else
                    rowCount = Math.Min(rowCount, childCount);
            }

            // Reduce the number according to the filters
            var filters = items.OfType<filter>();
            var filterMultiple = 1.0;

            foreach (var filter in filters)
            {
                filterMultiple *= EstimateFilterRate(entityMetadata, filter, dataSource.TableSizeCache, out var singleRow);

                if (singleRow)
                    return 1;
            }

            return (int) (rowCount * filterMultiple);
        }

        private double EstimateFilterRate(EntityMetadata metadata, filter filter, ITableSizeCache tableSize, out bool singleRow)
        {
            singleRow = false;
            var multiple = 1.0;

            if (filter.Items == null)
                return multiple;

            if (filter.type == filterType.or)
                multiple = 0;

            foreach (var childFilter in filter.Items.OfType<filter>())
            {
                var childFilterMultiple = EstimateFilterRate(metadata, childFilter, tableSize, out var childSingleRow);
                
                if (filter.type == filterType.and)
                {
                    if (childSingleRow)
                    {
                        singleRow = true;
                        return 0;
                    }

                    multiple *= childFilterMultiple;
                }
                else
                {
                    multiple += childFilterMultiple;
                }
            }

            // Use simple heuristics for common conditions
            foreach (var condition in filter.Items.OfType<condition>())
            {
                if (!String.IsNullOrEmpty(condition.entityname))
                    continue;

                double conditionMultiple;

                switch (condition.@operator)
                {
                    case @operator.le:
                    case @operator.lt:
                    case @operator.ge:
                    case @operator.gt:
                        conditionMultiple = 0.5;
                        break;

                    case @operator.like:
                        conditionMultiple = 0.1;
                        break;

                    case @operator.ne:
                    case @operator.neq:
                    case @operator.eq:
                        if (condition.attribute == metadata.PrimaryIdAttribute)
                        {
                            singleRow = condition.@operator == @operator.eq;
                            conditionMultiple = 0;
                        }
                        else if (condition.attribute == "statecode")
                        {
                            conditionMultiple = condition.value == "0" ? 0.8 : 0.2;
                        }
                        else
                        {
                            var attribute = metadata.Attributes.Single(a => a.LogicalName.Equals(condition.attribute, StringComparison.OrdinalIgnoreCase));

                            if (attribute is EnumAttributeMetadata enumAttr)
                                conditionMultiple = 1.0 / enumAttr.OptionSet.Options.Count;
                            else
                                conditionMultiple = 0.01;
                        }

                        if (condition.@operator == @operator.ne || condition.@operator == @operator.neq)
                            conditionMultiple = 1 - conditionMultiple;
                        break;

                    case @operator.eqbusinessid:
                        conditionMultiple = 1.0 / tableSize["businessunit"];
                        break;

                    case @operator.nebusinessid:
                        conditionMultiple = 1 - 1.0 / tableSize["businessunit"];
                        break;

                    case @operator.equserid:
                        conditionMultiple = 1.0 / tableSize["systemuser"];
                        break;

                    case @operator.neuserid:
                        conditionMultiple = 1 - 1.0 / tableSize["systemuser"];
                        break;

                    case @operator.eqorabove:
                    case @operator.eqorunder:
                        conditionMultiple = 0.1;
                        break;

                    default:
                        conditionMultiple = 0.8;
                        break;
                }

                if (filter.type == filterType.and)
                    multiple *= conditionMultiple;
                else
                    multiple += conditionMultiple;
            }

            if (multiple > 1)
                multiple = 1;

            return multiple;
        }

        public override string ToString()
        {
            return "FetchXML Query";
        }
    }
}
