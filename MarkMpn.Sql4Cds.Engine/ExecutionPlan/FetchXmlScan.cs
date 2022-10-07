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
using Microsoft.SqlServer.TransactSql.ScriptDom;
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
            private readonly conditionValue _value;
            private readonly filter _contradiction;

            public ParameterizedCondition(filter filter, condition condition, conditionValue value)
            {
                _filter = filter;
                _condition = condition;
                _value = value;
                _contradiction = new filter
                {
                    Items = new object[]
                    {
                        new condition { attribute = condition.attribute, @operator = @operator.@null },
                        new condition { attribute = condition.attribute, @operator = @operator.notnull },
                    }
                };
            }

            public void SetValue(object value, IQueryExecutionOptions options)
            {
                if (value == null || (value is INullable nullable && nullable.IsNull))
                {
                    if (_filter.Items.Contains(_contradiction))
                        return;

                    _filter.Items = _filter.Items.Except(new[] { _condition }).Concat(new[] { _contradiction }).ToArray();
                }
                else
                {
                    if (!_filter.Items.Contains(_condition))
                        _filter.Items = _filter.Items.Except(new[] { _contradiction }).Concat(new[] { _condition }).ToArray();

                    var formatted = value.ToString();

                    if (value is SqlDate d)
                        value = (SqlDateTime)d;
                    else if (value is SqlDateTime2 dt2)
                        value = (SqlDateTime)dt2;
                    else if (value is SqlTime t)
                        value = (SqlDateTime)t;
                    else if (value is SqlDateTimeOffset dto)
                        value = (SqlDateTime)dto;

                    if (value is SqlDateTime dt)
                    {
                        DateTimeOffset dto;

                        if (options.UseLocalTimeZone)
                            dto = new DateTimeOffset(dt.Value, TimeZoneInfo.Local.GetUtcOffset(dt.Value));
                        else
                            dto = new DateTimeOffset(dt.Value, TimeSpan.Zero);

                        formatted = dto.ToString("yyyy-MM-ddTHH':'mm':'ss.FFFzzz");
                    }

                    if (_value != null)
                        _value.Value = formatted;
                    else
                        _condition.value = formatted;
                }
            }
        }

        public class InvalidPagingException : Exception
        {
            public InvalidPagingException(string message) : base(message)
            {
            }
        }

        public const int LabelMaxLength = 200;
        private static readonly XmlSerializer _serializer = new XmlSerializer(typeof(FetchXml.FetchType));

        private Dictionary<string, ParameterizedCondition> _parameterizedConditions;
        private HashSet<string> _entityNameGroupings;
        private Dictionary<string, string> _primaryKeyColumns;
        private string _lastSchemaFetchXml;
        private string _lastSchemaAlias;
        private NodeSchema _lastSchema;
        private bool _lastFullSchema;
        private bool _resetPage;
        private string _startingPage;
        private List<string> _pagingFields;
        private List<SqlEntityReference> _lastPageValues;

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
        public FetchXml.FetchType FetchXml { get; set; }

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
        /// Shows the number of pages that were retrieved in the last execution of this node
        /// </summary>
        [Category("FetchXML Scan")]
        [Description("Shows the number of pages that were retrieved in the last execution of this node")]
        [DisplayName("Pages Retrieved")]
        public int PagesRetrieved { get; set; }

        /// <summary>
        /// Indicates if all available attributes should be returned as part of the schema, used while the execution plan is being built
        /// </summary>
        [Browsable(false)]
        public bool ReturnFullSchema { get; set; }

        [Category("FetchXML Scan")]
        [Description("Indicates that custom paging will be used to ensure all results are retrieved, instead of the standard FetchXML paging")]
        [DisplayName("Using Custom Paging")]
        public bool UsingCustomPaging => _pagingFields != null;

        public bool RequiresCustomPaging(IDictionary<string, DataSource> dataSources)
        {
            if (FetchXml.distinct)
                return false;

            if (FetchXml.aggregate)
                return false;

            if (!dataSources.TryGetValue(DataSource, out var dataSource))
                throw new NotSupportedQueryFragmentException("Missing datasource " + DataSource);

            foreach (var linkEntity in Entity.GetLinkEntities())
            {
                if (linkEntity.linktype == "exists" || linkEntity.linktype == "in")
                    continue;

                if (linkEntity.from != dataSource.Metadata[linkEntity.name].PrimaryIdAttribute)
                    return true;
            }

            return false;
        }

        protected override IEnumerable<Entity> ExecuteInternal(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, DataTypeReference> parameterTypes, IDictionary<string, object> parameterValues)
        {
            PagesRetrieved = 0;

            if (!dataSources.TryGetValue(DataSource, out var dataSource))
                throw new NotSupportedQueryFragmentException("Missing datasource " + DataSource);

            ReturnFullSchema = false;
            var schema = GetSchema(dataSources, parameterTypes);

            ApplyParameterValues(options, parameterValues);

            FindEntityNameGroupings(dataSource.Metadata);

            var mainEntity = FetchXml.Items.OfType<FetchEntityType>().Single();
            var name = mainEntity.name;
            var meta = dataSource.Metadata[name];

            if (!(Parent is PartitionedAggregateNode))
                options.Progress(0, $"Retrieving {GetDisplayName(0, meta)}...");

            // Get the first page of results
            if (!options.ContinueRetrieve(0))
                yield break;

            if (_pagingFields == null)
            {
                // Ensure we reset the page number & cookie for subsequent executions
                if (_resetPage)
                {
                    FetchXml.page = _startingPage;
                    FetchXml.pagingcookie = null;
                }
                else
                {
                    _startingPage = FetchXml.page;
                    _resetPage = true;
                }
            }
            else
            {
                _lastPageValues = new List<SqlEntityReference>();
            }

            var res = dataSource.Connection.RetrieveMultiple(new FetchExpression(Serialize(FetchXml)));
            PagesRetrieved++;

            var count = res.Entities.Count;

            // Aggregate queries return up to 5000 records and don't provide a method to move on to the next page
            // Throw an exception to indicate the error to the caller
            if (AllPages && FetchXml.aggregateSpecified && FetchXml.aggregate && count == 5000 && FetchXml.top != "5000" && !res.MoreRecords)
                throw new FaultException<OrganizationServiceFault>(new OrganizationServiceFault { ErrorCode = -2147164125, Message = "AggregateQueryRecordLimitExceeded" });

            // Aggregate queries with grouping on lookup columns don't provide reliable paging as the sorting is done by the name of the related
            // record, not the guid. Non-aggregate queries can also be sorted on the primary key as a tie-breaker.
            if (res.MoreRecords && FetchXml.aggregateSpecified && FetchXml.aggregate && ContainsSortOnLookupAttribute(dataSource.Metadata, Entity.name, Entity.Items, out var lookupAttr))
                throw new InvalidPagingException($"{lookupAttr.name} is a lookup attribute - paging with a sort order on this attribute is not reliable.");

            foreach (var entity in res.Entities)
            {
                OnRetrievedEntity(entity, schema, options, dataSource.Metadata);
                yield return entity;
            }

            // Move on to subsequent pages
            while (AllPages && res.MoreRecords && options.ContinueRetrieve(count))
            {
                if (!(Parent is PartitionedAggregateNode))
                    options.Progress(0, $"Retrieved {count:N0} {GetDisplayName(count, meta)}...");

                filter pagingFilter = null;

                if (_pagingFields == null)
                {
                    if (FetchXml.page == null)
                        FetchXml.page = "2";
                    else
                        FetchXml.page = (Int32.Parse(FetchXml.page, CultureInfo.InvariantCulture) + 1).ToString();

                    FetchXml.pagingcookie = res.PagingCookie;
                }
                else
                {
                    pagingFilter = new filter { type = filterType.or };
                    AddPagingFilters(pagingFilter);
                    Entity.AddItem(pagingFilter);
                }

                var nextPage = dataSource.Connection.RetrieveMultiple(new FetchExpression(Serialize(FetchXml)));
                PagesRetrieved++;

                foreach (var entity in nextPage.Entities)
                {
                    OnRetrievedEntity(entity, schema, options, dataSource.Metadata);
                    yield return entity;
                }

                count += nextPage.Entities.Count;
                res = nextPage;

                if (pagingFilter != null)
                    Entity.Items = Entity.Items.Except(new[] { pagingFilter }).ToArray();
            }
        }

        private void AddPagingFilters(filter filter)
        {
            for (var i = 0; i < _pagingFields.Count; i++)
            {
                if (_lastPageValues[i].IsNull)
                    continue;

                var subFilter = new filter();

                for (var j = 0; j < i; j++)
                    subFilter.AddItem(new condition { entityname = j == 0 ? null : _pagingFields[j].Split('.')[0], attribute = _pagingFields[j].Split('.')[1], @operator = _lastPageValues[j].IsNull ? @operator.@null : @operator.eq, value = _lastPageValues[j].IsNull ? null : _lastPageValues[j].Id.ToString() });

                subFilter.AddItem(new condition { entityname = i == 0 ? null : _pagingFields[i].Split('.')[0], attribute = _pagingFields[i].Split('.')[1], @operator = @operator.gt, value = _lastPageValues[i].Id.ToString() });

                filter.AddItem(subFilter);
            }
        }

        /// <summary>
        /// Updates the <see cref="FetchXml"/> with current parameter values
        /// </summary>
        /// <param name="options">The options to control how the query is executed</param>
        /// <param name="parameterValues">The parameter values to apply</param>
        public void ApplyParameterValues(IQueryExecutionOptions options, IDictionary<string, object> parameterValues)
        {
            if (parameterValues == null)
                return;
            
            if (_parameterizedConditions == null)
                _parameterizedConditions = FindParameterizedConditions();

            foreach (var param in parameterValues)
            {
                if (_parameterizedConditions.TryGetValue(param.Key, out var condition))
                    condition.SetValue(param.Value, options);
            }
        }

        private bool ContainsSortOnLookupAttribute(IAttributeMetadataCache metadata, string logicalName, object[] items, out FetchAttributeType lookupAttr)
        {
            if (items == null)
            {
                lookupAttr = null;
                return false;
            }

            foreach (var order in items.OfType<FetchOrderType>())
            {
                if (!String.IsNullOrEmpty(order.alias))
                    lookupAttr = items.OfType<FetchAttributeType>().FirstOrDefault(attr => attr.alias.Equals(order.alias, StringComparison.OrdinalIgnoreCase));
                else
                    lookupAttr = items.OfType<FetchAttributeType>().FirstOrDefault(attr => attr.name.Equals(order.attribute, StringComparison.OrdinalIgnoreCase));

                if (lookupAttr == null)
                    continue;

                var meta = metadata[logicalName];
                var attrName = lookupAttr.name;
                var attrMetadata = meta.Attributes.SingleOrDefault(a => a.LogicalName.Equals(attrName, StringComparison.OrdinalIgnoreCase));

                if (attrMetadata is LookupAttributeMetadata)
                    return true;
            }

            foreach (var linkEntity in items.OfType<FetchLinkEntityType>())
            {
                if (ContainsSortOnLookupAttribute(metadata, linkEntity.name, linkEntity.Items, out lookupAttr))
                    return true;
            }

            lookupAttr = null;
            return false;
        }

        public void RemoveSorts()
        {
            // Remove any existing sorts
            if (Entity.Items != null)
            {
                Entity.Items = Entity.Items.Where(i => !(i is FetchOrderType)).ToArray();

                foreach (var linkEntity in Entity.GetLinkEntities().Where(le => le.Items != null))
                    linkEntity.Items = linkEntity.Items.Where(i => !(i is FetchOrderType)).ToArray();
            }
        }

        private void OnRetrievedEntity(Entity entity, INodeSchema schema, IQueryExecutionOptions options, IAttributeMetadataCache metadata)
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
                    int otc;
                    if (aliasedValue.Value is OptionSetValue osv)
                        otc = osv.Value;
                    else if (aliasedValue.Value is int i)
                        otc = i;
                    else
                        throw new QueryExecutionException($"Expected ObjectTypeCode value, got {aliasedValue.Value} ({aliasedValue.Value?.GetType()})");

                    var meta = metadata[otc];
                    entity[attribute.Key] = meta.LogicalName;
                }
                else
                {
                    entity[attribute.Key] = aliasedValue.Value;
                }
            }

            // Expose the type of lookup values
            foreach (var attribute in entity.Attributes.Where(attr => attr.Value is EntityReference).ToList())
            {
                if (!entity.Contains(attribute.Key + "type"))
                    entity[attribute.Key + "type"] = ((EntityReference)attribute.Value).LogicalName;
            }

            // Convert values to SQL types
            foreach (var col in schema.Schema)
            {
                object sqlValue;

                if (entity.Attributes.TryGetValue(col.Key, out var value) && value != null)
                    sqlValue = SqlTypeConverter.NetToSqlType(DataSource, value, col.Value);
                else
                    sqlValue = SqlTypeConverter.GetNullValue(col.Value.ToNetType(out _));

                if (_primaryKeyColumns.TryGetValue(col.Key, out var logicalName) && sqlValue is SqlGuid guid)
                    sqlValue = new SqlEntityReference(DataSource, logicalName, guid);

                entity[col.Key] = sqlValue;
            }

            if (_pagingFields != null)
            {
                _lastPageValues.Clear();

                foreach (var pagingField in _pagingFields)
                    _lastPageValues.Add((SqlEntityReference)entity[pagingField]);
            }
        }

        private void PrefixAliasedScalarAttributes(Entity entity, object[] items, string alias)
        {
            if (items == null)
                return;

            foreach (var attr in items.OfType<FetchAttributeType>().Where(a => !String.IsNullOrEmpty(a.alias) && a.aggregateSpecified == false && a.dategroupingSpecified == false))
            {
                var value = entity.GetAttributeValue<AliasedValue>(attr.alias);

                if (value != null)
                    entity[$"{alias}.{attr.alias}"] = value;
            }

            foreach (var linkEntity in items.OfType<FetchLinkEntityType>())
                PrefixAliasedScalarAttributes(entity, linkEntity.Items, linkEntity.alias);
        }

        private Dictionary<string, ParameterizedCondition> FindParameterizedConditions()
        {
            var parameterizedConditions = new Dictionary<string, ParameterizedCondition>(StringComparer.OrdinalIgnoreCase);
            FindParameterizedConditions(parameterizedConditions, null, null, Entity.Items);
            return parameterizedConditions;
        }

        private void FindParameterizedConditions(Dictionary<string, ParameterizedCondition>  parameterizedConditions, filter filter, condition condition, object[] items)
        {
            if (items == null)
                return;

            foreach (var cond in items.OfType<condition>().Where(c => c.IsVariable))
                parameterizedConditions[cond.value] = new ParameterizedCondition(filter, cond, null);

            foreach (var value in items.OfType<conditionValue>().Where(v => v.IsVariable))
                parameterizedConditions[value.Value] = new ParameterizedCondition(filter, condition, value);

            foreach (var cond in items.OfType<condition>().Where(c => c.Items != null))
                FindParameterizedConditions(parameterizedConditions, filter, cond, cond.Items.Cast<object>().ToArray());

            foreach (var subFilter in items.OfType<filter>())
                FindParameterizedConditions(parameterizedConditions, subFilter, null, subFilter.Items);

            foreach (var link in items.OfType<FetchLinkEntityType>())
                FindParameterizedConditions(parameterizedConditions, null, null, link.Items);
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
        internal static string Serialize(FetchXml.FetchType fetch)
        {
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

                _serializer.Serialize(xmlWriter, fetch, xsn);
                return writer.ToString();
            }
        }

        /// <summary>
        /// Converts the FetchXML string to a query object
        /// </summary>
        /// <param name="fetchXml">The FetchXML string to convert</param>
        /// <returns>The object representation of the query</returns>
        internal static FetchXml.FetchType Deserialize(string fetchXml)
        {
            var serializer = new XmlSerializer(typeof(FetchXml.FetchType));

            using (var reader = new StringReader(fetchXml))
            {
                return (FetchXml.FetchType)serializer.Deserialize(reader);
            }
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            return Array.Empty<IDataExecutionPlanNode>();
        }

        public override INodeSchema GetSchema(IDictionary<string, DataSource> dataSources, IDictionary<string, DataTypeReference> parameterTypes)
        {
            if (!dataSources.TryGetValue(DataSource, out var dataSource))
                throw new NotSupportedQueryFragmentException("Missing datasource " + DataSource);

            var fetchXmlString = FetchXmlString;
            if (_lastSchema != null && Alias == _lastSchemaAlias && fetchXmlString == _lastSchemaFetchXml && ReturnFullSchema == _lastFullSchema)
                return _lastSchema;

            _primaryKeyColumns = new Dictionary<string, string>();
            
            // Add each attribute from the main entity and recurse into link entities
            var entity = FetchXml.Items.OfType<FetchEntityType>().Single();
            var meta = dataSource.Metadata[entity.name];

            var schema = new Dictionary<string, DataTypeReference>(StringComparer.OrdinalIgnoreCase);
            var aliases = new Dictionary<string, IReadOnlyList<string>>(StringComparer.OrdinalIgnoreCase);
            var primaryKey = FetchXml.aggregate ? null : $"{Alias}.{meta.PrimaryIdAttribute}";
            var notNullColumns = new HashSet<string>();
            var sortOrder = new List<string>();

            AddSchemaAttributes(schema, aliases, ref primaryKey, notNullColumns, sortOrder, dataSource.Metadata, entity.name, Alias, entity.Items, true, false);

            _lastSchema = new NodeSchema(
                primaryKey: primaryKey,
                schema: schema,
                aliases: aliases,
                notNullColumns: notNullColumns.ToList(),
                sortOrder: sortOrder
                ); ;
            _lastSchemaFetchXml = fetchXmlString;
            _lastSchemaAlias = Alias;
            _lastFullSchema = ReturnFullSchema;

            return _lastSchema;
        }

        internal void ResetSchemaCache()
        {
            _lastSchema = null;
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
                    // TODO: What about if there is already an all-attributes element? What should we return?
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

        private void AddSchemaAttributes(Dictionary<string, DataTypeReference> schema, Dictionary<string, IReadOnlyList<string>> aliases, ref string primaryKey, HashSet<string> notNullColumns, List<string> sortOrder, IAttributeMetadataCache metadata, string entityName, string alias, object[] items, bool innerJoin, bool requireTablePrefix)
        {
            if (items == null && !ReturnFullSchema)
                return;

            var meta = metadata[entityName];

            if (ReturnFullSchema && !FetchXml.aggregate)
            {
                foreach (var attrMetadata in meta.Attributes)
                {
                    if (attrMetadata.IsValidForRead == false)
                        continue;

                    if (attrMetadata.AttributeOf != null)
                        continue;

                    var fullName = $"{alias}.{attrMetadata.LogicalName}";
                    var simpleName = requireTablePrefix ? null : attrMetadata.LogicalName;
                    var attrType = attrMetadata.GetAttributeSqlType(metadata, false);
                    AddSchemaAttribute(schema, aliases, notNullColumns, metadata, fullName, simpleName, attrType, attrMetadata, innerJoin);
                }
            }

            if (items != null)
            {
                foreach (var attribute in items.OfType<FetchAttributeType>())
                {
                    var attrMetadata = meta.Attributes.Single(a => a.LogicalName == attribute.name);
                    var attrType = attrMetadata.GetAttributeSqlType(metadata, false);

                    if (attribute.aggregateSpecified && (attribute.aggregate == Engine.FetchXml.AggregateType.count || attribute.aggregate == Engine.FetchXml.AggregateType.countcolumn) ||
                        attribute.dategroupingSpecified)
                    {
                        attrType = DataTypeHelpers.Int;
                    }
                    else if (attribute.aggregateSpecified && (attribute.aggregate == Engine.FetchXml.AggregateType.sum || attribute.aggregate == Engine.FetchXml.AggregateType.avg))
                    {
                        // Return type of SUM and AVG is based on the input type with some modifications
                        // https://docs.microsoft.com/en-us/sql/t-sql/functions/avg-transact-sql?view=sql-server-ver15#return-types
                        if (attrType is SqlDataTypeReference sqlRetType)
                        {
                            if (sqlRetType.SqlDataTypeOption == SqlDataTypeOption.TinyInt || sqlRetType.SqlDataTypeOption == SqlDataTypeOption.SmallInt)
                                attrType = DataTypeHelpers.Int;
                            else if (sqlRetType.SqlDataTypeOption == SqlDataTypeOption.Decimal || sqlRetType.SqlDataTypeOption == SqlDataTypeOption.Numeric)
                                attrType = DataTypeHelpers.Decimal(38, Math.Max(sqlRetType.GetScale(), (short)6));
                            else if (sqlRetType.SqlDataTypeOption == SqlDataTypeOption.SmallMoney)
                                attrType = DataTypeHelpers.Money;
                            else if (sqlRetType.SqlDataTypeOption == SqlDataTypeOption.Real)
                                attrType = DataTypeHelpers.Float;
                        }
                    }

                    string fullName;
                    string attrAlias;

                    if (!String.IsNullOrEmpty(attribute.alias))
                    {
                        if (!FetchXml.aggregate || attribute.groupbySpecified && attribute.groupby == FetchBoolType.@true && !attribute.dategroupingSpecified)
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

                    if (requireTablePrefix)
                        attrAlias = null;

                    AddSchemaAttribute(schema, aliases, notNullColumns, metadata, fullName, attrAlias, attrType, attrMetadata, innerJoin);
                }

                if (items.OfType<allattributes>().Any())
                {
                    foreach (var attrMetadata in meta.Attributes)
                    {
                        if (attrMetadata.IsValidForRead == false)
                            continue;

                        if (attrMetadata.AttributeOf != null)
                            continue;

                        var attrType = attrMetadata.GetAttributeSqlType(metadata, false);
                        var attrName = requireTablePrefix ? null : attrMetadata.LogicalName;
                        var fullName = $"{alias}.{attrName}";

                        AddSchemaAttribute(schema, aliases, notNullColumns, metadata, fullName, attrName, attrType, attrMetadata, innerJoin);
                    }
                }

                foreach (var sort in items.OfType<FetchOrderType>())
                {
                    string fullName;
                    string attributeName;

                    if (!String.IsNullOrEmpty(sort.alias))
                    {
                        var attribute = items.OfType<FetchAttributeType>().SingleOrDefault(a => a.alias.Equals(sort.alias, StringComparison.OrdinalIgnoreCase));

                        if (!FetchXml.aggregate || attribute != null && attribute.groupbySpecified && attribute.groupby == FetchBoolType.@true)
                            fullName = $"{alias}.{attribute.alias}";
                        else
                            fullName = attribute.alias;

                        attributeName = attribute.name;
                    }
                    else
                    {
                        fullName = $"{alias}.{sort.attribute}";
                        attributeName = sort.attribute;
                    }

                    // Sorts applied to lookup or enum fields are actually performed on the associated ___name virtual attribute
                    var attrMeta = meta.Attributes.SingleOrDefault(a => a.LogicalName.Equals(attributeName, StringComparison.OrdinalIgnoreCase));

                    if (attrMeta is LookupAttributeMetadata || attrMeta is EnumAttributeMetadata || attrMeta is BooleanAttributeMetadata)
                        fullName += "name";

                    sortOrder.Add(fullName);
                }

                foreach (var linkEntity in items.OfType<FetchLinkEntityType>())
                {
                    if (linkEntity.SemiJoin)
                        continue;

                    if (primaryKey != null)
                    {
                        var childMeta = metadata[linkEntity.name];

                        if (linkEntity.from != childMeta.PrimaryIdAttribute)
                        {
                            if (linkEntity.linktype == "inner")
                                primaryKey = $"{linkEntity.alias}.{childMeta.PrimaryIdAttribute}";
                            else
                                primaryKey = null;
                        }
                    }

                    AddSchemaAttributes(schema, aliases, ref primaryKey, notNullColumns, sortOrder, metadata, linkEntity.name, linkEntity.alias, linkEntity.Items, innerJoin && linkEntity.linktype == "inner", requireTablePrefix || linkEntity.RequireTablePrefix);
                }

                if (innerJoin)
                {
                    foreach (var filter in items.OfType<filter>())
                        AddNotNullFilters(schema, aliases, notNullColumns, alias, filter);
                }
            }
        }

        private void AddNotNullFilters(Dictionary<string, DataTypeReference> schema, Dictionary<string, IReadOnlyList<string>> aliases, HashSet<string> notNullColumns, string alias, filter filter)
        {
            if (filter.Items == null)
                return;

            if (filter.type == filterType.or && filter.Items.Length > 1)
                return;

            foreach (var cond in filter.Items.OfType<condition>())
            {
                if (cond.@operator == @operator.@null || cond.@operator == @operator.ne || cond.@operator == @operator.nebusinessid || cond.@operator == @operator.neq || cond.@operator == @operator.neuserid)
                    continue;

                var fullname = (cond.entityname ?? alias) + "." + (cond.alias ?? cond.attribute);

                if (new NodeSchema(primaryKey: null, schema: schema, aliases: aliases, notNullColumns: null, sortOrder: null).ContainsColumn(fullname, out fullname))
                    notNullColumns.Add(fullname);
            }

            foreach (var subFilter in filter.Items.OfType<filter>())
                AddNotNullFilters(schema, aliases, notNullColumns, alias, subFilter);
        }

        private void AddSchemaAttribute(Dictionary<string, DataTypeReference> schema, Dictionary<string, IReadOnlyList<string>> aliases, HashSet<string> notNullColumns, IAttributeMetadataCache metadata, string fullName, string simpleName, DataTypeReference type, AttributeMetadata attrMetadata, bool innerJoin)
        {
            var notNull = innerJoin && (attrMetadata.RequiredLevel?.Value == AttributeRequiredLevel.SystemRequired || attrMetadata.LogicalName == "createdon" || attrMetadata.LogicalName == "createdby" || attrMetadata.AttributeOf == "createdby");

            // Add the logical attribute
            AddSchemaAttribute(schema, aliases, notNullColumns, fullName, simpleName, type, notNull);

            if (attrMetadata.IsPrimaryId == true)
                _primaryKeyColumns[fullName] = attrMetadata.EntityLogicalName;

            if (FetchXml.aggregate)
                return;

            // Add standard virtual attributes
            if (attrMetadata is EnumAttributeMetadata || attrMetadata is BooleanAttributeMetadata)
                AddSchemaAttribute(schema, aliases, notNullColumns, fullName + "name", attrMetadata.LogicalName + "name", DataTypeHelpers.NVarChar(LabelMaxLength), notNull);

            if (attrMetadata is LookupAttributeMetadata lookup)
            {
                AddSchemaAttribute(schema, aliases, notNullColumns, fullName + "name", attrMetadata.LogicalName + "name", DataTypeHelpers.NVarChar(lookup.Targets == null || lookup.Targets.Length == 0 ? 100 : lookup.Targets.Select(e => ((StringAttributeMetadata)metadata[e].Attributes.SingleOrDefault(a => a.LogicalName == metadata[e].PrimaryNameAttribute))?.MaxLength ?? 100).Max()), notNull);

                if (lookup.Targets?.Length != 1 && lookup.AttributeType != AttributeTypeCode.PartyList)
                    AddSchemaAttribute(schema, aliases, notNullColumns, fullName + "type", attrMetadata.LogicalName + "type", DataTypeHelpers.NVarChar(MetadataExtensions.EntityLogicalNameMaxLength), notNull);
            }
        }

        private void AddSchemaAttribute(Dictionary<string, DataTypeReference> schema, Dictionary<string, IReadOnlyList<string>> aliases, HashSet<string> notNullColumns, string fullName, string simpleName, DataTypeReference type, bool notNull)
        {
            schema[fullName] = type;

            if (notNull)
                notNullColumns.Add(fullName);

            if (simpleName == null)
                return;

            if (!aliases.TryGetValue(simpleName, out var simpleColumnNameAliases))
            {
                simpleColumnNameAliases = new List<string>();
                aliases[simpleName] = simpleColumnNameAliases;
            }

            if (!simpleColumnNameAliases.Contains(fullName))
                ((List<string>)simpleColumnNameAliases).Add(fullName);
        }

        public override IDataExecutionPlanNodeInternal FoldQuery(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, DataTypeReference> parameterTypes, IList<Microsoft.SqlServer.TransactSql.ScriptDom.OptimizerHint> hints)
        {
            NormalizeFilters();

            if (hints != null)
            {
                ConvertQueryHints(hints);
                ApplyPageSizeHint(hints);
            }

            return this;
        }

        private void ConvertQueryHints(IList<OptimizerHint> hints)
        {
            var options = String.Join(",", hints.Select(hint =>
            {
                switch (hint.HintKind)
                {
                    case OptimizerHintKind.OptimizeFor:
                        if (!((OptimizeForOptimizerHint)hint).IsForUnknown)
                            return null;

                        return "OptimizeForUnknown";

                    case OptimizerHintKind.ForceOrder:
                        return "ForceOrder";

                    case OptimizerHintKind.Recompile:
                        return "Recompile";

                    case OptimizerHintKind.Unspecified:
                        if (!(hint is UseHintList useHint))
                            return null;

                        return String.Join(",", useHint.Hints.Select(hintLiteral =>
                        {
                            switch (hintLiteral.Value.ToUpperInvariant())
                            {
                                case "DISABLE_OPTIMIZER_ROWGOAL":
                                    return "DisableRowGoal";

                                case "ENABLE_QUERY_OPTIMIZER_HOTFIXES":
                                    return "EnableOptimizerHotfixes";

                                default:
                                    return null;
                            }
                        }));

                    case OptimizerHintKind.LoopJoin:
                        return "LoopJoin";

                    case OptimizerHintKind.MergeJoin:
                        return "MergeJoin";

                    case OptimizerHintKind.HashJoin:
                        return "HashJoin";

                    case OptimizerHintKind.NoPerformanceSpool:
                        return "NO_PERFORMANCE_SPOOL";

                    case OptimizerHintKind.MaxRecursion:
                        return $"MaxRecursion={((LiteralOptimizerHint)hint).Value.Value}";

                    default:
                        return null;
                }
            })
                .Where(hint => hint != null));

            if (!String.IsNullOrEmpty(options))
                FetchXml.options = options;
        }

        private void ApplyPageSizeHint(IList<OptimizerHint> hints)
        {
            if (!String.IsNullOrEmpty(FetchXml.count) || !String.IsNullOrEmpty(FetchXml.top))
                return;

            const string pageSizePrefix = "FETCHXML_PAGE_SIZE_";

            var pageSizeHints = hints
                .OfType<UseHintList>()
                .SelectMany(hint => hint.Hints.Where(s => s.Value.StartsWith(pageSizePrefix, StringComparison.OrdinalIgnoreCase)))
                .ToList();

            if (pageSizeHints.Count == 0)
                return;

            if (pageSizeHints.Count > 1)
                throw new NotSupportedQueryFragmentException("Duplicate page size hint", pageSizeHints[1]);

            var pageSize = Int32.Parse(pageSizeHints[0].Value.Substring(pageSizePrefix.Length));

            if (pageSize < 1 || pageSize > 5000)
                throw new NotSupportedQueryFragmentException("Invalid page size, must be between 1 and 5000", pageSizeHints[0]);

            FetchXml.count = pageSize.ToString(CultureInfo.InvariantCulture);
        }

        private void NormalizeFilters()
        {
            MoveFiltersToLinkEntities();
            RemoveEmptyFilters();
            MergeRootFilters();
            MergeSingleConditionFilters();
            MergeNestedFilters();
        }

        private void MoveFiltersToLinkEntities()
        {
            // If we've got AND-ed conditions that have an entityname that refers to an inner-joined link entity, move
            // the condition to that link entity
            var innerLinkEntities = Entity.GetLinkEntities(innerOnly: true).ToDictionary(le => le.alias, StringComparer.OrdinalIgnoreCase);

            Entity.Items = MoveFiltersToLinkEntities(innerLinkEntities, Entity.Items);
        }

        private object[] MoveFiltersToLinkEntities(Dictionary<string, FetchLinkEntityType> innerLinkEntities, object[] items)
        {
            if (items == null)
                return items;

            var toRemove = items
                .OfType<condition>()
                .Where(c => !String.IsNullOrEmpty(c.entityname))
                .Where(c => innerLinkEntities.ContainsKey(c.entityname))
                .ToList();

            foreach (var condition in toRemove)
            {
                filter filter = null;

                if (innerLinkEntities[condition.entityname].Items != null)
                    filter = innerLinkEntities[condition.entityname].Items.OfType<filter>().Where(f => f.type == filterType.and).FirstOrDefault();

                if (filter == null)
                {
                    filter = new filter { Items = new object[] { condition } };
                    innerLinkEntities[condition.entityname].AddItem(filter);
                }
                else
                {
                    filter.AddItem(condition);
                }

                condition.entityname = null;
            }

            foreach (var subFilter in items.OfType<filter>().Where(f => f.type == filterType.and))
                subFilter.Items = MoveFiltersToLinkEntities(innerLinkEntities, subFilter.Items);

            return items.Except(toRemove).ToArray();
        }

        private void RemoveEmptyFilters()
        {
            Entity.Items = RemoveEmptyFilters(Entity.Items);

            foreach (var linkEntity in Entity.GetLinkEntities())
                linkEntity.Items = RemoveEmptyFilters(linkEntity.Items);
        }

        private object[] RemoveEmptyFilters(object[] items)
        {
            if (items == null)
                return items;

            foreach (var filter in items.OfType<filter>())
                filter.Items = RemoveEmptyFilters(filter.Items);

            var emptyFilters = items
                .OfType<filter>()
                .Where(f => f.Items == null || f.Items.Length == 0)
                .ToList();

            return items.Except(emptyFilters).ToArray();
        }

        private void MergeRootFilters()
        {
            Entity.Items = MergeRootFilters(Entity.Items);

            foreach (var linkEntity in Entity.GetLinkEntities())
                linkEntity.Items = MergeRootFilters(linkEntity.Items);
        }

        private object[] MergeRootFilters(object[] items)
        {
            if (items == null)
                return items;

            var rootFilters = items.OfType<filter>().Cast<object>().ToArray();

            if (rootFilters.Length < 2)
                return items;

            return items
                .Except(rootFilters)
                .Concat(new object[] { new filter { Items = rootFilters } })
                .ToArray();
        }

        private void MergeSingleConditionFilters()
        {
            MergeSingleConditionFilters(Entity.Items);

            foreach (var linkEntity in Entity.GetLinkEntities())
                MergeSingleConditionFilters(linkEntity.Items);
        }

        private void MergeSingleConditionFilters(object[] items)
        {
            if (items == null)
                return;

            var filter = items.OfType<filter>().SingleOrDefault();

            if (filter == null)
                return;

            MergeSingleConditionFilters(filter);
        }

        private void MergeSingleConditionFilters(filter filter)
        {
            var singleConditionFilters = filter.Items
                .OfType<filter>()
                .Where(f => f.Items != null && f.Items.Length == 1 && f.Items.OfType<condition>().Count() == 1)
                .ToDictionary(f => f, f => (condition)f.Items[0]);

            for (var i = 0; i < filter.Items.Length; i++)
            {
                if (filter.Items[i] is filter subFilter && singleConditionFilters.TryGetValue(subFilter, out var condition))
                    filter.Items[i] = condition;
            }

            foreach (var subFilter in filter.Items.OfType<filter>())
                MergeSingleConditionFilters(subFilter);
        }

        private void MergeNestedFilters()
        {
            MergeNestedFilters(Entity.Items);

            foreach (var linkEntity in Entity.GetLinkEntities())
                MergeNestedFilters(linkEntity.Items);
        }

        private void MergeNestedFilters(object[] items)
        {
            if (items == null)
                return;

            var filter = items.OfType<filter>().SingleOrDefault();

            if (filter == null)
                return;

            MergeNestedFilters(filter);
        }

        private void MergeNestedFilters(filter filter)
        {
            var items = new List<object>();

            foreach (var item in filter.Items)
            {
                if (item is filter f)
                {
                    MergeNestedFilters(f);

                    if (f.type == filter.type)
                        items.AddRange(f.Items);
                    else
                        items.Add(item);
                }
                else
                {
                    items.Add(item);
                }
            }

            filter.Items = items.ToArray();
        }

        public override void AddRequiredColumns(IDictionary<string, DataSource> dataSources, IDictionary<string, DataTypeReference> parameterTypes, IList<string> requiredColumns)
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

            // If there is no attribute requested the server will return everything instead of nothing, so
            // add the primary key in to limit it
            if ((!FetchXml.aggregate || !FetchXml.aggregateSpecified) && !HasAttribute(Entity.Items) && !Entity.GetLinkEntities().Any(link => HasAttribute(link.Items)))
            {
                var metadata = dataSource.Metadata[Entity.name];
                Entity.AddItem(new FetchAttributeType { name = metadata.PrimaryIdAttribute });
            }

            if (RequiresCustomPaging(dataSources))
            {
                RemoveSorts();

                _pagingFields = new List<string>();
                _lastPageValues = new List<SqlEntityReference>();

                // Ensure the primary key of each entity is included
                AddPrimaryIdAttribute(Entity, dataSource);

                foreach (var linkEntity in Entity.GetLinkEntities())
                    AddPrimaryIdAttribute(linkEntity, dataSource);
            }

            NormalizeAttributes(dataSources);
            SetDefaultPageSize(dataSources, parameterTypes);
        }

        private void AddPrimaryIdAttribute(FetchEntityType entity, DataSource dataSource)
        {
            entity.Items = AddPrimaryIdAttribute(entity.Items, Alias, dataSource.Metadata[entity.name]);
        }

        private void AddPrimaryIdAttribute(FetchLinkEntityType linkEntity, DataSource dataSource)
        {
            linkEntity.Items = AddPrimaryIdAttribute(linkEntity.Items, linkEntity.alias, dataSource.Metadata[linkEntity.name]);
        }

        private object[] AddPrimaryIdAttribute(object[] items, string alias, EntityMetadata metadata)
        {
            _pagingFields.Add(alias + "." + metadata.PrimaryIdAttribute);

            if (items == null || items.Length == 0)
            {
                return new object[]
                {
                    new FetchAttributeType { name = metadata.PrimaryIdAttribute },
                    new FetchOrderType { attribute = metadata.PrimaryIdAttribute }
                };
            }

            if (!items.OfType<allattributes>().Any() && !items.OfType<FetchAttributeType>().Any(a => a.name == metadata.PrimaryIdAttribute))
                items = items.Concat(new object[] { new FetchAttributeType { name = metadata.PrimaryIdAttribute } }).ToArray();

            if (!items.OfType<FetchOrderType>().Any(a => a.attribute == metadata.PrimaryIdAttribute))
                items = items.Concat(new object[] { new FetchOrderType { attribute = metadata.PrimaryIdAttribute } }).ToArray();

            return items;
        }

        private bool HasAttribute(object[] items)
        {
            if (items == null)
                return false;

            return items.OfType<FetchAttributeType>().Any() || items.OfType<allattributes>().Any();
        }

        private void NormalizeAttributes(IDictionary<string, DataSource> dataSources)
        {
            Entity.Items = NormalizeAttributes(dataSources, Entity.name, Entity.Items);
        }

        private object[] NormalizeAttributes(IDictionary<string, DataSource> dataSources, string entityName, object[] items)
        {
            if (items == null)
                return null;

            foreach (var linkEntity in items.OfType<FetchLinkEntityType>())
                linkEntity.Items = NormalizeAttributes(dataSources, linkEntity.name, linkEntity.Items);

            if (items.OfType<allattributes>().Any())
                return items;

            var attributes = items.OfType<FetchAttributeType>().ToList();

            if (attributes.Any(a => !String.IsNullOrEmpty(a.alias)))
                return items;

            var metadata = dataSources[DataSource].Metadata[entityName];

            var allAttributes = metadata.Attributes
                .Where(a => a.IsValidForRead != false && a.AttributeOf == null)
                .Select(a => a.LogicalName);

            var missingAttributes = allAttributes.Except(attributes.Select(a => a.name)).Any();

            if (missingAttributes)
                return items;

            return items
                .Where(obj => !(obj is FetchAttributeType))
                .Concat(new[] { new allattributes() })
                .ToArray();
        }

        private void SetDefaultPageSize(IDictionary<string, DataSource> dataSources, IDictionary<string, DataTypeReference> parameterTypes)
        {
            if (!String.IsNullOrEmpty(FetchXml.count) || !String.IsNullOrEmpty(FetchXml.top))
                return;

            // Reduce the page size from the default 5000 if there will be lots of columns returned. This helps keep memory
            // usage down as well as reducing the time to retrieve each page and so makes it easier to cancel a bad query.
            var fullSchema = ReturnFullSchema;
            ReturnFullSchema = false;

            var schema = GetSchema(dataSources, parameterTypes);

            if (schema.Schema.Count > 100)
                FetchXml.count = "1000";

            if (schema.Schema.Count > 500)
                FetchXml.count = "500";

            ReturnFullSchema = fullSchema;
        }

        protected override RowCountEstimate EstimateRowsOutInternal(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, DataTypeReference> parameterTypes)
        {
            if (FetchXml.aggregateSpecified && FetchXml.aggregate)
            {
                var hasGroups = HasGroups(Entity.Items);

                if (!hasGroups)
                    return RowCountEstimateDefiniteRange.ExactlyOne;

                return EstimateRowsOut(Entity.name, Entity.Items, dataSources, 0.4);
            }

            return EstimateRowsOut(Entity.name, Entity.Items, dataSources, 1.0);
        }

        private bool HasGroups(object[] items)
        {
            if (items == null)
                return false;

            if (items.OfType<FetchAttributeType>().Any(a => a.groupbySpecified && a.groupby == FetchBoolType.@true))
                return true;

            return items.OfType<FetchLinkEntityType>().Any(link => HasGroups(link.Items));
        }

        private RowCountEstimate EstimateRowsOut(string name, object[] items, IDictionary<string, DataSource> dataSources, double multiplier)
        {
            if (!String.IsNullOrEmpty(FetchXml.top))
                return new RowCountEstimateDefiniteRange(0, Int32.Parse(FetchXml.top, CultureInfo.InvariantCulture));

            if (!dataSources.TryGetValue(DataSource, out var dataSource))
                throw new NotSupportedQueryFragmentException("Missing datasource " + DataSource);

            // Start with the total number of records
            var rowCount = (int)(dataSource.TableSizeCache[name] * multiplier);

            if (items == null)
                return new RowCountEstimate(rowCount);

            // If there's any 1:N joins, use the larger number

            var entityMetadata = dataSource.Metadata[name];
            var joins = items.OfType<FetchLinkEntityType>();

            foreach (var join in joins)
            {
                if (join.to != entityMetadata.PrimaryIdAttribute)
                    continue;

                var childCount = EstimateRowsOut(join.name, join.Items, dataSources, 1.0).Value;

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
                    return RowCountEstimateDefiniteRange.ZeroOrOne;
            }

            var estimate = (int) (rowCount * filterMultiple);

            // An estimate of 1 or 0 can cause big differences to the query plan that might be very inefficient if the estimate
            // is wrong. If we're not really sure that the won't just be a single row, make sure we return at least 2.
            if (estimate <= 1 && rowCount > 1)
                estimate = 2;

            return new RowCountEstimate(estimate);
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

                            if (attribute is EntityNameAttributeMetadata entityNameAttr)
                                conditionMultiple = 0.01;
                            else if (attribute is EnumAttributeMetadata enumAttr)
                                conditionMultiple = 1.0 / enumAttr.OptionSet.Options.Count;
                            else if (attribute is BooleanAttributeMetadata)
                                conditionMultiple = 0.5;
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

        protected override IEnumerable<string> GetVariablesInternal()
        {
            return FindParameterizedConditions().Keys;
        }

        public override string ToString()
        {
            return "FetchXML Query";
        }

        public override object Clone()
        {
            var clone = new FetchXmlScan
            {
                Alias = Alias,
                AllPages = AllPages,
                DataSource = DataSource,
                FetchXml = Deserialize(FetchXmlString),
                ReturnFullSchema = ReturnFullSchema,
                _entityNameGroupings = _entityNameGroupings,
                _lastSchema = _lastSchema,
                _lastSchemaAlias = _lastSchemaAlias,
                _lastSchemaFetchXml = _lastSchemaFetchXml,
                _primaryKeyColumns = _primaryKeyColumns,
                _pagingFields = _pagingFields,
            };

            // Custom properties are not serialized, so need to copy them manually
            foreach (var link in Entity.GetLinkEntities())
            {
                var cloneLink = clone.Entity.GetLinkEntities().Single(l => l.alias == link.alias);
                cloneLink.SemiJoin = link.SemiJoin;
                cloneLink.RequireTablePrefix = link.RequireTablePrefix;
            }

            return clone;
        }
    }
}
