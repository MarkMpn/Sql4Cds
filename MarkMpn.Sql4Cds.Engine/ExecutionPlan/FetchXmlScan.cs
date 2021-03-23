using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlTypes;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Xml.Serialization;
using MarkMpn.Sql4Cds.Engine.FetchXml;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Metadata;
using Microsoft.Xrm.Sdk.Query;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    public class FetchXmlScan : BaseDataNode
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


        public FetchXmlScan()
        {
            AllPages = true;
        }

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
        public string Alias { get; set; }

        /// <summary>
        /// The string representation of the <see cref="FetchXml"/>
        /// </summary>
        public string FetchXmlString => Serialize(FetchXml);

        /// <summary>
        /// Indicates if the query will page across all the available data
        /// </summary>
        public bool AllPages { get; set; }

        /// <summary>
        /// Indicates if all available attributes should be returned as part of the schema, used while the execution plan is being built
        /// </summary>
        [Browsable(false)]
        public bool ReturnFullSchema { get; set; }

        protected override IEnumerable<Entity> ExecuteInternal(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues)
        {
            ReturnFullSchema = false;
            var schema = GetSchema(metadata, parameterTypes);

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

            var mainEntity = FetchXml.Items.OfType<FetchEntityType>().Single();
            var name = mainEntity.name;
            var meta = metadata[name];
            options.Progress(0, $"Retrieving {GetDisplayName(0, meta)}...");

            // Get the first page of results
            options.RetrievingNextPage();
            var res = org.RetrieveMultiple(new FetchExpression(Serialize(FetchXml)));

            foreach (var entity in res.Entities)
            {
                OnRetrievedEntity(entity, schema);
                yield return entity;
            }

            var count = res.Entities.Count;

            // Aggregate queries return up to 5000 records and don't provide a method to move on to the next page
            // Throw an exception to indicate the error to the caller
            if (AllPages && FetchXml.aggregateSpecified && FetchXml.aggregate && count == 5000 && FetchXml.top != "5000" && !res.MoreRecords)
                throw new ApplicationException("AggregateQueryRecordLimit");

            // Move on to subsequent pages
            while (AllPages && res.MoreRecords && options.ContinueRetrieve(count))
            {
                options.Progress(0, $"Retrieved {count:N0} {GetDisplayName(count, meta)}...");

                if (FetchXml.page == null)
                    FetchXml.page = "2";
                else
                    FetchXml.page = (Int32.Parse(FetchXml.page) + 1).ToString();

                FetchXml.pagingcookie = res.PagingCookie;

                options.RetrievingNextPage();
                var nextPage = org.RetrieveMultiple(new FetchExpression(Serialize(FetchXml)));

                foreach (var entity in nextPage.Entities)
                {
                    OnRetrievedEntity(entity, schema);
                    yield return entity;
                }

                count += nextPage.Entities.Count;
                res = nextPage;
            }
        }

        private void OnRetrievedEntity(Entity entity, NodeSchema schema)
        {
            // Expose any formatted values for OptionSetValue and EntityReference values
            foreach (var formatted in entity.FormattedValues)
            {
                if (!entity.Contains(formatted.Key + "name"))
                    entity[formatted.Key + "name"] = formatted.Value;
            }

            // Prefix all attributes of the main entity with the expected alias
            foreach (var attribute in entity.Attributes.Where(attr => !attr.Key.Contains('.') && !(attr.Value is AliasedValue)).ToList())
                entity[$"{Alias}.{attribute.Key}"] = attribute.Value;

            // Convert aliased values to the underlying value
            foreach (var attribute in entity.Attributes.Where(attr => attr.Value is AliasedValue).ToList())
                entity[attribute.Key] = ((AliasedValue)attribute.Value).Value;

            // Expose the type of lookup values
            foreach (var attribute in entity.Attributes.Where(attr => attr.Value is EntityReference).ToList())
            {
                if (!entity.Contains(attribute.Key + "type"))
                    entity[attribute.Key + "type"] = ((EntityReference)attribute.Value).LogicalName;

                entity[attribute.Key] = ((EntityReference)attribute.Value).Id;
            }

            // Extract Money and OptionSetValue values
            foreach (var attribute in entity.Attributes.Where(attr => attr.Value is Money).ToList())
                entity[attribute.Key] = ((Money)attribute.Value).Value;
            foreach (var attribute in entity.Attributes.Where(attr => attr.Value is OptionSetValue).ToList())
                entity[attribute.Key] = ((OptionSetValue)attribute.Value).Value;

            // Convert Guid to SqlGuid for consistent sorting
            foreach (var attribute in entity.Attributes.Where(attr => attr.Value is Guid).ToList())
                entity[attribute.Key] = new SqlGuid((Guid)attribute.Value);

            // Populate any missing attributes
            foreach (var col in schema.Schema.Keys)
            {
                if (!entity.Contains(col))
                    entity[col] = null;
            }
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

        protected string GetDisplayName(int count, EntityMetadata meta)
        {
            if (count == 1)
                return meta.DisplayName?.UserLocalizedLabel?.Label ?? meta.LogicalName;

            return meta.DisplayCollectionName?.UserLocalizedLabel?.Label ??
                meta.LogicalCollectionName ??
                meta.LogicalName;
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            return Array.Empty<IDataExecutionPlanNode>();
        }

        public override NodeSchema GetSchema(IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes)
        {
            var schema = new NodeSchema();

            // Add each attribute from the main entity and recurse into link entities
            var entity = FetchXml.Items.OfType<FetchEntityType>().Single();
            var meta = metadata[entity.name];
            schema.PrimaryKey = $"{Alias}.{meta.PrimaryIdAttribute}";
            AddAttributes(schema, metadata, entity.name, Alias, entity.Items);
            
            return schema;
        }

        internal FetchAttributeType AddAttribute(string colName, Func<FetchAttributeType, bool> predicate, IAttributeMetadataCache metadata, out bool added)
        {
            var parts = colName.Split('.');

            if (parts.Length == 1)
            {
                added = false;
                return FindAliasedAttribute(Entity.Items, colName, predicate);
            }

            var entityName = parts[0];
            var attr = new FetchAttributeType { name = parts[1].ToLowerInvariant() };

            if (Alias == entityName)
            {
                var meta = metadata[Entity.name].Attributes.SingleOrDefault(a => a.LogicalName == attr.name);
                if (meta?.AttributeOf != null)
                    attr.name = meta.AttributeOf;

                if (Entity.Items != null)
                {
                    var existing = Entity.Items.OfType<FetchAttributeType>().FirstOrDefault(a => a.name == attr.name);
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
                var linkEntity = Entity.FindLinkEntity(entityName);

                var meta = metadata[linkEntity.name].Attributes.SingleOrDefault(a => a.LogicalName == attr.name);
                if (meta?.AttributeOf != null)
                    attr.name = meta.AttributeOf;

                if (linkEntity.Items != null)
                {
                    var existing = linkEntity.Items.OfType<FetchAttributeType>().FirstOrDefault(a => a.name == attr.name);
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

            var hasSort = items.OfType<FetchOrderType>().Any(sort => sort.alias.Equals(alias, StringComparison.OrdinalIgnoreCase));

            if (hasSort)
                return true;

            var hasCondition = items.OfType<condition>().Any(condition => condition.alias.Equals(alias, StringComparison.OrdinalIgnoreCase));

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

        private void AddAttributes(NodeSchema schema, IAttributeMetadataCache metadata, string entityName, string alias, object[] items)
        {
            if (items == null && !ReturnFullSchema)
                return;

            var meta = metadata[entityName];

            if (ReturnFullSchema)
            {
                foreach (var attrMetadata in meta.Attributes)
                {
                    var attrType = attrMetadata.GetAttributeType();
                    var fullName = $"{alias}.{attrMetadata.LogicalName}";

                    schema.Schema[fullName] = attrType;

                    if (!schema.Aliases.TryGetValue(attrMetadata.LogicalName, out var simpleColumnNameAliases))
                    {
                        simpleColumnNameAliases = new List<string>();
                        schema.Aliases[attrMetadata.LogicalName] = simpleColumnNameAliases;
                    }

                    simpleColumnNameAliases.Add(fullName);
                }
            }

            if (items != null)
            {
                foreach (var attribute in items.OfType<FetchAttributeType>())
                {
                    var attrMetadata = meta.Attributes.Single(a => a.LogicalName == attribute.name);
                    var attrType = attrMetadata.GetAttributeType();

                    if (attribute.aggregateSpecified && (attribute.aggregate == Engine.FetchXml.AggregateType.count || attribute.aggregate == Engine.FetchXml.AggregateType.countcolumn))
                        attrType = typeof(int);

                    var attrName = attribute.alias ?? attribute.name;
                    var fullName = attribute.alias != null ? attribute.alias : $"{alias}.{attrName}";

                    schema.Schema[fullName] = attrType;

                    if (!schema.Aliases.TryGetValue(attrName, out var simpleColumnNameAliases))
                    {
                        simpleColumnNameAliases = new List<string>();
                        schema.Aliases[attrName] = simpleColumnNameAliases;
                    }

                    if (!simpleColumnNameAliases.Contains(fullName))
                        simpleColumnNameAliases.Add(fullName);

                    foreach (var virtualAttrMetadata in meta.Attributes.Where(a => a.AttributeOf == attrMetadata.LogicalName))
                    {
                        var virtualAttrType = virtualAttrMetadata.GetAttributeType();
                        var virtualAttrName = attrName + virtualAttrMetadata.LogicalName.Substring(attrMetadata.LogicalName.Length);
                        var virtualAttrFullName = attribute.alias != null ? virtualAttrName : $"{alias}.{virtualAttrName}";

                        schema.Schema[virtualAttrFullName] = virtualAttrType;

                        if (!schema.Aliases.TryGetValue(virtualAttrName, out var simpleVirtualColumnNameAliases))
                        {
                            simpleVirtualColumnNameAliases = new List<string>();
                            schema.Aliases[virtualAttrName] = simpleVirtualColumnNameAliases;
                        }

                        if (!simpleVirtualColumnNameAliases.Contains(virtualAttrFullName))
                            simpleVirtualColumnNameAliases.Add(virtualAttrFullName);
                    }
                }

                if (items.OfType<allattributes>().Any())
                {
                    foreach (var attrMetadata in meta.Attributes)
                    {
                        var attrType = attrMetadata.GetAttributeType();
                        var attrName = attrMetadata.LogicalName;
                        var fullName = $"{alias}.{attrName}";

                        schema.Schema[fullName] = attrType;

                        if (!schema.Aliases.TryGetValue(attrName, out var simpleColumnNameAliases))
                        {
                            simpleColumnNameAliases = new List<string>();
                            schema.Aliases[attrName] = simpleColumnNameAliases;
                        }

                        if (!simpleColumnNameAliases.Contains(fullName))
                            simpleColumnNameAliases.Add(fullName);
                    }
                }

                foreach (var linkEntity in items.OfType<FetchLinkEntityType>())
                {
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

                    AddAttributes(schema, metadata, linkEntity.name, linkEntity.alias, linkEntity.Items);
                }
            }
        }

        public override IDataExecutionPlanNode FoldQuery(IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes)
        {
            return this;
        }

        public override void AddRequiredColumns(IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes, IList<string> requiredColumns)
        {
            var schema = GetSchema(metadata, parameterTypes);

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
                        var attrMeta = metadata[Entity.name].Attributes.SingleOrDefault(a => a.LogicalName == ((FetchAttributeType)attr).name);
                        if (attrMeta?.AttributeOf != null)
                            ((FetchAttributeType)attr).name = attrMeta.AttributeOf;

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
                            var attrMeta = metadata[linkEntity.name].Attributes.SingleOrDefault(a => a.LogicalName == ((FetchAttributeType)attr).name);
                            if (attrMeta?.AttributeOf != null)
                                ((FetchAttributeType)attr).name = attrMeta.AttributeOf;

                            if (linkEntity.Items == null || (!linkEntity.Items.OfType<allattributes>().Any() && !linkEntity.Items.OfType<FetchAttributeType>().Any(a => (a.alias ?? a.name) == ((FetchAttributeType)attr).name)))
                                linkEntity.AddItem(attr);
                        }
                    }
                }
            }
        }

        public override int EstimateRowsOut(IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes, ITableSizeCache tableSize)
        {
            return EstimateRowsOut(Entity.name, Entity.Items, metadata, tableSize);
        }

        private int EstimateRowsOut(string name, object[] items, IAttributeMetadataCache metadata, ITableSizeCache tableSize)
        {
            if (!String.IsNullOrEmpty(FetchXml.top))
                return Int32.Parse(FetchXml.top);

            // Start with the total number of records
            var rowCount = tableSize[name];

            // If there's any 1:N joins, use the larger number
            if (items == null)
                return rowCount;

            var entityMetadata = metadata[name];
            var joins = items.OfType<FetchLinkEntityType>();

            foreach (var join in joins)
            {
                if (join.to != entityMetadata.PrimaryIdAttribute)
                    continue;

                var childCount = EstimateRowsOut(join.name, join.Items, metadata, tableSize);

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
                filterMultiple *= EstimateFilterRate(entityMetadata, filter, tableSize, out var singleRow);

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
