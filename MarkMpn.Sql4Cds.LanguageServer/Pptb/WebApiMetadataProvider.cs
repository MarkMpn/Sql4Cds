using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Newtonsoft.Json.Linq;

namespace MarkMpn.Sql4Cds.LanguageServer.Pptb
{
    /// <summary>
    /// Details of an attribute required to translate between SDK and Web API formats
    /// </summary>
    public class WebApiAttributeInfo
    {
        public string LogicalName { get; set; }

        /// <summary>
        /// The <see cref="Microsoft.Xrm.Sdk.Metadata.AttributeTypeCode"/> name, e.g. <c>Lookup</c>
        /// </summary>
        public string AttributeType { get; set; }

        /// <summary>
        /// The <see cref="Microsoft.Xrm.Sdk.Metadata.AttributeTypeDisplayName"/> value, e.g. <c>MultiSelectPicklistType</c>
        /// </summary>
        public string AttributeTypeName { get; set; }

        public string AttributeOf { get; set; }

        public bool IsDateOnly { get; set; }

        public bool IsLookup => AttributeType == "Lookup" || AttributeType == "Customer" || AttributeType == "Owner";
    }

    /// <summary>
    /// Details of an entity required to translate between SDK and Web API formats
    /// </summary>
    public class WebApiEntityInfo
    {
        public string LogicalName { get; set; }

        public string EntitySetName { get; set; }

        public string PrimaryIdAttribute { get; set; }

        public string PrimaryNameAttribute { get; set; }

        public Dictionary<string, WebApiAttributeInfo> Attributes { get; } = new Dictionary<string, WebApiAttributeInfo>(StringComparer.OrdinalIgnoreCase);

        /// <summary>
        /// The single-valued navigation properties for each lookup attribute, keyed by the target entity type
        /// </summary>
        public Dictionary<string, Dictionary<string, string>> LookupNavigationProperties { get; } = new Dictionary<string, Dictionary<string, string>>(StringComparer.OrdinalIgnoreCase);

        public WebApiAttributeInfo GetAttribute(string logicalName)
        {
            if (logicalName != null && Attributes.TryGetValue(logicalName, out var attr))
                return attr;

            return null;
        }

        /// <summary>
        /// Gets the name of the single-valued navigation property to use to set a lookup attribute to a specific type of record
        /// </summary>
        public string GetNavigationProperty(string attribute, string targetEntity)
        {
            if (!LookupNavigationProperties.TryGetValue(attribute, out var navProps) || navProps.Count == 0)
                return null;

            if (targetEntity != null && navProps.TryGetValue(targetEntity, out var navProp))
                return navProp;

            // Some lookups (e.g. ownerid) have a single navigation property covering multiple target types
            if (navProps.Values.Distinct().Count() == 1)
                return navProps.Values.First();

            return null;
        }

        /// <summary>
        /// Gets all the navigation properties associated with a lookup attribute
        /// </summary>
        public IEnumerable<string> GetNavigationProperties(string attribute)
        {
            if (!LookupNavigationProperties.TryGetValue(attribute, out var navProps))
                return Array.Empty<string>();

            return navProps.Values.Distinct();
        }
    }

    /// <summary>
    /// Details of a relationship required to associate &amp; disassociate records
    /// </summary>
    public class WebApiRelationshipInfo
    {
        public string SchemaName { get; set; }

        public bool IsManyToMany { get; set; }

        public string Entity1LogicalName { get; set; }

        public string Entity2LogicalName { get; set; }

        public string Entity1NavigationPropertyName { get; set; }

        public string Entity2NavigationPropertyName { get; set; }

        public string ReferencedEntity { get; set; }

        public string ReferencingEntity { get; set; }

        public string ReferencedEntityNavigationPropertyName { get; set; }

        public string ReferencingEntityNavigationPropertyName { get; set; }
    }

    /// <summary>
    /// Loads and caches the minimal metadata required to translate SDK requests to Web API requests
    /// </summary>
    public class WebApiMetadataProvider
    {
        private readonly Func<IReadOnlyList<WebApiRequest>, IReadOnlyList<WebApiResponse>> _send;
        private readonly ConcurrentDictionary<string, Lazy<WebApiEntityInfo>> _entities = new ConcurrentDictionary<string, Lazy<WebApiEntityInfo>>(StringComparer.OrdinalIgnoreCase);
        private readonly ConcurrentDictionary<int, Lazy<WebApiEntityInfo>> _entitiesByCode = new ConcurrentDictionary<int, Lazy<WebApiEntityInfo>>();
        private readonly ConcurrentDictionary<string, Lazy<WebApiRelationshipInfo>> _relationships = new ConcurrentDictionary<string, Lazy<WebApiRelationshipInfo>>(StringComparer.OrdinalIgnoreCase);

        public WebApiMetadataProvider(Func<IReadOnlyList<WebApiRequest>, IReadOnlyList<WebApiResponse>> send)
        {
            _send = send;
        }

        public WebApiEntityInfo GetEntity(int objectTypeCode)
        {
            var lazy = _entitiesByCode.GetOrAdd(objectTypeCode, code => new Lazy<WebApiEntityInfo>(() =>
            {
                var responses = _send(new[]
                {
                    new WebApiRequest { Method = "GET", Url = $"EntityDefinitions?$select=LogicalName&$filter=ObjectTypeCode eq {code}" }
                });
                var response = responses.Single();
                response.EnsureSuccess();
                var entities = response.Json["value"] as JArray;
                if (entities == null || entities.Count != 1 || String.IsNullOrEmpty(entities[0].Value<string>("LogicalName")))
                    throw new InvalidOperationException($"Cannot resolve entity object type code {code}");
                return GetEntity(entities[0].Value<string>("LogicalName"));
            }, LazyThreadSafetyMode.ExecutionAndPublication));
            try
            {
                return lazy.Value;
            }
            catch
            {
                _entitiesByCode.TryRemove(objectTypeCode, out _);
                throw;
            }
        }

        public WebApiEntityInfo GetEntity(string logicalName)
        {
            if (String.IsNullOrEmpty(logicalName))
                throw new ArgumentNullException(nameof(logicalName));

            var lazy = _entities.GetOrAdd(logicalName, name => new Lazy<WebApiEntityInfo>(() => LoadEntity(name.ToLowerInvariant()), LazyThreadSafetyMode.ExecutionAndPublication));

            try
            {
                return lazy.Value;
            }
            catch
            {
                _entities.TryRemove(logicalName, out _);
                throw;
            }
        }

        public WebApiRelationshipInfo GetRelationship(string schemaName)
        {
            if (String.IsNullOrEmpty(schemaName))
                throw new ArgumentNullException(nameof(schemaName));

            var lazy = _relationships.GetOrAdd(schemaName, name => new Lazy<WebApiRelationshipInfo>(() => LoadRelationship(name), LazyThreadSafetyMode.ExecutionAndPublication));

            try
            {
                return lazy.Value;
            }
            catch
            {
                _relationships.TryRemove(schemaName, out _);
                throw;
            }
        }

        private WebApiEntityInfo LoadEntity(string logicalName)
        {
            var key = $"EntityDefinitions(LogicalName={WebApiJson.FormatLiteral(logicalName)})";

            var requests = new[]
            {
                new WebApiRequest
                {
                    Method = "GET",
                    Url = key + "?$select=LogicalName,EntitySetName,PrimaryIdAttribute,PrimaryNameAttribute" +
                        "&$expand=Attributes($select=LogicalName,AttributeType,AttributeTypeName,AttributeOf)," +
                        "ManyToOneRelationships($select=ReferencingAttribute,ReferencedEntity,ReferencingEntityNavigationPropertyName)"
                },
                new WebApiRequest
                {
                    Method = "GET",
                    Url = key + "/Attributes/Microsoft.Dynamics.CRM.DateTimeAttributeMetadata?$select=LogicalName,Format"
                }
            };

            var responses = _send(requests);

            if (responses.Count < 1)
                throw new InvalidOperationException("Missing metadata response");

            responses[0].EnsureSuccess();
            var info = ParseEntity((JObject)responses[0].Json);

            if (responses.Count > 1 && responses[1].IsSuccess && responses[1].Json is JObject dates && dates["value"] is JArray dateAttrs)
            {
                foreach (var dateAttr in dateAttrs.OfType<JObject>())
                {
                    if (dateAttr.Value<string>("Format") == "DateOnly" && info.Attributes.TryGetValue(dateAttr.Value<string>("LogicalName"), out var attr))
                        attr.IsDateOnly = true;
                }
            }

            return info;
        }

        /// <summary>
        /// Parses the response from an <c>EntityDefinitions</c> request
        /// </summary>
        public static WebApiEntityInfo ParseEntity(JObject json)
        {
            var info = new WebApiEntityInfo
            {
                LogicalName = json.Value<string>("LogicalName"),
                EntitySetName = json.Value<string>("EntitySetName"),
                PrimaryIdAttribute = json.Value<string>("PrimaryIdAttribute"),
                PrimaryNameAttribute = json.Value<string>("PrimaryNameAttribute")
            };

            if (json["Attributes"] is JArray attrs)
            {
                foreach (var attr in attrs.OfType<JObject>())
                {
                    var attrInfo = new WebApiAttributeInfo
                    {
                        LogicalName = attr.Value<string>("LogicalName"),
                        AttributeType = attr.Value<string>("AttributeType"),
                        AttributeTypeName = (attr["AttributeTypeName"] as JObject)?.Value<string>("Value"),
                        AttributeOf = attr.Value<string>("AttributeOf")
                    };

                    if (attrInfo.LogicalName != null)
                        info.Attributes[attrInfo.LogicalName] = attrInfo;
                }
            }

            if (json["ManyToOneRelationships"] is JArray rels)
            {
                foreach (var rel in rels.OfType<JObject>())
                {
                    var attr = rel.Value<string>("ReferencingAttribute");
                    var target = rel.Value<string>("ReferencedEntity");
                    var navProp = rel.Value<string>("ReferencingEntityNavigationPropertyName");

                    if (attr == null || target == null || navProp == null)
                        continue;

                    if (!info.LookupNavigationProperties.TryGetValue(attr, out var navProps))
                    {
                        navProps = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                        info.LookupNavigationProperties[attr] = navProps;
                    }

                    navProps[target] = navProp;
                }
            }

            return info;
        }

        private WebApiRelationshipInfo LoadRelationship(string schemaName)
        {
            var responses = _send(new[]
            {
                new WebApiRequest
                {
                    Method = "GET",
                    Url = $"RelationshipDefinitions(SchemaName={WebApiJson.FormatLiteral(schemaName)})"
                }
            });

            if (responses.Count < 1)
                throw new InvalidOperationException("Missing metadata response");

            responses[0].EnsureSuccess();
            return ParseRelationship((JObject)responses[0].Json);
        }

        /// <summary>
        /// Parses the response from a <c>RelationshipDefinitions</c> request
        /// </summary>
        public static WebApiRelationshipInfo ParseRelationship(JObject json)
        {
            var type = json.Value<string>("@odata.type") ?? "";

            return new WebApiRelationshipInfo
            {
                SchemaName = json.Value<string>("SchemaName"),
                IsManyToMany = type.EndsWith("ManyToManyRelationshipMetadata") || json["Entity1LogicalName"] != null,
                Entity1LogicalName = json.Value<string>("Entity1LogicalName"),
                Entity2LogicalName = json.Value<string>("Entity2LogicalName"),
                Entity1NavigationPropertyName = json.Value<string>("Entity1NavigationPropertyName"),
                Entity2NavigationPropertyName = json.Value<string>("Entity2NavigationPropertyName"),
                ReferencedEntity = json.Value<string>("ReferencedEntity"),
                ReferencingEntity = json.Value<string>("ReferencingEntity"),
                ReferencedEntityNavigationPropertyName = json.Value<string>("ReferencedEntityNavigationPropertyName"),
                ReferencingEntityNavigationPropertyName = json.Value<string>("ReferencingEntityNavigationPropertyName")
            };
        }
    }
}
