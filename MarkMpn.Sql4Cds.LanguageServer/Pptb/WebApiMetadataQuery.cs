using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.ServiceModel;
using MarkMpn.Sql4Cds.LanguageServer.Connection;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Metadata;
using Microsoft.Xrm.Sdk.Metadata.Query;
using Microsoft.Xrm.Sdk.Query;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Serialization;

namespace MarkMpn.Sql4Cds.LanguageServer.Pptb
{
    /// <summary>
    /// Executes metadata requests using the Web API
    /// </summary>
    public class WebApiMetadataQuery
    {
        private readonly Func<WebApiRequest, WebApiResponse> _send;

        public WebApiMetadataQuery(Func<WebApiRequest, WebApiResponse> send)
        {
            _send = send;
        }

        public static JsonSerializer Serializer { get; } = CreateSerializer();

        private static JsonSerializer CreateSerializer()
        {
            var serializer = new JsonSerializer
            {
                ContractResolver = SdkMetadataContractResolver.Instance,
                ConstructorHandling = ConstructorHandling.AllowNonPublicDefaultConstructor,
                MissingMemberHandling = MissingMemberHandling.Ignore,
                DateParseHandling = DateParseHandling.None,
                FloatParseHandling = FloatParseHandling.Decimal
            };

            // Ignore any individual properties that can't be converted rather than failing the entire request
            serializer.Error += (sender, args) => args.ErrorContext.Handled = true;
            return serializer;
        }

        public RetrieveMetadataChangesResponse RetrieveMetadataChanges(RetrieveMetadataChangesRequest request)
        {
            return (RetrieveMetadataChangesResponse)Execute(PrepareRetrieveMetadataChanges(request));
        }

        public PptbOrganizationService.PreparedOperation PrepareRetrieveMetadataChanges(RetrieveMetadataChangesRequest request)
        {
            var query = request.Query ?? new EntityQueryExpression();
            var clientFilters = new ClientFilters();
            var serverQuery = PrepareQuery(query, clientFilters);

            return PptbOrganizationService.Single(CreateMetadataChangesRequest(serverQuery, request.ClientVersionStamp, request.DeletedMetadataFilters), response =>
            {
                var json = (JObject)response.Json;
                var serverVersionStamp = json.Value<string>("ServerVersionStamp");
                var entities = DeserializeEntities(json).ToList();

                clientFilters.Apply(entities);

                var result = new RetrieveMetadataChangesResponse();
                var collection = new EntityMetadataCollection();
                collection.AddRange(entities);
                result.Results["EntityMetadata"] = collection;
                result.Results["ServerVersionStamp"] = serverVersionStamp;
                result.Results["DeletedMetadata"] = new DeletedMetadataCollection();
                return result;
            });
        }

        public RetrieveEntityResponse RetrieveEntity(RetrieveEntityRequest request)
        {
            return (RetrieveEntityResponse)Execute(PrepareRetrieveEntity(request));
        }

        public PptbOrganizationService.PreparedOperation PrepareRetrieveEntity(RetrieveEntityRequest request)
        {
            var query = new EntityQueryExpression
            {
                Criteria = new MetadataFilterExpression(LogicalOperator.And),
                Properties = new MetadataPropertiesExpression()
            };

            if (request.MetadataId != Guid.Empty)
                query.Criteria.Conditions.Add(new MetadataConditionExpression(nameof(EntityMetadata.MetadataId), MetadataConditionOperator.Equals, request.MetadataId));
            else
                query.Criteria.Conditions.Add(new MetadataConditionExpression(nameof(EntityMetadata.LogicalName), MetadataConditionOperator.Equals, request.LogicalName));

            var excluded = new HashSet<string>();

            if (!request.EntityFilters.HasFlag(EntityFilters.Attributes))
                excluded.Add(nameof(EntityMetadata.Attributes));

            if (!request.EntityFilters.HasFlag(EntityFilters.Privileges))
                excluded.Add(nameof(EntityMetadata.Privileges));

            if (!request.EntityFilters.HasFlag(EntityFilters.Relationships))
            {
                excluded.Add(nameof(EntityMetadata.OneToManyRelationships));
                excluded.Add(nameof(EntityMetadata.ManyToOneRelationships));
                excluded.Add(nameof(EntityMetadata.ManyToManyRelationships));
            }

            if (excluded.Count == 0)
            {
                query.Properties.AllProperties = true;
            }
            else
            {
                foreach (var prop in typeof(EntityMetadata).GetProperties(BindingFlags.Public | BindingFlags.Instance))
                {
                    if (!excluded.Contains(prop.Name) && prop.GetCustomAttribute<System.Runtime.Serialization.DataMemberAttribute>() != null)
                        query.Properties.PropertyNames.Add(prop.Name);
                }
            }

            if (request.EntityFilters.HasFlag(EntityFilters.Attributes))
                query.AttributeQuery = new AttributeQueryExpression { Properties = new MetadataPropertiesExpression { AllProperties = true } };

            if (request.EntityFilters.HasFlag(EntityFilters.Relationships))
                query.RelationshipQuery = new RelationshipQueryExpression { Properties = new MetadataPropertiesExpression { AllProperties = true } };

            var operation = PrepareRetrieveMetadataChanges(new RetrieveMetadataChangesRequest { Query = query });
            return PptbOrganizationService.Single(operation.Requests.Single(), response =>
            {
                var metadata = (RetrieveMetadataChangesResponse)operation.MapResponses(new[] { response });
                var entity = metadata.EntityMetadata.FirstOrDefault();

                if (entity == null)
                {
                    var fault = new OrganizationServiceFault
                    {
                        ErrorCode = unchecked((int)0x80040217),
                        Message = $"Could not find entity {request.LogicalName ?? request.MetadataId.ToString()}"
                    };

                    throw new FaultException<OrganizationServiceFault>(fault, new FaultReason(fault.Message));
                }

                var result = new RetrieveEntityResponse();
                result.Results["EntityMetadata"] = entity;
                return result;
            });
        }

        public RetrieveAllOptionSetsResponse RetrieveAllOptionSets(RetrieveAllOptionSetsRequest request)
        {
            return (RetrieveAllOptionSetsResponse)Execute(PrepareRetrieveAllOptionSets(request));
        }

        public PptbOrganizationService.PreparedOperation PrepareRetrieveAllOptionSets(RetrieveAllOptionSetsRequest request)
        {
            return PptbOrganizationService.Single(new WebApiRequest { Method = "GET", Url = "GlobalOptionSetDefinitions" }, response =>
            {
                var optionSets = ((response.Json as JObject)?["value"] as JArray ?? new JArray())
                    .Select(token => token.ToObject<OptionSetMetadataBase>(Serializer))
                    .Where(os => os != null)
                    .ToArray();

                var result = new RetrieveAllOptionSetsResponse();
                result.Results["OptionSetMetadata"] = optionSets;
                return result;
            });
        }

        private OrganizationResponse Execute(PptbOrganizationService.PreparedOperation operation)
        {
            return operation.MapResponses(new[] { _send(operation.Requests.Single()) });
        }

        private static WebApiRequest CreateMetadataChangesRequest(JObject query, string clientVersionStamp, DeletedMetadataFilters deletedMetadataFilters)
        {
            var parameters = new List<string> { "Query=@p1" };
            var values = new List<string> { "@p1=" + Uri.EscapeDataString(WebApiJson.Serialize(query)) };

            if (clientVersionStamp != null)
            {
                parameters.Add("ClientVersionStamp=@p2");
                values.Add("@p2=" + Uri.EscapeDataString(WebApiJson.FormatLiteral(clientVersionStamp)));

                if (deletedMetadataFilters != DeletedMetadataFilters.Default)
                {
                    parameters.Add("DeletedMetadataFilters=@p3");
                    values.Add("@p3=" + Uri.EscapeDataString("Microsoft.Dynamics.CRM.DeletedMetadataFilters'" + deletedMetadataFilters.ToString().Replace(" ", "") + "'"));
                }
            }

            return new WebApiRequest
            {
                Method = "GET",
                Url = "RetrieveMetadataChanges(" + String.Join(",", parameters) + ")?" + String.Join("&", values)
            };
        }

        private static IEnumerable<EntityMetadata> DeserializeEntities(JObject response)
        {
            if (!(response["EntityMetadata"] is JArray array))
                yield break;

            foreach (var token in array)
            {
                var entity = token.ToObject<EntityMetadata>(Serializer);

                if (entity != null)
                    yield return entity;
            }
        }

        /// <summary>
        /// Converts the SDK query to the Web API JSON format, extracting any conditions that need to be applied on the client
        /// </summary>
        private static JObject PrepareQuery(EntityQueryExpression query, ClientFilters clientFilters)
        {
            var json = new JObject();

            var entityCriteria = RelaxFilter(query.Criteria, out var entityNeedsClientFilter, clientFilters.EntityProperties);

            if (entityNeedsClientFilter)
                clientFilters.Entity = query.Criteria;

            if (entityCriteria != null)
                json["Criteria"] = entityCriteria;

            json["Properties"] = SerializeProperties(query.Properties, clientFilters.EntityProperties);

            if (query.AttributeQuery != null)
                json["AttributeQuery"] = PrepareSubQuery(query.AttributeQuery, f => clientFilters.Attribute = f);

            if (query.RelationshipQuery != null)
                json["RelationshipQuery"] = PrepareSubQuery(query.RelationshipQuery, f => clientFilters.Relationship = f);

            if (query.KeyQuery != null)
                json["KeyQuery"] = PrepareSubQuery(query.KeyQuery, f => clientFilters.Key = f);

            if (query.LabelQuery != null)
            {
                json["LabelQuery"] = new JObject
                {
                    ["FilterLanguages"] = new JArray(query.LabelQuery.FilterLanguages.Cast<object>()),
                    ["MissingLabelBehavior"] = query.LabelQuery.MissingLabelBehavior
                };
            }

            return json;
        }

        private static JObject PrepareSubQuery(MetadataQueryExpression query, Action<MetadataFilterExpression> setClientFilter)
        {
            var json = new JObject();
            var properties = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var criteria = RelaxFilter(query.Criteria, out var needsClientFilter, properties);

            if (needsClientFilter)
                setClientFilter(query.Criteria);

            if (criteria != null)
                json["Criteria"] = criteria;

            json["Properties"] = SerializeProperties(query.Properties, properties);
            return json;
        }

        private static JObject SerializeProperties(MetadataPropertiesExpression properties, ISet<string> additionalProperties)
        {
            if (properties == null || properties.AllProperties)
                return new JObject { ["AllProperties"] = true };

            var names = properties.PropertyNames.Union(additionalProperties, StringComparer.OrdinalIgnoreCase).ToList();

            return new JObject
            {
                ["AllProperties"] = false,
                ["PropertyNames"] = new JArray(names)
            };
        }

        /// <summary>
        /// Converts a filter to the Web API format, removing any conditions that can't be reliably handled by the server
        /// </summary>
        /// <param name="filter">The filter to convert</param>
        /// <param name="needsClientFilter">Indicates if any conditions were removed so the filter must also be applied on the client</param>
        /// <param name="clientProperties">The names of any properties required to evaluate the filter on the client</param>
        /// <returns>The converted filter, or <c>null</c> if the filter does not restrict the results on the server</returns>
        private static JObject RelaxFilter(MetadataFilterExpression filter, out bool needsClientFilter, ISet<string> clientProperties)
        {
            needsClientFilter = false;

            if (filter == null)
                return null;

            var conditions = new JArray();
            var filters = new JArray();
            var isOr = filter.FilterOperator == LogicalOperator.Or;

            foreach (var condition in filter.Conditions)
            {
                var converted = ConvertCondition(condition);

                if (converted == null)
                {
                    needsClientFilter = true;
                    clientProperties.Add(condition.PropertyName);

                    if (isOr)
                        return null;

                    continue;
                }

                if (converted["FilterOperator"] != null)
                    filters.Add(converted);
                else
                    conditions.Add(converted);
            }

            foreach (var child in filter.Filters)
            {
                var converted = RelaxFilter(child, out var childNeedsClientFilter, clientProperties);
                needsClientFilter |= childNeedsClientFilter;

                if (converted == null)
                {
                    if (isOr)
                        return null;

                    continue;
                }

                filters.Add(converted);
            }

            if (conditions.Count == 0 && filters.Count == 0)
                return null;

            var json = new JObject
            {
                ["FilterOperator"] = isOr ? "Or" : "And",
                ["Conditions"] = conditions
            };

            if (filters.Count > 0)
                json["Filters"] = filters;

            return json;
        }

        private static JObject ConvertCondition(MetadataConditionExpression condition)
        {
            switch (condition.ConditionOperator)
            {
                case MetadataConditionOperator.Equals:
                case MetadataConditionOperator.NotEquals:
                case MetadataConditionOperator.GreaterThan:
                case MetadataConditionOperator.LessThan:
                    if (!IsServerSafeValue(condition.Value))
                        return null;

                    return SerializeCondition(condition.PropertyName, condition.ConditionOperator, condition.Value);

                case MetadataConditionOperator.In:
                case MetadataConditionOperator.NotIn:
                    if (!(condition.Value is IEnumerable values) || condition.Value is string)
                        return null;

                    var items = values.Cast<object>().ToList();

                    if (items.Count == 0 || !items.All(IsServerSafeValue))
                        return null;

                    var isIn = condition.ConditionOperator == MetadataConditionOperator.In;

                    return new JObject
                    {
                        ["FilterOperator"] = isIn ? "Or" : "And",
                        ["Conditions"] = new JArray(items.Select(v => SerializeCondition(condition.PropertyName, isIn ? MetadataConditionOperator.Equals : MetadataConditionOperator.NotEquals, v)))
                    };

                default:
                    return null;
            }
        }

        private static bool IsServerSafeValue(object value)
        {
            return value is string || value is int || value is bool || value is Guid;
        }

        private static JObject SerializeCondition(string propertyName, MetadataConditionOperator op, object value)
        {
            string type;
            string str;

            switch (value)
            {
                case string s:
                    type = "System.String";
                    str = s;
                    break;

                case int i:
                    type = "System.Int32";
                    str = i.ToString(CultureInfo.InvariantCulture);
                    break;

                case bool b:
                    type = "System.Boolean";
                    str = b ? "true" : "false";
                    break;

                case Guid g:
                    type = "System.Guid";
                    str = g.ToString("D");
                    break;

                default:
                    throw new NotSupportedException();
            }

            return new JObject
            {
                ["PropertyName"] = propertyName,
                ["ConditionOperator"] = op.ToString(),
                ["Value"] = new JObject
                {
                    ["Type"] = type,
                    ["Value"] = str
                }
            };
        }

        /// <summary>
        /// Filters that need to be applied on the client after the results have been retrieved from the server
        /// </summary>
        private class ClientFilters
        {
            public MetadataFilterExpression Entity { get; set; }

            public MetadataFilterExpression Attribute { get; set; }

            public MetadataFilterExpression Relationship { get; set; }

            public MetadataFilterExpression Key { get; set; }

            public HashSet<string> EntityProperties { get; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            public void Apply(List<EntityMetadata> entities)
            {
                if (Entity != null)
                    entities.RemoveAll(e => !Evaluate(e, Entity));

                foreach (var entity in entities)
                {
                    if (Attribute != null && entity.Attributes != null)
                        SetProperty(entity, nameof(EntityMetadata.Attributes), entity.Attributes.Where(a => Evaluate(a, Attribute)).ToArray());

                    if (Relationship != null)
                    {
                        if (entity.OneToManyRelationships != null)
                            SetProperty(entity, nameof(EntityMetadata.OneToManyRelationships), entity.OneToManyRelationships.Where(r => Evaluate(r, Relationship)).ToArray());

                        if (entity.ManyToOneRelationships != null)
                            SetProperty(entity, nameof(EntityMetadata.ManyToOneRelationships), entity.ManyToOneRelationships.Where(r => Evaluate(r, Relationship)).ToArray());

                        if (entity.ManyToManyRelationships != null)
                            SetProperty(entity, nameof(EntityMetadata.ManyToManyRelationships), entity.ManyToManyRelationships.Where(r => Evaluate(r, Relationship)).ToArray());
                    }

                    if (Key != null && entity.Keys != null)
                        SetProperty(entity, nameof(EntityMetadata.Keys), entity.Keys.Where(k => Evaluate(k, Key)).ToArray());
                }
            }

            private static void SetProperty(object target, string name, object value)
            {
                target.GetType().GetProperty(name).SetValue(target, value);
            }

            public static bool Evaluate(object target, MetadataFilterExpression filter)
            {
                if (filter == null)
                    return true;

                var results = filter.Conditions.Select(c => Evaluate(target, c))
                    .Concat(filter.Filters.Select(f => Evaluate(target, f)));

                return filter.FilterOperator == LogicalOperator.Or ? results.Any(r => r) : results.All(r => r);
            }

            private static bool Evaluate(object target, MetadataConditionExpression condition)
            {
                var prop = target.GetType().GetProperty(condition.PropertyName, BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase);

                if (prop == null)
                    return true;

                var value = Unwrap(prop.GetValue(target));

                switch (condition.ConditionOperator)
                {
                    case MetadataConditionOperator.Equals:
                        return AreEqual(value, condition.Value);

                    case MetadataConditionOperator.NotEquals:
                        return !AreEqual(value, condition.Value);

                    case MetadataConditionOperator.In:
                        return condition.Value is IEnumerable inValues && inValues.Cast<object>().Any(v => AreEqual(value, v));

                    case MetadataConditionOperator.NotIn:
                        return !(condition.Value is IEnumerable notInValues && notInValues.Cast<object>().Any(v => AreEqual(value, v)));

                    case MetadataConditionOperator.GreaterThan:
                        return Compare(value, condition.Value) > 0;

                    case MetadataConditionOperator.LessThan:
                        return Compare(value, condition.Value) < 0;

                    default:
                        return true;
                }
            }

            private static object Unwrap(object value)
            {
                if (value == null)
                    return null;

                var type = value.GetType();

                // Managed properties and AttributeTypeDisplayName have a Value property containing the actual value to compare
                if (type.Namespace == "Microsoft.Xrm.Sdk" && type.Name.EndsWith("ManagedProperty") || value is AttributeTypeDisplayName)
                    return type.GetProperty("Value")?.GetValue(value);

                return value;
            }

            private static bool AreEqual(object x, object y)
            {
                x = Unwrap(x);
                y = Unwrap(y);

                if (x == null || y == null)
                    return x == null && y == null;

                if (x is Enum || y is Enum)
                {
                    if (x is Enum xe && y is Enum ye)
                        return Convert.ToInt64(xe, CultureInfo.InvariantCulture) == Convert.ToInt64(ye, CultureInfo.InvariantCulture);

                    var e = x as Enum ?? (Enum)y;
                    var other = x is Enum ? y : x;

                    if (other is string s)
                        return e.ToString().Equals(s, StringComparison.OrdinalIgnoreCase);

                    return Convert.ToInt64(e, CultureInfo.InvariantCulture) == Convert.ToInt64(other, CultureInfo.InvariantCulture);
                }

                if (x is string xs && y is string ys)
                    return xs.Equals(ys, StringComparison.OrdinalIgnoreCase);

                if (IsNumeric(x) && IsNumeric(y))
                    return Convert.ToDecimal(x, CultureInfo.InvariantCulture) == Convert.ToDecimal(y, CultureInfo.InvariantCulture);

                return x.Equals(y) || String.Equals(Convert.ToString(x, CultureInfo.InvariantCulture), Convert.ToString(y, CultureInfo.InvariantCulture), StringComparison.OrdinalIgnoreCase);
            }

            private static int Compare(object x, object y)
            {
                x = Unwrap(x);
                y = Unwrap(y);

                if (x == null || y == null)
                    return 0;

                if (x is Enum)
                    x = Convert.ToInt64(x, CultureInfo.InvariantCulture);

                if (y is Enum)
                    y = Convert.ToInt64(y, CultureInfo.InvariantCulture);

                if (IsNumeric(x) && IsNumeric(y))
                    return Convert.ToDecimal(x, CultureInfo.InvariantCulture).CompareTo(Convert.ToDecimal(y, CultureInfo.InvariantCulture));

                if (x is IComparable cx && x.GetType() == y.GetType())
                    return cx.CompareTo(y);

                return String.Compare(Convert.ToString(x, CultureInfo.InvariantCulture), Convert.ToString(y, CultureInfo.InvariantCulture), StringComparison.OrdinalIgnoreCase);
            }

            private static bool IsNumeric(object value)
            {
                return value is int || value is long || value is short || value is byte || value is decimal || value is double || value is float;
            }
        }
    }

    /// <summary>
    /// Allows SDK metadata objects to be deserialized from the Web API format
    /// </summary>
    /// <remarks>
    /// The Web API uses <c>@odata.type</c> annotations to indicate the specific type of each object, e.g.
    /// <c>#Microsoft.Dynamics.CRM.ComplexStringAttributeMetadata</c> or <c>#Microsoft.Dynamics.CRM.StringAttributeMetadata</c>.
    /// These are mapped to the equivalent SDK type.
    /// </remarks>
    public class SdkMetadataContractResolver : DefaultContractResolver
    {
        public static readonly SdkMetadataContractResolver Instance = new SdkMetadataContractResolver();

        private static readonly Dictionary<string, Type> _metadataTypes = typeof(EntityMetadata).Assembly
            .GetTypes()
            .Where(t => typeof(MetadataBase).IsAssignableFrom(t) && !t.IsAbstract && t.IsPublic)
            .GroupBy(t => t.Name)
            .ToDictionary(g => g.Key, g => g.First());

        protected override JsonContract CreateContract(Type objectType)
        {
            var contract = base.CreateContract(objectType);

            if (typeof(MetadataBase).IsAssignableFrom(objectType))
                contract.Converter = new MetadataConverter();
            else if (objectType == typeof(KeyAttributeCollection))
                contract.Converter = MetadataCacheContractResolver.Instance.ResolveContract(objectType).Converter;

            return contract;
        }

        /// <summary>
        /// Gets the SDK type that corresponds to an <c>@odata.type</c> annotation
        /// </summary>
        public static Type GetSdkType(string odataType, Type expectedType)
        {
            if (!String.IsNullOrEmpty(odataType))
            {
                var name = odataType.Substring(odataType.LastIndexOf('.') + 1);

                if (name.StartsWith("Complex"))
                    name = name.Substring(7);

                if (_metadataTypes.TryGetValue(name, out var type) && expectedType.IsAssignableFrom(type))
                    return type;
            }

            if (!expectedType.IsAbstract)
                return expectedType;

            if (expectedType == typeof(OptionSetMetadataBase))
                return typeof(OptionSetMetadata);

            if (expectedType == typeof(RelationshipMetadataBase))
                return typeof(OneToManyRelationshipMetadata);

            return null;
        }

        private class MetadataConverter : JsonConverter
        {
            public override bool CanWrite => false;

            public override bool CanConvert(Type objectType)
            {
                return typeof(MetadataBase).IsAssignableFrom(objectType);
            }

            public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
            {
                if (reader.TokenType == JsonToken.Null)
                    return null;

                var json = JObject.Load(reader);
                var type = GetSdkType(json.Value<string>("@odata.type"), objectType);

                if (type == null)
                    return null;

                var instance = Activator.CreateInstance(type, nonPublic: true);

                using (var subReader = json.CreateReader())
                {
                    subReader.DateParseHandling = DateParseHandling.None;
                    subReader.FloatParseHandling = FloatParseHandling.Decimal;
                    serializer.Populate(subReader, instance);
                }

                return instance;
            }

            public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
            {
                throw new NotSupportedException();
            }
        }
    }
}
