using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.ServiceModel;
using System.Threading;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.Engine;
using Microsoft.Crm.Sdk.Messages;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Metadata;
using Microsoft.Xrm.Sdk.Query;
using Newtonsoft.Json.Linq;
using StreamJsonRpc;

namespace MarkMpn.Sql4Cds.LanguageServer.Pptb
{
    /// <summary>
    /// An <see cref="IOrganizationService"/> implementation that converts SDK requests to Web API requests and
    /// executes them via the PPTB host, as the language server never has direct access to the access token.
    /// </summary>
    public class PptbOrganizationService : IOrganizationService, IServiceClient
    {
        private static readonly Dictionary<string, string> _headerParameters = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["BypassCustomPluginExecution"] = "MSCRM.BypassCustomPluginExecution",
            ["BypassBusinessLogicExecution"] = "MSCRM.BypassBusinessLogicExecution",
            ["BypassBusinessLogicExecutionStepIds"] = "MSCRM.BypassBusinessLogicExecutionStepIds",
            ["SolutionUniqueName"] = "MSCRM.SolutionUniqueName",
            ["SuppressDuplicateDetection"] = "MSCRM.SuppressDuplicateDetection",
            ["SuppressCallbackRegistrationExpanderJob"] = "MSCRM.SuppressCallbackRegistrationExpanderJob",
            ["tag"] = "MSCRM.tag",
        };

        private readonly IWebApiTransport _transport;
        private readonly string _serviceRoot;
        private Version _version;
        private readonly ParallelismRecommendation _parallelism = new ParallelismRecommendation();

        private sealed class ParallelismRecommendation
        {
            public int Value = 1;
        }

        public PptbOrganizationService(IWebApiTransport transport, string url, string name)
        {
            _transport = transport ?? throw new ArgumentNullException(nameof(transport));
            _serviceRoot = (url ?? "").TrimEnd('/') + "/api/data/v9.2/";
            Uri = new Uri(url);

            SetMetadataCache(new AttributeMetadataCache(this));
            MetadataQuery = new WebApiMetadataQuery(request => SendSingle(request));
            MessageCache = new MessageCache(this, Metadata);

            Name = name;
        }

        public IAttributeMetadataCache Metadata { get; private set; }

        public WebApiEntitySerializer Serializer { get; private set; }

        /// <summary>
        /// Uses the data source's metadata cache for Web API request and response translation.
        /// </summary>
        public void SetMetadataCache(IAttributeMetadataCache metadata)
        {
            Metadata = metadata ?? throw new ArgumentNullException(nameof(metadata));
            Serializer = new WebApiEntitySerializer(metadata);
        }

        public WebApiMetadataQuery MetadataQuery { get; }

        public IMessageCache MessageCache { get; set; }

        public Version Version
        {
            get
            {
                if (_version != null)
                    return _version;

                _version = new Version(((RetrieveVersionResponse)Execute(new RetrieveVersionRequest())).Version);
                return _version;
            }
        }

        public string Name { get; init; }

        public Uri Uri { get; }

        public bool CanParallelise => true;

        public bool UsesWebAPI => true;

        public Guid CallerId { get; set; }

        public int RecommendedDegreesOfParallelism => Volatile.Read(ref _parallelism.Value);

        /// <summary>
        /// Shares the correlated RPC transport and thread-safe, read-only-after-loading metadata caches.
        /// </summary>
        public IOrganizationService Clone() => (PptbOrganizationService)MemberwiseClone();

        #region IOrganizationService

        public Guid Create(Entity entity)
        {
            return ((CreateResponse)Execute(new CreateRequest { Target = entity })).id;
        }

        public Entity Retrieve(string entityName, Guid id, ColumnSet columnSet)
        {
            return ((RetrieveResponse)Execute(new RetrieveRequest { Target = new EntityReference(entityName, id), ColumnSet = columnSet })).Entity;
        }

        public void Update(Entity entity)
        {
            Execute(new UpdateRequest { Target = entity });
        }

        public void Delete(string entityName, Guid id)
        {
            Execute(new DeleteRequest { Target = new EntityReference(entityName, id) });
        }

        public void Associate(string entityName, Guid entityId, Relationship relationship, EntityReferenceCollection relatedEntities)
        {
            Execute(new AssociateRequest { Target = new EntityReference(entityName, entityId), Relationship = relationship, RelatedEntities = relatedEntities });
        }

        public void Disassociate(string entityName, Guid entityId, Relationship relationship, EntityReferenceCollection relatedEntities)
        {
            Execute(new DisassociateRequest { Target = new EntityReference(entityName, entityId), Relationship = relationship, RelatedEntities = relatedEntities });
        }

        public EntityCollection RetrieveMultiple(QueryBase query)
        {
            return ((RetrieveMultipleResponse)Execute(new RetrieveMultipleRequest { Query = query })).EntityCollection;
        }

        public OrganizationResponse Execute(OrganizationRequest request)
        {
            if (request == null)
                throw new ArgumentNullException(nameof(request));

            return Prepare(request).ExecuteStandalone(this);
        }

        #endregion

        #region Transport

        /// <summary>
        /// Sends a set of requests to the PPTB host to execute
        /// </summary>
        public IReadOnlyList<WebApiResponse> Send(IReadOnlyList<WebApiRequest> requests, bool continueOnError = false)
        {
            if (requests.Count == 0)
                return Array.Empty<WebApiResponse>();

            var headers = new Dictionary<string, string>();
            var callerId = CallerId;
            if (callerId != Guid.Empty)
                headers["MSCRMCallerID"] = callerId.ToString("D");

            IReadOnlyList<WebApiResponse> responses;

            try
            {
                // Run the RPC call on the thread pool to avoid deadlocks if the caller has a synchronization context
                responses = Task.Run(() => _transport.ExecuteBatchAsync(requests, continueOnError, headers, CancellationToken.None)).GetAwaiter().GetResult();
            }
            catch (RemoteInvocationException ex)
            {
                throw CreateFault(ex.Message);
            }
            catch (ConnectionLostException ex)
            {
                throw CreateFault("Connection to Power Platform ToolBox lost: " + ex.Message);
            }

            foreach (var response in responses)
            {
                if (Int32.TryParse(response.GetBatchHeader("x-ms-dop-hint"), NumberStyles.Integer, CultureInfo.InvariantCulture, out var recommendedDop) && recommendedDop > 0)
                    Volatile.Write(ref _parallelism.Value, recommendedDop);
            }

            // A failed changeset can return only its failing operation. Restore the other slots so
            // later batch responses still line up with their SDK requests.
            if (responses.Count < requests.Count && responses.All(r => r.ContentId != null) && requests.All(r => r.ContentId != null))
            {
                var byId = responses.ToDictionary(r => r.ContentId);
                var changesetFaults = requests
                    .Where(r => r.ChangesetId != null && byId.TryGetValue(r.ContentId, out var response) && !response.IsSuccess)
                    .GroupBy(r => r.ChangesetId)
                    .ToDictionary(g => g.Key, g => byId[g.First().ContentId]);
                var aligned = new List<WebApiResponse>();
                foreach (var operation in requests)
                {
                    if (byId.TryGetValue(operation.ContentId, out var response))
                        aligned.Add(response);
                    else if (operation.ChangesetId != null && changesetFaults.TryGetValue(operation.ChangesetId, out var fault))
                        aligned.Add(fault);
                    else
                        break;
                }
                if (aligned.Count >= responses.Count)
                    responses = aligned;
            }

            if (!continueOnError && responses.Count < requests.Count && responses.Count > 0 && !responses[responses.Count - 1].IsSuccess)
            {
                // The batch stopped at the first error - that's all we need to report
                return responses;
            }

            if (responses.Count != requests.Count)
                throw CreateFault($"Expected {requests.Count} responses but received {responses.Count}");

            // Match up responses with requests by content ID in case they come back in a different order
            if (responses.All(r => r.ContentId != null) && requests.All(r => r.ContentId != null))
            {
                var byId = responses.GroupBy(r => r.ContentId).ToDictionary(g => g.Key, g => g.First());

                if (requests.All(r => byId.ContainsKey(r.ContentId)))
                    responses = requests.Select(r => byId[r.ContentId]).ToList();
            }

            return responses;
        }

        /// <summary>
        /// Sends a single request to the PPTB host to execute
        /// </summary>
        public WebApiResponse SendSingle(WebApiRequest request)
        {
            var responses = Send(new[] { request });

            if (responses.Count == 0)
                throw CreateFault("No response received");

            return responses[0];
        }

        internal static FaultException<OrganizationServiceFault> CreateFault(string message, int errorCode = 0)
        {
            var fault = new OrganizationServiceFault { Message = message, ErrorCode = errorCode };
            return new FaultException<OrganizationServiceFault>(fault, new FaultReason(message));
        }

        internal static FaultException<OrganizationServiceFault> CreateFault(OrganizationServiceFault fault)
        {
            return new FaultException<OrganizationServiceFault>(fault, new FaultReason(fault.Message ?? "Unknown error"));
        }

        #endregion

        #region Request preparation

        /// <summary>
        /// A request that has been converted to the equivalent Web API request(s)
        /// </summary>
        public class PreparedOperation
        {
            /// <summary>
            /// The Web API requests to execute, or <c>null</c> if the operation can't be batched
            /// </summary>
            public List<WebApiRequest> Requests { get; set; }

            /// <summary>
            /// Converts the Web API responses to the SDK response
            /// </summary>
            public Func<IReadOnlyList<WebApiResponse>, OrganizationResponse> Map { get; set; }

            /// <summary>
            /// Executes an operation that can't be batched
            /// </summary>
            public Func<OrganizationResponse> Custom { get; set; }

            public bool IsTransaction { get; set; }

            public OrganizationResponse ExecuteStandalone(PptbOrganizationService svc)
            {
                if (Custom != null)
                    return Custom();

                var responses = svc.Send(Requests);
                return MapResponses(responses);
            }

            public OrganizationResponse MapResponses(IReadOnlyList<WebApiResponse> responses)
            {
                if (!IsTransaction)
                {
                    foreach (var response in responses)
                        response.EnsureSuccess();

                    if (responses.Count != Requests.Count)
                        throw CreateFault($"Expected {Requests.Count} responses but received {responses.Count}");
                }

                return Map(responses);
            }
        }

        internal static PreparedOperation Single(WebApiRequest request, Func<WebApiResponse, OrganizationResponse> map)
        {
            return new PreparedOperation
            {
                Requests = new List<WebApiRequest> { request },
                Map = responses => map(responses[0])
            };
        }

        private static PreparedOperation Multiple(List<WebApiRequest> requests, Func<OrganizationResponse> map)
        {
            return new PreparedOperation
            {
                Requests = requests,
                Map = _ => map()
            };
        }

        private static PreparedOperation CustomOperation(Func<OrganizationResponse> execute)
        {
            return new PreparedOperation { Custom = execute };
        }

        private static Dictionary<string, string> GetHeaders(OrganizationRequest request, Dictionary<string, string> headers = null)
        {
            foreach (var param in request.Parameters)
            {
                if (!_headerParameters.TryGetValue(param.Key, out var header) || param.Value == null)
                    continue;

                string value;

                if (param.Value is bool b)
                    value = b ? "true" : "false";
                else if (param.Value is IEnumerable enumerable && !(param.Value is string))
                    value = String.Join(",", enumerable.Cast<object>());
                else
                    value = Convert.ToString(param.Value, CultureInfo.InvariantCulture);

                if (String.IsNullOrEmpty(value))
                    continue;

                if (headers == null)
                    headers = new Dictionary<string, string>();

                headers[header] = value;
            }

            return headers;
        }

        private static T As<T>(OrganizationRequest request) where T : OrganizationRequest, new()
        {
            if (request is T typed)
                return typed;

            return new T { Parameters = request.Parameters, RequestId = request.RequestId };
        }

        /// <summary>
        /// Converts an SDK request to the equivalent Web API request(s)
        /// </summary>
        public PreparedOperation Prepare(OrganizationRequest request)
        {
            switch (request.RequestName)
            {
                case "ExecuteMultiple":
                    return CustomOperation(() => ExecuteMultiple(As<ExecuteMultipleRequest>(request)));

                case "ExecuteTransaction":
                    return PrepareTransaction(As<ExecuteTransactionRequest>(request));

                case "RetrieveMetadataChanges":
                    return MetadataQuery.PrepareRetrieveMetadataChanges(As<Microsoft.Xrm.Sdk.Messages.RetrieveMetadataChangesRequest>(request));

                case "RetrieveEntity":
                    return MetadataQuery.PrepareRetrieveEntity(As<RetrieveEntityRequest>(request));

                case "RetrieveAllOptionSets":
                    return MetadataQuery.PrepareRetrieveAllOptionSets(As<RetrieveAllOptionSetsRequest>(request));

                case "WhoAmI":
                    return PrepareWhoAmI();

                case "RetrieveVersion":
                    return PrepareRetrieveVersion();

                case "RetrieveTotalRecordCount":
                    return PrepareRetrieveTotalRecordCount(As<RetrieveTotalRecordCountRequest>(request));

                case "Create":
                    return PrepareCreate(As<CreateRequest>(request));

                case "Update":
                    return PrepareUpdate(As<UpdateRequest>(request));

                case "Delete":
                    return PrepareDelete(As<DeleteRequest>(request));

                case "Retrieve":
                    return PrepareRetrieve(As<RetrieveRequest>(request));

                case "RetrieveMultiple":
                    return PrepareRetrieveMultiple(As<RetrieveMultipleRequest>(request));

                case "Associate":
                    return PrepareAssociate(As<AssociateRequest>(request));

                case "Disassociate":
                    return PrepareDisassociate(As<DisassociateRequest>(request));

                case "SetState":
                    return PrepareSetState(As<SetStateRequest>(request));

                case "Assign":
                    return PrepareAssign(As<AssignRequest>(request));

                case "CreateMultiple":
                    return PrepareCreateMultiple(request);

                case "UpdateMultiple":
                    return PrepareUpdateMultiple(request);

                case "DeleteMultiple":
                    return PrepareDeleteMultiple(request);

                case "BulkDelete":
                    return Single(CreateBulkDeleteRequest((BulkDeleteRequest)request), response =>
                    {
                        response.EnsureSuccess();
                        var result = new BulkDeleteResponse();
                        result.Results["JobId"] = Guid.Parse(response.Json.Value<string>("JobId") ??
                            throw new InvalidOperationException("Dataverse did not return the bulk delete job ID"));
                        return result;
                    });

                case "FetchXmlToQueryExpression":
                    return Single(CreateFunctionRequest(request), response =>
                    {
                        response.EnsureSuccess();
                        var result = new FetchXmlToQueryExpressionResponse();
                        result.Results["Query"] = WebApiQuerySerializer.Deserialize(response.Json["Query"]);
                        return result;
                    });

                case "QueryExpressionToFetchXml":
                    return Single(CreateActionRequest(request), response =>
                    {
                        response.EnsureSuccess();
                        var result = new QueryExpressionToFetchXmlResponse();
                        result.Results["FetchXml"] = response.Json.Value<string>("FetchXml") ??
                            throw new InvalidOperationException("Dataverse did not return FetchXML");
                        return result;
                    });

                default:
                    return PrepareGeneric(request);
            }
        }

        private PreparedOperation PrepareWhoAmI()
        {
            return Single(new WebApiRequest { Method = "GET", Url = "WhoAmI" }, response =>
            {
                var json = (JObject)response.Json;
                var result = new WhoAmIResponse();
                result.Results["UserId"] = Guid.Parse(json.Value<string>("UserId"));
                result.Results["BusinessUnitId"] = Guid.Parse(json.Value<string>("BusinessUnitId"));
                result.Results["OrganizationId"] = Guid.Parse(json.Value<string>("OrganizationId"));
                return result;
            });
        }

        private PreparedOperation PrepareRetrieveVersion()
        {
            return Single(new WebApiRequest { Method = "GET", Url = "RetrieveVersion" }, response =>
            {
                var result = new RetrieveVersionResponse();
                result.Results["Version"] = ((JObject)response.Json).Value<string>("Version");
                return result;
            });
        }

        private PreparedOperation PrepareRetrieveTotalRecordCount(RetrieveTotalRecordCountRequest request)
        {
            var names = new JArray(request.EntityNames ?? Array.Empty<string>());

            return Single(new WebApiRequest
            {
                Method = "GET",
                Url = "RetrieveTotalRecordCount(EntityNames=@p1)?@p1=" + System.Uri.EscapeDataString(WebApiJson.Serialize(names))
            }, response =>
            {
                var json = (JObject)response.Json;
                var counts = new EntityRecordCountCollection();

                if (json["EntityRecordCountCollection"] is JObject collection && collection["Keys"] is JArray keys && collection["Values"] is JArray values)
                {
                    for (var i = 0; i < keys.Count && i < values.Count; i++)
                        counts[(string)keys[i]] = values[i].Value<long>();
                }

                var result = new RetrieveTotalRecordCountResponse();
                result.Results["EntityRecordCountCollection"] = counts;
                return result;
            });
        }

        private PreparedOperation PrepareCreate(CreateRequest request)
        {
            var target = request.Target;
            var info = Metadata[target.LogicalName];

            return Single(new WebApiRequest
            {
                Method = "POST",
                Url = info.EntitySetName,
                Headers = GetHeaders(request),
                Body = WebApiJson.Serialize(Serializer.Serialize(target, true))
            }, response =>
            {
                var id = WebApiJson.ParseEntityId(response.GetHeader("odata-entityid")) ?? target.Id;
                var result = new CreateResponse();
                result.Results["id"] = id;
                return result;
            });
        }

        private PreparedOperation PrepareUpdate(UpdateRequest request)
        {
            var headers = GetHeaders(request, new Dictionary<string, string> { ["If-Match"] = "*" });

            return Single(new WebApiRequest
            {
                Method = "PATCH",
                Url = Serializer.GetRecordUrl(request.Target),
                Headers = headers,
                Body = WebApiJson.Serialize(Serializer.Serialize(request.Target, false))
            }, _ => new UpdateResponse());
        }

        private PreparedOperation PrepareDelete(DeleteRequest request)
        {
            return Single(new WebApiRequest
            {
                Method = "DELETE",
                Url = Serializer.GetRecordUrl(request.Target),
                Headers = GetHeaders(request)
            }, _ => new DeleteResponse());
        }

        private static WebApiRequest CreateFetchXmlRequest(string entitySetName, string fetchXml)
        {
            return new WebApiRequest
            {
                Method = "GET",
                Url = entitySetName + "?fetchXml=" + System.Uri.EscapeDataString(fetchXml),
                Headers = new Dictionary<string, string> { ["Prefer"] = WebApiJson.IncludeAnnotationsPreference }
            };
        }

        private PreparedOperation PrepareRetrieve(RetrieveRequest request)
        {
            var target = request.Target;
            var info = Metadata[target.LogicalName];
            var expansions = new List<string>();
            var options = GetRetrieveOptions(info, request.ColumnSet, expansions);
            var related = new List<(Relationship relationship, string navigation, bool collection, FetchXmlResultParser parser)>();

            if (request.RelatedEntitiesQuery != null)
            {
                foreach (var entry in request.RelatedEntitiesQuery)
                {
                    if (!(entry.Value is QueryExpression query))
                        throw new NotSupportedException("Related entity retrieval requires a QueryExpression");

                    if (query.LinkEntities.Count > 0 || query.Distinct || query.PageInfo?.Count > 0 ||
                        query.PageInfo?.PageNumber > 1 || !String.IsNullOrEmpty(query.PageInfo?.PagingCookie))
                        throw new NotSupportedException("Related entity retrieval does not support links, distinct queries or paging");

                    var (navigation, collection, entityName) = GetAssociationNavigationProperty(target, entry.Key);
                    if (!String.Equals(entityName, query.EntityName, StringComparison.OrdinalIgnoreCase))
                        throw new NotSupportedException($"Relationship {entry.Key.SchemaName} targets {entityName}, not {query.EntityName}");
                    var relatedInfo = Metadata[query.EntityName];
                    var nestedExpansions = new List<string>();
                    var nestedOptions = GetRetrieveOptions(relatedInfo, query.ColumnSet, nestedExpansions);
                    var filter = GetRelatedFilter(query.Criteria, relatedInfo);
                    if (filter != null)
                    {
                        if (!collection)
                            throw new NotSupportedException("Filters require a collection-valued relationship");
                        nestedOptions.Add("$filter=" + filter);
                    }
                    if (query.Orders.Count > 0)
                    {
                        if (!collection)
                            throw new NotSupportedException("Ordering requires a collection-valued relationship");
                        nestedOptions.Add("$orderby=" + String.Join(",", query.Orders.Select(order =>
                        {
                            if (!String.IsNullOrEmpty(order.EntityName) || !String.IsNullOrEmpty(order.Alias))
                                throw new NotSupportedException("Related entity ordering does not support entity names or aliases");
                            return GetRetrieveProperty(relatedInfo, order.AttributeName) + (order.OrderType == OrderType.Descending ? " desc" : " asc");
                        })));
                    }
                    if (query.TopCount.HasValue)
                    {
                        if (!collection)
                            throw new NotSupportedException("TopCount requires a collection-valued relationship");
                        nestedOptions.Add("$top=" + query.TopCount.Value.ToString(CultureInfo.InvariantCulture));
                    }
                    if (nestedExpansions.Count > 0)
                        nestedOptions.Add("$expand=" + String.Join(",", nestedExpansions));
                    expansions.Add(navigation + (nestedOptions.Count == 0 ? "" : "(" + String.Join(";", nestedOptions) + ")"));
                    related.Add((entry.Key, navigation, collection, new FetchXmlResultParser(relatedInfo, Metadata)));
                }
            }

            if (expansions.Count > 0)
                options.Add("$expand=" + String.Join(",", expansions.Distinct()));
            var url = Serializer.GetRecordUrl(target);
            if (options.Count > 0)
                url += "?" + String.Join("&", options.Select(option =>
                {
                    var equals = option.IndexOf('=');
                    return option.Substring(0, equals + 1) + System.Uri.EscapeDataString(option.Substring(equals + 1));
                }));
            var parser = new FetchXmlResultParser(info, Metadata);

            return Single(new WebApiRequest
            {
                Method = "GET",
                Url = url,
                Headers = GetHeaders(request, new Dictionary<string, string> { ["Prefer"] = WebApiJson.IncludeAnnotationsPreference })
            }, response =>
            {
                var json = (JObject)response.Json.DeepClone();
                var relatedEntities = new RelatedEntityCollection();
                foreach (var expansion in related)
                {
                    var token = json[expansion.navigation];
                    var entities = new EntityCollection { EntityName = expansion.parser.RootEntity };
                    if (expansion.collection && token is JArray records)
                        entities.Entities.AddRange(records.Cast<JObject>().Select(expansion.parser.ParseEntity));
                    else if (!expansion.collection && token is JObject record)
                        entities.Entities.Add(expansion.parser.ParseEntity(record));
                    else if (token == null || token.Type != JTokenType.Null || expansion.collection)
                        throw new InvalidOperationException($"Dataverse returned an invalid expanded relationship {expansion.navigation}");
                    var nextLink = json.Value<string>(expansion.navigation + "@odata.nextLink");
                    entities.MoreRecords = nextLink != null;
                    relatedEntities.Add(expansion.relationship, entities);
                    json.Remove(expansion.navigation);
                }
                var parsed = parser.ParseEntity(json);
                foreach (var expansion in relatedEntities)
                    parsed.RelatedEntities.Add(expansion.Key, expansion.Value);
                var result = new RetrieveResponse();
                result.Results["Entity"] = parsed;
                return result;
            });
        }

        private List<string> GetRetrieveOptions(EntityMetadata info, ColumnSet columns, List<string> expansions)
        {
            columns = columns ?? new ColumnSet();
            if (columns.AttributeExpressions.Count > 0)
                throw new NotSupportedException("Retrieve does not support column aliases or aggregates");
            var attributes = columns.AllColumns ? info.Attributes ?? Array.Empty<AttributeMetadata>() : columns.Columns.Select(info.GetAttribute).ToArray();
            if (attributes.Any(attribute => attribute?.AttributeType == AttributeTypeCode.PartyList))
                expansions.Add(info.LogicalName + "_activity_parties");
            var options = new List<string>();
            if (!columns.AllColumns)
            {
                var properties = columns.Columns
                    .Where(column => info.GetAttribute(column)?.AttributeType != AttributeTypeCode.PartyList)
                    .Select(column => GetRetrieveProperty(info, column))
                    .Union(new[] { info.PrimaryIdAttribute }, StringComparer.OrdinalIgnoreCase);
                options.Add("$select=" + String.Join(",", properties));
            }
            return options;
        }

        private static string GetRetrieveProperty(EntityMetadata info, string name)
        {
            return info.GetAttribute(name).IsLookup() ? "_" + name + "_value" : name;
        }

        private static string GetRelatedFilter(FilterExpression filter, EntityMetadata info)
        {
            if (filter == null)
                return null;
            var expressions = filter.Filters.Select(child => GetRelatedFilter(child, info)).Where(value => value != null).ToList();
            foreach (var condition in filter.Conditions)
            {
                if (condition.CompareColumns || !String.IsNullOrEmpty(condition.EntityName))
                    throw new NotSupportedException("Related entity filters do not support column comparisons or entity names");
                var property = GetRetrieveProperty(info, condition.AttributeName);
                if (condition.Operator == ConditionOperator.Null || condition.Operator == ConditionOperator.NotNull)
                {
                    expressions.Add(property + (condition.Operator == ConditionOperator.Null ? " eq null" : " ne null"));
                    continue;
                }
                var op = condition.Operator switch
                {
                    ConditionOperator.Equal => "eq",
                    ConditionOperator.NotEqual => "ne",
                    ConditionOperator.GreaterThan => "gt",
                    ConditionOperator.GreaterEqual => "ge",
                    ConditionOperator.LessThan => "lt",
                    ConditionOperator.LessEqual => "le",
                    _ => throw new NotSupportedException($"Related entity filter operator {condition.Operator} is not supported")
                };
                if (condition.Values.Count != 1)
                    throw new NotSupportedException($"Related entity filter operator {condition.Operator} requires one value");
                expressions.Add(property + " " + op + " " + WebApiJson.FormatLiteral(condition.Values[0]));
            }
            return expressions.Count == 0 ? null : "(" + String.Join(filter.FilterOperator == LogicalOperator.Or ? " or " : " and ", expressions) + ")";
        }

        private PreparedOperation PrepareRetrieveMultiple(RetrieveMultipleRequest request)
        {
            var fetchXml = QueryExpressionConverter.ToFetchXml(request.Query);
            var parser = new FetchXmlResultParser(fetchXml, Metadata);

            // Hard-code some well known entity set names to speed up loading the required information when establishing
            // a connection
            var entitySetName = parser.RootEntity switch
            {
                "organization" => "organizations",
                "systemuser" => "systemusers",
                _ => Metadata[parser.RootEntity].EntitySetName
            };

            return Single(CreateFetchXmlRequest(entitySetName, fetchXml), response =>
            {
                var result = new RetrieveMultipleResponse();
                result.Results["EntityCollection"] = parser.Parse((JObject)response.Json);
                return result;
            });
        }

        private (string navigationProperty, bool isCollection, string relatedEntity) GetAssociationNavigationProperty(EntityReference target, Relationship relationship)
        {
            var entity = Metadata[target.LogicalName];
            var manyToMany = entity.ManyToManyRelationships?.FirstOrDefault(r =>
                String.Equals(r.SchemaName, relationship.SchemaName, StringComparison.OrdinalIgnoreCase));

            if (manyToMany != null)
            {
                string nav;

                if (manyToMany.Entity1LogicalName == manyToMany.Entity2LogicalName)
                    nav = relationship.PrimaryEntityRole == EntityRole.Referenced ? manyToMany.Entity2NavigationPropertyName : manyToMany.Entity1NavigationPropertyName;
                else if (target.LogicalName.Equals(manyToMany.Entity1LogicalName, StringComparison.OrdinalIgnoreCase))
                    nav = manyToMany.Entity1NavigationPropertyName;
                else
                    nav = manyToMany.Entity2NavigationPropertyName;

                return (nav, true, target.LogicalName.Equals(manyToMany.Entity1LogicalName, StringComparison.OrdinalIgnoreCase)
                    ? manyToMany.Entity2LogicalName : manyToMany.Entity1LogicalName);
            }

            var rel = (entity.OneToManyRelationships ?? Array.Empty<OneToManyRelationshipMetadata>())
                .Concat(entity.ManyToOneRelationships ?? Array.Empty<OneToManyRelationshipMetadata>())
                .FirstOrDefault(r => String.Equals(r.SchemaName, relationship.SchemaName, StringComparison.OrdinalIgnoreCase))
                ?? throw new NotSupportedException($"Cannot find relationship {relationship.SchemaName} for {target.LogicalName}");
            bool targetIsReferenced;

            if (rel.ReferencedEntity == rel.ReferencingEntity)
                targetIsReferenced = relationship.PrimaryEntityRole != EntityRole.Referencing;
            else
                targetIsReferenced = target.LogicalName.Equals(rel.ReferencedEntity, StringComparison.OrdinalIgnoreCase);

            if (targetIsReferenced)
                return (rel.ReferencedEntityNavigationPropertyName, true, rel.ReferencingEntity);

            return (rel.ReferencingEntityNavigationPropertyName, false, rel.ReferencedEntity);
        }

        private PreparedOperation PrepareAssociate(AssociateRequest request)
        {
            var (nav, isCollection, _) = GetAssociationNavigationProperty(request.Target, request.Relationship);
            var targetUrl = Serializer.GetRecordUrl(request.Target);
            var headers = GetHeaders(request);

            var requests = request.RelatedEntities
                .Select(related => new WebApiRequest
                {
                    Method = isCollection ? "POST" : "PUT",
                    Url = $"{targetUrl}/{nav}/$ref",
                    Headers = headers,
                    Body = WebApiJson.Serialize(new JObject { ["@odata.id"] = _serviceRoot + Serializer.GetRecordUrl(related) })
                })
                .ToList();

            return Multiple(requests, () => new AssociateResponse());
        }

        private PreparedOperation PrepareDisassociate(DisassociateRequest request)
        {
            var (nav, isCollection, _) = GetAssociationNavigationProperty(request.Target, request.Relationship);
            var targetUrl = Serializer.GetRecordUrl(request.Target);
            var headers = GetHeaders(request);

            var requests = request.RelatedEntities
                .Select(related => new WebApiRequest
                {
                    Method = "DELETE",
                    Url = isCollection ? $"{targetUrl}/{nav}({related.Id:D})/$ref" : $"{targetUrl}/{nav}/$ref",
                    Headers = headers
                })
                .ToList();

            return Multiple(requests, () => new DisassociateResponse());
        }

        private PreparedOperation PrepareSetState(SetStateRequest request)
        {
            var body = new JObject
            {
                ["statecode"] = request.State?.Value,
                ["statuscode"] = request.Status?.Value
            };

            return Single(new WebApiRequest
            {
                Method = "PATCH",
                Url = Serializer.GetRecordUrl(request.EntityMoniker),
                Headers = GetHeaders(request, new Dictionary<string, string> { ["If-Match"] = "*" }),
                Body = WebApiJson.Serialize(body)
            }, _ => new SetStateResponse());
        }

        private PreparedOperation PrepareAssign(AssignRequest request)
        {
            var update = new Entity(request.Target.LogicalName, request.Target.Id)
            {
                ["ownerid"] = request.Assignee
            };

            return Single(new WebApiRequest
            {
                Method = "PATCH",
                Url = Serializer.GetRecordUrl(request.Target),
                Headers = GetHeaders(request, new Dictionary<string, string> { ["If-Match"] = "*" }),
                Body = WebApiJson.Serialize(Serializer.Serialize(update, false))
            }, _ => new AssignResponse());
        }

        private static EntityCollection GetTargets(OrganizationRequest request)
        {
            if (!request.Parameters.TryGetValue("Targets", out var value) || !(value is EntityCollection targets))
                throw CreateFault($"Missing Targets parameter for {request.RequestName}");

            return targets;
        }

        private PreparedOperation PrepareCreateMultiple(OrganizationRequest request)
        {
            var targets = GetTargets(request);
            var info = Metadata[targets.EntityName ?? targets.Entities[0].LogicalName];

            var body = new JObject
            {
                ["Targets"] = new JArray(targets.Entities.Select(e => Serializer.Serialize(e, true, includeType: true)))
            };

            return Single(new WebApiRequest
            {
                Method = "POST",
                Url = info.EntitySetName + "/Microsoft.Dynamics.CRM.CreateMultiple",
                Headers = GetHeaders(request),
                Body = WebApiJson.Serialize(body)
            }, response =>
            {
                var ids = ((response.Json as JObject)?["Ids"] as JArray ?? new JArray())
                    .Select(id => Guid.Parse((string)id))
                    .ToArray();

                var result = new OrganizationResponse { ResponseName = "CreateMultiple" };
                result.Results["Ids"] = ids;
                return result;
            });
        }

        private PreparedOperation PrepareUpdateMultiple(OrganizationRequest request)
        {
            var targets = GetTargets(request);
            var info = Metadata[targets.EntityName ?? targets.Entities[0].LogicalName];

            var body = new JObject
            {
                ["Targets"] = new JArray(targets.Entities.Select(e =>
                {
                    var json = Serializer.Serialize(e, false, includeType: true);

                    if (e.Id != Guid.Empty)
                        json[info.PrimaryIdAttribute] = e.Id.ToString("D");

                    return json;
                }))
            };

            return Single(new WebApiRequest
            {
                Method = "POST",
                Url = info.EntitySetName + "/Microsoft.Dynamics.CRM.UpdateMultiple",
                Headers = GetHeaders(request),
                Body = WebApiJson.Serialize(body)
            }, _ => new OrganizationResponse { ResponseName = "UpdateMultiple" });
        }

        private PreparedOperation PrepareDeleteMultiple(OrganizationRequest request)
        {
            if (!request.Parameters.TryGetValue("Targets", out var value) || !(value is EntityReferenceCollection targets) || targets.Count == 0)
                throw CreateFault("Missing Targets parameter for DeleteMultiple");

            var info = Metadata[targets[0].LogicalName];

            var body = new JObject
            {
                ["Targets"] = new JArray(targets.Select(t => new JObject
                {
                    ["@odata.type"] = "Microsoft.Dynamics.CRM." + info.LogicalName,
                    [info.PrimaryIdAttribute] = t.Id.ToString("D")
                }))
            };

            return Single(new WebApiRequest
            {
                Method = "POST",
                Url = info.EntitySetName + "/Microsoft.Dynamics.CRM.DeleteMultiple",
                Headers = GetHeaders(request),
                Body = WebApiJson.Serialize(body)
            }, _ => new OrganizationResponse { ResponseName = "DeleteMultiple" });
        }

        #endregion

        #region Generic messages

        private PreparedOperation PrepareGeneric(OrganizationRequest request)
        {
            if (!MessageCache.TryGetValue(request.RequestName, out var message))
                throw CreateFault($"Web API metadata was not found for message {request.RequestName}");

            var operation = message.IsReadOnly ? CreateFunctionRequest(request, message) : CreateActionRequest(request, message);
            return Single(operation, response => MapGenericResponse(request, response, message));
        }

        private string GetBoundPrefix(OrganizationRequest request, Message message, out string boundParameter)
        {
            boundParameter = null;

            if (String.IsNullOrEmpty(message?.BoundEntityLogicalName))
                return "";

            var binding = message.InputParameters.SingleOrDefault(p => p.IsBindingParameter) ??
                throw new InvalidOperationException($"No binding parameter is defined for {request.RequestName}");
            if (!request.Parameters.TryGetValue(binding.Name, out var value))
                throw CreateFault($"Missing binding parameter {binding.Name} for {request.RequestName}");
            Guid id;
            if (value is Guid guid)
                id = guid;
            else if (value is EntityReference reference && String.Equals(reference.LogicalName, message.BoundEntityLogicalName, StringComparison.OrdinalIgnoreCase))
                id = reference.Id;
            else if (value is Entity entity && String.Equals(entity.LogicalName, message.BoundEntityLogicalName, StringComparison.OrdinalIgnoreCase))
                id = entity.Id;
            else
                throw CreateFault($"Invalid binding parameter {binding.Name} for {request.RequestName}");
            if (id == Guid.Empty)
                throw CreateFault($"Empty binding ID for {request.RequestName}");
            boundParameter = binding.Name;
            return Serializer.GetRecordUrl(message.BoundEntityLogicalName, id) + "/Microsoft.Dynamics.CRM.";
        }

        private WebApiRequest CreateBulkDeleteRequest(BulkDeleteRequest request)
        {
            var action = CreateActionRequest(request);
            var body = (JObject)WebApiJson.Parse(action.Body);
            foreach (var name in new[] { "ToRecipients", "CCRecipients" })
            {
                if (request.Parameters.TryGetValue(name, out var recipients))
                {
                    body[name] = new JArray(((Guid[])recipients ?? Array.Empty<Guid>()).Select(id => new JObject
                    {
                        ["partyid_systemuser@odata.bind"] = $"systemusers({id:D})"
                    }));
                }
            }
            action.Body = WebApiJson.Serialize(body);
            return action;
        }

        private WebApiRequest CreateActionRequest(OrganizationRequest request, Message message = null)
        {
            var prefix = GetBoundPrefix(request, message, out var boundParameter);
            var body = new JObject();

            foreach (var param in request.Parameters)
            {
                if (_headerParameters.ContainsKey(param.Key) || param.Key == boundParameter)
                    continue;

                var metadata = message?.InputParameters.SingleOrDefault(p => p.Name.Equals(param.Key, StringComparison.OrdinalIgnoreCase));
                body[metadata?.EntityParameterName ?? param.Key] = SerializeMessageParameter(param.Value, metadata);
            }

            return new WebApiRequest
            {
                Method = "POST",
                Url = prefix + request.RequestName,
                Headers = GetHeaders(request),
                Body = WebApiJson.Serialize(body)
            };
        }

        private WebApiRequest CreateFunctionRequest(OrganizationRequest request, Message message = null)
        {
            var prefix = GetBoundPrefix(request, message, out var boundParameter);
            var parameters = new List<string>();
            var values = new List<string>();
            var i = 1;

            foreach (var param in request.Parameters)
            {
                if (_headerParameters.ContainsKey(param.Key) || param.Key == boundParameter)
                    continue;

                var metadata = message?.InputParameters.SingleOrDefault(p => p.Name.Equals(param.Key, StringComparison.OrdinalIgnoreCase));
                parameters.Add($"{metadata?.EntityParameterName ?? param.Key}=@p{i}");
                values.Add($"@p{i}=" + System.Uri.EscapeDataString(metadata?.EntityParameterName == null ?
                    FormatFunctionParameter(param.Value) : WebApiJson.Serialize(SerializeMessageParameter(param.Value, metadata))));
                i++;
            }

            var url = prefix + request.RequestName + "(" + String.Join(",", parameters) + ")";

            if (values.Count > 0)
                url += "?" + String.Join("&", values);

            return new WebApiRequest
            {
                Method = "GET",
                Url = url,
                Headers = GetHeaders(request)
            };
        }

        private string FormatFunctionParameter(object value)
        {
            switch (value)
            {
                case null:
                case string _:
                case Guid _:
                case bool _:
                case DateTime _:
                    return WebApiJson.FormatLiteral(value);

                case Enum e:
                    return $"Microsoft.Dynamics.CRM.{e.GetType().Name}'{e}'";

                case EntityReference er:
                    return WebApiJson.Serialize(new JObject { ["@odata.id"] = Serializer.GetRecordUrl(er) });

                case int _:
                case long _:
                case decimal _:
                case double _:
                case float _:
                    return WebApiJson.FormatLiteral(value);

                default:
                    return WebApiJson.Serialize(SerializeParameter(value));
            }
        }

        /// <summary>
        /// Converts an SDK request parameter value to the equivalent Web API JSON value
        /// </summary>
        public JToken SerializeParameter(object value)
        {
            switch (value)
            {
                case null:
                    return JValue.CreateNull();

                case string _:
                case bool _:
                case int _:
                case long _:
                case decimal _:
                case double _:
                case float _:
                    return new JValue(value);

                case Guid g:
                    return g.ToString("D");

                case DateTime dt:
                    return WebApiJson.FormatDateTime(dt, false);

                case Enum e:
                    return e.ToString().Replace(" ", "");

                case OptionSetValue osv:
                    return osv.Value;

                case Money m:
                    return m.Value;

                case EntityReference er:
                    {
                        var info = Metadata[er.LogicalName];

                        return new JObject
                        {
                            ["@odata.type"] = "Microsoft.Dynamics.CRM." + info.LogicalName,
                            [info.PrimaryIdAttribute] = er.Id.ToString("D")
                        };
                    }

                case Entity entity:
                    return Serializer.Serialize(entity, true, includeType: true);

                case EntityCollection collection:
                    return new JArray(collection.Entities.Select(SerializeParameter));

                case EntityReferenceCollection refs:
                    return new JArray(refs.Select(SerializeParameter));

                case ColumnSet columnSet:
                    return new JObject
                    {
                        ["AllColumns"] = columnSet.AllColumns,
                        ["Columns"] = new JArray(columnSet.Columns)
                    };

                case QueryExpression query:
                    return WebApiQuerySerializer.Serialize(query);

                case QueryBase query:
                    return new JObject
                    {
                        ["@odata.type"] = "Microsoft.Dynamics.CRM.FetchExpression",
                        ["Query"] = QueryExpressionConverter.ToFetchXml(query)
                    };

                case byte[] bytes:
                    return System.Convert.ToBase64String(bytes);

                case IEnumerable enumerable:
                    return new JArray(enumerable.Cast<object>().Select(SerializeParameter));

                default:
                    {
                        var json = new JObject
                        {
                            ["@odata.type"] = "Microsoft.Dynamics.CRM." + value.GetType().Name
                        };

                        foreach (var prop in value.GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance))
                        {
                            if (!prop.CanRead || prop.GetIndexParameters().Length > 0 || prop.Name == "ExtensionData")
                                continue;

                            json[prop.Name] = SerializeParameter(prop.GetValue(value));
                        }

                        return json;
                    }
            }
        }

        private JToken SerializeMessageParameter(object value, MessageParameter parameter)
        {
            if (parameter?.EntityParameterName == null || value == null)
                return SerializeParameter(value);
            if (value is Guid id)
                return SerializeParameter(new EntityReference(Metadata[parameter.OTC ??
                    throw new InvalidOperationException($"Missing object type code for {parameter.Name}")].LogicalName, id));
            if (value is Guid[] ids)
                return new JArray(ids.Select(g => SerializeMessageParameter(g, parameter)));
            return SerializeParameter(value);
        }

        private OrganizationResponse MapGenericResponse(OrganizationRequest request, WebApiResponse response, Message message)
        {
            response.EnsureSuccess();
            if (!(response.Json is JObject) && message?.OutputParameters.Any(p => p.EntityParameterName != null) == true)
                throw new InvalidOperationException($"Dataverse did not return the entity response for {request.RequestName}");

            var responseType = request.GetType().FullName.EndsWith("Request", StringComparison.Ordinal) ?
                request.GetType().Assembly.GetType(request.GetType().FullName.Substring(0, request.GetType().FullName.Length - 7) + "Response") : null;
            var result = responseType != null && typeof(OrganizationResponse).IsAssignableFrom(responseType) ?
                (OrganizationResponse)Activator.CreateInstance(responseType) : new OrganizationResponse();
            result.ResponseName = request.RequestName;

            if (response.Json is JObject json)
            {
                foreach (var prop in json.Properties())
                {
                    if (prop.Name.StartsWith("@") || prop.Name.Contains("@"))
                        continue;

                    result.Results[prop.Name] = WebApiJson.ToClrValue(prop.Value);
                }

                foreach (var parameter in message?.OutputParameters ?? Array.Empty<MessageParameter>())
                {
                    if (parameter.EntityParameterName == null)
                        continue;
                    var token = json[parameter.EntityParameterName];
                    // Actions returning a single entity expose its properties directly rather than a response wrapper.
                    if (token == null && message.OutputParameters.Count == 1)
                    {
                        token = json;
                        result.Results.Clear();
                    }
                    if (token == null)
                        throw new InvalidOperationException($"Dataverse did not return {parameter.EntityParameterName} for {request.RequestName}");
                    var info = Metadata[parameter.OTC ??
                        throw new InvalidOperationException($"Missing object type code for {parameter.Name}")];
                    Guid ReadId(JToken entity)
                    {
                        if (!(entity is JObject objectValue))
                            throw new InvalidOperationException($"Dataverse returned an invalid entity for {parameter.Name}");
                        var idValue = objectValue.Value<string>(info.PrimaryIdAttribute);
                        if (idValue != null)
                            return Guid.Parse(idValue);
                        return WebApiJson.ParseEntityId(objectValue.Value<string>("@odata.id")) ??
                            throw new InvalidOperationException($"Dataverse did not return {info.PrimaryIdAttribute} for {request.RequestName}");
                    }
                    object value;
                    if (parameter.Type == typeof(Guid))
                        value = ReadId(token);
                    else if (parameter.Type == typeof(Guid[]))
                        value = token is JArray entities ? entities.Select(ReadId).ToArray() :
                            throw new InvalidOperationException($"Dataverse did not return an entity array for {parameter.Name}");
                    else if (parameter.Type == typeof(EntityReference))
                        value = new EntityReference(info.LogicalName, ReadId(token));
                    else if (parameter.Type == typeof(EntityReferenceCollection))
                        value = token is JArray references ?
                            new EntityReferenceCollection(references.Select(e => new EntityReference(info.LogicalName, ReadId(e))).ToList()) :
                            throw new InvalidOperationException($"Dataverse did not return an entity array for {parameter.Name}");
                    else
                        value = WebApiJson.ToClrValue(token);
                    result.Results.Remove(parameter.EntityParameterName);
                    result.Results[parameter.Name] = value;
                }
            }

            return result;
        }

        #endregion

        #region ExecuteMultiple & ExecuteTransaction

        private static OrganizationServiceFault GetFault(Exception ex)
        {
            if (ex is FaultException<OrganizationServiceFault> fault)
                return fault.Detail;

            return new OrganizationServiceFault { Message = ex.Message };
        }

        private OrganizationResponse ExecuteMultiple(ExecuteMultipleRequest request)
        {
            var settings = request.Settings ?? new ExecuteMultipleSettings();
            var requests = request.Requests ?? new OrganizationRequestCollection();
            var prepared = requests.Select(Prepare).ToList();
            if (prepared.Any(op => op.Custom != null))
                throw CreateFault("ExecuteMultiple cannot contain ExecuteMultiple requests");

            var responses = Send(prepared.SelectMany(op => op.Requests).ToList(), continueOnError: settings.ContinueOnError);
            var items = new ExecuteMultipleResponseItemCollection();
            var offset = 0;

            for (var index = 0; index < prepared.Count; index++)
            {
                var op = prepared[index];
                var opResponses = responses.Skip(offset).Take(op.Requests.Count).ToList();
                offset += op.Requests.Count;
                try
                {
                    var response = op.MapResponses(opResponses);
                    if (settings.ReturnResponses)
                        items.Add(new ExecuteMultipleResponseItem { RequestIndex = index, Response = response });
                }
                catch (Exception ex)
                {
                    items.Add(new ExecuteMultipleResponseItem { RequestIndex = index, Fault = GetFault(ex) });
                    if (!settings.ContinueOnError)
                        break;
                }
            }

            var result = new ExecuteMultipleResponse();
            result.Results["Responses"] = items;
            result.Results["IsFaulted"] = items.Any(item => item.Fault != null);
            return result;
        }

        private PreparedOperation PrepareTransaction(ExecuteTransactionRequest request)
        {
            var requests = request.Requests ?? new OrganizationRequestCollection();
            var prepared = requests.Select(Prepare).ToList();
            if (prepared.Any(op => op.Custom != null || op.IsTransaction))
                throw CreateFault("ExecuteTransaction cannot contain ExecuteMultiple or ExecuteTransaction requests");
            var allRequests = prepared.SelectMany(op => op.Requests).ToList();
            var changesetId = Guid.NewGuid().ToString("N");
            foreach (var operation in allRequests)
                operation.ChangesetId = changesetId;

            return new PreparedOperation
            {
                Requests = allRequests,
                IsTransaction = true,
                Map = webResponses =>
                {
                    var failed = webResponses.FirstOrDefault(r => !r.IsSuccess);
                    if (failed != null)
                    {
                        var failedOffset = failed.ContentId == null ? -1 : allRequests.FindIndex(r => r.ContentId == failed.ContentId);
                        if (failedOffset < 0)
                            failedOffset = webResponses.ToList().IndexOf(failed);
                        var index = 0;
                        while (index < prepared.Count - 1 && failedOffset >= prepared[index].Requests.Count)
                        {
                            failedOffset -= prepared[index].Requests.Count;
                            index++;
                        }
                        var fault = failed.ToFault();
                        var txFault = new ExecuteTransactionFault
                        {
                            FaultedRequestIndex = index,
                            ErrorCode = fault.ErrorCode,
                            Message = fault.Message,
                            ErrorDetails = fault.ErrorDetails
                        };
                        throw new FaultException<OrganizationServiceFault>(txFault, new FaultReason(fault.Message));
                    }

                    if (webResponses.Count != allRequests.Count)
                        throw CreateFault("The transaction failed");

                    var responses = new OrganizationResponseCollection();
                    var offset = 0;
                    foreach (var op in prepared)
                    {
                        responses.Add(op.MapResponses(webResponses.Skip(offset).Take(op.Requests.Count).ToList()));
                        offset += op.Requests.Count;
                    }

                    var result = new ExecuteTransactionResponse();

                    if (request.ReturnResponses == true)
                        result.Results["Responses"] = responses;

                    return result;
                }
            };
        }

        #endregion
    }
}
