using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.ServiceModel;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Linq;
using MarkMpn.Sql4Cds.Engine;
using Microsoft.Crm.Sdk.Messages;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
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
        /// <summary>
        /// The maximum number of operations PPTB accepts in a single batch
        /// </summary>
        public const int MaxBatchSize = 1000;

        private const int ObjectDoesNotExist = unchecked((int)0x80040217);

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

        /// <summary>
        /// Messages that are implemented as functions rather than actions in the Web API
        /// </summary>
        private static readonly string[] _knownFunctions = new[]
        {
            "CalculateTotalTimeIncident",
            "CheckIncomingEmail",
            "DownloadReportDefinition",
            "ExpandCalendar",
            "FormatAddress",
            "GetAllTimeZonesWithDisplayName",
            "GetDefaultPriceLevel",
            "GetTimeZoneCodeByLocalizedName",
            "GetValidManyToMany",
            "GetValidReferencedEntities",
            "GetValidReferencingEntities",
            "IsValidStateTransition",
            "LocalTimeFromUtcTime",
            "RetrieveAadUserPrivileges",
            "RetrieveAadUserRoles",
            "RetrieveAadUserSetOfPrivilegesByNames",
            "RetrieveAllEntities",
            "RetrieveAppComponents",
            "RetrieveBusinessHierarchyBusinessUnit",
            "RetrieveCurrentOrganization",
            "RetrieveDeploymentLicenseType",
            "RetrieveDuplicates",
            "RetrieveFormattedImportJobResults",
            "RetrieveLocLabels",
            "RetrieveMissingComponents",
            "RetrieveOrganizationInfo",
            "RetrieveOrganizationResources",
            "RetrieveParsedDataImportFile",
            "RetrievePrincipalAccess",
            "RetrievePrincipalAccessInfo",
            "RetrieveProvisionedLanguagePackVersion",
            "RetrieveProvisionedLanguages",
            "RetrieveTeamPrivileges",
            "RetrieveTimelineWallRecords",
            "RetrieveTotalRecordCount",
            "RetrieveUserLicenseInfo",
            "RetrieveUserPrivilegeByPrivilegeId",
            "RetrieveUserPrivilegeByPrivilegeName",
            "RetrieveUserPrivileges",
            "RetrieveUserQueues",
            "RetrieveUserSetOfPrivilegesByIds",
            "RetrieveUserSetOfPrivilegesByNames",
            "RetrieveUserSettingsRecord",
            "RetrieveVersion",
            "SearchByBodyLegacy",
            "SearchByKeywordsLegacy",
            "SearchByTitleLegacy",
            "UtcTimeFromLocalTime",
            "ValidateFetchXmlExpression",
            "WhoAmI",
        };

        /// <summary>
        /// Messages that are bound to a specific record, and the parameter that identifies the record
        /// </summary>
        private static readonly Dictionary<string, (string parameter, string entity)> _boundActions = new Dictionary<string, (string, string)>(StringComparer.OrdinalIgnoreCase)
        {
            ["AddMemberList"] = ("ListId", "list"),
            ["RemoveMemberList"] = ("ListId", "list"),
        };

        private readonly IWebApiTransport _transport;
        private readonly string _serviceRoot;
        private readonly ConcurrentDictionary<string, bool> _functions;
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
            _functions = new ConcurrentDictionary<string, bool>(_knownFunctions.ToDictionary(f => f, f => true), StringComparer.OrdinalIgnoreCase);

            Metadata = new WebApiMetadataProvider(requests => Send(requests));
            Serializer = new WebApiEntitySerializer(Metadata);
            MetadataQuery = new WebApiMetadataQuery(request => SendSingle(request));

            Name = name;
        }

        public WebApiMetadataProvider Metadata { get; }

        public WebApiEntitySerializer Serializer { get; }

        public WebApiMetadataQuery MetadataQuery { get; }

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

        public bool CanParallelise => true;

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
        public IReadOnlyList<WebApiResponse> Send(IReadOnlyList<WebApiRequest> requests, bool transaction = false, bool continueOnError = false)
        {
            if (requests.Count == 0)
                return Array.Empty<WebApiResponse>();

            IReadOnlyList<WebApiResponse> responses;

            // PPTB reports an error for the whole batch if a non-transactional batch stops early without
            // returning a result for every operation, so always ask it to continue past errors. Callers
            // that need stop-on-first-error semantics either use a transaction or send requests one by one.
            if (!transaction && requests.Count > 1)
                continueOnError = true;

            try
            {
                // Run the RPC call on the thread pool to avoid deadlocks if the caller has a synchronization context
                responses = Task.Run(() => _transport.ExecuteBatchAsync(requests, transaction, continueOnError, CancellationToken.None)).GetAwaiter().GetResult();
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
                if (Int32.TryParse(response.GetHeader("x-ms-dop-hint"), NumberStyles.Integer, CultureInfo.InvariantCulture, out var recommendedDop) && recommendedDop > 0)
                    Volatile.Write(ref _parallelism.Value, recommendedDop);
            }

            if (!transaction && !continueOnError && responses.Count < requests.Count && responses.Count > 0 && !responses[responses.Count - 1].IsSuccess)
            {
                // The batch stopped at the first error - that's all we need to report
                return responses;
            }

            if (responses.Count != requests.Count && !transaction)
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

            public bool IsBatchable => Requests != null;

            public bool IsAllWrites => Requests != null && Requests.All(r => r.IsWrite);

            public OrganizationResponse ExecuteStandalone(PptbOrganizationService svc)
            {
                if (Custom != null)
                    return Custom();

                var responses = svc.Send(Requests, transaction: Requests.Count > 1 && IsAllWrites);
                return MapResponses(responses);
            }

            public OrganizationResponse MapResponses(IReadOnlyList<WebApiResponse> responses)
            {
                foreach (var response in responses)
                    response.EnsureSuccess();

                if (responses.Count != Requests.Count)
                    throw CreateFault($"Expected {Requests.Count} responses but received {responses.Count}");

                return Map(responses);
            }
        }

        private static PreparedOperation Single(WebApiRequest request, Func<WebApiResponse, OrganizationResponse> map)
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
                    return CustomOperation(() => ExecuteTransaction(As<ExecuteTransactionRequest>(request)));

                case "RetrieveMetadataChanges":
                    return CustomOperation(() => MetadataQuery.RetrieveMetadataChanges(As<Microsoft.Xrm.Sdk.Messages.RetrieveMetadataChangesRequest>(request)));

                case "RetrieveEntity":
                    return CustomOperation(() => MetadataQuery.RetrieveEntity(As<RetrieveEntityRequest>(request)));

                case "RetrieveAllOptionSets":
                    return CustomOperation(() => MetadataQuery.RetrieveAllOptionSets(As<RetrieveAllOptionSetsRequest>(request)));

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
                case "FetchXmlToQueryExpression":
                case "QueryExpressionToFetchXml":
                    throw CreateFault($"The {request.RequestName} message is not supported when connected through Power Platform ToolBox");

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
                Url = "RetrieveTotalRecordCount(EntityNames=@p1)?@p1=" + Uri.EscapeDataString(WebApiJson.Serialize(names))
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
            var info = Metadata.GetEntity(target.LogicalName);

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
                Url = entitySetName + "?fetchXml=" + Uri.EscapeDataString(fetchXml),
                Headers = new Dictionary<string, string> { ["Prefer"] = WebApiJson.IncludeAnnotationsPreference }
            };
        }

        private PreparedOperation PrepareRetrieve(RetrieveRequest request)
        {
            if (request.RelatedEntitiesQuery != null && request.RelatedEntitiesQuery.Count > 0)
                throw CreateFault("Retrieving related entities is not supported when connected through Power Platform ToolBox");

            var target = request.Target;
            var info = Metadata.GetEntity(target.LogicalName);
            var entity = new XElement("entity", new XAttribute("name", info.LogicalName));
            var columns = request.ColumnSet ?? new ColumnSet();

            if (columns.AllColumns)
            {
                entity.Add(new XElement("all-attributes"));
            }
            else
            {
                foreach (var col in columns.Columns.Union(new[] { info.PrimaryIdAttribute }, StringComparer.OrdinalIgnoreCase))
                    entity.Add(new XElement("attribute", new XAttribute("name", col)));
            }

            var filter = new XElement("filter");

            if (target.Id != Guid.Empty)
            {
                filter.Add(new XElement("condition",
                    new XAttribute("attribute", info.PrimaryIdAttribute),
                    new XAttribute("operator", "eq"),
                    new XAttribute("value", target.Id.ToString("D"))));
            }
            else if (target.KeyAttributes != null && target.KeyAttributes.Count > 0)
            {
                foreach (var key in target.KeyAttributes)
                {
                    filter.Add(new XElement("condition",
                        new XAttribute("attribute", key.Key),
                        new XAttribute("operator", key.Value == null ? "null" : "eq"),
                        key.Value == null ? null : new XAttribute("value", QueryExpressionConverter.FormatValue(key.Value))));
                }
            }
            else
            {
                throw CreateFault($"No ID or alternate key specified for {target.LogicalName} record");
            }

            entity.Add(filter);

            var fetchXml = new XElement("fetch", new XAttribute("top", "1"), entity).ToString(SaveOptions.DisableFormatting);
            var parser = new FetchXmlResultParser(fetchXml, Metadata);

            return Single(CreateFetchXmlRequest(info.EntitySetName, fetchXml), response =>
            {
                var results = parser.Parse((JObject)response.Json);

                if (results.Entities.Count == 0)
                    throw CreateFault($"{target.LogicalName} With Id = {target.Id} Does Not Exist", ObjectDoesNotExist);

                var result = new RetrieveResponse();
                result.Results["Entity"] = results.Entities[0];
                return result;
            });
        }

        private PreparedOperation PrepareRetrieveMultiple(RetrieveMultipleRequest request)
        {
            var fetchXml = QueryExpressionConverter.ToFetchXml(request.Query);
            var parser = new FetchXmlResultParser(fetchXml, Metadata);
            var info = Metadata.GetEntity(parser.RootEntity);

            return Single(CreateFetchXmlRequest(info.EntitySetName, fetchXml), response =>
            {
                var result = new RetrieveMultipleResponse();
                result.Results["EntityCollection"] = parser.Parse((JObject)response.Json);
                return result;
            });
        }

        private (string navigationProperty, bool isCollection) GetAssociationNavigationProperty(EntityReference target, Relationship relationship)
        {
            var rel = Metadata.GetRelationship(relationship.SchemaName);

            if (rel.IsManyToMany)
            {
                string nav;

                if (rel.Entity1LogicalName == rel.Entity2LogicalName)
                    nav = relationship.PrimaryEntityRole == EntityRole.Referenced ? rel.Entity2NavigationPropertyName : rel.Entity1NavigationPropertyName;
                else if (target.LogicalName.Equals(rel.Entity1LogicalName, StringComparison.OrdinalIgnoreCase))
                    nav = rel.Entity1NavigationPropertyName;
                else
                    nav = rel.Entity2NavigationPropertyName;

                return (nav, true);
            }

            bool targetIsReferenced;

            if (rel.ReferencedEntity == rel.ReferencingEntity)
                targetIsReferenced = relationship.PrimaryEntityRole != EntityRole.Referencing;
            else
                targetIsReferenced = target.LogicalName.Equals(rel.ReferencedEntity, StringComparison.OrdinalIgnoreCase);

            if (targetIsReferenced)
                return (rel.ReferencedEntityNavigationPropertyName, true);

            return (rel.ReferencingEntityNavigationPropertyName, false);
        }

        private PreparedOperation PrepareAssociate(AssociateRequest request)
        {
            var (nav, isCollection) = GetAssociationNavigationProperty(request.Target, request.Relationship);
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
            var (nav, isCollection) = GetAssociationNavigationProperty(request.Target, request.Relationship);
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
            var info = Metadata.GetEntity(targets.EntityName ?? targets.Entities[0].LogicalName);

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
            var info = Metadata.GetEntity(targets.EntityName ?? targets.Entities[0].LogicalName);

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

            var info = Metadata.GetEntity(targets[0].LogicalName);

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
            if (_functions.TryGetValue(request.RequestName, out var isFunction) && isFunction)
                return Single(CreateFunctionRequest(request), response => MapGenericResponse(request, response));

            var action = CreateActionRequest(request);

            if (_functions.ContainsKey(request.RequestName))
                return Single(action, response => MapGenericResponse(request, response));

            // We don't know if this message is an action or a function yet, so try it as an action first and fall
            // back to a function if that fails
            return new PreparedOperation
            {
                Requests = new List<WebApiRequest> { action },
                Map = responses => MapGenericResponse(request, responses[0]),
                Custom = () =>
                {
                    var response = SendSingle(action);

                    if (response.Status == 404 || response.Status == 405 || (response.Status == 400 && request.Parameters.Count == 0))
                    {
                        var functionResponse = SendSingle(CreateFunctionRequest(request));

                        if (functionResponse.IsSuccess)
                        {
                            _functions[request.RequestName] = true;
                            return MapGenericResponse(request, functionResponse);
                        }
                    }

                    response.EnsureSuccess();
                    _functions[request.RequestName] = false;
                    return MapGenericResponse(request, response);
                }
            };
        }

        private string GetBoundPrefix(OrganizationRequest request, out string boundParameter)
        {
            boundParameter = null;

            if (!_boundActions.TryGetValue(request.RequestName, out var binding))
                return "";

            if (!request.Parameters.TryGetValue(binding.parameter, out var value) || !(value is Guid id))
                return "";

            boundParameter = binding.parameter;
            return Serializer.GetRecordUrl(binding.entity, id) + "/Microsoft.Dynamics.CRM.";
        }

        private WebApiRequest CreateActionRequest(OrganizationRequest request)
        {
            var prefix = GetBoundPrefix(request, out var boundParameter);
            var body = new JObject();

            foreach (var param in request.Parameters)
            {
                if (_headerParameters.ContainsKey(param.Key) || param.Key == boundParameter)
                    continue;

                body[param.Key] = SerializeParameter(param.Value);
            }

            return new WebApiRequest
            {
                Method = "POST",
                Url = prefix + request.RequestName,
                Headers = GetHeaders(request),
                Body = WebApiJson.Serialize(body)
            };
        }

        private WebApiRequest CreateFunctionRequest(OrganizationRequest request)
        {
            var prefix = GetBoundPrefix(request, out var boundParameter);
            var parameters = new List<string>();
            var values = new List<string>();
            var i = 1;

            foreach (var param in request.Parameters)
            {
                if (_headerParameters.ContainsKey(param.Key) || param.Key == boundParameter)
                    continue;

                parameters.Add($"{param.Key}=@p{i}");
                values.Add($"@p{i}=" + Uri.EscapeDataString(FormatFunctionParameter(param.Value)));
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
                        var info = Metadata.GetEntity(er.LogicalName);

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

        private static OrganizationResponse MapGenericResponse(OrganizationRequest request, WebApiResponse response)
        {
            response.EnsureSuccess();

            var result = new OrganizationResponse { ResponseName = request.RequestName };

            if (response.Json is JObject json)
            {
                foreach (var prop in json.Properties())
                {
                    if (prop.Name.StartsWith("@") || prop.Name.Contains("@"))
                        continue;

                    result.Results[prop.Name] = WebApiJson.ToClrValue(prop.Value);
                }
            }

            return result;
        }

        #endregion

        #region ExecuteMultiple & ExecuteTransaction

        private (PreparedOperation op, OrganizationServiceFault fault) TryPrepare(OrganizationRequest request)
        {
            try
            {
                return (Prepare(request), null);
            }
            catch (FaultException<OrganizationServiceFault> ex)
            {
                return (null, ex.Detail);
            }
            catch (Exception ex)
            {
                return (null, new OrganizationServiceFault { Message = ex.Message });
            }
        }

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
            var prepared = requests.Select(TryPrepare).ToList();
            var items = new ExecuteMultipleResponseItemCollection();

            void AddResponse(int index, OrganizationResponse response)
            {
                if (settings.ReturnResponses)
                    items.Add(new ExecuteMultipleResponseItem { RequestIndex = index, Response = response });
            }

            void AddFault(int index, OrganizationServiceFault fault)
            {
                items.Add(new ExecuteMultipleResponseItem { RequestIndex = index, Fault = fault });
            }

            if (settings.ContinueOnError)
            {
                var i = 0;

                while (i < prepared.Count)
                {
                    var (op, prepareFault) = prepared[i];

                    if (prepareFault != null)
                    {
                        AddFault(i, prepareFault);
                        i++;
                        continue;
                    }

                    if (!op.IsBatchable || op.Custom != null)
                    {
                        try
                        {
                            AddResponse(i, op.ExecuteStandalone(this));
                        }
                        catch (Exception ex)
                        {
                            AddFault(i, GetFault(ex));
                        }

                        i++;
                        continue;
                    }

                    // Collect as many consecutive batchable operations as we can into a single batch
                    var chunk = new List<int>();
                    var count = 0;

                    while (i < prepared.Count && prepared[i].op != null && prepared[i].op.IsBatchable && prepared[i].op.Custom == null && (count == 0 || count + prepared[i].op.Requests.Count <= MaxBatchSize))
                    {
                        chunk.Add(i);
                        count += prepared[i].op.Requests.Count;
                        i++;
                    }

                    IReadOnlyList<WebApiResponse> responses;

                    try
                    {
                        responses = Send(chunk.SelectMany(index => prepared[index].op.Requests).ToList(), continueOnError: true);
                    }
                    catch (Exception ex)
                    {
                        var fault = GetFault(ex);

                        foreach (var index in chunk)
                            AddFault(index, fault);

                        continue;
                    }

                    var offset = 0;

                    foreach (var index in chunk)
                    {
                        var op2 = prepared[index].op;
                        var opResponses = responses.Skip(offset).Take(op2.Requests.Count).ToList();
                        offset += op2.Requests.Count;

                        try
                        {
                            AddResponse(index, op2.MapResponses(opResponses));
                        }
                        catch (Exception ex)
                        {
                            AddFault(index, GetFault(ex));
                        }
                    }
                }
            }
            else
            {
                var executed = false;

                // If all the requests are writes we can try them all in a single transaction. If they all succeed we've
                // got the same result as executing them sequentially. If any fail nothing will have been committed, so
                // we can fall back to executing them sequentially to get the correct result.
                if (prepared.Count > 1 &&
                    prepared.All(p => p.op != null && p.op.IsBatchable && p.op.IsAllWrites) &&
                    prepared.Sum(p => p.op.Requests.Count) <= MaxBatchSize)
                {
                    try
                    {
                        var allRequests = prepared.SelectMany(p => p.op.Requests).ToList();
                        var responses = Send(allRequests, transaction: true);

                        if (responses.Count == allRequests.Count && responses.All(r => r.IsSuccess))
                        {
                            var offset = 0;
                            var mapped = new List<OrganizationResponse>();

                            foreach (var (op, _) in prepared)
                            {
                                mapped.Add(op.MapResponses(responses.Skip(offset).Take(op.Requests.Count).ToList()));
                                offset += op.Requests.Count;
                            }

                            for (var index = 0; index < mapped.Count; index++)
                                AddResponse(index, mapped[index]);

                            executed = true;
                        }
                    }
                    catch
                    {
                        // Fall back to sequential execution
                        items.Clear();
                    }
                }

                if (!executed)
                {
                    for (var index = 0; index < prepared.Count; index++)
                    {
                        var (op, prepareFault) = prepared[index];

                        if (prepareFault != null)
                        {
                            AddFault(index, prepareFault);
                            break;
                        }

                        try
                        {
                            AddResponse(index, op.ExecuteStandalone(this));
                        }
                        catch (Exception ex)
                        {
                            AddFault(index, GetFault(ex));
                            break;
                        }
                    }
                }
            }

            var result = new ExecuteMultipleResponse();
            result.Results["Responses"] = items;
            result.Results["IsFaulted"] = items.Any(item => item.Fault != null);
            return result;
        }

        private OrganizationResponse ExecuteTransaction(ExecuteTransactionRequest request)
        {
            var requests = request.Requests ?? new OrganizationRequestCollection();
            var prepared = requests.Select(Prepare).ToList();
            var responses = new OrganizationResponseCollection();

            if (prepared.Count == 1)
            {
                responses.Add(prepared[0].ExecuteStandalone(this));
            }
            else if (prepared.Count > 1)
            {
                if (!prepared.All(op => op.IsBatchable && op.IsAllWrites))
                    throw CreateFault("ExecuteTransaction can only contain create, update, delete and other data modification requests when connected through Power Platform ToolBox");

                var allRequests = prepared.SelectMany(op => op.Requests).ToList();

                if (allRequests.Count > MaxBatchSize)
                    throw CreateFault($"ExecuteTransaction can contain a maximum of {MaxBatchSize} operations");

                var webResponses = Send(allRequests, transaction: true);
                var offset = 0;

                for (var index = 0; index < prepared.Count; index++)
                {
                    var op = prepared[index];
                    var opResponses = webResponses.Skip(offset).Take(op.Requests.Count).ToList();
                    offset += op.Requests.Count;

                    var failed = opResponses.FirstOrDefault(r => !r.IsSuccess);

                    if (failed != null)
                    {
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

                    if (opResponses.Count != op.Requests.Count)
                        throw CreateFault("The transaction failed");

                    responses.Add(op.MapResponses(opResponses));
                }
            }

            var result = new ExecuteTransactionResponse();

            if (request.ReturnResponses == true)
                result.Results["Responses"] = responses;

            return result;
        }

        #endregion
    }
}
