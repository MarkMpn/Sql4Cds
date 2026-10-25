using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceModel;
using System.Threading;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.LanguageServer.Pptb;
using Microsoft.Crm.Sdk.Messages;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Metadata;
using Microsoft.Xrm.Sdk.Metadata.Query;
using Newtonsoft.Json.Linq;

namespace MarkMpn.Sql4Cds.LanguageServer.Tests
{
    [TestClass]
    public class MetadataBatchTests
    {
        private static OrganizationRequest[] MetadataRequests() => new OrganizationRequest[]
        {
            new RetrieveMetadataChangesRequest
            {
                Query = new EntityQueryExpression { Properties = new MetadataPropertiesExpression("LogicalName") },
                ClientVersionStamp = "client!stamp",
                DeletedMetadataFilters = DeletedMetadataFilters.All
            },
            new RetrieveEntityRequest { LogicalName = "account", EntityFilters = EntityFilters.All },
            new RetrieveAllOptionSetsRequest()
        };

        [TestMethod]
        public void PreparingMetadataDoesNotSendRequests()
        {
            var transport = new BatchTransport();
            var service = Service(transport);
            var operations = MetadataRequests().Select(service.Prepare).ToArray();

            Assert.AreEqual(0, transport.Batches.Count);
            foreach (var operation in operations)
            {
                Assert.IsNotNull(operation.Requests);
                Assert.IsNull(operation.Custom);
                Assert.AreEqual("GET", operation.Requests.Single().Method);
            }

            var changes = operations[0].Requests.Single();
            StringAssert.Contains(changes.Url, "ClientVersionStamp=@p2,DeletedMetadataFilters=@p3");
            StringAssert.Contains(Uri.UnescapeDataString(changes.Url), "@p2='client!stamp'");
            StringAssert.Contains(Uri.UnescapeDataString(changes.Url), "Microsoft.Dynamics.CRM.DeletedMetadataFilters'All'");
            Assert.AreEqual("LogicalName", MetadataTestHelper.Query(changes)["Properties"]["PropertyNames"][0].Value<string>());
            var entityQuery = MetadataTestHelper.Query(operations[1].Requests.Single());
            Assert.AreEqual("account", entityQuery["Criteria"]["Conditions"][0]["Value"].Value<string>("Value"));
            Assert.IsTrue(entityQuery["AttributeQuery"]["Properties"].Value<bool>("AllProperties"));
            Assert.IsTrue(entityQuery["RelationshipQuery"]["Properties"].Value<bool>("AllProperties"));
            Assert.AreEqual("GlobalOptionSetDefinitions", operations[2].Requests.Single().Url);
        }

        [TestMethod]
        public void MetadataAndStandardReadsShareOneBatch()
        {
            var transport = new BatchTransport();
            var service = Service(transport);
            var requests = new OrganizationRequestCollection { new WhoAmIRequest() };
            requests.AddRange(MetadataRequests());
            requests.Add(new RetrieveVersionRequest());

            var result = (ExecuteMultipleResponse)service.Execute(new ExecuteMultipleRequest
            {
                Requests = requests,
                Settings = new ExecuteMultipleSettings { ContinueOnError = true, ReturnResponses = true }
            });

            Assert.AreEqual(1, transport.Batches.Count);
            Assert.AreEqual(5, transport.Batches.Single().Count);
            Assert.IsFalse(transport.Transactions.Single());
            Assert.IsFalse(result.IsFaulted);
            CollectionAssert.AreEqual(new[] { 0, 1, 2, 3, 4 }, result.Responses.Select(r => r.RequestIndex).ToArray());
            Assert.IsInstanceOfType<WhoAmIResponse>(result.Responses[0].Response);
            var changes = (RetrieveMetadataChangesResponse)result.Responses[1].Response;
            Assert.AreEqual("server!stamp", changes.ServerVersionStamp);
            Assert.AreEqual("account", changes.EntityMetadata.Single().LogicalName);
            Assert.IsNotNull(changes.DeletedMetadata);
            Assert.AreEqual("account", ((RetrieveEntityResponse)result.Responses[2].Response).EntityMetadata.LogicalName);
            Assert.AreEqual("sample_options", ((RetrieveAllOptionSetsResponse)result.Responses[3].Response).OptionSetMetadata.Single().Name);
            Assert.AreEqual("9.2.0.0", ((RetrieveVersionResponse)result.Responses[4].Response).Version);
        }

        [TestMethod]
        public void MetadataErrorsAreMappedWithoutLosingOtherBatchResults()
        {
            var transport = new BatchTransport { FailOptionSets = true };
            var result = (ExecuteMultipleResponse)Service(transport).Execute(new ExecuteMultipleRequest
            {
                Requests = new OrganizationRequestCollection
                {
                    new RetrieveEntityRequest { LogicalName = "missing" },
                    new RetrieveAllOptionSetsRequest(),
                    new RetrieveVersionRequest()
                },
                Settings = new ExecuteMultipleSettings { ContinueOnError = true, ReturnResponses = true }
            });

            Assert.AreEqual(1, transport.Batches.Count);
            Assert.AreEqual(3, result.Responses.Count);
            Assert.AreEqual(unchecked((int)0x80040217), result.Responses[0].Fault.ErrorCode);
            StringAssert.Contains(result.Responses[0].Fault.Message, "missing");
            Assert.AreEqual(429, result.Responses[1].Fault.ErrorCode);
            Assert.AreEqual(TimeSpan.FromSeconds(12), result.Responses[1].Fault.ErrorDetails["Retry-After"]);
            Assert.IsInstanceOfType<RetrieveVersionResponse>(result.Responses[2].Response);
        }

        [TestMethod]
        public void ClientFiltersAreAppliedIndependentlyWhenMappingMetadata()
        {
            var transport = new BatchTransport();
            var service = Service(transport);
            var query = new EntityQueryExpression
            {
                Criteria = new MetadataFilterExpression(),
                Properties = new MetadataPropertiesExpression("LogicalName")
            };
            query.Criteria.Conditions.Add(new MetadataConditionExpression("OwnershipType", MetadataConditionOperator.Equals, OwnershipTypes.UserOwned));
            var operation = service.Prepare(new RetrieveMetadataChangesRequest { Query = query });
            var serverQuery = MetadataTestHelper.Query(operation.Requests.Single());
            Assert.IsNull(serverQuery["Criteria"]);
            CollectionAssert.Contains(serverQuery["Properties"]["PropertyNames"].Values<string>().ToArray(), "OwnershipType");

            var response = MetadataTestHelper.Response(
                new JObject { ["LogicalName"] = "account", ["OwnershipType"] = "UserOwned" },
                new JObject { ["LogicalName"] = "organization", ["OwnershipType"] = "OrganizationOwned" });
            var result = (RetrieveMetadataChangesResponse)operation.MapResponses(new[] { response });
            Assert.AreEqual("account", result.EntityMetadata.Single().LogicalName);
            Assert.AreEqual(0, transport.Batches.Count);
        }

        [TestMethod]
        public void RetrieveEntityPreservesIdAndPropertySelection()
        {
            var id = Guid.NewGuid();
            var operation = Service(new BatchTransport()).Prepare(new RetrieveEntityRequest
            {
                MetadataId = id,
                EntityFilters = EntityFilters.Entity
            });
            var query = MetadataTestHelper.Query(operation.Requests.Single());
            Assert.AreEqual("MetadataId", query["Criteria"]["Conditions"][0].Value<string>("PropertyName"));
            Assert.AreEqual(id.ToString("D"), query["Criteria"]["Conditions"][0]["Value"].Value<string>("Value"));
            var properties = query["Properties"]["PropertyNames"].Values<string>().ToArray();
            CollectionAssert.DoesNotContain(properties, "Attributes");
            CollectionAssert.DoesNotContain(properties, "Privileges");
            CollectionAssert.DoesNotContain(properties, "OneToManyRelationships");
            Assert.IsNull(query["AttributeQuery"]);
            Assert.IsNull(query["RelationshipQuery"]);
        }

        [TestMethod]
        public void StandaloneMetadataStillUsesTheSameMappings()
        {
            var transport = new BatchTransport();
            var service = Service(transport);
            foreach (var request in MetadataRequests())
                Assert.IsNotNull(service.Execute(request));

            Assert.AreEqual("account", service.MetadataQuery.RetrieveEntity(new RetrieveEntityRequest { LogicalName = "account" }).EntityMetadata.LogicalName);
            Assert.AreEqual("server!stamp", service.MetadataQuery.RetrieveMetadataChanges(new RetrieveMetadataChangesRequest()).ServerVersionStamp);
            Assert.AreEqual("sample_options", service.MetadataQuery.RetrieveAllOptionSets(new RetrieveAllOptionSetsRequest()).OptionSetMetadata.Single().Name);
            Assert.AreEqual(6, transport.Batches.Count);
            Assert.IsTrue(transport.Batches.All(b => b.Count == 1));
            Assert.IsTrue(transport.Transactions.All(t => !t));
        }

        [TestMethod]
        public void ExplicitMetadataTransactionsAreSentToTheServer()
        {
            var transport = new BatchTransport();
            var requests = new OrganizationRequestCollection();
            requests.AddRange(MetadataRequests());
            Service(transport).Execute(new ExecuteTransactionRequest { Requests = requests });
            Assert.AreEqual(1, transport.Batches.Count);
            Assert.IsTrue(transport.Transactions.Single());
            Assert.AreEqual(3, transport.Batches.Single().Count);
        }

        private static PptbOrganizationService Service(BatchTransport transport) =>
            new PptbOrganizationService(transport, "https://example.invalid", "test");

        private sealed class BatchTransport : IWebApiTransport
        {
            public List<IReadOnlyList<WebApiRequest>> Batches { get; } = new List<IReadOnlyList<WebApiRequest>>();
            public List<bool> Transactions { get; } = new List<bool>();
            public bool FailOptionSets { get; set; }

            public Task<IReadOnlyList<WebApiResponse>> ExecuteBatchAsync(IReadOnlyList<WebApiRequest> requests, bool continueOnError, IReadOnlyDictionary<string, string> headers, CancellationToken cancellationToken)
            {
                Batches.Add(requests.ToArray());
                Transactions.Add(requests.All(r => r.ChangesetId != null));
                return Task.FromResult<IReadOnlyList<WebApiResponse>>(requests.Select(Response).ToArray());
            }

            private WebApiResponse Response(WebApiRequest request)
            {
                if (request.Url.StartsWith("RetrieveMetadataChanges(", StringComparison.Ordinal))
                {
                    var response = MetadataTestHelper.Response(
                        Uri.UnescapeDataString(request.Url).Contains("missing")
                            ? Array.Empty<JObject>()
                            : new[] { new JObject { ["LogicalName"] = "account" } });
                    var json = JObject.Parse(response.Body);
                    json["ServerVersionStamp"] = "server!stamp";
                    response.Body = json.ToString();
                    return response;
                }

                JObject body;
                switch (request.Url)
                {
                    case "WhoAmI":
                        body = new JObject { ["UserId"] = Guid.NewGuid(), ["BusinessUnitId"] = Guid.NewGuid(), ["OrganizationId"] = Guid.NewGuid() };
                        break;
                    case "RetrieveVersion":
                        body = new JObject { ["Version"] = "9.2.0.0" };
                        break;
                    case "GlobalOptionSetDefinitions":
                        if (FailOptionSets)
                        {
                            return new WebApiResponse
                            {
                                Status = 429,
                                Body = "{\"error\":{\"message\":\"Rate limited\"}}",
                                BatchHeaders = new Dictionary<string, string> { ["Retry-After"] = "12" }
                            };
                        }
                        body = new JObject
                        {
                            ["value"] = new JArray(new JObject
                            {
                                ["@odata.type"] = "#Microsoft.Dynamics.CRM.OptionSetMetadata",
                                ["Name"] = "sample_options"
                            })
                        };
                        break;
                    default:
                        throw new InvalidOperationException("Unexpected request: " + request.Url);
                }
                return new WebApiResponse { Status = 200, Body = body.ToString() };
            }
        }
    }
}
