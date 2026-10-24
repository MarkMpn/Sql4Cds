using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceModel;
using System.Threading;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.LanguageServer.Pptb;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Newtonsoft.Json.Linq;

namespace MarkMpn.Sql4Cds.LanguageServer.Tests
{
    [TestClass]
    public class BatchExecutionTests
    {
        private static PptbOrganizationService Service(RecordingTransport transport)
        {
            var service = new PptbOrganizationService(transport, "https://example.invalid", "test");
            service.SetMetadataCache(MetadataTestHelper.Cache(new JObject
            {
                ["LogicalName"] = "account",
                ["EntitySetName"] = "accounts",
                ["PrimaryIdAttribute"] = "accountid",
                ["ManyToManyRelationships"] = new JArray(new JObject
                {
                    ["SchemaName"] = "account_related",
                    ["Entity1LogicalName"] = "account",
                    ["Entity2LogicalName"] = "account",
                    ["Entity1NavigationPropertyName"] = "related_accounts",
                    ["Entity2NavigationPropertyName"] = "related_accounts_reverse"
                })
            }));
            return service;
        }

        private static DeleteRequest Delete() =>
            new DeleteRequest { Target = new EntityReference("account", Guid.NewGuid()) };

        private static AssociateRequest Associate() => new AssociateRequest
        {
            Target = new EntityReference("account", Guid.NewGuid()),
            Relationship = new Relationship("account_related"),
            RelatedEntities = new EntityReferenceCollection
            {
                new EntityReference("account", Guid.NewGuid()),
                new EntityReference("account", Guid.NewGuid())
            }
        };

        [DataTestMethod]
        [DataRow(false)]
        [DataRow(true)]
        public void StandaloneMultiOperationWritesNeverUseTransactions(bool fail)
        {
            var transport = new RecordingTransport { FailAt = fail ? 1 : -1 };
            var service = Service(transport);
            if (fail)
                Assert.ThrowsException<FaultException<OrganizationServiceFault>>(() => service.Execute(Associate()));
            else
                Assert.IsInstanceOfType<AssociateResponse>(service.Execute(Associate()));

            Assert.AreEqual(1, transport.Calls.Count);
            Assert.AreEqual(2, transport.Calls.Single().Requests.Count);
            Assert.IsFalse(transport.Calls.Single().Transaction);
        }

        [DataTestMethod]
        [DataRow(false, false, false)]
        [DataRow(false, false, true)]
        [DataRow(false, true, false)]
        [DataRow(false, true, true)]
        [DataRow(true, false, false)]
        [DataRow(true, false, true)]
        [DataRow(true, true, false)]
        [DataRow(true, true, true)]
        public void ExecuteMultipleUsesOnlyNonTransactionalExecution(bool continueOnError, bool fail, bool returnResponses)
        {
            var transport = new RecordingTransport { FailAt = fail ? 1 : -1 };
            var result = (ExecuteMultipleResponse)Service(transport).Execute(new ExecuteMultipleRequest
            {
                Requests = new OrganizationRequestCollection { Delete(), Delete(), Delete() },
                Settings = new ExecuteMultipleSettings { ContinueOnError = continueOnError, ReturnResponses = returnResponses }
            });

            Assert.IsTrue(transport.Calls.All(c => !c.Transaction));
            Assert.AreEqual(1, transport.Calls.Count);
            Assert.AreEqual(3, transport.Calls.Single().Requests.Count);
            Assert.AreEqual(continueOnError, transport.ContinueOnError.Single());
            Assert.AreEqual(!continueOnError && fail ? 2 : 3, transport.ExecutedCount);
            Assert.AreEqual(fail, result.IsFaulted);
            var expectedIndexes = Enumerable.Range(0, !continueOnError && fail ? 2 : 3)
                .Where(index => returnResponses || (fail && index == 1)).ToArray();
            CollectionAssert.AreEqual(expectedIndexes, result.Responses.Select(r => r.RequestIndex).ToArray());
            if (fail)
                Assert.AreEqual("Write failed", result.Responses.Single(r => r.RequestIndex == 1).Fault.Message);
        }

        [DataTestMethod]
        [DataRow(1)]
        [DataRow(2)]
        public void ExplicitTransactionsUseTransactionsEvenForOneWrite(int count)
        {
            var transport = new RecordingTransport();
            var requests = new OrganizationRequestCollection();
            requests.AddRange(Enumerable.Range(0, count).Select(_ => Delete()));
            var result = (ExecuteTransactionResponse)Service(transport).Execute(new ExecuteTransactionRequest
            {
                Requests = requests,
                ReturnResponses = true
            });

            Assert.AreEqual(1, transport.Calls.Count);
            Assert.IsTrue(transport.Calls.Single().Transaction);
            Assert.AreEqual(count, transport.Calls.Single().Requests.Count);
            Assert.AreEqual(count, result.Responses.Count);
        }

        [TestMethod]
        public void ExplicitTransactionIncludesExpandedOperationsAndDoesNotReplayFailures()
        {
            var transport = new RecordingTransport { FailAt = 2 };
            var exception = Assert.ThrowsException<FaultException<OrganizationServiceFault>>(() =>
                Service(transport).Execute(new ExecuteTransactionRequest
                {
                    Requests = new OrganizationRequestCollection { Delete(), Associate() },
                    ReturnResponses = true
                }));

            Assert.AreEqual(1, transport.Calls.Count);
            Assert.IsTrue(transport.Calls.Single().Transaction);
            Assert.AreEqual(3, transport.Calls.Single().Requests.Count);
            var fault = (ExecuteTransactionFault)exception.Detail;
            Assert.AreEqual(1, fault.FaultedRequestIndex);
            Assert.AreEqual("Write failed", fault.Message);
        }

        [TestMethod]
        public void StopOnErrorMapsPartialExpandedOperationAndSkipsLaterRequests()
        {
            var transport = new RecordingTransport { FailAt = 1 };
            var result = (ExecuteMultipleResponse)Service(transport).Execute(new ExecuteMultipleRequest
            {
                Requests = new OrganizationRequestCollection { Delete(), Associate(), Delete() },
                Settings = new ExecuteMultipleSettings { ContinueOnError = false, ReturnResponses = true }
            });

            Assert.AreEqual(1, transport.Calls.Count);
            Assert.AreEqual(4, transport.Calls.Single().Requests.Count);
            Assert.AreEqual(2, transport.ExecutedCount);
            Assert.AreEqual(2, result.Responses.Count);
            Assert.IsInstanceOfType<DeleteResponse>(result.Responses[0].Response);
            Assert.AreEqual(1, result.Responses[1].RequestIndex);
            Assert.AreEqual("Write failed", result.Responses[1].Fault.Message);
        }

        [DataTestMethod]
        [DataRow(false)]
        [DataRow(true)]
        public void PreparationFaultThrowsBeforeSendingAnyBatchOperations(bool continueOnError)
        {
            var transport = new RecordingTransport();
            var exception = Assert.ThrowsException<FaultException<OrganizationServiceFault>>(() => Service(transport).Execute(new ExecuteMultipleRequest
            {
                Requests = new OrganizationRequestCollection
                {
                    Delete(),
                    new OrganizationRequest("DeleteMultiple"),
                    Delete()
                },
                Settings = new ExecuteMultipleSettings { ContinueOnError = continueOnError, ReturnResponses = true }
            }));

            StringAssert.Contains(exception.Detail.Message, "Missing Targets parameter");
            Assert.AreEqual(0, transport.Calls.Count);
        }

        [DataTestMethod]
        [DataRow(false)]
        [DataRow(true)]
        public void OversizedBatchIsSentIntactAndServerFaultIsPropagated(bool continueOnError)
        {
            var fault = new OrganizationServiceFault { ErrorCode = 12345, Message = "Server batch size limit exceeded" };
            fault.ErrorDetails["MaxBatchSize"] = 1000;
            var transport = new RecordingTransport { BatchFault = fault };
            var requests = new OrganizationRequestCollection();
            requests.AddRange(Enumerable.Range(0, 1001).Select(_ => Delete()));
            var exception = Assert.ThrowsException<FaultException<OrganizationServiceFault>>(() =>
                Service(transport).Execute(new ExecuteMultipleRequest
                {
                    Requests = requests,
                    Settings = new ExecuteMultipleSettings { ContinueOnError = continueOnError }
                }));

            Assert.AreSame(fault, exception.Detail);
            Assert.AreEqual(1, transport.Calls.Count);
            Assert.AreEqual(1001, transport.Calls.Single().Requests.Count);
            Assert.AreEqual(continueOnError, transport.ContinueOnError.Single());
        }

        [DataTestMethod]
        [DataRow(false)]
        [DataRow(true)]
        public void NestedContainersCannotBeTranslatedAndAreNotExecuted(bool transaction)
        {
            var transport = new RecordingTransport();
            OrganizationRequest nested = transaction
                ? new ExecuteTransactionRequest { Requests = new OrganizationRequestCollection { Delete() } }
                : new ExecuteMultipleRequest { Requests = new OrganizationRequestCollection { Delete() } };
            var exception = Assert.ThrowsException<FaultException<OrganizationServiceFault>>(() =>
                Service(transport).Execute(new ExecuteMultipleRequest
                {
                    Requests = new OrganizationRequestCollection { Delete(), nested, Delete() },
                    Settings = new ExecuteMultipleSettings { ContinueOnError = true }
                }));
            StringAssert.Contains(exception.Detail.Message, "cannot contain");
            Assert.AreEqual(0, transport.Calls.Count);
        }

        private sealed class RecordingTransport : IWebApiTransport
        {
            public List<(IReadOnlyList<WebApiRequest> Requests, bool Transaction)> Calls { get; } =
                new List<(IReadOnlyList<WebApiRequest>, bool)>();
            public int FailAt { get; set; } = -1;
            public OrganizationServiceFault BatchFault { get; set; }
            public List<bool> ContinueOnError { get; } = new List<bool>();
            public int ExecutedCount => _index;
            private int _index;

            public Task<IReadOnlyList<WebApiResponse>> ExecuteBatchAsync(IReadOnlyList<WebApiRequest> requests, bool transaction, bool continueOnError, IReadOnlyDictionary<string, string> headers, CancellationToken cancellationToken)
            {
                Calls.Add((requests.ToArray(), transaction));
                ContinueOnError.Add(continueOnError);
                if (BatchFault != null)
                    throw new FaultException<OrganizationServiceFault>(BatchFault, new FaultReason(BatchFault.Message));
                var responses = new List<WebApiResponse>();
                foreach (var request in requests)
                {
                    var response = _index++ == FailAt
                        ? new WebApiResponse { Status = 400, Body = "{\"error\":{\"message\":\"Write failed\"}}" }
                        : new WebApiResponse { Status = 204 };
                    responses.Add(response);
                    if (!response.IsSuccess && (!continueOnError || transaction))
                        break;
                }
                return Task.FromResult<IReadOnlyList<WebApiResponse>>(responses);
            }
        }
    }
}
