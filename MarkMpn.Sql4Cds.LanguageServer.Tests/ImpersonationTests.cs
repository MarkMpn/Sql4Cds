using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.Engine;
using MarkMpn.Sql4Cds.LanguageServer.Pptb;
using Microsoft.Crm.Sdk.Messages;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace MarkMpn.Sql4Cds.LanguageServer.Tests
{
    [TestClass]
    public class ImpersonationTests
    {
        [DataTestMethod]
        [DataRow(false)]
        [DataRow(true)]
        public void CallerIdIsSentAtBatchLevelWithoutAffectingRequests(bool transaction)
        {
            var transport = new RecordingTransport();
            var service = new PptbOrganizationService(transport, "https://example.invalid", "test");
            var request = new WebApiRequest
            {
                Method = "PATCH",
                Url = "accounts(" + Guid.NewGuid().ToString("D") + ")",
                Body = "{\"name\":\"test\"}",
                ContentId = "1",
                Headers = new Dictionary<string, string> { ["If-Match"] = "*" }
            };
            var requests = new[] { request, new WebApiRequest { Method = "GET", Url = "WhoAmI", ContentId = "2" } };
            var callerId = Guid.NewGuid();
            var client = (IServiceClient)service;
            client.CallerId = callerId;
            Assert.AreEqual(callerId, client.CallerId);
            Assert.IsNull(transport.Requests);
            foreach (var operation in requests)
                operation.ChangesetId = transaction ? "transaction" : null;
            service.Send(requests);
            Assert.AreEqual(transaction, transport.Transaction);
            Assert.AreEqual(callerId.ToString("D"), transport.Headers["MSCRMCallerID"]);
            Assert.IsFalse(transport.Requests.Any(r => r.Headers?.ContainsKey("MSCRMCallerID") == true));
            Assert.AreEqual("*", transport.Requests[0].Headers["If-Match"]);
            Assert.AreEqual(request.Method, transport.Requests[0].Method);
            Assert.AreEqual(request.Url, transport.Requests[0].Url);
            Assert.AreEqual(request.Body, transport.Requests[0].Body);
            Assert.AreEqual(request.ContentId, transport.Requests[0].ContentId);
            Assert.IsFalse(request.Headers.ContainsKey("MSCRMCallerID"));
            Assert.IsNull(requests[1].Headers);

            var previousHeaders = transport.Headers;
            client.CallerId = Guid.Empty;
            service.Send(requests);
            Assert.AreEqual(0, transport.Headers.Count);
            Assert.AreEqual(callerId.ToString("D"), previousHeaders["MSCRMCallerID"]);
            Assert.IsFalse(transport.Requests.Any(r => r.Headers?.ContainsKey("MSCRMCallerID") == true));
        }

        [TestMethod]
        public void ClonedConnectionsInheritCallerIdAndRevertIndependently()
        {
            var transport = new RecordingTransport();
            var service = new PptbOrganizationService(transport, "https://example.invalid", "test") { CallerId = Guid.NewGuid() };
            var clone = (PptbOrganizationService)service.Clone();
            Assert.AreEqual(service.CallerId, clone.CallerId);
            service.CallerId = Guid.Empty;
            clone.Execute(new WhoAmIRequest());
            Assert.AreEqual(clone.CallerId.ToString("D"), transport.Headers["MSCRMCallerID"]);
            Assert.IsFalse(transport.Requests.Single().Headers?.ContainsKey("MSCRMCallerID") == true);
            service.Execute(new WhoAmIRequest());
            Assert.AreEqual(0, transport.Headers.Count);
            Assert.IsFalse(transport.Requests.Single().Headers?.ContainsKey("MSCRMCallerID") == true);
            clone.CallerId = Guid.Empty;
            clone.Execute(new WhoAmIRequest());
            Assert.AreEqual(0, transport.Headers.Count);
            Assert.IsFalse(transport.Requests.Single().Headers?.ContainsKey("MSCRMCallerID") == true);
        }

        private sealed class RecordingTransport : IWebApiTransport
        {
            public IReadOnlyList<WebApiRequest> Requests;
            public bool Transaction;
            public IReadOnlyDictionary<string, string> Headers;

            public Task<IReadOnlyList<WebApiResponse>> ExecuteBatchAsync(IReadOnlyList<WebApiRequest> requests, bool continueOnError, IReadOnlyDictionary<string, string> headers, CancellationToken cancellationToken)
            {
                Requests = requests;
                Transaction = requests.All(r => r.ChangesetId != null);
                Headers = headers;
                return Task.FromResult<IReadOnlyList<WebApiResponse>>(requests.Select(r => new WebApiResponse
                {
                    Status = 200,
                    ContentId = r.ContentId,
                    Body = "{\"UserId\":\"00000000-0000-0000-0000-000000000001\",\"BusinessUnitId\":\"00000000-0000-0000-0000-000000000002\",\"OrganizationId\":\"00000000-0000-0000-0000-000000000003\"}"
                }).ToArray());
            }
        }
    }
}
