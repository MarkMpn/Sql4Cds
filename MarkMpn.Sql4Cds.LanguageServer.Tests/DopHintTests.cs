using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.LanguageServer.Pptb;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json;

namespace MarkMpn.Sql4Cds.LanguageServer.Tests
{
    [TestClass]
    public class DopHintTests
    {
        [DataTestMethod]
        [DataRow(false)]
        [DataRow(true)]
        public void BatchHeadersSupplyDopHint(bool transaction)
        {
            var response = JsonConvert.DeserializeObject<WebApiResponse>(
                "{\"status\":204,\"headers\":{\"x-ms-dop-hint\":\"2\"},\"batchHeaders\":{\"X-MS-DOP-HINT\":\"8\"}}");
            var service = new PptbOrganizationService(new ResponseTransport(response), "https://example.invalid", "test");
            var clone = (PptbOrganizationService)service.Clone();

            service.Send(new[] { new WebApiRequest { Method = "GET", Url = "accounts" } }, transaction);

            Assert.AreEqual(8, service.RecommendedDegreesOfParallelism);
            Assert.AreEqual(8, clone.RecommendedDegreesOfParallelism);
            Assert.AreEqual("2", response.GetHeader("X-MS-DOP-HINT"));
        }

        [DataTestMethod]
        [DataRow(null)]
        [DataRow("")]
        [DataRow("invalid")]
        [DataRow("0")]
        [DataRow("-1")]
        [DataRow("2147483648")]
        public void MissingOrInvalidBatchHintPreservesRecommendation(string hint)
        {
            var response = new WebApiResponse
            {
                Status = 204,
                Headers = new Dictionary<string, string> { ["x-ms-dop-hint"] = "2" },
                BatchHeaders = new Dictionary<string, string> { ["x-ms-dop-hint"] = "8" }
            };
            var service = new PptbOrganizationService(new ResponseTransport(response), "https://example.invalid", "test");
            var request = new WebApiRequest { Method = "GET", Url = "accounts" };
            service.SendSingle(request);

            response.BatchHeaders = hint == null ? null : new Dictionary<string, string> { ["x-ms-dop-hint"] = hint };
            service.SendSingle(request);
            Assert.AreEqual(8, service.RecommendedDegreesOfParallelism);

            response.BatchHeaders = new Dictionary<string, string>();
            service.SendSingle(request);
            Assert.AreEqual(8, service.RecommendedDegreesOfParallelism);
        }

        private sealed class ResponseTransport : IWebApiTransport
        {
            private readonly WebApiResponse _response;

            public ResponseTransport(WebApiResponse response)
            {
                _response = response;
            }

            public Task<IReadOnlyList<WebApiResponse>> ExecuteBatchAsync(IReadOnlyList<WebApiRequest> requests, bool transaction, bool continueOnError, IReadOnlyDictionary<string, string> headers, CancellationToken cancellationToken)
            {
                return Task.FromResult<IReadOnlyList<WebApiResponse>>(new[] { _response });
            }
        }
    }
}
