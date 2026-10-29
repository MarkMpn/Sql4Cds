using System;
using System.Collections.Generic;
using System.ServiceModel;
using System.Threading;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.LanguageServer.Pptb;
using Microsoft.Crm.Sdk.Messages;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.Xrm.Sdk;
using Newtonsoft.Json;

namespace MarkMpn.Sql4Cds.LanguageServer.Tests
{
    [TestClass]
    public class ThrottlingTests
    {
        [DataTestMethod]
        [DataRow(null)]
        [DataRow("{\"error\":{\"code\":\"0x80072322\",\"message\":\"Throttled\"}}")]
        [DataRow("{\"error\":{\"code\":\"123\",\"message\":\"Throttled\"}}")]
        public void ThrottlingResponseUsesBatchRetryDelay(string body)
        {
            var response = JsonConvert.DeserializeObject<WebApiResponse>(
                "{\"status\":429,\"statusText\":\"Too Many Requests\",\"headers\":{\"Retry-After\":\"2\"},\"batchHeaders\":{\"rEtRy-AfTeR\":\"12\"}}");
            response.Body = body;
            var service = new PptbOrganizationService(new ResponseTransport(response), "https://example.invalid", "test");

            var exception = Assert.ThrowsException<FaultException<OrganizationServiceFault>>(
                () => service.Execute(new WhoAmIRequest()));

            Assert.AreEqual(429, exception.Detail.ErrorCode);
            Assert.IsInstanceOfType<TimeSpan>(exception.Detail.ErrorDetails["Retry-After"]);
            Assert.AreEqual(TimeSpan.FromSeconds(12), exception.Detail.ErrorDetails["Retry-After"]);
            Assert.AreEqual(body == null ? "429 Too Many Requests" : "Throttled", exception.Detail.Message);
        }

        [DataTestMethod]
        [DataRow("0", 0)]
        [DataRow("60", 60)]
        [DataRow("2147483647", 2147483647)]
        public void RetryDelayIsConvertedFromSeconds(string header, int seconds)
        {
            var response = new WebApiResponse
            {
                Status = 429,
                BatchHeaders = new Dictionary<string, string> { ["Retry-After"] = header }
            };

            var exception = Assert.ThrowsException<FaultException<OrganizationServiceFault>>(response.EnsureSuccess);

            Assert.AreEqual(429, exception.Detail.ErrorCode);
            Assert.AreEqual(TimeSpan.FromSeconds(seconds), exception.Detail.ErrorDetails["Retry-After"]);
        }

        [DataTestMethod]
        [DataRow(null)]
        [DataRow("")]
        [DataRow("invalid")]
        [DataRow("-1")]
        [DataRow("2147483648")]
        public void MissingOrInvalidBatchDelayStillProducesThrottlingFault(string header)
        {
            var response = new WebApiResponse
            {
                Status = 429,
                Headers = new Dictionary<string, string> { ["Retry-After"] = "2" },
                BatchHeaders = header == null ? null : new Dictionary<string, string> { ["Retry-After"] = header }
            };

            var exception = Assert.ThrowsException<FaultException<OrganizationServiceFault>>(response.EnsureSuccess);

            Assert.AreEqual(429, exception.Detail.ErrorCode);
            Assert.IsFalse(exception.Detail.ErrorDetails.ContainsKey("Retry-After"));
        }

        [TestMethod]
        public void OtherErrorsPreserveTheirCodeAndDoNotSetRetryDelay()
        {
            var response = new WebApiResponse
            {
                Status = 400,
                Body = "{\"error\":{\"code\":\"0x80040217\",\"message\":\"Not found\"}}",
                BatchHeaders = new Dictionary<string, string> { ["Retry-After"] = "12" }
            };

            var exception = Assert.ThrowsException<FaultException<OrganizationServiceFault>>(response.EnsureSuccess);

            Assert.AreEqual(unchecked((int)0x80040217), exception.Detail.ErrorCode);
            Assert.AreEqual("Not found", exception.Detail.Message);
            Assert.IsFalse(exception.Detail.ErrorDetails.ContainsKey("Retry-After"));
        }

        private sealed class ResponseTransport : IWebApiTransport
        {
            private readonly WebApiResponse _response;

            public ResponseTransport(WebApiResponse response)
            {
                _response = response;
            }

            public Task<IReadOnlyList<WebApiResponse>> ExecuteBatchAsync(IReadOnlyList<WebApiRequest> requests, bool continueOnError, IReadOnlyDictionary<string, string> headers, CancellationToken cancellationToken)
            {
                return Task.FromResult<IReadOnlyList<WebApiResponse>>(new[] { _response });
            }
        }
    }
}
