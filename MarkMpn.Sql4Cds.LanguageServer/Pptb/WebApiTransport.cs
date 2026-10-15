using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.ServiceModel;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk;
using Newtonsoft.Json.Linq;
using StreamJsonRpc;

namespace MarkMpn.Sql4Cds.LanguageServer.Pptb
{
    /// <summary>
    /// A single Dataverse Web API request, expressed relative to <c>api/data/v9.2/</c>
    /// </summary>
    /// <remarks>
    /// This matches the shape of the PPTB <c>dataverseAPI.executeBatch</c> request so the client can pass it
    /// straight through without any transformation.
    /// </remarks>
    public class WebApiRequest
    {
        public string Method { get; set; }

        public string Url { get; set; }

        public Dictionary<string, string> Headers { get; set; }

        /// <summary>
        /// The pre-serialized JSON body. Serializing on the server side avoids any loss of precision in
        /// numeric values when the request passes through JavaScript.
        /// </summary>
        public string Body { get; set; }

        public string ContentId { get; set; }

        [Newtonsoft.Json.JsonIgnore]
        public bool IsWrite => Method != "GET";
    }

    /// <summary>
    /// The response to a single <see cref="WebApiRequest"/>
    /// </summary>
    public class WebApiResponse
    {
        public string ContentId { get; set; }

        public int Status { get; set; }

        public string StatusText { get; set; }

        public Dictionary<string, string> Headers { get; set; }

        /// <summary>
        /// The response body, re-serialized as JSON text by the client
        /// </summary>
        /// <remarks>
        /// The body is passed as text rather than a JSON object so it can be parsed here without
        /// automatic date conversion, which would otherwise change the values of string attributes
        /// that happen to look like dates.
        /// </remarks>
        public string Body { get; set; }

        private JToken _json;
        private bool _jsonParsed;

        [Newtonsoft.Json.JsonIgnore]
        public JToken Json
        {
            get
            {
                if (!_jsonParsed)
                {
                    _json = WebApiJson.Parse(Body);
                    _jsonParsed = true;
                }

                return _json;
            }
            set
            {
                _json = value;
                _jsonParsed = true;
            }
        }

        public bool IsSuccess => Status >= 200 && Status < 300;

        public string GetHeader(string name)
        {
            if (Headers == null)
                return null;

            foreach (var kvp in Headers)
            {
                if (kvp.Key.Equals(name, StringComparison.OrdinalIgnoreCase))
                    return kvp.Value;
            }

            return null;
        }

        /// <summary>
        /// Converts an error response to the equivalent SDK fault
        /// </summary>
        public OrganizationServiceFault ToFault()
        {
            var fault = new OrganizationServiceFault
            {
                Message = $"{Status} {StatusText}".Trim()
            };

            if (Json is JObject obj && obj["error"] is JObject error)
            {
                var code = error.Value<string>("code");

                if (code != null && code.StartsWith("0x", StringComparison.OrdinalIgnoreCase) && Int32.TryParse(code.Substring(2), System.Globalization.NumberStyles.HexNumber, null, out var errorCode))
                    fault.ErrorCode = errorCode;
                else if (Int32.TryParse(code, out errorCode))
                    fault.ErrorCode = errorCode;

                fault.Message = error.Value<string>("message") ?? fault.Message;
            }
            else if (Json is JValue value && value.Type == JTokenType.String && !String.IsNullOrWhiteSpace((string)value))
            {
                fault.Message = (string)value;
            }

            // Map service protection limits to the error codes the engine recognises for retries
            if (Status == 429 && fault.ErrorCode == 0)
                fault.ErrorCode = -2147015902;

            if (Status == 429)
            {
                var retryAfter = GetHeader("retry-after");

                if (retryAfter != null && Int32.TryParse(retryAfter, out var seconds))
                    fault.ErrorDetails["Retry-After"] = TimeSpan.FromSeconds(seconds);
            }

            return fault;
        }

        public void EnsureSuccess()
        {
            if (!IsSuccess)
            {
                var fault = ToFault();
                throw new FaultException<OrganizationServiceFault>(fault, new FaultReason(fault.Message));
            }
        }
    }

    /// <summary>
    /// Sends Web API requests to Dataverse
    /// </summary>
    public interface IWebApiTransport
    {
        /// <summary>
        /// Executes a set of requests in a single batch
        /// </summary>
        /// <param name="requests">The requests to execute</param>
        /// <param name="transaction">Indicates if the requests should be executed in a single atomic change set</param>
        /// <param name="continueOnError">Indicates if later requests should still be executed after an earlier request fails</param>
        /// <param name="headers">Additional headers for the batch as a whole</param>
        /// <param name="cancellationToken">A token to cancel the request</param>
        /// <returns>The responses to each request, in the same order as the requests</returns>
        Task<IReadOnlyList<WebApiResponse>> ExecuteBatchAsync(IReadOnlyList<WebApiRequest> requests, bool transaction, bool continueOnError, IReadOnlyDictionary<string, string> headers, CancellationToken cancellationToken);
    }

    /// <summary>
    /// Sends Web API requests back to the PPTB client over the JSON-RPC connection so it can execute them
    /// using <c>dataverseAPI.executeBatch</c>, as the worker never has access to the access token.
    /// </summary>
    class JsonRpcWebApiTransport : IWebApiTransport
    {
        public const string MethodName = "dataverse/executeBatch";

        private readonly JsonRpc _rpc;
        private readonly PptbRpcChunking _chunking;
        private readonly object _target;

        public JsonRpcWebApiTransport(JsonRpc rpc, PptbRpcChunking chunking, object target)
        {
            _rpc = rpc;
            _chunking = chunking;
            _target = target;
        }

        public async Task<IReadOnlyList<WebApiResponse>> ExecuteBatchAsync(IReadOnlyList<WebApiRequest> requests, bool transaction, bool continueOnError, IReadOnlyDictionary<string, string> headers, CancellationToken cancellationToken)
        {
            for (var i = 0; i < requests.Count; i++)
                requests[i].ContentId = (i + 1).ToString();

            var request = new ExecuteBatchParams
            {
                Target = _target,
                Requests = requests.ToArray(),
                Transaction = transaction,
                ContinueOnError = continueOnError,
                Headers = headers
            };

            var result = _chunking.MaxMessageSize.HasValue
                ? await _chunking.RequestAsync<ExecuteBatchParams, WebApiResponse[]>(request, cancellationToken).ConfigureAwait(false)
                : await _rpc.InvokeWithParameterObjectAsync<WebApiResponse[]>(MethodName, request, cancellationToken).ConfigureAwait(false);
            return result ?? Array.Empty<WebApiResponse>();
        }

        class ExecuteBatchParams
        {
            public object Target { get; set; }

            public WebApiRequest[] Requests { get; set; }

            public bool Transaction { get; set; }

            public bool ContinueOnError { get; set; }

            public IReadOnlyDictionary<string, string> Headers { get; set; }
        }
    }
}
