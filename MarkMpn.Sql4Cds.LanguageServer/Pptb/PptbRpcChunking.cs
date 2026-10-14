using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Serialization;
using StreamJsonRpc;

namespace MarkMpn.Sql4Cds.LanguageServer.Pptb
{
    /// <summary>
    /// Transports Dataverse RPC payloads as bounded JSON-RPC notifications.
    /// </summary>
    class PptbRpcChunking : IJsonRpcMethodHandler
    {
        public const string MethodName = "dataverse/executeBatch/chunk";

        private static readonly JsonSerializerSettings SerializerSettings = new JsonSerializerSettings
        {
            ContractResolver = new DefaultContractResolver
            {
                NamingStrategy = new CamelCaseNamingStrategy { OverrideSpecifiedNames = false }
            }
        };

        private readonly ConcurrentDictionary<string, PendingTransfer> _pending = new ConcurrentDictionary<string, PendingTransfer>();
        private JsonRpc _rpc;
        private int _maxMessageSize;

        public PptbRpcChunking()
        {
        }

        public int? MaxMessageSize => _maxMessageSize > 0 ? _maxMessageSize : (int?)null;

        public void Initialize(JsonRpc rpc)
        {
            _rpc = rpc;
            rpc.AddLocalRpcMethod(MethodName, (JToken token) => ReceiveChunk(token.ToObject<PptbRpcChunk>()));
        }

        public void SetMaxMessageSize(int? maxMessageSize)
        {
            _maxMessageSize = maxMessageSize.GetValueOrDefault() > 0 ? maxMessageSize.Value : 0;
        }

        public async Task<TResponse> RequestAsync<TRequest, TResponse>(TRequest request, System.Threading.CancellationToken cancellationToken)
        {
            var limit = MaxMessageSize;
            if (limit == null)
                throw new InvalidOperationException("Chunked Dataverse RPC was not enabled by the client");

            var id = Guid.NewGuid().ToString("N");
            var pending = new PendingTransfer();
            if (!_pending.TryAdd(id, pending))
                throw new InvalidOperationException("Failed to register Dataverse RPC transfer");

            try
            {
                var payload = JsonConvert.SerializeObject(request, SerializerSettings);
                foreach (var chunk in Split(payload, id, "request", limit.Value))
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    await _rpc.NotifyWithParameterObjectAsync(MethodName, chunk).ConfigureAwait(false);
                }

                var responseJson = await pending.Completion.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
                var response = JObject.Parse(responseJson);
                if (response["error"] is JObject error)
                    throw new InvalidOperationException(error.Value<string>("message") ?? "Dataverse RPC failed");

                if (response["result"] == null)
                    throw new InvalidOperationException("Dataverse RPC response did not contain a result");

                return response["result"].ToObject<TResponse>(JsonSerializer.Create(SerializerSettings));
            }
            finally
            {
                _pending.TryRemove(id, out _);
            }
        }

        private void ReceiveChunk(PptbRpcChunk chunk)
        {
            if (chunk == null || String.IsNullOrEmpty(chunk.Id) || chunk.Direction != "response")
                return;

            if (!_pending.TryGetValue(chunk.Id, out var pending))
                return;

            try
            {
                var limit = MaxMessageSize;
                if (limit == null || GetMessageSize(chunk) > limit.Value)
                    throw new InvalidOperationException("Received a Dataverse RPC chunk larger than the negotiated message size");

                if (chunk.Sequence != pending.NextSequence)
                    throw new InvalidOperationException($"Unexpected Dataverse RPC chunk sequence {chunk.Sequence}; expected {pending.NextSequence}");

                if (chunk.Data == null)
                    throw new InvalidOperationException("Dataverse RPC chunk data is missing");

                pending.Chunks.Add(chunk.Data);
                pending.NextSequence++;

                if (chunk.Complete)
                    pending.Completion.TrySetResult(String.Concat(pending.Chunks));
            }
            catch (Exception ex)
            {
                pending.Completion.TrySetException(ex);
            }
        }

        private static IReadOnlyList<PptbRpcChunk> Split(string payload, string id, string direction, int maxMessageSize)
        {
            var elements = new List<string>();
            foreach (var codePoint in payload.EnumerateRunes())
                elements.Add(codePoint.ToString());

            var chunks = new List<PptbRpcChunk>();
            var offset = 0;

            while (offset < elements.Count)
            {
                var sequence = chunks.Count;
                var low = 1;
                var high = elements.Count - offset;
                var best = 0;

                while (low <= high)
                {
                    var count = low + (high - low) / 2;
                    var data = String.Concat(elements.Skip(offset).Take(count));
                    var candidate = new PptbRpcChunk
                    {
                        Id = id,
                        Direction = direction,
                        Sequence = sequence,
                        Complete = false,
                        Data = data
                    };

                    if (GetMessageSize(candidate) <= maxMessageSize)
                    {
                        best = count;
                        low = count + 1;
                    }
                    else
                    {
                        high = count - 1;
                    }
                }

                if (best == 0)
                    throw new InvalidOperationException("Unable to fit a Dataverse RPC chunk within the negotiated message size");

                chunks.Add(new PptbRpcChunk
                {
                    Id = id,
                    Direction = direction,
                    Sequence = sequence,
                    Complete = false,
                    Data = String.Concat(elements.Skip(offset).Take(best))
                });
                offset += best;
            }

            if (chunks.Count == 0)
                chunks.Add(new PptbRpcChunk { Id = id, Direction = direction, Sequence = 0, Complete = true, Data = "" });
            else
                chunks[chunks.Count - 1].Complete = true;

            if (chunks.Any(chunk => GetMessageSize(chunk) > maxMessageSize))
                throw new InvalidOperationException("A Dataverse RPC chunk exceeds the negotiated message size");

            return chunks;
        }

        private static int GetMessageSize(PptbRpcChunk chunk)
        {
            var message = new JObject
            {
                ["jsonrpc"] = "2.0",
                ["method"] = MethodName,
                ["params"] = JObject.FromObject(chunk, JsonSerializer.Create(SerializerSettings))
            };
            return Encoding.UTF8.GetByteCount(message.ToString(Formatting.None));
        }

        class PendingTransfer
        {
            public TaskCompletionSource<string> Completion { get; } = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
            public List<string> Chunks { get; } = new List<string>();
            public int NextSequence { get; set; }
        }

        class PptbRpcChunk
        {
            public string Id { get; set; }
            public string Direction { get; set; }
            public int Sequence { get; set; }
            public bool Complete { get; set; }
            public string Data { get; set; }
        }
    }
}
