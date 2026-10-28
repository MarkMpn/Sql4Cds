using System;
using System.Collections.Generic;
using System.IO.Pipes;
using System.Threading;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.LanguageServer.Pptb;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Serialization;
using StreamJsonRpc;

namespace MarkMpn.Sql4Cds.LanguageServer.Tests
{
    [TestClass]
    public class PptbRpcChunkingTests
    {
        private const string MessageMethod = "dataverse/executeBatch/message";
        private const string ChunkMethod = "dataverse/executeBatch/chunk";

        private static JsonRpc CreateRpc(System.IO.Stream stream)
        {
            var formatter = new JsonMessageFormatter();
            formatter.JsonSerializer.ContractResolver = new CamelCasePropertyNamesContractResolver();
            return new JsonRpc(new HeaderDelimitedMessageHandler(stream, stream, formatter));
        }

        [DataTestMethod]
        [DataRow(false, false)]
        [DataRow(false, true)]
        [DataRow(true, false)]
        [DataRow(true, true)]
        public async Task RequestAndResponseIndependentlyChooseFraming(bool largeRequest, bool chunkedResponse)
        {
            var request = new JObject { ["text"] = largeRequest ? new string('x', 4000) + "\u00e9\ud83d\ude00\"\\\r\n" : "small" };
            var response = new JObject { ["result"] = new JObject { ["text"] = chunkedResponse ? new string('y', 4000) + "\ud83d\ude00" : "small" } };
            var result = await Exchange(request, response, 512, chunkedResponse, false, largeRequest ? ChunkMethod : MessageMethod);
            Assert.IsTrue(JToken.DeepEquals(response["result"], result));
        }

        [TestMethod]
        public async Task DirectMessageBoundaryIncludesEnvelopeAndUtf8Bytes()
        {
            var request = new JObject { ["text"] = new string('\u00e9', 100) + "\ud83d\ude00\"\\\r\n" };
            var size = MessageSize(MessageMethod, new JObject { ["id"] = new string('a', 32), ["data"] = request });
            var response = new JObject { ["result"] = new JObject() };
            await Exchange(request, response, size, false, false, MessageMethod);
            await Exchange(request, response, size - 1, false, false, ChunkMethod);
        }

        [DataTestMethod]
        [DataRow(false)]
        [DataRow(true)]
        public async Task CorrelatedErrorsPropagateForEitherResponseFraming(bool chunkedResponse)
        {
            var response = new JObject { ["error"] = new JObject { ["message"] = "Dataverse failed" } };
            var error = await Assert.ThrowsExceptionAsync<InvalidOperationException>(() => Exchange(new JObject(), response, 512, chunkedResponse, false, MessageMethod));
            Assert.AreEqual("Dataverse failed", error.Message);
        }

        [TestMethod]
        public async Task OversizedDirectResponseFailsRatherThanHanging()
        {
            var response = new JObject { ["result"] = new string('x', 4000) };
            var error = await Assert.ThrowsExceptionAsync<InvalidOperationException>(() => Exchange(new JObject(), response, 512, false, false, MessageMethod));
            StringAssert.Contains(error.Message, "larger than");
        }

        [TestMethod]
        public async Task CancellationRemovesPendingTransfer()
        {
            await Assert.ThrowsExceptionAsync<TaskCanceledException>(() => Exchange(new JObject(), new JObject(), 512, false, true, MessageMethod));
        }

        private static int MessageSize(string method, JToken parameters)
        {
            return System.Text.Encoding.UTF8.GetByteCount(new JObject { ["jsonrpc"] = "2.0", ["method"] = method, ["params"] = parameters }.ToString(Formatting.None));
        }

        private static async Task<JObject> Exchange(JObject request, JObject response, int limit, bool chunkedResponse, bool cancel, string expectedMethod)
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            var pipeName = "sql4cds-rpc-test-" + Guid.NewGuid().ToString("N");
            using var server = new NamedPipeServerStream(pipeName, PipeDirection.InOut, 1, PipeTransmissionMode.Byte, PipeOptions.Asynchronous);
            using var client = new NamedPipeClientStream(".", pipeName, PipeDirection.InOut, PipeOptions.Asynchronous);
            var connected = server.WaitForConnectionAsync(timeout.Token);
            await client.ConnectAsync(timeout.Token);
            await connected;
            using var workerRpc = CreateRpc(server);
            using var toolRpc = CreateRpc(client);
            var transport = new PptbRpcChunking();
            transport.Initialize(workerRpc);
            transport.SetMaxMessageSize(limit);
            var requestChunks = new List<string>();
            var handled = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var requestCancellation = new CancellationTokenSource();
            using (requestCancellation)
            {
                async Task Receive(string method, JToken parameters)
                {
                    try
                    {
                        Assert.AreEqual(expectedMethod, method);
                        Assert.IsNull(parameters["direction"]);
                        Assert.IsTrue(MessageSize(method, parameters) <= limit);
                        if (method == ChunkMethod)
                        {
                            Assert.AreEqual(requestChunks.Count, parameters.Value<int>("sequence"));
                            requestChunks.Add(parameters.Value<string>("data"));
                            if (!parameters.Value<bool>("complete"))
                                return;
                            Assert.IsTrue(JToken.DeepEquals(request, JObject.Parse(String.Concat(requestChunks))));
                        }
                        else
                        {
                            Assert.IsInstanceOfType<JObject>(parameters["data"]);
                            Assert.IsTrue(JToken.DeepEquals(request, parameters["data"]));
                        }
                        var id = parameters.Value<string>("id");
                        if (cancel)
                        {
                            requestCancellation.Cancel();
                        }
                        else if (chunkedResponse)
                        {
                            var chunks = PptbRpcChunking.Split(response.ToString(Formatting.None), id, limit);
                            foreach (var chunk in chunks)
                                await toolRpc.NotifyWithParameterObjectAsync(ChunkMethod, chunk);
                        }
                        else
                        {
                            await toolRpc.NotifyWithParameterObjectAsync(MessageMethod, new { id, data = response });
                        }
                        handled.TrySetResult(true);
                    }
                    catch (Exception ex)
                    {
                        handled.TrySetException(ex);
                        requestCancellation.Cancel();
                    }
                }

                toolRpc.AddLocalRpcMethod(MessageMethod, (JToken parameters) => Receive(MessageMethod, parameters));
                toolRpc.AddLocalRpcMethod(ChunkMethod, (JToken parameters) => Receive(ChunkMethod, parameters));
                toolRpc.StartListening();
                workerRpc.StartListening();
                var task = transport.RequestAsync<JObject, JObject>(request, requestCancellation.Token);
                await handled.Task.WaitAsync(timeout.Token);
                try
                {
                    return await task.WaitAsync(timeout.Token);
                }
                finally
                {
                    Assert.AreEqual(0, transport.PendingTransferCount);
                }
            }
        }
    }
}
