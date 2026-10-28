using System;
using System.Collections.Generic;
using System.IO.Pipes;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.Export.Contracts;
using MarkMpn.Sql4Cds.LanguageServer.Configuration;
using MarkMpn.Sql4Cds.LanguageServer.Capabilities;
using MarkMpn.Sql4Cds.LanguageServer.Capabilities.Contracts;
using MarkMpn.Sql4Cds.LanguageServer.Connection;
using MarkMpn.Sql4Cds.LanguageServer.Pptb;
using MarkMpn.Sql4Cds.LanguageServer.QueryExecution;
using MarkMpn.Sql4Cds.LanguageServer.QueryExecution.Contracts;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Serialization;
using StreamJsonRpc;

namespace MarkMpn.Sql4Cds.LanguageServer.Tests
{
    [TestClass]
    public class QueryEventStreamTests
    {
        private Sql4CdsSettings _previousSettings;

        [TestInitialize]
        public void InitializeSettings()
        {
            _previousSettings = Sql4CdsSettings.Instance;
            Sql4CdsSettings.Instance = new Sql4CdsSettings();
        }

        [TestCleanup]
        public void RestoreSettings() => Sql4CdsSettings.Instance = _previousSettings;

        private static Task Send(QueryEventBatch batch, string method, object data) =>
            batch.NotifyAsync(method, data);

        [DataTestMethod]
        [DataRow(null, 1024, false)]
        [DataRow(1, 1024, true)]
        [DataRow(2, 1024, false)]
        [DataRow(1, null, false)]
        public void RequiresSupportedOptInAndMessageLimit(int? version, int? limit, bool expected)
        {
            var stream = new QueryEventStream();
            stream.Configure(version, limit, null);
            Assert.AreEqual(expected, stream.Enabled);
        }

        [DataTestMethod]
        [DataRow(null, 0)]
        [DataRow(200, 200)]
        public void InitializationAcknowledgesTheOptIn(int? initialRowCount, int expected)
        {
            var stream = new QueryEventStream();
            var handler = new CapabilitiesHandler(null, new PptbRpcChunking(), stream, new ConnectionManager(null, null, null));
            var request = JsonConvert.DeserializeObject<ClientInitializeParams>(
                "{\"capabilities\":{\"experimental\":{\"pptb\":{\"maxMessageSizeBytes\":33554432},\"sql4cds\":{\"queryEvents\":{\"version\":1,\"initialRowCount\":" +
                (initialRowCount?.ToString() ?? "null") + "}}}}}");
            var result = handler.HandleInitialize(request);
            var json = JObject.FromObject(result, JsonSerializer.Create(new JsonSerializerSettings { ContractResolver = new CamelCasePropertyNamesContractResolver() }));
            Assert.AreEqual(1, json["capabilities"]["experimental"]["sql4cds"]["queryEvents"].Value<int>("version"));
            Assert.IsTrue(stream.Enabled);
            Assert.AreEqual(expected, json["capabilities"]["experimental"]["sql4cds"]["queryEvents"].Value<int>("initialRowCount"));
        }

        [DataTestMethod]
        [DataRow(null, 1, 0)]
        [DataRow(0, 1, 0)]
        [DataRow(3, 1, 3)]
        [DataRow(3, 2, 0)]
        public void InitialRowsRequireAnExplicitSupportedOptIn(int? requested, int version, int expected)
        {
            var stream = new QueryEventStream();
            stream.Configure(version, 1024, requested);
            Assert.AreEqual(expected, stream.InitialRowCount);
        }

        [TestMethod]
        public void NegativeInitialRowCountIsRejected()
        {
            var stream = new QueryEventStream();
            Assert.ThrowsException<ArgumentOutOfRangeException>(() => stream.Configure(1, 1024, -1));
        }

        private static void AddRow(ResultSetSummary summary, params object[] values)
        {
            summary.Values.Add(values);
            summary.RowCount++;
        }

        [TestMethod]
        public async Task StreamsOnlyNewInitialRowsWhileQueryIsStillRunning()
        {
            var summary = new ResultSetSummary { ColumnInfo = new[] { new DbColumnWrapper("name", "nvarchar", null) } };
            var firstRows = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var messages = await Exchange(async sender =>
            {
                await Send(sender, "query/resultSetAvailable", new ResultSetAvailableEventParams { ResultSetSummary = summary });
                AddRow(summary, "first");
                await Send(sender, "query/resultSetUpdated", new ResultSetUpdatedEventParams { ResultSetSummary = summary });
                await firstRows.Task.WaitAsync(TimeSpan.FromSeconds(2));
                AddRow(summary, "second");
                await Send(sender, "query/resultSetUpdated", new ResultSetUpdatedEventParams { ResultSetSummary = summary });
                AddRow(summary, "not pushed");
                summary.Complete = true;
                await Send(sender, "query/resultSetUpdated", new ResultSetUpdatedEventParams { ResultSetSummary = summary });
                await Send(sender, "query/resultSetComplete", new ResultSetCompleteEventParams { ResultSetSummary = summary });
                await Send(sender, "query/complete", new QueryCompleteParams());
            }, initialRowCount: 2, onReceive: message =>
            {
                if (message["events"].Any(entry => entry["data"]?["rows"] != null))
                    firstRows.TrySetResult(true);
            });
            var results = messages.SelectMany(message => (JArray)message["events"]).Where(entry => entry.Value<string>("type") == "resultSet").ToArray();
            var rows = results.Where(entry => entry["data"]["rows"] != null).ToArray();
            Assert.AreEqual(2, rows.Length);
            Assert.AreEqual(0, rows[0]["data"].Value<int>("rowsStartIndex"));
            Assert.AreEqual(1, rows[1]["data"].Value<int>("rowsStartIndex"));
            Assert.AreEqual("first", rows[0]["data"]["rows"][0][0].Value<string>("displayValue"));
            Assert.AreEqual("second", rows[1]["data"]["rows"][0][0].Value<string>("displayValue"));
            Assert.IsFalse(rows[0]["data"].Value<bool>("complete"));
            Assert.AreEqual(3L, results.Last()["data"].Value<long>("rowCount"));
            Assert.IsTrue(results.Last()["data"].Value<bool>("complete"));
        }

        [TestMethod]
        public async Task InitialRowsAreBoundedPerResultAndReconciledAfterPartialFailure()
        {
            var summary = new ResultSetSummary
            {
                ColumnInfo = new[] { new DbColumnWrapper("value", "nvarchar", null) }
            };
            AddRow(summary, "first");
            AddRow(summary, DBNull.Value);
            var second = new ResultSetSummary { Id = 1, ColumnInfo = summary.ColumnInfo };
            AddRow(second, "other result");
            var messages = await Exchange(async sender =>
            {
                await Send(sender, "query/resultSetAvailable", new ResultSetAvailableEventParams { ResultSetSummary = summary });
                AddRow(summary, "partial error");
                await Send(sender, "query/batchComplete", new BatchEventParams { BatchSummary = new BatchSummary { Id = 0, HasError = true, ResultSetSummaries = new[] { summary, second } } });
                await Send(sender, "query/complete", new QueryCompleteParams());
            }, initialRowCount: 3);
            var results = messages.SelectMany(message => (JArray)message["events"])
                .Where(entry => entry.Value<string>("type") == "resultSet").Select(entry => entry["data"]).ToArray();
            var firstRows = results.Where(data => data.Value<int>("id") == 0).SelectMany(data => data["rows"] ?? new JArray()).ToArray();
            Assert.AreEqual(3, firstRows.Length);
            Assert.IsTrue(firstRows[1][0].Value<bool>("isNull"));
            Assert.AreEqual("partial error", firstRows[2][0].Value<string>("displayValue"));
            Assert.AreEqual("other result", results.Single(data => data.Value<int>("id") == 1)["rows"][0][0].Value<string>("displayValue"));
        }

        [TestMethod]
        public async Task RowDeltasSplitAtUtf8MessageLimitWithoutGapsOrDuplicates()
        {
            var summary = new ResultSetSummary { ColumnInfo = new[] { new DbColumnWrapper("value", "nvarchar", null) } };
            for (var i = 0; i < 12; i++) AddRow(summary, new string('\u00e9', 50) + i);
            var messages = await Exchange(async sender =>
            {
                await Send(sender, "query/resultSetAvailable", new ResultSetAvailableEventParams { ResultSetSummary = summary });
                await Send(sender, "query/resultSetComplete", new ResultSetCompleteEventParams { ResultSetSummary = summary });
                await Send(sender, "query/complete", new QueryCompleteParams());
            }, limit: 1400, initialRowCount: 10);
            var count = 0;
            foreach (var data in messages.SelectMany(message => (JArray)message["events"]).Select(entry => entry["data"]).Where(data => data?["rows"] != null))
            {
                Assert.AreEqual(count, data.Value<int>("rowsStartIndex"));
                count += data["rows"].Count();
            }
            Assert.AreEqual(10, count);
            Assert.IsTrue(messages.Count > 1);
        }

        [TestMethod]
        public async Task MissingOptInAndPlanResultsDoNotPushRows()
        {
            foreach (var plan in new[] { false, true })
            {
                var summary = new ResultSetSummary { ColumnInfo = new[] { new DbColumnWrapper("value", "nvarchar", null) }, SpecialAction = new SpecialAction { ExpectYukonXMLShowPlan = plan } };
                AddRow(summary, "value");
                var messages = await Exchange(async sender =>
                {
                    await Send(sender, "query/resultSetAvailable", new ResultSetAvailableEventParams { ResultSetSummary = summary });
                    await Send(sender, "query/complete", new QueryCompleteParams());
                }, initialRowCount: plan ? 200 : 0);
                Assert.IsFalse(messages.SelectMany(message => (JArray)message["events"]).Any(entry => entry["data"]?["rows"] != null));
            }
        }

        [TestMethod]
        public async Task OversizedRowStopsPushingAtTheContiguousPrefixButDoesNotFailTheQuery()
        {
            var summary = new ResultSetSummary { ColumnInfo = new[] { new DbColumnWrapper("value", "nvarchar", null) } };
            AddRow(summary, "fits");
            AddRow(summary, new string('x', 2000));
            AddRow(summary, "do not send after gap");
            var messages = await Exchange(async sender =>
            {
                await Send(sender, "query/resultSetAvailable", new ResultSetAvailableEventParams { ResultSetSummary = summary });
                await Send(sender, "query/resultSetUpdated", new ResultSetUpdatedEventParams { ResultSetSummary = summary });
                await Send(sender, "query/complete", new QueryCompleteParams());
            }, limit: 1400, initialRowCount: 3);
            var rows = messages.SelectMany(message => (JArray)message["events"]).Where(entry => entry["data"]?["rows"] != null).SelectMany(entry => entry["data"]["rows"]).ToArray();
            Assert.AreEqual(1, rows.Length);
            Assert.AreEqual("fits", rows[0][0].Value<string>("displayValue"));
        }

        [TestMethod]
        public async Task PushedCellsUseTheSameFormattingAsSubsetCells()
        {
            var summary = new ResultSetSummary
            {
                ColumnInfo = new[]
                {
                    new DbColumnWrapper("text", "nvarchar", null),
                    new DbColumnWrapper("null", "nvarchar", null),
                    new DbColumnWrapper("date", "datetime", 3),
                    new DbColumnWrapper("number", "decimal", 2),
                    new DbColumnWrapper("binary", "binary", null),
                }
            };
            var values = new object[] { "original", DBNull.Value, new DateTime(2026, 10, 25, 12, 34, 56), 123.45m, new byte[] { 1, 255 } };
            AddRow(summary, values);
            var expected = JArray.FromObject(values.Select((value, index) => MarkMpn.Sql4Cds.Export.ValueFormatter.Format(
                value, summary.ColumnInfo[index].DataTypeName, summary.ColumnInfo[index].NumericScale.GetValueOrDefault(), false)),
                JsonSerializer.Create(new JsonSerializerSettings { ContractResolver = new CamelCasePropertyNamesContractResolver() }));
            var messages = await Exchange(async sender =>
            {
                await Send(sender, "query/resultSetAvailable", new ResultSetAvailableEventParams { ResultSetSummary = summary });
                values[0] = "changed after queuing";
                await Send(sender, "query/complete", new QueryCompleteParams());
            }, initialRowCount: 200);
            var row = messages.SelectMany(message => (JArray)message["events"]).Single(entry => entry["data"]?["rows"] != null)["data"]["rows"][0];
            Assert.AreEqual(expected.ToString(Formatting.None), row.ToString(Formatting.None));
        }

        [TestMethod]
        public async Task FullFirstPageIsCoalescedWithoutRepeatedRowsOrMetadata()
        {
            var summary = new ResultSetSummary { ColumnInfo = new[] { new DbColumnWrapper("value", "nvarchar", null) } };
            var messages = await Exchange(async sender =>
            {
                await Send(sender, "query/resultSetAvailable", new ResultSetAvailableEventParams { ResultSetSummary = summary });
                for (var i = 0; i < 205; i++)
                {
                    AddRow(summary, i.ToString());
                    await Send(sender, "query/resultSetUpdated", new ResultSetUpdatedEventParams { ResultSetSummary = summary });
                }
                await Send(sender, "query/complete", new QueryCompleteParams());
            }, initialRowCount: 200);
            var results = messages.SelectMany(message => (JArray)message["events"]).Where(entry => entry.Value<string>("type") == "resultSet").ToArray();
            var rows = results.Where(entry => entry["data"]["rows"] != null).SelectMany(entry => entry["data"]["rows"]).ToArray();
            Assert.AreEqual(200, rows.Length);
            for (var i = 0; i < rows.Length; i++) Assert.AreEqual(i.ToString(), rows[i][0].Value<string>("displayValue"));
            Assert.AreEqual(1, results.Count(entry => entry["data"]["columnInfo"] != null));
            Assert.IsTrue(messages.Count < 200);
        }

        [TestMethod]
        public async Task CoalescesShortQueryAndSendsColumnsOnceWithoutRepeatedCompletionSummaries()
        {
            var summary = new ResultSetSummary
            {
                Id = 0, BatchId = 0, ColumnInfo = new[] { new DbColumnWrapper("name", "nvarchar", null) }
            };
            var batch = new BatchSummary { Id = 0 };
            var messages = await Exchange(async sender =>
            {
                await Send(sender, "query/batchStart", new BatchEventParams { BatchSummary = batch });
                await Send(sender, "query/resultSetAvailable", new ResultSetAvailableEventParams { ResultSetSummary = summary });
                summary.RowCount = 10;
                summary.Complete = true;
                await Send(sender, "query/resultSetUpdated", new ResultSetUpdatedEventParams { ResultSetSummary = summary });
                await Send(sender, "query/resultSetComplete", new ResultSetCompleteEventParams { ResultSetSummary = summary });
                batch.ResultSetSummaries = new[] { summary };
                await Send(sender, "query/batchComplete", new BatchEventParams { BatchSummary = batch });
                await Send(sender, "query/complete", new QueryCompleteParams { BatchSummaries = new[] { batch } });
            });
            var events = messages.SelectMany(message => (JArray)message["events"]).ToArray();
            var results = events.Where(entry => entry.Value<string>("type") == "resultSet").ToArray();
            Assert.AreEqual(1, results.Count(entry => entry["data"]["columnInfo"] != null));
            Assert.AreEqual(10L, results.Last()["data"].Value<long>("rowCount"));
            Assert.IsTrue(results.Last()["data"].Value<bool>("complete"));
            Assert.IsTrue(messages.Count < 6, "The six lifecycle notifications must be batched");
            Assert.IsNull(events.Single(entry => entry.Value<string>("type") == "query/batchComplete")["data"]["resultSetSummaries"]);
            Assert.IsNull(events.Last()["data"]);
        }

        [TestMethod]
        public async Task TimerFlushesDuringExecutionAndSnapshotsMutableMetadata()
        {
            var summary = new ResultSetSummary { Id = 0, BatchId = 0, ColumnInfo = new[] { new DbColumnWrapper("original", "nvarchar", null) } };
            var first = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var messages = await Exchange(async sender =>
            {
                await Send(sender, "query/resultSetAvailable", new ResultSetAvailableEventParams { ResultSetSummary = summary });
                summary.ColumnInfo = new[] { new DbColumnWrapper("changed", "nvarchar", null) };
                summary.RowCount = 25;
                await first.Task.WaitAsync(TimeSpan.FromSeconds(2));
                await Send(sender, "query/resultSetUpdated", new ResultSetUpdatedEventParams { ResultSetSummary = summary });
                await Send(sender, "query/complete", new QueryCompleteParams());
            }, onReceive: _ => first.TrySetResult(true));
            var results = messages.SelectMany(message => (JArray)message["events"]).Where(entry => entry.Value<string>("type") == "resultSet").ToArray();
            Assert.AreEqual(0L, results[0]["data"].Value<long>("rowCount"));
            Assert.AreEqual("original", results[0]["data"]["columnInfo"][0].Value<string>("columnName"));
            Assert.IsNull(results[1]["data"]["columnInfo"]);
            Assert.AreEqual(25L, results[1]["data"].Value<long>("rowCount"));
        }

        [TestMethod]
        public async Task PreservesMessagesAndFlushesWithinByteLimit()
        {
            var messages = await Exchange(async sender =>
            {
                for (var i = 0; i < 12; i++)
                    await Send(sender, "query/message", new MessageParams { Message = new ResultMessage { Message = i + new string('\u00e9', 80), IsError = true } });
                await Send(sender, "query/complete", new QueryCompleteParams());
            }, limit: 600);
            var events = messages.SelectMany(message => (JArray)message["events"]).ToArray();
            Assert.AreEqual(13, events.Length);
            for (var i = 0; i < 12; i++)
            {
                StringAssert.StartsWith(events[i]["data"].Value<string>("message"), i.ToString());
                Assert.IsTrue(events[i]["data"].Value<bool>("isError"));
            }
            Assert.IsTrue(messages.Count > 1);
        }

        [TestMethod]
        public async Task LatestProgressWinsAndMessageOrderIsPreserved()
        {
            var messages = await Exchange(async sender =>
            {
                await Send(sender, "sql4cds/progress", "old");
                await Send(sender, "query/message", new MessageParams { Message = new ResultMessage { Message = "first" } });
                await Send(sender, "sql4cds/progress", "new");
                await Send(sender, "query/complete", new QueryCompleteParams());
            });
            var events = messages.SelectMany(message => (JArray)message["events"]).ToArray();
            Assert.IsFalse(events.Any(entry => entry["data"]?.Type == JTokenType.String && entry.Value<string>("data") == "old"));
            Assert.IsTrue(events.Any(entry => entry.Value<string>("type") == "sql4cds/progress" && entry.Value<string>("data") == "new"));
        }

        [TestMethod]
        public async Task FinalBatchIncludesRowCountChangesAfterPartialReadFailure()
        {
            var summary = new ResultSetSummary { Id = 0, BatchId = 0, ColumnInfo = Array.Empty<DbColumnWrapper>() };
            var messages = await Exchange(async sender =>
            {
                await Send(sender, "query/resultSetAvailable", new ResultSetAvailableEventParams { ResultSetSummary = summary });
                summary.RowCount = 42;
                await Send(sender, "query/message", new MessageParams { Message = new ResultMessage { Message = "Canceled", IsError = true } });
                await Send(sender, "query/batchComplete", new BatchEventParams
                {
                    BatchSummary = new BatchSummary { Id = 0, HasError = true, ResultSetSummaries = new[] { summary } }
                });
                await Send(sender, "query/complete", new QueryCompleteParams());
            });
            var events = messages.SelectMany(message => (JArray)message["events"]).ToArray();
            Assert.AreEqual(42L, events.Last(entry => entry.Value<string>("type") == "resultSet")["data"].Value<long>("rowCount"));
            Assert.IsTrue(events.Single(entry => entry.Value<string>("type") == "query/batchComplete")["data"].Value<bool>("hasError"));
        }

        [TestMethod]
        public async Task LimitsEventCountAndPreservesMultipleResultSetsAndExecutionPlans()
        {
            var messages = await Exchange(async sender =>
            {
                for (var i = 0; i < 130; i++)
                    await Send(sender, "query/message", new MessageParams { Message = new ResultMessage { Message = i.ToString() } });
                for (var id = 0; id < 2; id++)
                {
                    var summary = new ResultSetSummary
                    {
                        Id = id, BatchId = 0, Complete = true, ColumnInfo = Array.Empty<DbColumnWrapper>(),
                        SpecialAction = new SpecialAction { ExpectYukonXMLShowPlan = true }
                    };
                    await Send(sender, "query/resultSetAvailable", new ResultSetAvailableEventParams { ResultSetSummary = summary });
                    await Send(sender, "query/resultSetUpdated", new ResultSetUpdatedEventParams
                    {
                        ResultSetSummary = summary,
                        ExecutionPlans = new List<Microsoft.SqlTools.ServiceLayer.ExecutionPlan.Contracts.ExecutionPlanGraph>()
                    });
                    await Send(sender, "query/resultSetComplete", new ResultSetCompleteEventParams { ResultSetSummary = summary });
                }
                await Send(sender, "query/complete", new QueryCompleteParams());
            });
            Assert.IsTrue(messages.All(message => message["events"].Count() <= 64));
            var results = messages.SelectMany(message => (JArray)message["events"]).Where(entry => entry.Value<string>("type") == "resultSet").ToArray();
            Assert.AreEqual(2, results.Count(entry => entry["data"]["columnInfo"] != null));
            Assert.AreEqual(2, results.Count(entry => entry["data"]["executionPlans"] != null));
        }

        [TestMethod]
        public async Task DisabledStreamPreservesLegacyPayloads()
        {
            var messages = await Exchange(async sender =>
            {
                await Send(sender, "sql4cds/progress", "legacy progress");
                await Send(sender, "query/complete", new QueryCompleteParams { OwnerUri = "owner", BatchSummaries = new[] { new BatchSummary { Id = 0 } } });
            }, enabled: false);
            Assert.AreEqual(2, messages.Count);
            Assert.AreEqual("legacy progress", messages[0].Value<string>("progress"));
            Assert.AreEqual("owner", messages[1].Value<string>("ownerUri"));
            Assert.IsNotNull(messages[1]["batchSummaries"]);
        }

        [TestMethod]
        public async Task LargeMetadataIsTransferredOnceAndCutsPayloadByAtLeastSixtyPercent()
        {
            var summary = new ResultSetSummary { Id = 0, BatchId = 0, ColumnInfo = new[] { new DbColumnWrapper(new string('x', 112000), "nvarchar", null) } };
            var final = new BatchSummary { Id = 0, ResultSetSummaries = new[] { summary } };
            var legacyBytes = 5 * System.Text.Encoding.UTF8.GetByteCount(JsonConvert.SerializeObject(summary));
            var messages = await Exchange(async sender =>
            {
                await Send(sender, "query/batchStart", new BatchEventParams { BatchSummary = new BatchSummary { Id = 0 } });
                await Send(sender, "query/resultSetAvailable", new ResultSetAvailableEventParams { ResultSetSummary = summary });
                summary.RowCount = 1;
                summary.Complete = true;
                await Send(sender, "query/resultSetUpdated", new ResultSetUpdatedEventParams { ResultSetSummary = summary });
                await Send(sender, "query/resultSetComplete", new ResultSetCompleteEventParams { ResultSetSummary = summary });
                await Send(sender, "query/batchComplete", new BatchEventParams { BatchSummary = final });
                await Send(sender, "query/complete", new QueryCompleteParams { BatchSummaries = new[] { final } });
            });
            var bytes = messages.Sum(message => System.Text.Encoding.UTF8.GetByteCount(message.ToString(Formatting.None)));
            Assert.IsTrue(bytes < legacyBytes * 0.4, $"Compact: {bytes} bytes; repeated summaries: {legacyBytes} bytes");
            Console.WriteLine($"Compact lifecycle: {messages.Count} notifications, {bytes} bytes vs {legacyBytes} bytes of repeated summaries.");
        }

        [TestMethod]
        public async Task ExactUtf8EnvelopeLimitIsAcceptedAndOneByteLessIsRejected()
        {
            var reference = await Exchange(sender => Send(sender, "query/complete", new QueryCompleteParams()));
            var limit = System.Text.Encoding.UTF8.GetByteCount(new JObject
            {
                ["jsonrpc"] = "2.0", ["method"] = "sql4cds/queryEvents", ["params"] = reference.Single()
            }.ToString(Formatting.None));
            await Exchange(sender => Send(sender, "query/complete", new QueryCompleteParams()), limit: limit);
            await Assert.ThrowsExceptionAsync<InvalidOperationException>(() =>
                Exchange(sender => Send(sender, "query/complete", new QueryCompleteParams()), limit: limit - 1));
        }

        private static async Task<List<JObject>> Exchange(Func<QueryEventBatch, Task> send, bool enabled = true, int limit = 1024 * 1024, Action<JObject> onReceive = null, int initialRowCount = 0)
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            var pipeName = "query-events-" + Guid.NewGuid().ToString("N");
            using var server = new NamedPipeServerStream(pipeName, PipeDirection.InOut, 1, PipeTransmissionMode.Byte, PipeOptions.Asynchronous);
            using var client = new NamedPipeClientStream(".", pipeName, PipeDirection.InOut, PipeOptions.Asynchronous);
            var connected = server.WaitForConnectionAsync(timeout.Token);
            await client.ConnectAsync(timeout.Token);
            await connected;
            JsonRpc Rpc(System.IO.Stream pipe)
            {
                var formatter = new JsonMessageFormatter();
                formatter.JsonSerializer.ContractResolver = new CamelCasePropertyNamesContractResolver();
                return new JsonRpc(new HeaderDelimitedMessageHandler(pipe, pipe, formatter));
            }
            using var worker = Rpc(server);
            using var tool = Rpc(client);
            var messages = new List<JObject>();
            var completed = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            if (!enabled)
                tool.AddLocalRpcMethod("sql4cds/progress", (JToken token) => messages.Add(new JObject { ["progress"] = token }));
            tool.AddLocalRpcMethod(enabled ? "sql4cds/queryEvents" : "query/complete", (JToken token) =>
            {
                try
                {
                    var message = (JObject)token;
                    if (enabled)
                    {
                        Assert.AreEqual("owner", message.Value<string>("ownerUri"));
                        Assert.AreEqual("execution", message.Value<string>("executionId"));
                        var bytes = System.Text.Encoding.UTF8.GetByteCount(new JObject { ["jsonrpc"] = "2.0", ["method"] = "sql4cds/queryEvents", ["params"] = message }.ToString(Formatting.None));
                        Assert.IsTrue(bytes <= limit);
                    }
                    messages.Add((JObject)message.DeepClone());
                    onReceive?.Invoke(message);
                    if (!enabled || message["events"].Any(entry => entry.Value<string>("type") == "query/complete")) completed.TrySetResult(true);
                }
                catch (Exception ex) { completed.TrySetException(ex); }
            });
            tool.StartListening();
            worker.StartListening();
            using var batch = new QueryEventBatch(worker, "owner", "execution", enabled, limit, initialRowCount);
            await send(batch);
            await completed.Task.WaitAsync(timeout.Token);
            return messages;
        }
    }
}
