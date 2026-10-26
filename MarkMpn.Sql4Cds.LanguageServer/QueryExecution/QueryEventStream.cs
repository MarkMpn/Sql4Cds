using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.Export;
using MarkMpn.Sql4Cds.LanguageServer.Configuration;
using MarkMpn.Sql4Cds.LanguageServer.QueryExecution.Contracts;
using Microsoft.VisualStudio.LanguageServer.Protocol;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Serialization;
using StreamJsonRpc;

namespace MarkMpn.Sql4Cds.LanguageServer.QueryExecution
{
    class QueryEventStream : IJsonRpcMethodHandler
    {
        public bool Enabled { get; private set; }
        public int MaxMessageSize { get; private set; }
        public int InitialRowCount { get; private set; }

        public void Initialize(JsonRpc rpc) { }

        public void Configure(int? version, int? maxMessageSize, int? initialRowCount = null)
        {
            Enabled = version == 1 && maxMessageSize > 0;
            MaxMessageSize = maxMessageSize.GetValueOrDefault();
            if (initialRowCount < 0)
                throw new ArgumentOutOfRangeException(nameof(initialRowCount));
            InitialRowCount = Enabled ? initialRowCount.GetValueOrDefault() : 0;
        }

        public QueryEventBatch Create(JsonRpc rpc, string ownerUri, string executionId)
        {
            return new QueryEventBatch(rpc, ownerUri, executionId ?? Guid.NewGuid().ToString("N"), Enabled, MaxMessageSize, InitialRowCount);
        }
    }

    sealed class QueryEventBatch : IDisposable
    {
        public const string Method = "sql4cds/queryEvents";
        private readonly JsonRpc _rpc;
        private readonly string _ownerUri;
        private readonly string _executionId;
        private readonly bool _enabled;
        private readonly int _limit;
        private readonly int _envelopeBytes;
        private readonly object _gate = new object();
        private readonly Dictionary<(int, int), (long RowCount, bool Complete, int RowsSent)> _resultStates = new Dictionary<(int, int), (long, bool, int)>();
        private readonly JsonSerializer _serializer = JsonSerializer.Create(new JsonSerializerSettings
        {
            ContractResolver = new DefaultContractResolver { NamingStrategy = new CamelCaseNamingStrategy { OverrideSpecifiedNames = false } }
        });
        private JArray _events = new JArray();
        private readonly Dictionary<JToken, int> _sizes = new Dictionary<JToken, int>();
        private int _eventBytes;
        private Task _sending = Task.CompletedTask;
        private Timer _timer;
        private bool _completed;

        public int InitialRowCount { get; }

        public QueryEventBatch(JsonRpc rpc, string ownerUri, string executionId, bool enabled, int limit, int initialRowCount = 0)
        {
            _rpc = rpc;
            _ownerUri = ownerUri;
            _executionId = executionId;
            _enabled = enabled;
            _limit = limit;
            InitialRowCount = enabled ? initialRowCount : 0;
            _envelopeBytes = Encoding.UTF8.GetByteCount(new JObject { ["jsonrpc"] = "2.0", ["method"] = Method, ["params"] = Envelope() }.ToString(Formatting.None));
        }

        public Task NotifyAsync<T>(LspNotification<T> notification, T parameters) => NotifyAsync(notification.Name, parameters);

        public Task NotifyAsync(string method, object parameters)
        {
            if (!_enabled || !(method.StartsWith("query/") || method == "sql4cds/progress"))
                return method == "sql4cds/progress" ? _rpc.NotifyAsync(method, parameters) : _rpc.NotifyWithParameterObjectAsync(method, parameters);

            lock (_gate)
            {
                if (_completed)
                    throw new InvalidOperationException("Query events have already completed");

                JObject entry;
                if (parameters is ResultSetEventParams result)
                {
                    var summary = result.ResultSetSummary;
                    var key = (summary.BatchId, summary.Id);
                    var introduced = _resultStates.TryGetValue(key, out var state);
                    var data = new JObject
                    {
                        ["id"] = summary.Id,
                        ["batchId"] = summary.BatchId,
                        ["rowCount"] = summary.RowCount,
                        ["complete"] = summary.Complete,
                    };

                    if (!introduced)
                    {
                        data["columnInfo"] = Token(summary.ColumnInfo);
                        data["specialAction"] = Token(summary.SpecialAction);
                        data["visualization"] = Token(summary.Visualization);
                    }
                    
                    if (parameters is ResultSetUpdatedEventParams updated)
                    {
                        if (updated.ExecutionPlans != null) data["executionPlans"] = Token(updated.ExecutionPlans);
                        if (updated.ExecutionPlanErrorMessage != null) data["executionPlanErrorMessage"] = updated.ExecutionPlanErrorMessage;
                    }

                    entry = new JObject
                    {
                        ["type"] = "resultSet",
                        ["data"] = data
                    };
                    
                    Enqueue(entry);
                    
                    var rowsSent = state.RowsSent;
                    
                    if (summary.SpecialAction?.ExpectYukonXMLShowPlan != true)
                    {
                        var end = Math.Min(InitialRowCount, summary.Values.Count);
                        for (; rowsSent < end; rowsSent++)
                        {
                            var row = Token(ValueFormatter.FormatRow(summary.Values[rowsSent], summary.ColumnInfo, Sql4CdsSettings.Instance.LocalFormatDates));
                            var rowEntry = new JObject
                            {
                                ["type"] = "resultSet",
                                ["data"] = new JObject
                                {
                                    ["id"] = summary.Id,
                                    ["batchId"] = summary.BatchId,
                                    ["rowCount"] = summary.RowCount,
                                    ["complete"] = summary.Complete,
                                    ["rowsStartIndex"] = rowsSent,
                                    ["rows"] = new JArray(row)
                                }
                            };
                            // Oversized cells stay available through ordinary paging, without a gap in the pushed prefix.
                            if (_envelopeBytes + Encoding.UTF8.GetByteCount(rowEntry.ToString(Formatting.None)) > _limit)
                            {
                                rowsSent = InitialRowCount;
                                break;
                            }
                            Enqueue(rowEntry);
                        }
                    }
                    _resultStates[key] = (summary.RowCount, summary.Complete, rowsSent);
                    return Task.CompletedTask;
                }
                else if (parameters is BatchEventParams batch)
                {
                    var summary = batch.BatchSummary;
                    if (method == "query/batchComplete" && summary.ResultSetSummaries != null)
                    {
                        foreach (var finalResult in summary.ResultSetSummaries)
                        {
                            if (!_resultStates.TryGetValue((finalResult.BatchId, finalResult.Id), out var state) ||
                                state.RowCount != finalResult.RowCount || state.Complete != finalResult.Complete)
                                NotifyAsync(ResultSetUpdatedEvent.Type, new ResultSetUpdatedEventParams { OwnerUri = _ownerUri, ResultSetSummary = finalResult }).GetAwaiter().GetResult();
                        }
                    }
                    entry = new JObject
                    {
                        ["type"] = method,
                        ["data"] = Token(new
                        {
                            summary.Id, summary.ExecutionStart, summary.ExecutionEnd, summary.ExecutionElapsed,
                            summary.HasError, summary.Selection, summary.SpecialAction
                        })
                    };
                }
                else if (method == "query/complete")
                    entry = new JObject { ["type"] = method };
                else if (parameters is MessageParams message)
                    entry = new JObject { ["type"] = method, ["data"] = Token(message.Message) };
                else
                    entry = new JObject { ["type"] = method, ["data"] = Token(parameters) };

                if (method == "sql4cds/progress")
                {
                    for (var i = _events.Count - 1; i >= 0; i--)
                        if (_events[i].Value<string>("type") == method) Remove(_events[i]);
                }

                Enqueue(entry, method == "query/complete");
                if (method == "query/complete")
                {
                    _completed = true;
                    return _sending;
                }
                return Task.CompletedTask;
            }
        }

        private void Enqueue(JObject entry, bool complete = false)
        {
            var bytes = Encoding.UTF8.GetByteCount(entry.ToString(Formatting.None));
            // Only adjacent state updates are merged, so messages and plan ordering remain intact.
            if (entry.Value<string>("type") == "resultSet" && _events.Last is JObject previous &&
                previous.Value<string>("type") == "resultSet" &&
                previous["data"].Value<int>("id") == entry["data"].Value<int>("id") &&
                previous["data"].Value<int>("batchId") == entry["data"].Value<int>("batchId") &&
                entry["data"]["executionPlans"] == null)
            {
                var previousData = (JObject)previous["data"];
                var data = (JObject)entry["data"];
                var previousBytes = _sizes[previous];
                var mergedBytes = previousBytes;
                foreach (var property in data.Properties())
                {
                    var oldValue = previousData[property.Name];
                    if (property.Name == "rowsStartIndex" && previousData["rows"] != null)
                        continue;
                    if (property.Name == "rows" && oldValue is JArray rows)
                        mergedBytes += Encoding.UTF8.GetByteCount(property.Value.ToString(Formatting.None)) - 2 +
                            (rows.Count > 0 && ((JArray)property.Value).Count > 0 ? 1 : 0);
                    else if (oldValue != null)
                        mergedBytes += Encoding.UTF8.GetByteCount(property.Value.ToString(Formatting.None)) -
                            Encoding.UTF8.GetByteCount(oldValue.ToString(Formatting.None));
                    else
                        mergedBytes += Encoding.UTF8.GetByteCount(property.ToString(Formatting.None)) + 1;
                }
                if (_envelopeBytes + _eventBytes - previousBytes + mergedBytes + _events.Count - 1 <= Math.Min(_limit, 256 * 1024))
                {
                    foreach (var property in data.Properties())
                    {
                        if (property.Name == "rowsStartIndex" && previousData["rows"] != null)
                            continue;
                        if (property.Name == "rows" && previousData["rows"] is JArray rows)
                        {
                            foreach (var row in (JArray)property.Value) rows.Add(row.DeepClone());
                        }
                        else
                            previousData[property.Name] = property.Value.DeepClone();
                    }
                    _sizes[previous] = mergedBytes;
                    _eventBytes += mergedBytes - previousBytes;
                    return;
                }
            }
            if (_envelopeBytes + bytes > _limit)
                throw new InvalidOperationException("A query event exceeds the negotiated message size");
            if (_envelopeBytes + _eventBytes + bytes + _events.Count > Math.Min(_limit, 256 * 1024) && _events.Count > 0)
                Flush();
            _events.Add(entry);
            _sizes.Add(entry, bytes);
            _eventBytes += bytes;
            if (complete || _events.Count >= 64)
                Flush();
            else if (_timer == null)
                _timer = new Timer(_ => { lock (_gate) { if (!_completed) Flush(); } }, null, 10, Timeout.Infinite);
        }

        private JToken Token(object value) => value == null ? JValue.CreateNull() : JToken.FromObject(value, _serializer);

        private JObject Envelope() => new JObject { ["ownerUri"] = _ownerUri, ["executionId"] = _executionId, ["events"] = _events };

        private void Remove(JToken entry)
        {
            _eventBytes -= _sizes[entry];
            _sizes.Remove(entry);
            _events.Remove(entry);
        }

        private void Flush()
        {
            _timer?.Dispose();
            _timer = null;
            if (_events.Count == 0) return;
            var envelope = Envelope();
            _events = new JArray();
            _sizes.Clear();
            _eventBytes = 0;
            _sending = SendAfterAsync(_sending, envelope);
        }

        private async Task SendAfterAsync(Task previous, JObject envelope)
        {
            await previous.ConfigureAwait(false);
            await _rpc.NotifyWithParameterObjectAsync(Method, envelope).ConfigureAwait(false);
        }

        public void Dispose()
        {
            lock (_gate)
            {
                _timer?.Dispose();
                _timer = null;
                _completed = true;
            }
        }
    }
}
