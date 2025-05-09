﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Reflection;
using System.ServiceModel;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.Engine;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using MarkMpn.Sql4Cds.Export;
using MarkMpn.Sql4Cds.LanguageServer.Configuration;
using MarkMpn.Sql4Cds.LanguageServer.Connection;
using MarkMpn.Sql4Cds.LanguageServer.QueryExecution.Contracts;
using MarkMpn.Sql4Cds.LanguageServer.Workspace;
using Microsoft.SqlTools.ServiceLayer.ExecutionPlan.Contracts;
using MarkMpn.Sql4Cds.Export.Contracts;
using MarkMpn.Sql4Cds.Export.DataStorage;
using Microsoft.VisualStudio.LanguageServer.Protocol;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Metadata;
using StreamJsonRpc;

namespace MarkMpn.Sql4Cds.LanguageServer.QueryExecution
{
    class QueryExecutionHandler : IJsonRpcMethodHandler
    {
        private readonly JsonRpc _lsp;
        private readonly ConnectionManager _connectionManager;
        private readonly TextDocumentManager _documentManager;
        private readonly ConcurrentDictionary<string, List<ResultSetSummary>> _resultSets;
        private readonly ConcurrentDictionary<string, IDbCommand> _commands;
        private readonly ConcurrentDictionary<string, ManualResetEventSlim> _confirmationEvents;
        private readonly ConcurrentDictionary<string, ConfirmationResult> _confirmationResults;

        public QueryExecutionHandler(JsonRpc lsp, ConnectionManager connectionManager, TextDocumentManager documentManager)
        {
            _lsp = lsp;
            _connectionManager = connectionManager;
            _documentManager = documentManager;
            _resultSets = new ConcurrentDictionary<string, List<ResultSetSummary>>();
            _commands = new ConcurrentDictionary<string, IDbCommand>();
            _confirmationEvents = new ConcurrentDictionary<string, ManualResetEventSlim>();
            _confirmationResults = new ConcurrentDictionary<string, ConfirmationResult>();
        }

        public void Initialize(JsonRpc lsp)
        {
            lsp.AddHandler(ExecuteDocumentSelectionRequest.Type, HandleExecuteDocumentSelection);
            lsp.AddHandler(ExecuteStringRequest.Type, HandleExecuteString);
            lsp.AddHandler(SubsetRequest.Type, HandleSubset);
            lsp.AddHandler(QueryCancelRequest.Type, HandleQueryCancel);
            lsp.AddHandler(QueryExecutionPlanRequest.Type, HandleQueryExecutionPlan);
            lsp.AddHandler(QueryDisposeRequest.Type, HandleQueryDispose);
            lsp.AddHandler(ConfirmationResponse.Type, HandleConfirmation);
            lsp.AddHandler(ExportRequests.CsvType, HandleExportCsv);
            lsp.AddHandler(ExportRequests.ExcelType, HandleExportExcel);
            lsp.AddHandler(ExportRequests.JsonType, HandleExportJson);
            lsp.AddHandler(ExportRequests.MarkdownType, HandleExportMarkdown);
            lsp.AddHandler(ExportRequests.XmlType, HandleExportXml);
        }

        private void HandleConfirmation(ConfirmationResponseParams arg)
        {
            _confirmationResults[arg.OwnerUri] = arg.Result;
            _confirmationEvents[arg.OwnerUri].Set();
        }

        public ExecuteRequestResult HandleExecuteDocumentSelection(ExecuteDocumentSelectionParams request)
        {
            var session = _connectionManager.GetConnection(request.OwnerUri);

            if (session == null)
            {
                _ = _lsp.NotifyAsync(Methods.WindowLogMessage, new LogMessageParams
                {
                    Message = "No connection available for " + request.OwnerUri,
                    MessageType = MessageType.Error
                });
                return new ExecuteRequestResult();
            }

            _ = Task.Run(async () =>
            {
                var doc = _documentManager.GetContent(request.OwnerUri).Split('\n');

                if (request.QuerySelection == null)
                    request.QuerySelection = new SelectionData { StartLine = 0, StartColumn = 0, EndLine = doc.Length - 1, EndColumn = doc[doc.Length - 1].Length };

                var qry = "";

                for (var i = request.QuerySelection.StartLine; i <= request.QuerySelection.EndLine; i++)
                {
                    if (i == request.QuerySelection.StartLine && i == request.QuerySelection.EndLine)
                        qry += doc[i].Substring(request.QuerySelection.StartColumn, request.QuerySelection.EndColumn - request.QuerySelection.StartColumn);
                    else if (i == request.QuerySelection.StartLine)
                        qry += doc[i].Substring(request.QuerySelection.StartColumn);
                    else if (i == request.QuerySelection.EndLine)
                        qry += doc[i].Substring(0, request.QuerySelection.EndColumn);
                    else
                        qry += doc[i];

                    qry += '\n';
                }

                await ExecuteAsync(session, request, qry, request.QuerySelection);
            });

            return new ExecuteRequestResult();
        }

        private ExecuteRequestResult HandleExecuteString(ExecuteStringParams request)
        {
            var session = _connectionManager.GetConnection(request.OwnerUri);

            if (session == null)
            {
                _ = _lsp.NotifyAsync(Methods.WindowLogMessage, new LogMessageParams
                {
                    Message = "No connection available for " + request.OwnerUri,
                    MessageType = MessageType.Error
                });
                return new ExecuteRequestResult();
            }

            _ = Task.Run(async () =>
            {
                var lines = request.Query.Split('\n');
                await ExecuteAsync(session, request, request.Query, new SelectionData { StartLine =0, StartColumn = 0, EndLine = lines.Length - 1, EndColumn = lines[lines.Length - 1].Length  });
            });

            return new ExecuteRequestResult();
        }

        private async Task ExecuteAsync(Connection.Session session, ExecuteRequestParamsBase request, string qry, SelectionData selection)
        {
            // query/batchStart (BatchEventParams)
            // query/message (MessagePArams)
            // query/resultSetAvailable (ResultSetAvailableEventPArams)
            // query/resultSetUpdated (ResultSetUpdatedEventParams)
            // query/resultSetComplete (ResultSetCompleteEventParams)
            // query/batchComplete (BatchEventParams)
            var startTime = DateTime.UtcNow;
            ResultSetSummary resultSetInProgress = null;
            var bypassWarnings = false;

            var batchSummary = new BatchSummary
            {
                Id = 0,
                ExecutionStart = startTime.ToLocalTime().ToString("o"),
                Selection = selection
            };

            await _lsp.NotifyAsync(BatchStartEvent.Type, new BatchEventParams
            {
                OwnerUri = request.OwnerUri,
                BatchSummary = batchSummary
            });

            var infoMessageHandler = (EventHandler<InfoMessageEventArgs>)((object sender, InfoMessageEventArgs msg) =>
            {
                var message = msg.Message.Message;

                if (msg.Message.Class != 0 && msg.Message.Class != 10)
                    message += $"\r\nMsg {msg.Message.Number}, Level {msg.Message.Class}, State {msg.Message.State}";

                _ = _lsp.NotifyAsync(MessageEvent.Type, new MessageParams
                {
                    OwnerUri = request.OwnerUri,
                    Message = new ResultMessage
                    {
                        BatchId = batchSummary.Id,
                        Time = DateTime.UtcNow.ToString("o"),
                        Message = message
                    }
                });
            });
            var progressHandler = (EventHandler<ProgressEventArgs>)((object sender, ProgressEventArgs progress) =>
            {
                var progressParam = new Dictionary<string, object>
                {
                    ["msg"] = progress.Message,
                };

                if (progress.Progress != null)
                    progressParam["progress"] = progress.Progress.Value * 100;

                _ = _lsp.NotifyAsync("sql4cds/progress", progress.Message);

                if (resultSetInProgress != null)
                {
                    _ = _lsp.NotifyAsync(ResultSetUpdatedEvent.Type, new ResultSetUpdatedEventParams
                    {
                        OwnerUri = request.OwnerUri,
                        ResultSetSummary = resultSetInProgress,
                    });
                }
            });
            var confirm = (ConfirmDmlStatementEventArgs e, int threshold, string operation) =>
            {
                e.Cancel = false;

                if ((e.Count > threshold || e.BypassCustomPluginExecution) && e.Metadata != null && !bypassWarnings)
                {
                    var msg = $"{operation} will affect {(e.Count == Int32.MaxValue ? "an unknown number of" : e.Count.ToString("N0"))} {GetDisplayName(e.Count, e.Metadata)}.";
                    if (e.BypassCustomPluginExecution)
                        msg += " This operation will bypass any custom plugins.";
                    msg += " Do you want to proceed?";

                    var evt = new ManualResetEventSlim();
                    _confirmationEvents[request.OwnerUri] = evt;
                    _ = _lsp.NotifyAsync(ConfirmationRequest.Type, new ConfirmationParams { OwnerUri = request.OwnerUri, Msg = msg });
                    evt.Wait();
                    e.Cancel = _confirmationResults[request.OwnerUri] == ConfirmationResult.No;
                    bypassWarnings = _confirmationResults[request.OwnerUri] == ConfirmationResult.All;
                    _confirmationEvents.Remove(request.OwnerUri, out _);
                    _confirmationResults.Remove(request.OwnerUri, out _);
                };
            };
            var confirmInsert = (EventHandler<ConfirmDmlStatementEventArgs>)((object sender, ConfirmDmlStatementEventArgs e) =>
            {
                confirm(e, Sql4CdsSettings.Instance.InsertWarnThreshold, "Insert");
            });
            var confirmUpdate = (EventHandler<ConfirmDmlStatementEventArgs>)((object sender, ConfirmDmlStatementEventArgs e) =>
            {
                confirm(e, Sql4CdsSettings.Instance.UpdateWarnThreshold, "Update");
            });
            var confirmDelete = (EventHandler<ConfirmDmlStatementEventArgs>)((object sender, ConfirmDmlStatementEventArgs e) =>
            {
                confirm(e, Sql4CdsSettings.Instance.DeleteWarnThreshold, "Delete");
            });
            var pages = 0;
            var confirmRetrieve = (EventHandler<ConfirmRetrieveEventArgs>)((object sender, ConfirmRetrieveEventArgs e) =>
            {
                e.Cancel = Sql4CdsSettings.Instance.SelectLimit > 0 && e.Count >= Sql4CdsSettings.Instance.SelectLimit;

                if (!e.Cancel)
                {
                    pages++;

                    if (Sql4CdsSettings.Instance.MaxRetrievesPerQuery != 0 && pages > Sql4CdsSettings.Instance.MaxRetrievesPerQuery)
                        throw new QueryExecutionException($"Hit maximum retrieval limit. This limit is in place to protect against excessive API requests. Try restricting the data to retrieve with WHERE clauses or eliminating subqueries.\r\nYour limit of {Sql4CdsSettings.Instance.MaxRetrievesPerQuery:N0} retrievals per query can be modified in Settings.");
                }
            });

            session.Connection.InfoMessage += infoMessageHandler;
            session.Connection.Progress += progressHandler;
            session.Connection.PreInsert += confirmInsert;
            session.Connection.PreUpdate += confirmUpdate;
            session.Connection.PreDelete += confirmDelete;
            session.Connection.PreRetrieve += confirmRetrieve;

            var resultSets = new List<ResultSetSummary>();
            _resultSets[request.OwnerUri] = resultSets;

            try
            {
                session.Connection.UseTDSEndpoint = Sql4CdsSettings.Instance.UseTdsEndpoint;
                session.Connection.BlockDeleteWithoutWhere = Sql4CdsSettings.Instance.BlockDeleteWithoutWhere;
                session.Connection.BlockUpdateWithoutWhere = Sql4CdsSettings.Instance.BlockUpdateWithoutWhere;
                session.Connection.UseBulkDelete = Sql4CdsSettings.Instance.UseBulkDelete;
                session.Connection.BatchSize = Sql4CdsSettings.Instance.BatchSize;
                session.Connection.MaxDegreeOfParallelism = Sql4CdsSettings.Instance.MaxDegreeOfParallelism;
                session.Connection.UseLocalTimeZone = Sql4CdsSettings.Instance.UseLocalTimeZone;
                session.Connection.BypassCustomPlugins = Sql4CdsSettings.Instance.BypassCustomPlugins;
                session.Connection.QuotedIdentifiers = Sql4CdsSettings.Instance.QuotedIdentifiers;

                using (var cmd = session.Connection.CreateCommand())
                {
                    cmd.CommandTimeout = 0;

                    _commands[request.OwnerUri] = cmd;

                    cmd.CommandText = qry;

                    cmd.StatementCompleted += (_, stmt) =>
                    {
                        if (stmt.Message != null)
                        {
                            _ = _lsp.NotifyAsync(MessageEvent.Type, new MessageParams
                            {
                                OwnerUri = request.OwnerUri,
                                Message = new ResultMessage
                                {
                                    BatchId = batchSummary.Id,
                                    Time = DateTime.UtcNow.ToString("o"),
                                    Message = stmt.Message
                                }
                            });
                        }
                    };

                    if (!request.ExecutionPlanOptions.IncludeEstimatedExecutionPlanXml)
                    {
                        if (request.ExecutionPlanOptions.IncludeActualExecutionPlanXml)
                        {
                            cmd.StatementCompleted += (_, stmt) =>
                            {
                                var resultSet = new ResultSetSummary
                                {
                                    Id = resultSets.Count,
                                    BatchId = batchSummary.Id,
                                    Complete = true,
                                    ColumnInfo = new[] { new DbColumnWrapper("Microsoft SQL Server 2005 XML Showplan", "xml", null) },
                                    RowCount = 0,
                                    SpecialAction = new SpecialAction { ExpectYukonXMLShowPlan = true },
                                };
                                resultSets.Add(resultSet);

                                _lsp.NotifyAsync(ResultSetAvailableEvent.Type, new ResultSetAvailableEventParams
                                {
                                    OwnerUri = request.OwnerUri,
                                    ResultSetSummary = resultSet,
                                }).ConfigureAwait(false).GetAwaiter().GetResult();
                                _lsp.NotifyAsync(ResultSetUpdatedEvent.Type, new ResultSetUpdatedEventParams
                                {
                                    OwnerUri = request.OwnerUri,
                                    ResultSetSummary = resultSet,
                                    ExecutionPlans = new List<ExecutionPlanGraph> { ConvertExecutionPlan(stmt.Statement, true) }
                                }).ConfigureAwait(false).GetAwaiter().GetResult();
                                _lsp.NotifyAsync(ResultSetCompleteEvent.Type, new ResultSetCompleteEventParams
                                {
                                    OwnerUri = request.OwnerUri,
                                    ResultSetSummary = resultSet,
                                }).ConfigureAwait(false).GetAwaiter().GetResult();
                            };
                        }

                        using (var reader = await cmd.ExecuteReaderAsync())
                        {
                            while (!reader.IsClosed)
                            {
                                var resultSet = new ResultSetSummary
                                {
                                    Id = resultSets.Count,
                                    BatchId = batchSummary.Id,
                                    Complete = false,
                                    ColumnInfo = new DbColumnWrapper[reader.FieldCount],
                                    RowCount = 0,
                                    SpecialAction = new SpecialAction(),
                                };

                                var schemaTable = reader.GetSchemaTable();

                                for (var i = 0; i < reader.FieldCount; i++)
                                    resultSet.ColumnInfo[i] = new DbColumnWrapper(schemaTable.Rows[i]["ColumnName"] as string, (string)schemaTable.Rows[i]["DataTypeName"], (short?)schemaTable.Rows[i]["NumericScale"]);

                                resultSetInProgress = resultSet;
                                resultSets.Add(resultSet);

                                await _lsp.NotifyAsync(ResultSetAvailableEvent.Type, new ResultSetAvailableEventParams
                                {
                                    OwnerUri = request.OwnerUri,
                                    ResultSetSummary = resultSet,
                                });

                                while (await reader.ReadAsync())
                                {
                                    var row = new object[reader.FieldCount];
                                    reader.GetValues(row);
                                    resultSet.Values.Add(row);
                                    resultSet.RowCount++;
                                }

                                resultSet.Complete = true;
                                resultSetInProgress = null;

                                await _lsp.NotifyAsync(ResultSetUpdatedEvent.Type, new ResultSetUpdatedEventParams
                                {
                                    OwnerUri = request.OwnerUri,
                                    ResultSetSummary = resultSet,
                                });

                                await _lsp.NotifyAsync(ResultSetCompleteEvent.Type, new ResultSetCompleteEventParams
                                {
                                    OwnerUri = request.OwnerUri,
                                    ResultSetSummary = resultSet
                                });

                                if (!await reader.NextResultAsync())
                                    break;
                            }
                        }
                    }
                    else
                    {
                        var resultSet = new ResultSetSummary
                        {
                            Id = resultSets.Count,
                            BatchId = batchSummary.Id,
                            Complete = true,
                            ColumnInfo = new[] { new DbColumnWrapper("Microsoft SQL Server 2005 XML Showplan", "xml", null) },
                            RowCount = 0,
                            SpecialAction = new SpecialAction { ExpectYukonXMLShowPlan = true },
                        };
                        resultSets.Add(resultSet);

                        await _lsp.NotifyAsync(ResultSetAvailableEvent.Type, new ResultSetAvailableEventParams
                        {
                            OwnerUri = request.OwnerUri,
                            ResultSetSummary = resultSet,
                        });
                        await _lsp.NotifyAsync(ResultSetUpdatedEvent.Type, new ResultSetUpdatedEventParams
                        {
                            OwnerUri = request.OwnerUri,
                            ResultSetSummary = resultSet,
                            ExecutionPlans = cmd.GeneratePlan(false).Select(plan => ConvertExecutionPlan(plan, false)).ToList()
                        });
                        await _lsp.NotifyAsync(ResultSetCompleteEvent.Type, new ResultSetCompleteEventParams
                        {
                            OwnerUri = request.OwnerUri,
                            ResultSetSummary = resultSet,
                        });
                    }
                }
            }
            catch (Exception ex)
            {
                var sql4CdsException = ex as Sql4CdsException;

                if (sql4CdsException?.Errors != null)
                {
                    var diagnostics = new List<Diagnostic>();

                    foreach (var sql4CdsError in sql4CdsException.Errors)
                    {
                        var parts = new List<string>
                        {
                            $"Msg {sql4CdsError.Number}",
                            $"Level {sql4CdsError.Class}",
                            $"State {sql4CdsError.State}"
                        };

                        if (sql4CdsError.Procedure != null)
                        {
                            parts.Add($"Procedure {sql4CdsError.Procedure}");
                            parts.Add($"Line 0 [Batch Start Line {sql4CdsError.LineNumber}]");
                        }
                        else
                        {
                            parts.Add($"Line {Math.Max(sql4CdsError.LineNumber, 1)}");
                        }

                        await _lsp.NotifyAsync(MessageEvent.Type, new MessageParams
                        {
                            OwnerUri = request.OwnerUri,
                            Message = new ResultMessage
                            {
                                BatchId = batchSummary.Id,
                                Time = DateTime.UtcNow.ToString("o"),
                                Message = String.Join(", ", parts),
                                IsError = true
                            }
                        });

                        await _lsp.NotifyAsync(MessageEvent.Type, new MessageParams
                        {
                            OwnerUri = request.OwnerUri,
                            Message = new ResultMessage
                            {
                                BatchId = batchSummary.Id,
                                Time = DateTime.UtcNow.ToString("o"),
                                Message = sql4CdsError.Message,
                                IsError = true
                            }
                        });

                        if (sql4CdsError.Fragment != null && sql4CdsError.Fragment.FragmentLength > 0)
                        {
                            var lines = sql4CdsError.Fragment.ToSql().Split('\n');

                            diagnostics.Add(new Diagnostic
                            {
                                Range = new Microsoft.VisualStudio.LanguageServer.Protocol.Range
                                {
                                    Start = new Position
                                    {
                                        Line = sql4CdsError.Fragment.StartLine - 1 + selection.StartLine,
                                        Character = sql4CdsError.Fragment.StartColumn - 1 + (sql4CdsError.Fragment.StartLine == 1 ? selection.StartColumn : 0)
                                    },
                                    End = new Position
                                    {
                                        Line = sql4CdsError.Fragment.StartLine - 1 + selection.StartLine + lines.Length - 1,
                                        Character = lines.Length > 1 ? lines.Last().Length - 1 : sql4CdsError.Fragment.StartColumn - 1 + (sql4CdsError.Fragment.StartLine == 1 ? selection.StartColumn : 0) + sql4CdsError.Fragment.FragmentLength
                                    }
                                },
                                Message = sql4CdsError.Message
                            });
                        }
                        else
                        {
                            diagnostics.Add(new Diagnostic
                            {
                                Range = new Microsoft.VisualStudio.LanguageServer.Protocol.Range
                                {
                                    Start = new Position
                                    {
                                        Line = Math.Max(sql4CdsError.LineNumber, 1) - 1 + selection.StartLine,
                                        Character = 0
                                    },
                                    End = new Position
                                    {
                                        Line = Math.Max(sql4CdsError.LineNumber, 1) - 1 + selection.StartLine,
                                        Character = qry.Split('\n')[Math.Max(sql4CdsError.LineNumber, 1) - 1].Length
                                    }
                                },
                                Message = sql4CdsError.Message
                            });
                        }
                    }

                    await _lsp.NotifyAsync(Methods.TextDocumentPublishDiagnostics, new PublishDiagnosticParams
                    {
                        Uri = new Uri(request.OwnerUri),
                        Diagnostics = diagnostics.ToArray()
                    });
                }

                await _lsp.NotifyAsync(MessageEvent.Type, new MessageParams
                {
                    OwnerUri = request.OwnerUri,
                    Message = new ResultMessage
                    {
                        BatchId = batchSummary.Id,
                        Time = DateTime.UtcNow.ToString("o"),
                        Message = GetErrorMessage(ex, sql4CdsException),
                        IsError = true
                    }
                });

                var error = ex;

                while (error.InnerException != null)
                    error = error.InnerException;

                if (error is QueryParseException parseException)
                {
                    var length = Regex.Match(qry.Substring(parseException.Error.Offset), "\\b", RegexOptions.None, TimeSpan.FromSeconds(1)).Index;

                    await _lsp.NotifyAsync(Methods.TextDocumentPublishDiagnostics, new PublishDiagnosticParams
                    {
                        Uri = new Uri(request.OwnerUri),
                        Diagnostics = new[]
                        {
                            new Diagnostic
                            {
                                Range = new Microsoft.VisualStudio.LanguageServer.Protocol.Range
                                {
                                    Start = new Position
                                    {
                                        Line = parseException.Error.Line - 1 + selection.StartLine,
                                        Character = parseException.Error.Column - 1 + (parseException.Error.Line == 1 ? selection.StartColumn : 0)
                                    },
                                    End = new Position
                                    {
                                        Line = parseException.Error.Line - 1 + selection.StartLine,
                                        Character = parseException.Error.Column - 1 + (parseException.Error.Line == 1 ? selection.StartColumn : 0) + length
                                    }
                                },
                                Message = parseException.Message
                            }
                        }
                    });
                }
            }
            finally
            {
                _commands.TryRemove(request.OwnerUri, out _);

                session.Connection.InfoMessage -= infoMessageHandler;
                session.Connection.Progress -= progressHandler;
                session.Connection.PreInsert -= confirmInsert;
                session.Connection.PreUpdate -= confirmUpdate;
                session.Connection.PreDelete -= confirmDelete;
                session.Connection.PreRetrieve -= confirmRetrieve;
            }

            var endTime = DateTime.UtcNow;

            batchSummary.ExecutionEnd = endTime.ToLocalTime().ToString("o");
            batchSummary.ExecutionElapsed = (endTime - startTime).ToString();
            batchSummary.ResultSetSummaries = resultSets.ToArray();
            batchSummary.SpecialAction = new SpecialAction();

            if (request.ExecutionPlanOptions.IncludeActualExecutionPlanXml || request.ExecutionPlanOptions.IncludeEstimatedExecutionPlanXml)
                batchSummary.SpecialAction.ExpectYukonXMLShowPlan = true;

            await _lsp.NotifyAsync(BatchCompleteEvent.Type, new BatchEventParams
            {
                OwnerUri = request.OwnerUri,
                BatchSummary = batchSummary
            });

            await _lsp.NotifyAsync(QueryCompleteEvent.Type, new QueryCompleteParams
            {
                OwnerUri = request.OwnerUri,
                BatchSummaries = new[]
                {
                    batchSummary
                }
            });
        }

        private string GetErrorMessage(Exception error, Sql4CdsException rootException)
        {
            string msg;

            if (error is AggregateException aggregateException)
                msg = String.Join("\r\n", aggregateException.InnerExceptions.Select(ex => GetErrorMessage(ex, rootException)).Where(m => !String.IsNullOrEmpty(m)));
            else if (rootException != null && rootException.Errors.Any(err => err.Message == error.Message))
                msg = "";
            else
                msg = error.Message;

            while (error.InnerException != null)
            {
                if (error.InnerException.Message != error.Message)
                    msg += "\r\n" + error.InnerException.Message;

                error = error.InnerException;
            }

            if (error is FaultException<OrganizationServiceFault> faultEx)
            {
                var fault = faultEx.Detail;

                if (fault.Message != error.Message)
                    msg += "\r\n" + fault.Message;

                if (fault.ErrorDetails.TryGetValue("Plugin.ExceptionFromPluginExecute", out var plugin))
                    msg += "\r\nError from plugin: " + plugin;

                if (!String.IsNullOrEmpty(fault.TraceText))
                    msg += "\r\nTrace log: " + fault.TraceText;

                while (fault.InnerFault != null)
                {
                    if (fault.InnerFault.Message != fault.Message)
                        msg += "\r\n" + fault.InnerFault.Message;

                    fault = fault.InnerFault;

                    if (fault.ErrorDetails.TryGetValue("Plugin.ExceptionFromPluginExecute", out plugin))
                        msg += "\r\nError from plugin: " + plugin;

                    if (!String.IsNullOrEmpty(fault.TraceText))
                        msg += "\r\nTrace log: " + fault.TraceText;
                }
            }

            return msg;
        }

        private string GetDisplayName(int count, EntityMetadata meta)
        {
            if (count == 1)
                return meta.DisplayName.UserLocalizedLabel?.Label ?? meta.LogicalName;

            return meta.DisplayCollectionName.UserLocalizedLabel?.Label ??
                meta.LogicalCollectionName ??
                meta.LogicalName;
        }

        private ExecutionPlanGraph ConvertExecutionPlan(IRootExecutionPlanNode plan, bool executed)
        {
            var id = 1;

            var sb = new StringBuilder();
            sb.Append("<!--\r\nCreated from query:\r\n\r\n");
            sb.Append(plan.Sql);

            var nodes = GetAllNodes(plan).ToList();
            var fetchXmlNodes = nodes.OfType<IFetchXmlExecutionPlanNode>().ToList();

            if (nodes.Count > fetchXmlNodes.Count)
            {
                sb.Append("\r\n\r\n‼ WARNING ‼\r\n");
                sb.Append("This query requires additional processing. This FetchXML gives the required data, but needs additional processing to format it in the same way as returned by the TDS Endpoint or SQL 4 CDS.\r\n\r\n");
                sb.Append("See the estimated execution plan to see what extra processing is performed by SQL 4 CDS");
            }

            sb.Append("\r\n\r\n-->");

            foreach (var fetchXml in nodes.OfType<IFetchXmlExecutionPlanNode>())
            {
                sb.Append("\r\n\r\n");
                sb.Append(fetchXml.FetchXmlString);
            }

            var graph = new ExecutionPlanGraph
            {
                GraphFile = new ExecutionPlanGraphInfo
                {
                    PlanIndexInFile = 0,
                    GraphFileType = "xml",
                    GraphFileContent = sb.ToString()
                },
                Query = plan.Sql,
                Recommendations = new List<ExecutionPlanRecommendation>(),
                Root = ConvertExecutionPlanNode(plan, plan.Duration, executed, ref id)
            };

            return graph;
        }

        private IEnumerable<IExecutionPlanNode> GetAllNodes(IExecutionPlanNode node)
        {
            foreach (var source in node.GetSources())
            {
                yield return source;

                foreach (var subSource in GetAllNodes(source))
                    yield return subSource;
            }
        }

        private ExecutionPlanNode ConvertExecutionPlanNode(IExecutionPlanNode node, TimeSpan totalDuration, bool executed, ref int id)
        {
            var nodeInternalDurationMS = node.Duration.TotalMilliseconds - node.GetSources().Sum(n => n.Duration.TotalMilliseconds);
            var rows = node is IDataExecutionPlanNode dataNode ? (executed ? dataNode.RowsOut : dataNode.EstimatedRowsOut).ToString() : "";

            var converted = new ExecutionPlanNode
            {
                Badges = new List<Badge>(),
                Children = new List<ExecutionPlanNode>(),
                Cost = totalDuration == TimeSpan.Zero ? 0 : nodeInternalDurationMS / totalDuration.TotalMilliseconds,
                CostMetrics = new List<CostMetric>(),
                Description = node is IFetchXmlExecutionPlanNode fetchNode ? "<pre style='line-height: 60%'>" + WebUtility.HtmlEncode(fetchNode.FetchXmlString) + "</pre>" : null,
                Edges = new List<ExecutionPlanEdges>(),
                ElapsedTimeInMs = (long)nodeInternalDurationMS,
                ID = id++,
                Name = node.ToString(),
                Properties = new List<ExecutionPlanGraphPropertyBase>(),
                RowCountDisplayString = rows,
                Subtext = node.ToString().Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries),
                TopOperationsData = GetTopOperations(node, executed),
                Type = node.GetType().Name.Replace("Node", "")
            };

            if (node is IExecutionPlanNodeWarning warning && warning.Warning != null)
            {
                converted.Badges.Add(new Badge { Type = BadgeType.Warning });
                converted.Description = "<p>" + WebUtility.HtmlEncode(warning.Warning) + "</p>";
            }

            if (executed)
            {
                converted.CostMetrics.Add(new CostMetric
                {
                    Name = "ElapsedCpuTime",
                    Value = nodeInternalDurationMS.ToString()
                });

                if (node is IDataExecutionPlanNode)
                {
                    converted.CostMetrics.Add(new CostMetric
                    {
                        Name = "ActualRows",
                        Value = rows
                    });
                }

                converted.CostDisplayString = converted.Cost.ToString("P1");
            }
            else
            {
                converted.CostMetrics.Add(new CostMetric
                {
                    Name = "EstimatedRows",
                    Value = rows
                });

                converted.CostDisplayString = "";
            }

            // Tweak the Type so we get the right icon
            converted.Type = converted.Type.Substring(0, 1).ToLower() + converted.Type.Substring(1);

            if (converted.Type == "fetchXmlScan")
                converted.Type = "fetchQuery";
            else if (converted.Type == "select")
                converted.Type = "result";
            else if (converted.Type == "nestedLoop")
                converted.Type = "nestedLoops";
            else if (converted.Type == "hashJoin")
                converted.Type = "hashMatch";
            else if (converted.Type == "metadataQuery")
                converted.Type = "fetchQuery";
            else if (converted.Type == "executeMessage")
                converted.Type = "tableValuedFunction";
            else if (converted.Type == "declareVariables")
                converted.Type = "declare";
            else if (converted.Type == "assignVariables")
                converted.Type = "assign";
            else if (converted.Type == "sql")
                converted.Type = "tsql";
            else if (converted.Type == "alias")
                converted.Type = "computeScalar";
            else if (converted.Type == "hashMatchAggregate")
                converted.Type = "hashMatch";
            else if (converted.Type == "insert")
                converted.Type = "tableInsert";
            else if (converted.Type == "delete")
                converted.Type = "tableDelete";
            else if (converted.Type == "bulkDelete")
                converted.Type = "remoteDelete";
            else if (converted.Type == "concatenate")
                converted.Type = "concatenation";
            else if (converted.Type == "distinct")
                converted.Type = "hashMatch";
            else if (converted.Type == "conditional")
                converted.Type = "ifOperator";
            else if (converted.Type == "executeAs")
                converted.Type = "languageConstructCatchAll";
            else if (converted.Type == "revert")
                converted.Type = "languageConstructCatchAll";
            else if (converted.Type == "globalOptionSetQuery")
                converted.Type = "fetchQuery";
            else if (converted.Type == "offsetFetch")
                converted.Type = "top";
            else if (converted.Type == "partitionedAggregate")
                converted.Type = "hashMatchRoot";
            else if (converted.Type == "tryCatch")
                converted.Type = "languageConstructCatchAll";
            else if (converted.Type == "waitFor")
                converted.Type = "languageConstructCatchAll";
            else if (converted.Type == "xmlWriter")
                converted.Type = "udx";
            else if (converted.Type == "adaptiveIndexSpool")
                converted.Type = "indexSpool";
            else if (converted.Type == "openJson")
                converted.Type = "tableValuedFunction";
            else if (converted.Type == "systemFunction")
                converted.Type = "tableValuedFunction";

            // Get the filtered list of properties
            var typeDescriptor = new ExecutionPlanNodeTypeDescriptor(node, !executed, _ => null);
            converted.Properties = ConvertProperties(typeDescriptor, typeDescriptor.GetProperties(null));

            if (!String.IsNullOrEmpty(converted.Description) && converted.Properties.Count > 0)
                converted.Description += "<hr />";

            converted.Description += "<table>";
            foreach (var prop in converted.Properties)
            {
                if (prop.Name != "FetchXML")
                    converted.Description += "<tr><th style='text-align: left'>" + prop.Name + "</th><td>" + prop.DisplayValue + "</td></tr>";
            }
            converted.Description += "</table>";

            foreach (var source in node.GetSources())
            {
                var sourceRows = source is IDataExecutionPlanNode dataChild ? executed ? dataChild.RowsOut : dataChild.EstimatedRowsOut : 1;

                converted.Children.Add(ConvertExecutionPlanNode(source, totalDuration, executed, ref id));
                converted.Edges.Add(new ExecutionPlanEdges
                {
                    RowCount = sourceRows,
                    RowSize = 1,
                    Properties = new List<ExecutionPlanGraphPropertyBase>
                    {
                        new ExecutionPlanGraphProperty
                        {

                        }
                    }
                });
            }

            return converted;
        }

        private List<TopOperationsDataItem> GetTopOperations(IExecutionPlanNode node, bool executed)
        {
            var result = new List<TopOperationsDataItem>();

            result.Add(new TopOperationsDataItem
            {
                ColumnName = "Operation",
                DataType = PropertyValueDataType.String,
                DisplayValue = node.ToString()
            });

            if (node is IDataExecutionPlanNode dataNode)
            {
                if (executed)
                {
                    result.Add(new TopOperationsDataItem
                    {
                        ColumnName = "ActualRows",
                        DataType = PropertyValueDataType.Number,
                        DisplayValue = dataNode.RowsOut.ToString()
                    });

                    result.Add(new TopOperationsDataItem
                    {
                        ColumnName = "ActualExecutions",
                        DataType = PropertyValueDataType.Number,
                        DisplayValue = node.ExecutionCount.ToString()
                    });

                    result.Add(new TopOperationsDataItem
                    {
                        ColumnName = "SubtreeDuration",
                        DataType = PropertyValueDataType.Number,
                        DisplayValue = node.Duration.TotalMilliseconds.ToString()
                    });

                    result.Add(new TopOperationsDataItem
                    {
                        ColumnName = "NodeDuration",
                        DataType = PropertyValueDataType.Number,
                        DisplayValue = (node.Duration.TotalMilliseconds - node.GetSources().Sum(n => n.Duration.TotalMilliseconds)).ToString()
                    });
                }
                else
                {
                    result.Add(new TopOperationsDataItem
                    {
                        ColumnName = "EstimatedRows",
                        DataType = PropertyValueDataType.Number,
                        DisplayValue = dataNode.EstimatedRowsOut.ToString()
                    });
                }
            }

            return result;
        }

        class TypeDescriptorContext : ITypeDescriptorContext
        {
            public TypeDescriptorContext(object instance, PropertyDescriptor prop)
            {
                Instance = instance;
                PropertyDescriptor = prop;
            }

            public IContainer Container => null;

            public object Instance { get; }

            public PropertyDescriptor PropertyDescriptor { get; }

            public object GetService(Type serviceType) => null;

            public void OnComponentChanged()
            {
            }

            public bool OnComponentChanging() => false;
        }

        private List<ExecutionPlanGraphPropertyBase> ConvertProperties(object value, PropertyDescriptorCollection props)
        {
            var converted = new List<ExecutionPlanGraphPropertyBase>();

            foreach (PropertyDescriptor prop in props)
            {
                var context = new TypeDescriptorContext(value, prop);
                var propValue = prop.GetValue(value);
                var displayValue = prop.Converter.ConvertToString(context, CultureInfo.CurrentCulture, propValue);

                if (prop.Converter.GetPropertiesSupported(context))
                {
                    converted.Add(new NestedExecutionPlanGraphProperty
                    {
                        Name = prop.DisplayName,
                        Value = ConvertProperties(propValue, prop.Converter.GetProperties(context, propValue, null)),
                        DisplayValue = displayValue,
                        DisplayOrder = converted.Count,
                        DataType = PropertyValueDataType.Nested
                    });
                }
                else
                {
                    converted.Add(new ExecutionPlanGraphProperty
                    {
                        Name = prop.DisplayName,
                        Value = displayValue,
                        DisplayValue = displayValue,
                        DisplayOrder = converted.Count,
                        DataType = prop.PropertyType == typeof(bool) ? PropertyValueDataType.Boolean : prop.PropertyType == typeof(int) ? PropertyValueDataType.Number : PropertyValueDataType.String,
                        ShowInTooltip = prop.Name == nameof(IFetchXmlExecutionPlanNode.FetchXmlString)
                    });
                }
            }

            return converted;
        }

        public SubsetResult HandleSubset(SubsetParams request)
        {
            var resultSet = _resultSets[request.OwnerUri][request.ResultSetIndex];

            if (resultSet.SpecialAction.ExpectYukonXMLShowPlan)
            {
                return new SubsetResult
                {
                    ResultSubset = new ResultSetSubset
                    {
                        RowCount = 1,
                        Rows = new[]
                        {
                            new[]
                            {
                                new DbCellValue
                                {
                                    DisplayValue = "", //DemoExecutionPlan,
                                    InvariantCultureDisplayValue = "", //DemoExecutionPlan,
                                    IsNull = false,
                                    RawObject = "" //DemoExecutionPlan
                                }
                            }
                        }
                    }
                };
            }

            return new SubsetResult
            {
                ResultSubset = new ResultSetSubset
                {
                    RowCount = 1,
                    Rows = resultSet.Values
                        .Skip((int)request.RowsStartIndex)
                        .Take(request.RowsCount)
                        .Select(row => row
                            .Select((value, colIndex) =>
                            {
                                var col = resultSet.ColumnInfo[colIndex];
                                return ValueFormatter.Format(value, col.DataTypeName, col.NumericScale.GetValueOrDefault(), Sql4CdsSettings.Instance.LocalFormatDates);
                            })
                            .ToArray())
                        .ToArray()
                }
            };
        }

        public QueryCancelResult HandleQueryCancel(QueryCancelParams request)
        {
            if (_commands.TryGetValue(request.OwnerUri, out var cmd))
            {
                try
                {
                    cmd.Cancel();
                    return new QueryCancelResult();
                }
                catch (Exception ex)
                {
                    return new QueryCancelResult { Messages = ex.Message };
                }
            }

            return new QueryCancelResult();
        }

        public QueryExecutionPlanResult HandleQueryExecutionPlan(QueryExecutionPlanParams request)
        {
            return new QueryExecutionPlanResult
            {
                ExecutionPlan = new ExecutionPlan
                {
                    Format = "xml",
                    Content = "" //DemoExecutionPlan
                }
            };
        }

        public QueryDisposeResult HandleQueryDispose(QueryDisposeParams request)
        {
            _resultSets.Remove(request.OwnerUri, out _);
            return new QueryDisposeResult();
        }

        public SaveResultRequestResult HandleExportCsv(SaveResultsAsCsvRequestParams request)
        {
            var factory = new SaveAsCsvFileStreamFactory { SaveRequestParams = request };
            return SaveResultsHelper(request, factory);
        }

        public SaveResultRequestResult HandleExportExcel(SaveResultsAsExcelRequestParams request)
        {
            var factory = new SaveAsExcelFileStreamFactory
            {
                SaveRequestParams = request,
                UrlGenerator = _connectionManager.GetConnection(request.OwnerUri).DataSource.GetEntityReferenceUrl
            };
            return SaveResultsHelper(request, factory);
        }

        public SaveResultRequestResult HandleExportJson(SaveResultsAsJsonRequestParams request)
        {
            var factory = new SaveAsJsonFileStreamFactory
            {
                SaveRequestParams = request
            };
            return SaveResultsHelper(request, factory);
        }

        public SaveResultRequestResult HandleExportMarkdown(SaveResultsAsMarkdownRequestParams request)
        {
            var factory = new SaveAsMarkdownFileStreamFactory(request, _connectionManager.GetConnection(request.OwnerUri).DataSource.GetEntityReferenceUrl);
            return SaveResultsHelper(request, factory);
        }

        public SaveResultRequestResult HandleExportXml(SaveResultsAsXmlRequestParams request)
        {
            var factory = new SaveAsXmlFileStreamFactory
            {
                SaveRequestParams = request
            };
            return SaveResultsHelper(request, factory);
        }

        private SaveResultRequestResult SaveResultsHelper(SaveResultsRequestParams request, IFileStreamFactory factory)
        {
            try
            {
                var resultSet = _resultSets[request.OwnerUri][request.ResultSetIndex];

                using (var writer = factory.GetWriter(request.FilePath, resultSet.ColumnInfo))
                {
                    foreach (var row in resultSet.Values)
                    {
                        writer.WriteRow(row.Select((value, colIndex) =>
                        {
                            var col = resultSet.ColumnInfo[colIndex];
                            return ValueFormatter.Format(value, col.DataTypeName, col.NumericScale.GetValueOrDefault(), Sql4CdsSettings.Instance.LocalFormatDates);
                        }).ToArray(), resultSet.ColumnInfo);
                    }
                }

                return new SaveResultRequestResult();
            }
            catch (Exception ex)
            {
                return new SaveResultRequestResult { Messages = ex.Message };
            }
        }
    }
}
