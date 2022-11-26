using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Data.SqlTypes;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.Engine;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using MediatR;
using Microsoft.SqlTools.ServiceLayer.ExecutionPlan.Contracts;
using Microsoft.SqlTools.ServiceLayer.ExecutionPlan.ShowPlan;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json.Linq;
using OmniSharp.Extensions.JsonRpc;
using OmniSharp.Extensions.LanguageServer.Protocol.Models;
using OmniSharp.Extensions.LanguageServer.Protocol.Server;

namespace MarkMpn.Sql4Cds.LanguageServer
{
    [Method("query/executeDocumentSelection")]
    [Serial]
    class ExecuteHandler : IRequestHandler<ExecuteDocumentSelectionParams,ExecuteRequestResult>, IRequestHandler<SubsetParams, SubsetResult>, IRequestHandler<QueryCancelParams, QueryCancelResult>, IRequestHandler<QueryExecutionPlanParams, QueryExecutionPlanResult>, IJsonRpcHandler
    {
        private readonly ILanguageServerFacade _lsp;
        private readonly ConnectionManager _connectionManager;
        private readonly TextDocumentManager _documentManager;
        private readonly ConcurrentDictionary<string, List<ResultSetSummary>> _resultSets;
        private readonly ConcurrentDictionary<string, IDbCommand> _commands;

        public ExecuteHandler(ILanguageServerFacade lsp, ConnectionManager connectionManager, TextDocumentManager documentManager)
        {
            _lsp = lsp;
            _connectionManager = connectionManager;
            _documentManager = documentManager;
            _resultSets = new ConcurrentDictionary<string, List<ResultSetSummary>>();
            _commands = new ConcurrentDictionary<string, IDbCommand>();
        }

        public Task<ExecuteRequestResult> Handle(ExecuteDocumentSelectionParams request, CancellationToken cancellationToken)
        {
            var session = _connectionManager.GetConnection(request.OwnerUri);

            if (session == null)
                return Task.FromResult(new ExecuteRequestResult()); // TODO: Send error

            Task.Run(async () =>
            {
                var doc = _documentManager.GetContent(request.OwnerUri).Split('\n');

                if (request.QuerySelection == null)
                    request.QuerySelection = new SelectionData { StartLine = 0, StartColumn = 0, EndLine = doc.Length - 1, EndColumn = doc[doc.Length - 1].Length };

                // query/batchStart (BatchEventParams)
                // query/message (MessagePArams)
                // query/resultSetAvailable (ResultSetAvailableEventPArams)
                // query/resultSetUpdated (ResultSetUpdatedEventParams)
                // query/resultSetComplete (ResultSetCompleteEventParams)
                // query/batchComplete (BatchEventParams)
                var startTime = DateTime.UtcNow;

                var batchSummary = new BatchSummary
                {
                    Id = 0,
                    ExecutionStart = startTime.ToLocalTime().ToString("o"),
                    Selection = request.QuerySelection
                };

                _lsp.SendNotification("query/batchStart", new BatchEventParams
                {
                    OwnerUri = request.OwnerUri,
                    BatchSummary = batchSummary
                });

                session.Connection.InfoMessage += (sender, msg) =>
                {
                    _lsp.SendNotification("query/message", new MessageParams
                    {
                        OwnerUri = request.OwnerUri,
                        Message = new ResultMessage
                        {
                            BatchId = batchSummary.Id,
                            Time = DateTime.UtcNow.ToString("o"),
                            Message = msg.Message
                        }
                    });
                };

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

                        cmd.CommandText = qry;

                        if (!request.ExecutionPlanOptions.IncludeEstimatedExecutionPlanXml)
                        {
                            cmd.StatementCompleted += (_, stmt) =>
                            {
                                var resultSet = new ResultSetSummary
                                {
                                    Id = resultSets.Count,
                                    BatchId = batchSummary.Id,
                                    Complete = true,
                                    ColumnInfo = new[] { new DbColumnWrapper(new ColumnInfo("Microsoft SQL Server 2005 XML Showplan", "xml")) },
                                    RowCount = 0,
                                    SpecialAction = new SpecialAction { ExpectYukonXMLShowPlan = true },
                                };
                                resultSets.Add(resultSet);

                                _lsp.SendNotification("query/resultSetAvailable", new ResultSetAvailableEventParams
                                {
                                    OwnerUri = request.OwnerUri,
                                    ResultSetSummary = resultSet,
                                });
                                _lsp.SendNotification("query/resultSetUpdated", new ResultSetUpdatedEventParams
                                {
                                    OwnerUri = request.OwnerUri,
                                    ResultSetSummary = resultSet,
                                    ExecutionPlans = new List<ExecutionPlanGraph> { ConvertExecutionPlan(stmt.Statement, true) }
                                });
                                _lsp.SendNotification("query/resultSetComplete", new ResultSetCompleteEventParams
                                {
                                    OwnerUri = request.OwnerUri,
                                    ResultSetSummary = resultSet,
                                });
                            };

                            using (var reader = await cmd.ExecuteReaderAsync())
                            {
                                while (reader.FieldCount > 0)
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

                                    for (var i = 0; i < reader.FieldCount; i++)
                                        resultSet.ColumnInfo[i] = new DbColumnWrapper(new ColumnInfo(reader.GetName(i), reader.GetDataTypeName(i)));

                                    resultSets.Add(resultSet);

                                    _lsp.SendNotification("query/resultSetAvailable", new ResultSetAvailableEventParams
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

                                        _lsp.SendNotification("query/resultSetUpdated", new ResultSetUpdatedEventParams
                                        {
                                            OwnerUri = request.OwnerUri,
                                            ResultSetSummary = resultSet,
                                        });
                                    }

                                    resultSet.Complete = true;

                                    _lsp.SendNotification("query/resultSetComplete", new ResultSetCompleteEventParams
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
                                ColumnInfo = new[] { new DbColumnWrapper(new ColumnInfo("Microsoft SQL Server 2005 XML Showplan", "xml")) },
                                RowCount = 0,
                                SpecialAction = new SpecialAction { ExpectYukonXMLShowPlan = true },
                            };
                            resultSets.Add(resultSet);
                                
                            _lsp.SendNotification("query/resultSetAvailable", new ResultSetAvailableEventParams
                            {
                                OwnerUri = request.OwnerUri,
                                ResultSetSummary = resultSet,
                            });
                            _lsp.SendNotification("query/resultSetUpdated", new ResultSetUpdatedEventParams
                            {
                                OwnerUri = request.OwnerUri,
                                ResultSetSummary = resultSet,
                                ExecutionPlans = cmd.GeneratePlan(false).Select(plan => ConvertExecutionPlan(plan, false)).ToList()// ExecutionPlanGraphUtils.CreateShowPlanGraph(DemoExecutionPlan, null)
                            });
                            _lsp.SendNotification("query/resultSetComplete", new ResultSetCompleteEventParams
                            {
                                OwnerUri = request.OwnerUri,
                                ResultSetSummary = resultSet,
                            });
                        }
                    }
                }
                catch (Exception ex)
                {
                    _lsp.SendNotification("query/message", new MessageParams
                    {
                        OwnerUri = request.OwnerUri,
                        Message = new ResultMessage
                        {
                            BatchId = batchSummary.Id,
                            Time = DateTime.UtcNow.ToString("o"),
                            Message = ex.Message,
                            IsError = true
                        }
                    });
                }
                finally
                {
                    _commands.TryRemove(request.OwnerUri, out _);
                }

                var endTime = DateTime.UtcNow;

                batchSummary.ExecutionEnd = endTime.ToLocalTime().ToString("o");
                batchSummary.ExecutionElapsed = (endTime - startTime).ToString();
                batchSummary.ResultSetSummaries = resultSets.ToArray();
                batchSummary.SpecialAction = new SpecialAction();

                if (request.ExecutionPlanOptions.IncludeActualExecutionPlanXml || request.ExecutionPlanOptions.IncludeEstimatedExecutionPlanXml)
                    batchSummary.SpecialAction.ExpectYukonXMLShowPlan = true;

                _lsp.SendNotification("query/batchComplete", new BatchEventParams
                {
                    OwnerUri = request.OwnerUri,
                    BatchSummary = batchSummary
                });

                _lsp.SendNotification("query/complete", new QueryCompleteParams
                {
                    OwnerUri = request.OwnerUri,
                    BatchSummaries = new[]
                    {
                        batchSummary
                    }
                });
            });
            return Task.FromResult(new ExecuteRequestResult());
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
            var rows = node is IDataExecutionPlanNode dataNode ? executed ? dataNode.RowsOut : dataNode.EstimatedRowsOut : 1;

            var converted = new ExecutionPlanNode
            {
                Badges = new List<Badge>(),
                Children = new List<ExecutionPlanNode>(),
                Cost = totalDuration == TimeSpan.Zero ? 0 : nodeInternalDurationMS / totalDuration.TotalMilliseconds,
                CostMetrics = new List<CostMetric>(),
                Description = null,
                Edges = new List<ExecutionPlanEdges>(),
                ElapsedTimeInMs = (long) nodeInternalDurationMS,
                ID = id++,
                Name = node.ToString(),
                Properties = new List<ExecutionPlanGraphPropertyBase>(),
                RowCountDisplayString = rows.ToString(),
                Subtext = node.ToString().Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries),
                TopOperationsData = GetTopOperations(node, executed),
                Type = node.GetType().Name.Replace("Node", "")
            };

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
                        Value = rows.ToString()
                    });
                }
            }
            else
            {
                converted.CostMetrics.Add(new CostMetric
                {
                    Name = "EstimatedRows",
                    Value = rows.ToString()
                });
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

            // Get the filtered list of properties
            var typeDescriptor = new ExecutionPlanNodeTypeDescriptor(node, !executed, _ => null);
            converted.Properties = ConvertProperties(typeDescriptor, typeDescriptor.GetProperties(null));

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

        public Task<SubsetResult> Handle(SubsetParams request, CancellationToken cancellationToken)
        {
            var resultSet = _resultSets[request.OwnerUri][request.ResultSetIndex];

            if (resultSet.SpecialAction.ExpectYukonXMLShowPlan)
            {
                return Task.FromResult(new SubsetResult
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
                });
            }

            return Task.FromResult(new SubsetResult
            {
                ResultSubset = new ResultSetSubset
                {
                    RowCount = 1,
                    Rows = resultSet.Values
                        .Skip((int)request.RowsStartIndex)
                        .Take(request.RowsCount)
                        .Select(row => row
                            .Select(value => new DbCellValue
                            {
                                DisplayValue = value?.ToString(),
                                InvariantCultureDisplayValue = value?.ToString(),
                                IsNull = value == null || value == DBNull.Value,
                                RawObject = value,
                            })
                            .ToArray())
                        .ToArray()
                }
            });
        }

        public Task<QueryCancelResult> Handle(QueryCancelParams request, CancellationToken cancellationToken)
        {
            if (_commands.TryGetValue(request.OwnerUri, out var cmd))
            {
                try
                {
                    cmd.Cancel();
                    return Task.FromResult(new QueryCancelResult());
                }
                catch (Exception ex)
                {
                    return Task.FromResult(new QueryCancelResult { Messages = ex.Message });
                }
            }

            return Task.FromResult(new QueryCancelResult());
        }

        public Task<QueryExecutionPlanResult> Handle(QueryExecutionPlanParams request, CancellationToken cancellationToken)
        {
            return Task.FromResult(new QueryExecutionPlanResult
            {
                ExecutionPlan = new ExecutionPlan
                {
                    Format = "xml",
                    Content = "" //DemoExecutionPlan
                }
            });
        }
    }


    [Method("query/subset")]
    [Serial]
    public class SubsetParams : IRequest<SubsetResult>
    {
        /// <summary>
        /// URI for the file that owns the query to look up the results for
        /// </summary>
        public string OwnerUri { get; set; }

        /// <summary>
        /// Index of the batch to get the results from
        /// </summary>
        public int BatchIndex { get; set; }

        /// <summary>
        /// Index of the result set to get the results from
        /// </summary>
        public int ResultSetIndex { get; set; }

        /// <summary>
        /// Beginning index of the rows to return from the selected resultset. This index will be
        /// included in the results.
        /// </summary>
        public long RowsStartIndex { get; set; }

        /// <summary>
        /// Number of rows to include in the result of this request. If the number of the rows
        /// exceeds the number of rows available after the start index, all available rows after
        /// the start index will be returned.
        /// </summary>
        public int RowsCount { get; set; }
    }


    [Method("query/cancel")]
    [Serial]
    public class QueryCancelParams : IRequest<QueryCancelResult>
    {
        public string OwnerUri { get; set; }
    }

    public class QueryCancelResult
    {
        /// <summary>
        /// Any error messages that occurred during disposing the result set. Optional, can be set
        /// to null if there were no errors.
        /// </summary>
        public string Messages { get; set; }
    }

    public class SubsetResult
    {
        /// <summary>
        /// The requested subset of results. Optional, can be set to null to indicate an error
        /// </summary>
        public ResultSetSubset ResultSubset { get; set; }
    }

    public class ResultSetSubset
    {
        /// <summary>
        /// The number of rows returned from result set, useful for determining if less rows were
        /// returned than requested.
        /// </summary>
        public int RowCount { get; set; }

        /// <summary>
        /// 2D array of the cell values requested from result set
        /// </summary>
        public DbCellValue[][] Rows { get; set; }
    }


    /// <summary>
    /// Class used for internally passing results from a cell around.
    /// </summary>
    public class DbCellValue
    {
        /// <summary>
        /// Display value for the cell, suitable to be passed back to the client
        /// </summary>
        public string DisplayValue { get; set; }

        /// <summary>
        /// Whether or not the cell is NULL
        /// </summary>
        public bool IsNull { get; set; }

        /// <summary>
        /// Culture invariant display value for the cell, this value can later be used by the client to convert back to the original value.
        /// </summary>
        public string InvariantCultureDisplayValue { get; set; }

        /// <summary>
        /// The raw object for the cell, for use internally
        /// </summary>
        internal object RawObject { get; set; }

        /// <summary>
        /// The internal ID for the row. Should be used when directly referencing the row for edit
        /// or other purposes.
        /// </summary>
        public long RowId { get; set; }

        /// <summary>
        /// Copies the values of this DbCellValue into another DbCellValue (or child object)
        /// </summary>
        /// <param name="other">The DbCellValue (or child) that will receive the values</param>
        public virtual void CopyTo(DbCellValue other)
        {
            other.DisplayValue = DisplayValue;
            other.InvariantCultureDisplayValue = InvariantCultureDisplayValue;
            other.IsNull = IsNull;
            other.RawObject = RawObject;
            other.RowId = RowId;
        }
    }

    public class QueryCompleteParams
    {
        /// <summary>
        /// URI for the editor that owns the query
        /// </summary>
        public string OwnerUri { get; set; }

        /// <summary>
        /// Summaries of the result sets that were returned with the query
        /// </summary>
        public BatchSummary[] BatchSummaries { get; set; }
    }

    /// <summary>
    /// Base class of parameters to return when a result set is available, updated or completed
    /// </summary>
    public abstract class ResultSetEventParams
    {
        public ResultSetSummary ResultSetSummary { get; set; }

        public string OwnerUri { get; set; }
    }

    /// <summary>
    /// Parameters to return when a result set is completed.
    /// </summary>
    public class ResultSetCompleteEventParams : ResultSetEventParams
    {
    }

    /// <summary>
    /// Parameters to return when a result set is available.
    /// </summary>
    public class ResultSetAvailableEventParams : ResultSetEventParams
    {
    }

    /// <summary>
    /// Parameters to return when a result set is updated
    /// </summary>
    public class ResultSetUpdatedEventParams : ResultSetEventParams
    {
        /// <summary>
        /// Execution plans for statements in the current batch.
        /// </summary>
        public List<ExecutionPlanGraph> ExecutionPlans { get; set; }
        /// <summary>
        /// Error message for exception raised while generating execution plan.
        /// </summary>
        public string ExecutionPlanErrorMessage { get; set; }
    }


    /// <summary>
    /// Parameters to be sent back with a message notification
    /// </summary>
    public class MessageParams
    {
        /// <summary>
        /// URI for the editor that owns the query
        /// </summary>
        public string OwnerUri { get; set; }

        /// <summary>
        /// The message that is being returned
        /// </summary>
        public ResultMessage Message { get; set; }
    }


    /// <summary>
    /// Result message object with timestamp and actual message
    /// </summary>
    public class ResultMessage
    {
        /// <summary>
        /// ID of the batch that generated this message. If null, this message
        /// was not generated as part of a batch
        /// </summary>
        public int? BatchId { get; set; }

        /// <summary>
        /// Whether or not this message is an error
        /// </summary>
        public bool IsError { get; set; }

        /// <summary>
        /// Timestamp of the message
        /// Stored in UTC ISO 8601 format; should be localized before displaying to any user
        /// </summary>
        public string Time { get; set; }

        /// <summary>
        /// Message contents
        /// </summary>
        public string Message { get; set; }

        /// <summary>
        /// Constructor with default "Now" time
        /// </summary>
        public ResultMessage(string message, bool isError, int? batchId)
        {
            BatchId = batchId;
            IsError = isError;
            Time = DateTime.Now.ToString("o");
            Message = message;
        }

        /// <summary>
        /// Default constructor, used for deserializing JSON RPC only
        /// </summary>
        public ResultMessage()
        {
        }
        public override string ToString() => $"Message on Batch Id:'{BatchId}', IsError:'{IsError}', Message:'{Message}'";
    }

    /// <summary>
    /// Parameters to be sent back as part of a batch start or complete event to indicate that a
    /// batch of a query started or completed.
    /// </summary>
    public class BatchEventParams
    {
        /// <summary>
        /// Summary of the batch that just completed
        /// </summary>
        public BatchSummary BatchSummary { get; set; }

        /// <summary>
        /// URI for the editor that owns the query
        /// </summary>
        public string OwnerUri { get; set; }
    }

    /// <summary>
    /// Summary of a batch within a query
    /// </summary>
    public class BatchSummary
    {
        /// <summary>
        /// Localized timestamp for how long it took for the execution to complete
        /// </summary>
        public string ExecutionElapsed { get; set; }

        /// <summary>
        /// Localized timestamp for when the execution completed.
        /// </summary>
        public string ExecutionEnd { get; set; }

        /// <summary>
        /// Localized timestamp for when the execution started.
        /// </summary>
        public string ExecutionStart { get; set; }

        /// <summary>
        /// Whether or not the batch encountered an error that halted execution
        /// </summary>
        public bool HasError { get; set; }

        /// <summary>
        /// The ID of the result set within the query results
        /// </summary>
        public int Id { get; set; }

        /// <summary>
        /// The selection from the file for this batch
        /// </summary>
        public SelectionData Selection { get; set; }

        /// <summary>
        /// The summaries of the result sets inside the batch
        /// </summary>
        public ResultSetSummary[] ResultSetSummaries { get; set; }

        /// <summary>
        /// The special action of the batch 
        /// </summary>
        public SpecialAction SpecialAction { get; set; }

        public override string ToString() => $"Batch Id:'{Id}', Elapsed:'{ExecutionElapsed}', HasError:'{HasError}'";
    }

    /// <summary>
    /// Class that represents a Special Action which occured by user request during the query 
    /// </summary>
    public class SpecialAction
    {

        #region Private Class variables 

        // Underlying representation as bitwise flags to simplify logic
        [Flags]
        private enum ActionFlags
        {
            None = 0,
            // All added options must be powers of 2
            ExpectYukonXmlShowPlan = 1
        }

        private ActionFlags flags;

        #endregion

        /// <summary>
        /// The type of XML execution plan that is contained with in a result set  
        /// </summary>
        public SpecialAction()
        {
            flags = ActionFlags.None;
        }

        #region Public Functions
        /// <summary>
        /// No Special action performed 
        /// </summary>
        public bool None
        {
            get { return flags == ActionFlags.None; }
            set
            {
                flags = ActionFlags.None;
            }
        }

        /// <summary>
        /// Contains an XML execution plan result set  
        /// </summary>
        public bool ExpectYukonXMLShowPlan
        {
            get { return flags.HasFlag(ActionFlags.ExpectYukonXmlShowPlan); }
            set
            {
                if (value)
                {
                    // OR flags with value to apply 
                    flags |= ActionFlags.ExpectYukonXmlShowPlan;
                }
                else
                {
                    // AND flags with the inverse of the value we want to remove
                    flags &= ~(ActionFlags.ExpectYukonXmlShowPlan);
                }
            }
        }

        /// <summary>
        /// Aggregate this special action with the input
        /// </summary>
        public void CombineSpecialAction(SpecialAction action)
        {
            flags |= ((action?.flags) ?? ActionFlags.None);
        }
        public override string ToString() => $"ActionFlag:'{flags}', ExpectYukonXMLShowPlan:'{ExpectYukonXMLShowPlan}'";
        #endregion
    };

    /// <summary>
    /// Represents a summary of information about a result without returning any cells of the results
    /// </summary>
    public class ResultSetSummary
    {
        /// <summary>
        /// The ID of the result set within the batch results
        /// </summary>
        public int Id { get; set; }

        /// <summary>
        /// The ID of the batch set within the query
        /// </summary>
        public int BatchId { get; set; }

        /// <summary>
        /// The number of rows that are available for the resultset thus far
        /// </summary>
        public long RowCount { get; set; }

        /// <summary>
        /// If true it indicates that all rows have been fetched and the RowCount being sent across is final for this ResultSet
        /// </summary>
        public bool Complete { get; set; }

        /// <summary>
        /// Details about the columns that are provided as solutions
        /// </summary>
        public DbColumnWrapper[] ColumnInfo { get; set; }

        /// <summary>
        /// The special action definition of the result set 
        /// </summary>
        public SpecialAction SpecialAction { get; set; }

        /// <summary>
        /// The visualization options for the client to render charts.
        /// </summary>
        public VisualizationOptions Visualization { get; set; }

        internal List<object[]> Values { get; } = new List<object[]>();

        /// <summary>
        /// Returns a string represents the current object.
        /// </summary>
        public override string ToString() => $"Result Summary Id:{Id}, Batch Id:'{BatchId}', RowCount:'{RowCount}', Complete:'{Complete}', SpecialAction:'{SpecialAction}', Visualization:'{Visualization}'";
    }

    /// <summary>
    /// Wrapper around a DbColumn, which provides extra functionality, but can be used as a
    /// regular DbColumn
    /// </summary>
    public class DbColumnWrapper : DbColumn
    {
        #region Constants

        /// <summary>
        /// All types supported by the server, stored as a hash set to provide O(1) lookup
        /// </summary>
        private static readonly HashSet<string> AllServerDataTypes = new HashSet<string>
        {
            "bigint",
            "binary",
            "bit",
            "char",
            "datetime",
            "decimal",
            "float",
            "image",
            "int",
            "money",
            "nchar",
            "ntext",
            "nvarchar",
            "real",
            "uniqueidentifier",
            "smalldatetime",
            "smallint",
            "smallmoney",
            "text",
            "timestamp",
            "tinyint",
            "varbinary",
            "varchar",
            "sql_variant",
            "xml",
            "date",
            "time",
            "datetimeoffset",
            "datetime2"
        };

        private const string SqlXmlDataTypeName = "xml";
        private const string DbTypeXmlDataTypeName = "DBTYPE_XML";
        private const string UnknownTypeName = "unknown";

        #endregion

        /// <summary>
        /// Constructor for a DbColumnWrapper
        /// </summary>
        /// <remarks>Most of this logic is taken from SSMS ColumnInfo class</remarks>
        /// <param name="column">The column we're wrapping around</param>
        public DbColumnWrapper(DbColumn column)
        {
            // Set all the fields for the base
            AllowDBNull = column.AllowDBNull;
            BaseCatalogName = column.BaseCatalogName;
            BaseColumnName = column.BaseColumnName;
            BaseSchemaName = column.BaseSchemaName;
            BaseServerName = column.BaseServerName;
            BaseTableName = column.BaseTableName;
            ColumnOrdinal = column.ColumnOrdinal;
            ColumnSize = column.ColumnSize;
            IsAliased = column.IsAliased;
            IsAutoIncrement = column.IsAutoIncrement;
            IsExpression = column.IsExpression;
            IsHidden = column.IsHidden;
            IsIdentity = column.IsIdentity;
            IsKey = column.IsKey;
            IsLong = column.IsLong;
            IsReadOnly = column.IsReadOnly;
            IsUnique = column.IsUnique;
            NumericPrecision = column.NumericPrecision;
            NumericScale = column.NumericScale;
            UdtAssemblyQualifiedName = column.UdtAssemblyQualifiedName;
            DataType = column.DataType;
            DataTypeName = column.DataTypeName.ToLowerInvariant();

            DetermineSqlDbType();
            AddNameAndDataFields(column.ColumnName);

            if (IsUdt)
            {
                // udtassemblyqualifiedname property is used to find if the datatype is of hierarchyid assembly type 
                // Internally hiearchyid is sqlbinary so providerspecific type and type is changed to sqlbinarytype
                object assemblyQualifiedName = column.UdtAssemblyQualifiedName;
                const string hierarchyId = "MICROSOFT.SQLSERVER.TYPES.SQLHIERARCHYID";

                if (assemblyQualifiedName != null
                && assemblyQualifiedName.ToString().StartsWith(hierarchyId, StringComparison.OrdinalIgnoreCase))
                {
                    DataType = typeof(SqlBinary);
                }
                else
                {
                    DataType = typeof(byte[]);
                }
            }
            else
            {
                DataType = column.DataType;
            }
        }

        public DbColumnWrapper(ColumnInfo columnInfo)
        {
            DataTypeName = columnInfo.DataTypeName.ToLowerInvariant();
            DetermineSqlDbType();
            DataType = TypeConvertor.ToNetType(this.SqlDbType);
            if (DataType == typeof(String))
            {
                this.ColumnSize = int.MaxValue;
            }
            AddNameAndDataFields(columnInfo.Name);
        }


        /// <summary>
        /// Default constructor, used for deserializing JSON RPC only
        /// </summary>
        public DbColumnWrapper()
        {
        }

        #region Properties

        /// <summary>
        /// Whether or not the column is bytes
        /// </summary>
        public bool IsBytes { get; private set; }

        /// <summary>
        /// Whether or not the column is a character type
        /// </summary>
        public bool IsChars { get; private set; }

        /// <summary>
        /// Whether or not the column is a SqlVariant type
        /// </summary>
        public bool IsSqlVariant { get; private set; }

        /// <summary>
        /// Whether or not the column is a user-defined type
        /// </summary>
        public bool IsUdt { get; private set; }

        /// <summary>
        /// Whether or not the column is XML
        /// </summary>
        public bool IsXml { get; set; }

        /// <summary>
        /// Whether or not the column is JSON
        /// </summary>
        public bool IsJson { get; set; }

        /// <summary>
        /// The SqlDbType of the column, for use in a SqlParameter
        /// </summary>
        public SqlDbType SqlDbType { get; private set; }

        /// <summary>
        /// Whther this is a HierarchyId column
        /// </summary>
        public bool IsHierarchyId { get; set; }

        /// <summary>
        /// Whether or not the column is an XML Reader type.
        /// </summary>
        /// <remarks>
        /// Logic taken from SSDT determination of whether a column is a SQL XML type. It may not
        /// be possible to have XML readers from .NET Core SqlClient.
        /// </remarks>
        public bool IsSqlXmlType => DataTypeName.Equals(SqlXmlDataTypeName, StringComparison.OrdinalIgnoreCase) ||
                                    DataTypeName.Equals(DbTypeXmlDataTypeName, StringComparison.OrdinalIgnoreCase) ||
                                    DataType == typeof(System.Xml.XmlReader);

        /// <summary>
        /// Whether or not the column is an unknown type
        /// </summary>
        /// <remarks>
        /// Logic taken from SSDT determination of unknown columns. It may not even be possible to
        /// have "unknown" column types with the .NET Core SqlClient.
        /// </remarks>
        public bool IsUnknownType => DataType == typeof(object) &&
                                     DataTypeName.Equals(UnknownTypeName, StringComparison.OrdinalIgnoreCase);

        /// <summary>
        /// Whether or not the column can be updated, based on whether it's an auto increment
        /// column, is an XML reader column, and if it's read only.
        /// </summary>
        /// <remarks>
        /// Logic taken from SSDT determination of updatable columns
        /// Special treatment for HierarchyId since we are using an Expression for HierarchyId column and expression column is readonly.
        /// </remarks>
        public bool IsUpdatable => (IsAutoIncrement != true &&
                                   IsReadOnly != true &&
                                   !IsSqlXmlType) || IsHierarchyId;

        #endregion


        private void DetermineSqlDbType()
        {
            // Determine the SqlDbType
            SqlDbType type;
            if (Enum.TryParse(DataTypeName, true, out type))
            {
                SqlDbType = type;
            }
            else
            {
                switch (DataTypeName)
                {
                    case "numeric":
                        SqlDbType = SqlDbType.Decimal;
                        break;
                    case "sql_variant":
                        SqlDbType = SqlDbType.Variant;
                        break;
                    case "timestamp":
                        SqlDbType = SqlDbType.VarBinary;
                        break;
                    case "sysname":
                        SqlDbType = SqlDbType.NVarChar;
                        break;
                    default:
                        SqlDbType = DataTypeName.EndsWith(".sys.hierarchyid") ? SqlDbType.Binary : SqlDbType.Udt;
                        break;
                }
            }
        }

        private void AddNameAndDataFields(string columnName)
        {
            // We want the display name for the column to always exist
            ColumnName = string.IsNullOrEmpty(columnName)
                ? null//SR.QueryServiceColumnNull
                : columnName;

            switch (DataTypeName)
            {
                case "varchar":
                case "nvarchar":
                    IsChars = true;

                    Debug.Assert(ColumnSize.HasValue);
                    if (ColumnSize.Value == int.MaxValue)
                    {
                        //For Yukon, special case nvarchar(max) with column name == "Microsoft SQL Server 2005 XML Showplan" -
                        //assume it is an XML showplan.
                        //Please note this field must be in sync with a similar field defined in QESQLBatch.cs.
                        //This is not the best fix that we could do but we are trying to minimize code impact
                        //at this point. Post Yukon we should review this code again and avoid
                        //hard-coding special column name in multiple places.
                        const string yukonXmlShowPlanColumn = "Microsoft SQL Server 2005 XML Showplan";
                        if (columnName == yukonXmlShowPlanColumn)
                        {
                            // Indicate that this is xml to apply the right size limit
                            // Note we leave chars type as well to use the right retrieval mechanism.
                            IsXml = true;
                        }
                        IsLong = true;
                    }
                    break;
                case "text":
                case "ntext":
                    IsChars = true;
                    IsLong = true;
                    break;
                case "xml":
                    IsXml = true;
                    IsLong = true;
                    break;
                case "binary":
                case "image":
                    IsBytes = true;
                    IsLong = true;
                    break;
                case "varbinary":
                case "rowversion":
                    IsBytes = true;

                    Debug.Assert(ColumnSize.HasValue);
                    if (ColumnSize.Value == int.MaxValue)
                    {
                        IsLong = true;
                    }
                    break;
                case "sql_variant":
                    IsSqlVariant = true;
                    break;
                default:
                    if (!AllServerDataTypes.Contains(DataTypeName))
                    {
                        // treat all UDT's as long/bytes data types to prevent the CLR from attempting
                        // to load the UDT assembly into our process to call ToString() on the object.

                        IsUdt = true;
                        IsBytes = true;
                        IsLong = true;
                    }
                    break;
            }
        }
    }



    public class ColumnInfo
    {
        /// <summary>
        /// Name of this column
        /// </summary>
        public string Name { get; set; }

        public string DataTypeName { get; set; }

        public ColumnInfo()
        {
        }

        public ColumnInfo(string name, string dataTypeName)
        {
            this.Name = name;
            this.DataTypeName = dataTypeName;
        }
    }


    /// <summary>
    /// Convert a base data type to another base data type
    /// </summary>
    public sealed class TypeConvertor
    {
        private static Dictionary<SqlDbType, Type> _typeMap = new Dictionary<SqlDbType, Type>();

        static TypeConvertor()
        {
            _typeMap[SqlDbType.BigInt] = typeof(Int64);
            _typeMap[SqlDbType.Binary] = typeof(Byte);
            _typeMap[SqlDbType.Bit] = typeof(Boolean);
            _typeMap[SqlDbType.Char] = typeof(String);
            _typeMap[SqlDbType.DateTime] = typeof(DateTime);
            _typeMap[SqlDbType.Decimal] = typeof(Decimal);
            _typeMap[SqlDbType.Float] = typeof(Double);
            _typeMap[SqlDbType.Image] = typeof(Byte[]);
            _typeMap[SqlDbType.Int] = typeof(Int32);
            _typeMap[SqlDbType.Money] = typeof(Decimal);
            _typeMap[SqlDbType.NChar] = typeof(String);
            _typeMap[SqlDbType.NChar] = typeof(String);
            _typeMap[SqlDbType.NChar] = typeof(String);
            _typeMap[SqlDbType.NText] = typeof(String);
            _typeMap[SqlDbType.NVarChar] = typeof(String);
            _typeMap[SqlDbType.Real] = typeof(Single);
            _typeMap[SqlDbType.UniqueIdentifier] = typeof(Guid);
            _typeMap[SqlDbType.SmallDateTime] = typeof(DateTime);
            _typeMap[SqlDbType.SmallInt] = typeof(Int16);
            _typeMap[SqlDbType.SmallMoney] = typeof(Decimal);
            _typeMap[SqlDbType.Text] = typeof(String);
            _typeMap[SqlDbType.Timestamp] = typeof(Byte[]);
            _typeMap[SqlDbType.TinyInt] = typeof(Byte);
            _typeMap[SqlDbType.VarBinary] = typeof(Byte[]);
            _typeMap[SqlDbType.VarChar] = typeof(String);
            _typeMap[SqlDbType.Variant] = typeof(Object);
            // Note: treating as string
            _typeMap[SqlDbType.Xml] = typeof(String);
            _typeMap[SqlDbType.TinyInt] = typeof(Byte);
            _typeMap[SqlDbType.TinyInt] = typeof(Byte);
            _typeMap[SqlDbType.TinyInt] = typeof(Byte);
            _typeMap[SqlDbType.TinyInt] = typeof(Byte);
        }

        private TypeConvertor()
        {

        }


        /// <summary>
        /// Convert TSQL type to .Net data type
        /// </summary>
        /// <param name="sqlDbType"></param>
        /// <returns></returns>
        public static Type ToNetType(SqlDbType sqlDbType)
        {
            Type netType;
            if (!_typeMap.TryGetValue(sqlDbType, out netType))
            {
                netType = typeof(String);
            }
            return netType;
        }
    }

    /// <summary>
    /// Represents the configuration options for data visualization
    /// </summary>
    public class VisualizationOptions
    {
        /// <summary>
        /// Gets or sets the type of the visualization
        /// </summary>
        public VisualizationType Type { get; set; }
    }

    /// <summary>
    /// The supported visualization types
    /// </summary>
    [JsonConverter(typeof(StringEnumConverter))]
    public enum VisualizationType
    {
        [EnumMember(Value = "bar")]
        Bar,
        [EnumMember(Value = "count")]
        Count,
        [EnumMember(Value = "doughnut")]
        Doughnut,
        [EnumMember(Value = "horizontalBar")]
        HorizontalBar,
        [EnumMember(Value = "image")]
        Image,
        [EnumMember(Value = "line")]
        Line,
        [EnumMember(Value = "pie")]
        Pie,
        [EnumMember(Value = "scatter")]
        Scatter,
        [EnumMember(Value = "table")]
        Table,
        [EnumMember(Value = "timeSeries")]
        TimeSeries
    }

    public abstract class ExecuteRequestParamsBase
    {
        /// <summary>
        /// URI for the editor that is asking for the query execute
        /// </summary>
        public string OwnerUri { get; set; }

        /// <summary>
        /// Execution plan options
        /// </summary>
        public ExecutionPlanOptions ExecutionPlanOptions { get; set; }

        /// <summary>
        /// Flag to get full column schema via additional queries.
        /// </summary>
        public bool GetFullColumnSchema { get; set; }
    }


    /// <summary> 
    /// Incoming execution plan options from the extension
    /// </summary>
    public struct ExecutionPlanOptions
    {

        /// <summary>
        /// Setting to return the actual execution plan as XML
        /// </summary>
        public bool IncludeActualExecutionPlanXml { get; set; }

        /// <summary>
        /// Setting to return the estimated execution plan as XML
        /// </summary>
        public bool IncludeEstimatedExecutionPlanXml { get; set; }
    }

    [Method("query/executeDocumentSelection")]
    [Serial]
    public class ExecuteDocumentSelectionParams : ExecuteRequestParamsBase, IRequest<ExecuteRequestResult>
    {
        /// <summary>
        /// The selection from the document
        /// </summary>
        public SelectionData QuerySelection { get; set; }
    }

    public class ExecuteRequestResult
    {

    }


    [Method("query/executionPlan")]
    [Serial]
    /// <summary>
    /// Parameters for query execution plan request
    /// </summary>
    public class QueryExecutionPlanParams : IRequest<QueryExecutionPlanResult>
    {
        /// <summary>
        /// URI for the file that owns the query to look up the results for
        /// </summary>
        public string OwnerUri { get; set; }

        /// <summary>
        /// Index of the batch to get the results from
        /// </summary>
        public int BatchIndex { get; set; }

        /// <summary>
        /// Index of the result set to get the results from
        /// </summary>
        public int ResultSetIndex { get; set; }

    }


    /// <summary>
    /// Parameters for the query execution plan request
    /// </summary>
    public class QueryExecutionPlanResult
    {
        /// <summary>
        /// The requested execution plan. Optional, can be set to null to indicate an error
        /// </summary>
        public ExecutionPlan ExecutionPlan { get; set; }
    }


    /// <summary>
    /// Class used to represent an execution plan from a query for transmission across JSON RPC
    /// </summary>
    public class ExecutionPlan
    {
        /// <summary>
        /// The format of the execution plan 
        /// </summary>
        public string Format { get; set; }

        /// <summary>
        /// The execution plan content
        /// </summary>
        public string Content { get; set; }
    }

    /// <summary> 
    /// Container class for a selection range from file 
    /// </summary>
    /// TODO: Remove this in favor of buffer range end-to-end
    public class SelectionData
    {
        public SelectionData() { }

        public SelectionData(int startLine, int startColumn, int endLine, int endColumn)
        {
            StartLine = startLine;
            StartColumn = startColumn;
            EndLine = endLine;
            EndColumn = endColumn;
        }

        #region Properties

        public int EndColumn { get; set; }

        public int EndLine { get; set; }

        public int StartColumn { get; set; }
        public int StartLine { get; set; }

        #endregion

        public BufferRange ToBufferRange()
        {
            return new BufferRange(StartLine, StartColumn, EndLine, EndColumn);
        }

        public static SelectionData FromBufferRange(BufferRange range)
        {
            return new SelectionData
            {
                StartLine = range.Start.Line,
                StartColumn = range.Start.Column,
                EndLine = range.End.Line,
                EndColumn = range.End.Column
            };
        }


        /// <summary>
        /// Provides details about a range between two positions in
        /// a file buffer.
        /// </summary>
        [DebuggerDisplay("Start = {Start.Line}:{Start.Column}, End = {End.Line}:{End.Column}")]
        public class BufferRange
        {
            #region Properties

            /// <summary>
            /// Provides an instance that represents a range that has not been set.
            /// </summary>
            public static readonly BufferRange None = new BufferRange(0, 0, 0, 0);

            /// <summary>
            /// Gets the start position of the range in the buffer.
            /// </summary>
            public BufferPosition Start { get; private set; }

            /// <summary>
            /// Gets the end position of the range in the buffer.
            /// </summary>
            public BufferPosition End { get; private set; }

            /// <summary>
            /// Returns true if the current range is non-zero, i.e.
            /// contains valid start and end positions.
            /// </summary>
            public bool HasRange
            {
                get
                {
                    return this.Equals(BufferRange.None);
                }
            }

            #endregion

            #region Constructors

            /// <summary>
            /// Creates a new instance of the BufferRange class.
            /// </summary>
            /// <param name="start">The start position of the range.</param>
            /// <param name="end">The end position of the range.</param>
            public BufferRange(BufferPosition start, BufferPosition end)
            {
                if (start > end)
                {
                    throw new ArgumentException();
                }

                this.Start = start;
                this.End = end;
            }

            /// <summary>
            /// Creates a new instance of the BufferRange class.
            /// </summary>
            /// <param name="startLine">The 1-based starting line number of the range.</param>
            /// <param name="startColumn">The 1-based starting column number of the range.</param>
            /// <param name="endLine">The 1-based ending line number of the range.</param>
            /// <param name="endColumn">The 1-based ending column number of the range.</param>
            public BufferRange(
                int startLine,
                int startColumn,
                int endLine,
                int endColumn)
            {
                this.Start = new BufferPosition(startLine, startColumn);
                this.End = new BufferPosition(endLine, endColumn);
            }

            #endregion

            #region Public Methods

            /// <summary>
            /// Compares two instances of the BufferRange class.
            /// </summary>
            /// <param name="obj">The object to which this instance will be compared.</param>
            /// <returns>True if the ranges are equal, false otherwise.</returns>
            public override bool Equals(object obj)
            {
                if (!(obj is BufferRange))
                {
                    return false;
                }

                BufferRange other = (BufferRange)obj;

                return
                    this.Start.Equals(other.Start) &&
                    this.End.Equals(other.End);
            }

            /// <summary>
            /// Calculates a unique hash code that represents this instance.
            /// </summary>
            /// <returns>A hash code representing this instance.</returns>
            public override int GetHashCode()
            {
                return this.Start.GetHashCode() ^ this.End.GetHashCode();
            }

            #endregion
        }


        /// <summary>
        /// Provides details about a position in a file buffer.  All
        /// positions are expressed in 1-based positions (i.e. the
        /// first line and column in the file is position 1,1).
        /// </summary>
        [DebuggerDisplay("Position = {Line}:{Column}")]
        public class BufferPosition
        {
            #region Properties

            /// <summary>
            /// Provides an instance that represents a position that has not been set.
            /// </summary>
            public static readonly BufferPosition None = new BufferPosition(-1, -1);

            /// <summary>
            /// Gets the line number of the position in the buffer.
            /// </summary>
            public int Line { get; private set; }

            /// <summary>
            /// Gets the column number of the position in the buffer.
            /// </summary>
            public int Column { get; private set; }

            #endregion

            #region Constructors

            /// <summary>
            /// Creates a new instance of the BufferPosition class.
            /// </summary>
            /// <param name="line">The line number of the position.</param>
            /// <param name="column">The column number of the position.</param>
            public BufferPosition(int line, int column)
            {
                this.Line = line;
                this.Column = column;
            }

            #endregion

            #region Public Methods

            /// <summary>
            /// Compares two instances of the BufferPosition class.
            /// </summary>
            /// <param name="obj">The object to which this instance will be compared.</param>
            /// <returns>True if the positions are equal, false otherwise.</returns>
            public override bool Equals(object obj)
            {
                if (!(obj is BufferPosition))
                {
                    return false;
                }

                BufferPosition other = (BufferPosition)obj;

                return
                    this.Line == other.Line &&
                    this.Column == other.Column;
            }

            /// <summary>
            /// Calculates a unique hash code that represents this instance.
            /// </summary>
            /// <returns>A hash code representing this instance.</returns>
            public override int GetHashCode()
            {
                return this.Line.GetHashCode() ^ this.Column.GetHashCode();
            }

            /// <summary>
            /// Compares two positions to check if one is greater than the other.
            /// </summary>
            /// <param name="positionOne">The first position to compare.</param>
            /// <param name="positionTwo">The second position to compare.</param>
            /// <returns>True if positionOne is greater than positionTwo.</returns>
            public static bool operator >(BufferPosition positionOne, BufferPosition positionTwo)
            {
                return
                    (positionOne != null && positionTwo == null) ||
                    (positionOne.Line > positionTwo.Line) ||
                    (positionOne.Line == positionTwo.Line &&
                     positionOne.Column > positionTwo.Column);
            }

            /// <summary>
            /// Compares two positions to check if one is less than the other.
            /// </summary>
            /// <param name="positionOne">The first position to compare.</param>
            /// <param name="positionTwo">The second position to compare.</param>
            /// <returns>True if positionOne is less than positionTwo.</returns>
            public static bool operator <(BufferPosition positionOne, BufferPosition positionTwo)
            {
                return positionTwo > positionOne;
            }

            #endregion
        }
    }
}
