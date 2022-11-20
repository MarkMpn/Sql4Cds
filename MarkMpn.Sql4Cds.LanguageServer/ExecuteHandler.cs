using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlTypes;
using System.Diagnostics;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using MediatR;
using Newtonsoft.Json.Converters;
using OmniSharp.Extensions.JsonRpc;
using OmniSharp.Extensions.LanguageServer.Protocol.Server;

namespace MarkMpn.Sql4Cds.LanguageServer
{
    [Method("query/executeDocumentSelection")]
    [Serial]
    class ExecuteHandler : IRequestHandler<ExecuteDocumentSelectionParams,ExecuteRequestResult>, IRequestHandler<SubsetParams, SubsetResult>, IJsonRpcHandler
    {
        private readonly ILanguageServerFacade _lsp;
        private readonly ConnectionManager _connectionManager;
        private readonly TextDocumentManager _documentManager;
        private readonly ConcurrentDictionary<string, List<ResultSetSummary>> _resultSets;

        public ExecuteHandler(ILanguageServerFacade lsp, ConnectionManager connectionManager, TextDocumentManager documentManager)
        {
            _lsp = lsp;
            _connectionManager = connectionManager;
            _documentManager = documentManager;
            _resultSets = new ConcurrentDictionary<string, List<ResultSetSummary>>();
        }

        public Task<ExecuteRequestResult> Handle(ExecuteDocumentSelectionParams request, CancellationToken cancellationToken)
        {
            var session = _connectionManager.GetConnection(request.OwnerUri);

            if (session == null)
                return Task.FromResult(new ExecuteRequestResult()); // TODO: Send error

            Task.Run(async () =>
            {
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

                using (var cmd = session.Connection.CreateCommand())
                {
                    var qry = "";

                    var doc = _documentManager.GetContent(request.OwnerUri).Split('\n');
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

                var endTime = DateTime.UtcNow;

                batchSummary.ExecutionEnd = endTime.ToLocalTime().ToString("o");
                batchSummary.ExecutionElapsed = (endTime - startTime).ToString();
                batchSummary.ResultSetSummaries = resultSets.ToArray();
                batchSummary.SpecialAction = new SpecialAction();

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

        public Task<SubsetResult> Handle(SubsetParams request, CancellationToken cancellationToken)
        {
            var resultSet = _resultSets[request.OwnerUri][request.ResultSetIndex];

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
    /// Execution plan graph object that is sent over JSON RPC
    /// </summary>
    public class ExecutionPlanGraph
    {
        /// <summary>
        /// Root of the execution plan tree
        /// </summary>
        public ExecutionPlanNode Root { get; set; }
        /// <summary>
        /// Underlying query for the execution plan graph
        /// </summary>
        public string Query { get; set; }
        /// <summary>
        /// Graph file that used to generate ExecutionPlanGraph
        /// </summary>
        public ExecutionPlanGraphInfo GraphFile { get; set; }
        /// <summary>
        /// Index recommendations given by show plan to improve query performance
        /// </summary>
        public List<ExecutionPlanRecommendation> Recommendations { get; set; }
    }

    public class ExecutionPlanNode
    {
        /// <summary>
        /// ID for the node.
        /// </summary>
        public int ID { get; set; }
        /// <summary>
        /// Type of the node. This determines the icon that is displayed for it
        /// </summary>
        public string Type { get; set; }
        /// <summary>
        /// Cost associated with the node
        /// </summary>
        public double Cost { get; set; }
        /// <summary>
        /// Output row count associated with the node
        /// </summary>
        public string RowCountDisplayString { get; set; }
        /// <summary>
        ///  Cost string for the node
        /// </summary>
        /// <value></value>
        public string CostDisplayString { get; set; }
        /// <summary>
        /// Cost of the node subtree
        /// </summary>
        public double SubTreeCost { get; set; }
        /// <summary>
        /// Relative cost of the node compared to its siblings.
        /// </summary>
        public double RelativeCost { get; set; }
        /// <summary>
        /// Time taken by the node operation in milliseconds
        /// </summary>
        public long? ElapsedTimeInMs { get; set; }
        /// <summary>
        /// Node properties to be shown in the tooltip
        /// </summary>
        public List<ExecutionPlanGraphPropertyBase> Properties { get; set; }
        /// <summary>
        /// Display name for the node
        /// </summary>
        public string Name { get; set; }
        /// <summary>
        /// Description associated with the node.
        /// </summary>
        public string Description { get; set; }
        /// <summary>
        /// Subtext displayed under the node name
        /// </summary>
        public string[] Subtext { get; set; }
        public List<ExecutionPlanNode> Children { get; set; }
        public List<ExecutionPlanEdges> Edges { get; set; }
        /// <summary>
        /// Add badge icon to nodes like warnings and parallelism
        /// </summary>
        public List<Badge> Badges { get; set; }
        /// <summary>
        /// Top operations table data for the node
        /// </summary>
        public List<TopOperationsDataItem> TopOperationsData { get; set; }
        /// <summary>
        /// The cost metrics for the node.
        /// </summary>
        public List<CostMetric> CostMetrics { get; set; }
    }

    public class CostMetric
    {
        /// <summary>
        /// Name of the cost metric
        /// </summary>
        public string Name { get; set; }
        /// <summary>
        /// The value for the cost metric
        /// </summary>
        public string Value { get; set; }
    }

    public class ExecutionPlanGraphPropertyBase
    {
        /// <summary>
        /// Name of the property
        /// </summary>
        public string Name { get; set; }
        /// <summary>
        /// Flag to show/hide props in tooltip
        /// </summary>
        public bool ShowInTooltip { get; set; }
        /// <summary>
        /// Display order of property
        /// </summary>
        public int DisplayOrder { get; set; }
        /// <summary>
        /// Flag to show property at the bottom of tooltip. Generally done for for properties with longer value.
        /// </summary>
        public bool PositionAtBottom { get; set; }
        /// <summary>
        /// Value to be displayed in UI like tooltips and properties View
        /// </summary>
        /// <value></value>
        public string DisplayValue { get; set; }
        /// <summary>
        /// Indicates what kind of value is better amongst 2 values of the same property
        /// </summary>
        public BetterValue BetterValue { get; set; }
        /// <summary>
        /// Indicates the data type of the property
        /// </summary>
        public PropertyValueDataType DataType { get; set; }
    }


    public enum BetterValue
    {
        LowerNumber = 0,
        HigherNumber = 1,
        True = 2,
        False = 3,
        None = 4
    }

    public class NestedExecutionPlanGraphProperty : ExecutionPlanGraphPropertyBase
    {
        /// <summary>
        /// In case of nested properties, the value field is a list of properties. 
        /// </summary>
        public List<ExecutionPlanGraphPropertyBase> Value { get; set; }
    }

    public class ExecutionPlanGraphProperty : ExecutionPlanGraphPropertyBase
    {
        /// <summary>
        /// Formatted value for the property
        /// </summary>
        public string Value { get; set; }
    }

    public class ExecutionPlanEdges
    {
        /// <summary>
        /// Count of the rows returned by the subtree of the edge.
        /// </summary>
        public double RowCount { get; set; }
        /// <summary>
        /// Size of the rows returned by the subtree of the edge.
        /// </summary>
        public double RowSize { get; set; }
        /// <summary>
        /// Edge properties to be shown in the tooltip.
        /// </summary>
        public List<ExecutionPlanGraphPropertyBase> Properties { get; set; }
    }


    public class ExecutionPlanRecommendation
    {
        /// <summary>
        /// Text displayed in the show plan graph control
        /// </summary>
        public string DisplayString { get; set; }
        /// <summary>
        /// Raw query that is recommended to the user
        /// </summary>
        public string Query { get; set; }
        /// <summary>
        /// Query that will be opened in a new file once the user click on the recommendation
        /// </summary>
        public string QueryWithDescription { get; set; }
    }

    public class ExecutionPlanGraphInfo
    {
        /// <summary>
        /// File contents
        /// </summary>
        public string GraphFileContent { get; set; }
        /// <summary>
        /// File type for execution plan. This will be the file type of the editor when the user opens the graph file
        /// </summary>
        public string GraphFileType { get; set; }
        /// <summary>
        /// Index of the execution plan in the file content
        /// </summary>
        public int PlanIndexInFile { get; set; }
    }

    public class Badge
    {
        /// <summary>
        /// Type of the node overlay. This determines the icon that is displayed for it
        /// </summary>
        public BadgeType Type { get; set; }

        /// <summary>
        /// Text to display for the overlay tooltip
        /// </summary>
        public string Tooltip { get; set; }
    }

    public enum BadgeType
    {
        Warning = 0,
        CriticalWarning = 1,
        Parallelism = 2
    }

    public enum PropertyValueDataType
    {
        Number = 0,
        String = 1,
        Boolean = 2,
        Nested = 3
    }

    public class TopOperationsDataItem
    {
        public string ColumnName { get; set; }
        public PropertyValueDataType DataType { get; set; }
        public object DisplayValue { get; set; }
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
