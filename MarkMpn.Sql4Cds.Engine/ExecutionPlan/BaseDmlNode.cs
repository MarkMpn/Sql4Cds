using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.SqlTypes;
using System.Linq;
using System.ServiceModel;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Metadata;
using System.Collections.Concurrent;
#if NETCOREAPP
using Microsoft.PowerPlatform.Dataverse.Client;
#else
using Microsoft.Xrm.Tooling.Connector;
#endif

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// A base class for execution plan nodes that implement a DML operation
    /// </summary>
    abstract class BaseDmlNode : BaseNode, IDmlQueryExecutionPlanNode
    {
        /// <summary>
        /// Temporarily applies global settings to improve the performance of parallel operations
        /// </summary>
        class ParallelConnectionSettings : IDisposable
        {
            private readonly int _connectionLimit;
            private readonly int _threadPoolThreads;
            private readonly int _iocpThreads;
            private readonly bool _expect100Continue;
            private readonly bool _useNagleAlgorithm;

            public ParallelConnectionSettings()
            {
                // Store the current settings
                _connectionLimit = System.Net.ServicePointManager.DefaultConnectionLimit;
                ThreadPool.GetMinThreads(out _threadPoolThreads, out _iocpThreads);
                _expect100Continue = System.Net.ServicePointManager.Expect100Continue;
                _useNagleAlgorithm = System.Net.ServicePointManager.UseNagleAlgorithm;

                // Apply the required settings
                System.Net.ServicePointManager.DefaultConnectionLimit = 65000;
                ThreadPool.SetMinThreads(100, 100);
                System.Net.ServicePointManager.Expect100Continue = false;
                System.Net.ServicePointManager.UseNagleAlgorithm = false;
            }

            public void Dispose()
            {
                // Restore the original settings
                System.Net.ServicePointManager.DefaultConnectionLimit = _connectionLimit;
                ThreadPool.SetMinThreads(_threadPoolThreads, _iocpThreads);
                System.Net.ServicePointManager.Expect100Continue = _expect100Continue;
                System.Net.ServicePointManager.UseNagleAlgorithm = _useNagleAlgorithm;
            }
        }

        class ParallelLoopState : DynamicParallelLoopState
        {
            private int _successfulExecution;

            public override void RequestReduceThreadCount()
            {
                if (Interlocked.CompareExchange(ref _successfulExecution, 0, 0) == 0)
                    return;

                base.RequestReduceThreadCount();
            }

            public override bool ResetReduceThreadCount()
            {
                if (base.ResetReduceThreadCount())
                {
                    Interlocked.Exchange(ref _successfulExecution, 0);
                    return true;
                }

                return false;
            }

            public void IncrementSuccessfulExecution()
            {
                Interlocked.Increment(ref _successfulExecution);
            }
        }

        class ParallelThreadState
        {
            public IOrganizationService Service { get; set; }

            public ExecuteMultipleRequest EMR { get; set; }

            public int NextBatchSize { get; set; }

            public bool Error { get; set; }
        }

        private int _entityCount;
        private int _inProgressCount;
        private int _successCount;
        private int _errorCount;
        private int _threadCount;
        private int _maxThreadCount;
        private int _pausedThreadCount;
        private int _batchExecutionCount;
        private ParallelOptions _parallelOptions;
        private ConcurrentQueue<OrganizationRequest> _retryQueue;
        private ConcurrentDictionary<ParallelThreadState, DateTime> _delayedUntil;
        private OrganizationServiceFault _fault;
        private int _serviceProtectionLimitHits;
        private int[] _threadCountHistory;
        private int[] _rpmHistory;
        private float[] _batchSizeHistory;

        /// <summary>
        /// The SQL string that the query was converted from
        /// </summary>
        [Browsable(false)]
        public string Sql { get; set; }

        /// <summary>
        /// The position of the SQL query within the entire query text
        /// </summary>
        [Browsable(false)]
        public int Index { get; set; }

        /// <summary>
        /// The length of the SQL query within the entire query text
        /// </summary>
        [Browsable(false)]
        public int Length { get; set; }

        /// <summary>
        /// The number of the first line of the statement
        /// </summary>
        [Browsable(false)]
        public int LineNumber { get; set; }

        [Browsable(false)]
        public IExecutionPlanNodeInternal Source { get; set; }

        /// <summary>
        /// The instance that this node will be executed against
        /// </summary>
        [Category("Data Source")]
        [Description("The data source this query is executed against")]
        public virtual string DataSource { get; set; }

        /// <summary>
        /// The maximum degree of parallelism to apply to this operation
        /// </summary>
        [DisplayName("Max Degree of Parallelism")]
        [Description("The maximum number of operations that will be performed in parallel")]
        public abstract int MaxDOP { get; set; }

        [Category("Statistics")]
        [DisplayName("Actual Degree of Parallelism")]
        [BrowsableInEstimatedPlan(false)]
        [Description("The number of threads that were running each minute during the operation")]
        [TypeConverter(typeof(MiniChartConverter))]
#if !NETCOREAPP
        [Editor(typeof(MiniChartEditor), typeof(System.Drawing.Design.UITypeEditor))]
#endif
        public float[] ActualDOP => _threadCountHistory.Select(i => (float)i).ToArray();

        [Category("Statistics")]
        [DisplayName("Records Per Minute")]
        [BrowsableInEstimatedPlan(false)]
        [Description("The number of records that were processed each minute during the operation")]
        [TypeConverter(typeof(MiniChartConverter))]
#if !NETCOREAPP
        [Editor(typeof(MiniChartEditor), typeof(System.Drawing.Design.UITypeEditor))]
#endif
        public float[] RPM => _rpmHistory.Select(i => (float)i).ToArray();

        [Category("Statistics")]
        [DisplayName("Actual Batch Size")]
        [BrowsableInEstimatedPlan(false)]
        [Description("The average number of records that were processed per batch each minute during the operation")]
        [TypeConverter(typeof(MiniChartConverter))]
#if !NETCOREAPP
        [Editor(typeof(MiniChartEditor), typeof(System.Drawing.Design.UITypeEditor))]
#endif
        public float[] ActualBatchSize => _batchSizeHistory;

        [Category("Statistics")]
        [DisplayName("Service Protection Limit Hits")]
        [BrowsableInEstimatedPlan(false)]
        [Description("The number of times execution was paused due to service protection limits")]
        public int ServiceProtectionLimitHits => _serviceProtectionLimitHits;

        /// <summary>
        /// The number of requests that will be submitted in a single batch
        /// </summary>
        [DisplayName("Max Batch Size")]
        [Description("The number of requests that will be submitted in a single batch")]
        public abstract int BatchSize { get; set; }

        /// <summary>
        /// Indicates if custom plugins should be skipped
        /// </summary>
        [DisplayName("Bypass Plugin Execution")]
        [Description("Indicates if custom plugins should be skipped")]
        public abstract bool BypassCustomPluginExecution { get; set; }

        /// <summary>
        /// Indicates if the operation should be attempted on all records or should fail on the first error
        /// </summary>
        [DisplayName("Continue On Error")]
        [Description("Indicates if the operation should be attempted on all records or should fail on the first error")]
        public abstract bool ContinueOnError { get; set; }

        /// <summary>
        /// Changes system settings to optimise for parallel connections
        /// </summary>
        /// <returns>An object to dispose of to reset the settings to their original values</returns>
        protected IDisposable UseParallelConnections() => new ParallelConnectionSettings();

        /// <summary>
        /// Executes the DML query and returns an appropriate log message
        /// </summary>
        /// <param name="context">The context in which the node is being executed</param>
        /// <param name="recordsAffected">The number of records that were affected by the query</param>
        /// <param name="message">A progress message to display</param>
        public abstract void Execute(NodeExecutionContext context, out int recordsAffected, out string message);

        /// <summary>
        /// Indicates if some errors returned by the server can be silently ignored
        /// </summary>
        protected virtual bool IgnoresSomeErrors => false;

        protected void AddRequiredColumns(IList<string> requiredColumns, List<AttributeAccessor> accessors)
        {
            foreach (var accessor in accessors)
            {
                foreach (var col in accessor.SourceAttributes)
                {
                    if (!requiredColumns.Contains(col))
                        requiredColumns.Add(col);
                }
            }
        }

        /// <summary>
        /// Attempts to fold this node into its source to simplify the query
        /// </summary>
        /// <param name="context">The context in which the node is being built</param>
        /// <param name="hints">Any hints that can control the folding of this node</param>
        /// <returns>The node that should be used in place of this node</returns>
        public virtual IRootExecutionPlanNodeInternal[] FoldQuery(NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            context.ResetGlobalCalculations();

            if (Source is IDataExecutionPlanNodeInternal dataNode)
                Source = dataNode.FoldQuery(context, hints);
            else if (Source is IDataReaderExecutionPlanNode dataSetNode)
                Source = dataSetNode.FoldQuery(context, hints).Single();

            MaxDOP = GetMaxDOP(context, hints);
            BatchSize = GetBatchSize(context, hints);
            BypassCustomPluginExecution = GetBypassPluginExecution(context, hints);
            ContinueOnError = GetContinueOnError(context, hints);

            if (Source is IDataExecutionPlanNodeInternal source)
                Source = context.InsertGlobalCalculations(this, source);

            return new[] { this };
        }

        private int GetMaxDOP(NodeCompilationContext context, IList<OptimizerHint> queryHints)
        {
            if (DataSource == null)
                return 1;

            if (!context.Session.DataSources.TryGetValue(DataSource, out var dataSource))
                throw new NotSupportedQueryFragmentException("Unknown datasource");

            return ParallelismHelper.GetMaxDOP(dataSource, context, queryHints);
        }

        private int GetBatchSize(NodeCompilationContext context, IList<OptimizerHint> queryHints)
        {
            if (queryHints == null)
                return context.Options.BatchSize;

            var batchSizeHint = queryHints
                .OfType<UseHintList>()
                .SelectMany(hint => hint.Hints)
                .Where(hint => hint.Value.StartsWith("BATCH_SIZE_", StringComparison.OrdinalIgnoreCase))
                .FirstOrDefault();

            if (batchSizeHint != null)
            {
                if (!Int32.TryParse(batchSizeHint.Value.Substring(11), out var value) || value < 1)
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidHint(batchSizeHint)) { Suggestion = "BATCH_SIZE requires a positive integer value" };

                return value;
            }

            return context.Options.BatchSize;
        }

        private bool GetBypassPluginExecution(NodeCompilationContext context, IList<OptimizerHint> queryHints)
        {
            if (queryHints == null)
                return context.Options.BypassCustomPlugins;

            var bypassPluginExecution = queryHints
                .OfType<UseHintList>()
                .Where(hint => hint.Hints.Any(s => s.Value.Equals("BYPASS_CUSTOM_PLUGIN_EXECUTION", StringComparison.OrdinalIgnoreCase)))
                .Any();

            return bypassPluginExecution || context.Options.BypassCustomPlugins;
        }

        private bool GetContinueOnError(NodeCompilationContext context, IList<OptimizerHint> queryHints)
        {
            if (queryHints == null)
                return false;

            var continueOnError = queryHints
                .OfType<UseHintList>()
                .Where(hint => hint.Hints.Any(s => s.Value.Equals("CONTINUE_ON_ERROR", StringComparison.OrdinalIgnoreCase)))
                .Any();

            return continueOnError;
        }

        protected void FoldIdsToConstantScan(NodeCompilationContext context, IList<OptimizerHint> hints, string logicalName, List<AttributeAccessor> accessors)
        {
            if (hints != null && hints.OfType<UseHintList>().Any(hint => hint.Hints.Any(s => s.Value.Equals("NO_DIRECT_DML", StringComparison.OrdinalIgnoreCase))))
                return;

            // Can't do DML operations on base activitypointer table, need to read the record to
            // find the concrete activity type.
            if (logicalName == "activitypointer")
                return;

            // Work out the fields that we should use as the primary key for these records.
            var dataSource = context.Session.DataSources[DataSource];
            var targetMetadata = dataSource.Metadata[logicalName];
            var keyAttributes = EntityReader.GetPrimaryKeyFields(targetMetadata, out _);

            var requiredColumns = accessors.SelectMany(a => a.SourceAttributes).Distinct().ToArray();

            // Skip any ComputeScalar node that is being used to generate additional values,
            // unless they reference additional values in the data source
            var compute = Source as ComputeScalarNode;

            if (compute != null)
            {
                if (compute.Columns.Any(c => c.Value.GetColumns().Except(keyAttributes).Any()))
                    return;

                // Ignore any columns being created by the ComputeScalar node
                foreach (var col in compute.Columns)
                    requiredColumns = requiredColumns.Except(new[] { col.Key }).ToArray();
            }

            if ((compute?.Source ?? Source) is FetchXmlScan fetch)
            {
                var folded = fetch.FoldDmlSource(context, hints, logicalName, requiredColumns, keyAttributes);

                if (compute != null)
                    compute.Source = folded;
                else
                    Source = folded;
            }
            else if (Source is SqlNode sql)
            {
                Source = sql.FoldDmlSource(context, hints, logicalName, requiredColumns, keyAttributes);
            }
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            yield return Source;
        }

        /// <summary>
        /// Gets the records to perform the DML operation on
        /// </summary>
        /// <param name="context">The context in which the node is being executed</param>
        /// <param name="schema">The schema of the data source</param>
        /// <returns>The entities to perform the DML operation on</returns>
        protected List<Entity> GetDmlSourceEntities(NodeExecutionContext context, out INodeSchema schema)
        {
            List<Entity> entities;

            if (Source is IDataExecutionPlanNodeInternal dataSource)
            {
                schema = dataSource.GetSchema(context);
                entities = dataSource.Execute(context).ToList();
            }
            else if (Source is IDataReaderExecutionPlanNode dataSetSource)
            {
                var dataReader = dataSetSource.Execute(context, CommandBehavior.Default);

                if (Source is SqlNode sql)
                {
                    schema = sql.GetSchema(context);
                }
                else
                {
                    var schemaTable = dataReader.GetSchemaTable();
                    schema = SchemaConverter.ConvertSchema(schemaTable, context.PrimaryDataSource);
                }

                entities = new List<Entity>();

                while (dataReader.Read())
                {
                    var entity = new Entity();
                    var colIndex = 0;

                    foreach (var col in schema.Schema)
                    {
                        var value = dataReader.GetProviderSpecificValue(colIndex++);

                        if (value is DateTime dt)
                        {
                            if (col.Value.Type.IsSameAs(DataTypeHelpers.Date))
                                value = new SqlDate(dt);
                            else
                                value = new SqlDateTime2(dt);
                        }
                        else if (value is DateTimeOffset dto)
                        {
                            value = new SqlDateTimeOffset(dto);
                        }
                        else if (value is TimeSpan ts)
                        {
                            value = new SqlTime(ts);
                        }
                        else if (value is DBNull)
                        {
                            value = SqlTypeConverter.GetNullValue(col.Value.Type.ToNetType(out _));
                        }

                        entity[col.Key] = value;
                    }

                    entities.Add(entity);
                }
            }
            else
            {
                throw new QueryExecutionException("Unexpected data source") { Node = this };
            }

            return entities;
        }

        /// <summary>
        /// Provides values to include in log messages
        /// </summary>
        protected class OperationNames
        {
            /// <summary>
            /// The name of the operation to include at the start of a log message, e.g. "Updating"
            /// </summary>
            public string InProgressUppercase { get; set; }

            /// <summary>
            /// The name of the operation to include in the middle of a log message, e.g. "updating"
            /// </summary>
            public string InProgressLowercase { get; set; }

            /// <summary>
            /// The completed name of the operation to include in the middle of a log message, e.g. "updated"
            /// </summary>
            public string CompletedLowercase { get; set; }

            /// <summary>
            /// The completed name of the operation to include at the start of a log message, e.g. "Updated"
            /// </summary>
            public string CompletedUppercase { get; set; }
        }

        /// <summary>
        /// Executes the DML operations required for a set of input records
        /// </summary>
        /// <param name="dataSource">The data source to get the data from</param>
        /// <param name="options"><see cref="IQueryExecutionOptions"/> to indicate how the query can be executed</param>
        /// <param name="entities">The data source entities</param>
        /// <param name="meta">The metadata of the entity that will be affected</param>
        /// <param name="requestGenerator">A function to generate a DML request from a data source entity</param>
        /// <param name="operationNames">The constant strings to use in log messages</param>
        /// <param name="context">The context in which the node is being executed</param>
        /// <param name="recordsAffected">The number of records affected by the operation</param>
        /// <param name="message">A human-readable message to show the number of records affected</param>
        /// <param name="responseHandler">An optional parameter to handle the response messages from the server</param>
        protected void ExecuteDmlOperation(DataSource dataSource, IQueryExecutionOptions options, List<Entity> entities, EntityMetadata meta, Func<Entity,OrganizationRequest> requestGenerator, OperationNames operationNames, NodeExecutionContext context, out int recordsAffected, out string message, Action<OrganizationResponse> responseHandler = null)
        {
            _entityCount = entities.Count;
            _inProgressCount = 0;
            _successCount = 0;
            _errorCount = 0;
            _threadCount = 0;
            _batchExecutionCount = 0;

#if NETCOREAPP
            var svc = dataSource.Connection as ServiceClient;
#else
            var svc = dataSource.Connection as CrmServiceClient;
#endif

            var threadCountHistory = new List<int>();
            var rpmHistory = new List<int>();
            var batchSizeHistory = new List<float>();

            var maxDop = MaxDOP;

            if (!ParallelismHelper.CanParallelise(dataSource.Connection))
                maxDop = 1;

            if (maxDop == 1)
                svc = null;

            var useAffinityCookie = maxDop == 1 || _entityCount < 100;
            var completed = new CancellationTokenSource();

            // Set up one background thread to monitor the performance for debugging
            var performanceMonitor = Task.Factory.StartNew(async () =>
            {
                var lastSuccess = 0;
                var lastBatchCount = 0;

                while (!completed.Token.IsCancellationRequested)
                {
                    try
                    {
                        await Task.Delay(TimeSpan.FromMinutes(1), completed.Token);
                    }
                    catch (TaskCanceledException)
                    {
                        break;
                    }

                    threadCountHistory.Add(_maxThreadCount);
                    var prevSuccess = Interlocked.Exchange(ref lastSuccess, _successCount);
                    var prevBatchCount = Interlocked.Exchange(ref lastBatchCount, _batchExecutionCount);
                    var recordCount = lastSuccess - prevSuccess;
                    var batchCount = lastBatchCount - prevBatchCount;
                    rpmHistory.Add(recordCount);
                    if (batchCount == 0)
                        batchSizeHistory.Add(0);
                    else
                        batchSizeHistory.Add((float)recordCount / batchCount);
                    _maxThreadCount = _threadCount;
                    lastSuccess = _successCount;
                }

                threadCountHistory.Add(_maxThreadCount);
                var finalRecordCount = _successCount - lastSuccess;
                var finalBatchCount = _batchExecutionCount - lastBatchCount;
                rpmHistory.Add(finalRecordCount);
                if (finalBatchCount == 0)
                    batchSizeHistory.Add(0);
                else
                    batchSizeHistory.Add((float)finalRecordCount / finalBatchCount);
            });

            try
            {
                OrganizationServiceFault fault = null;

                using (UseParallelConnections())
                {
                    _parallelOptions = new ParallelOptions { MaxDegreeOfParallelism = maxDop };
                    _retryQueue = new ConcurrentQueue<OrganizationRequest>();
                    _delayedUntil = new ConcurrentDictionary<ParallelThreadState, DateTime>();

                    new DynamicParallel<Entity, ParallelThreadState, ParallelLoopState>(
                        entities,
                        _parallelOptions,
                        () =>
                        {
                            var service = svc?.Clone() ?? dataSource.Connection;

#if NETCOREAPP
                            if (service is ServiceClient crmService)
                            {
                                crmService.MaxRetryCount = 0;
                                crmService.EnableAffinityCookie = useAffinityCookie;
                            }
#else
                            if (service is CrmServiceClient crmService)
                            {
                                crmService.MaxRetryCount = 0;
                                crmService.EnableAffinityCookie = useAffinityCookie;
                            }
#endif
                            Interlocked.Increment(ref _threadCount);

                            _maxThreadCount = Math.Max(_maxThreadCount, _threadCount);

                            return new ParallelThreadState
                            {
                                Service = service,
                                EMR = null,
                                Error = false,
                                NextBatchSize = 1
                            };
                        },
                        async (entity, loopState, threadLocalState) =>
                        {
                            var executed = false;

                            try
                            {
                                if (options.CancellationToken.IsCancellationRequested)
                                {
                                    loopState.Stop();
                                    return;
                                }

                                // Generate the request to insert/update/delete this record
                                var request = requestGenerator(entity);

                                if (BypassCustomPluginExecution)
                                    request.Parameters["BypassCustomPluginExecution"] = true;

                                if (threadLocalState.NextBatchSize == 1)
                                {
                                    await UpdateNextBatchSize(threadLocalState, async () =>
                                    {
                                        executed = await ExecuteSingleRequest(request, operationNames, meta, options, dataSource, threadLocalState.Service, context, responseHandler, threadLocalState, loopState);
                                    });
                                }
                                else
                                {
                                    if (threadLocalState.EMR == null)
                                    {
                                        threadLocalState.EMR = new ExecuteMultipleRequest
                                        {
                                            Requests = new OrganizationRequestCollection(),
                                            Settings = new ExecuteMultipleSettings
                                            {
                                                ContinueOnError = IgnoresSomeErrors,
                                                ReturnResponses = responseHandler != null
                                            }
                                        };
                                    }

                                    threadLocalState.EMR.Requests.Add(request);

                                    if (threadLocalState.EMR.Requests.Count < threadLocalState.NextBatchSize)
                                        return;

                                    await UpdateNextBatchSize(threadLocalState, async () =>
                                    {
                                        executed = await ProcessBatch(threadLocalState.EMR, operationNames, meta, options, dataSource, threadLocalState.Service, context, responseHandler, threadLocalState, loopState);
                                    });

                                    threadLocalState.EMR = null;
                                }
                            }
                            catch
                            {
                                threadLocalState.Error = true;
                                throw;
                            }

                            // Take any requests from the retry queue and execute them
                            while (executed && _retryQueue.TryDequeue(out var retryReq))
                            {
                                if (options.CancellationToken.IsCancellationRequested)
                                {
                                    loopState.Stop();
                                    return;
                                }

                                if (loopState.IsStopped)
                                    return;

                                if (retryReq is ExecuteMultipleRequest emr)
                                    executed = await ProcessBatch(emr, operationNames, meta, options, dataSource, threadLocalState.Service, context, responseHandler, threadLocalState, loopState);
                                else
                                    executed = await ExecuteSingleRequest(retryReq, operationNames, meta, options, dataSource, threadLocalState.Service, context, responseHandler, threadLocalState, loopState);
                            }

                            if (executed)
                                loopState.RequestIncreaseThreadCount();
                        },
                        async (loopState, threadLocalState) =>
                        {
                            // If we've got a partial batch, execute it now
                            if (threadLocalState.EMR != null && !threadLocalState.Error)
                                await ProcessBatch(threadLocalState.EMR, operationNames, meta, options, dataSource, threadLocalState.Service, context, responseHandler, threadLocalState, loopState);

                            // If this is the final thread, execute any remaining requests in the retry queue
                            if (!loopState.IsStopped &&
                                !options.CancellationToken.IsCancellationRequested &&
                                Interlocked.CompareExchange(ref _threadCount, 1, 1) == 1)
                            {
                                // Take any requests from the retry queue and execute them
                                while (_retryQueue.TryDequeue(out var retryReq))
                                {
                                    if (options.CancellationToken.IsCancellationRequested)
                                    {
                                        loopState.Stop();
                                        return;
                                    }

                                    if (loopState.IsStopped)
                                        return;

                                    if (retryReq is ExecuteMultipleRequest emr)
                                        await ProcessBatch(emr, operationNames, meta, options, dataSource, threadLocalState.Service, context, responseHandler, threadLocalState, loopState);
                                    else
                                        await ExecuteSingleRequest(retryReq, operationNames, meta, options, dataSource, threadLocalState.Service, context, responseHandler, threadLocalState, loopState);
                                }
                            }

                            Interlocked.Decrement(ref _threadCount);
                            ShowProgress(options, operationNames, meta);

                            if (threadLocalState.Service != dataSource.Connection && threadLocalState.Service is IDisposable disposableClient)
                                disposableClient.Dispose();
                        })
                        .ForEach(options.CancellationToken);
                }

                if (fault != null)
                    throw new FaultException<OrganizationServiceFault>(fault, new FaultReason(fault.Message));
            }
            catch (Exception ex)
            {
                var originalEx = ex;

                if (ex is AggregateException agg)
                {
                    if (agg.InnerExceptions.Count == 1)
                        ex = agg.InnerException;
                    else if (agg.InnerExceptions.All(inner => inner is OperationCanceledException))
                        ex = agg.InnerExceptions[0];
                }

                if (_successCount > 0)
                    context.Log(new Sql4CdsError(1, 0, $"{_successCount:N0} {GetDisplayName(_successCount, meta)} {operationNames.CompletedLowercase}"));

                if (ex == originalEx)
                    throw;
                else
                    throw ex;
            }
            finally
            {
                completed.Cancel();
                performanceMonitor.ConfigureAwait(false).GetAwaiter().GetResult();

                _threadCountHistory = threadCountHistory.ToArray();
                _rpmHistory = rpmHistory.ToArray();
                _batchSizeHistory = batchSizeHistory.ToArray();
            }

            recordsAffected = _successCount;
            message = $"({_successCount:N0} {GetDisplayName(_successCount, meta)} {operationNames.CompletedLowercase})";
            context.ParameterValues["@@ROWCOUNT"] = (SqlInt32)_successCount;
        }

        private async Task UpdateNextBatchSize(ParallelThreadState threadLocalState, Func<Task> action)
        {
            // Time how long the action takes
            var timer = new Timer();
            using (timer.Run())
            {
                await action();
            }

            // Adjust the batch size based on the time taken to try and keep the total time around 10sec
            var multiplier = TimeSpan.FromSeconds(10).TotalMilliseconds / timer.Duration.TotalMilliseconds;
            threadLocalState.NextBatchSize = Math.Max(1, Math.Min(BatchSize, (int)(threadLocalState.NextBatchSize * multiplier)));

            // Update the statistics of the number of batches we have executed
            Interlocked.Increment(ref _batchExecutionCount);
        }

        private bool IsRetryableFault(OrganizationServiceFault fault)
        {
            if (fault == null)
                return false;

            if (fault.ErrorCode == -2147188475) // More than one concurrent {0} results detected for an Entity {1} and ObjectTypeCode {2}
            {
                return true;
            }

            return false;
        }

        protected class BulkApiErrorDetail
        {
            public int RequestIndex { get; set; }
            public Guid Id { get; set; }
            public int StatusCode { get; set; }
        }

        private async Task<bool> ExecuteSingleRequest(OrganizationRequest req, OperationNames operationNames, EntityMetadata meta, IQueryExecutionOptions options, DataSource dataSource, IOrganizationService org, NodeExecutionContext context, Action<OrganizationResponse> responseHandler, ParallelThreadState threadState, ParallelLoopState loopState)
        {
            Interlocked.Increment(ref _inProgressCount);

            for (var retry = 0; ; retry++)
            {
                try
                {
                    ShowProgress(options, operationNames, meta);
                    var response = dataSource.Execute(org, req);
                    Interlocked.Increment(ref _successCount);

                    loopState.IncrementSuccessfulExecution();

                    responseHandler?.Invoke(response);
                    return true;
                }
                catch (FaultException<OrganizationServiceFault> ex)
                {
                    if (ex.IsThrottlingException(out var retryDelay))
                    {
                        // Handle service protection limit retries ourselves to manage multi-threading
                        // Wait for the recommended retry time, then add the request to the queue for retrying
                        // Terminate this thread so we don't continue to overload the server
                        Interlocked.Decrement(ref _inProgressCount);

                        // The server can report too-long delays. Wait the full 5 minutes to start with,
                        // then reduce to 2 minutes then 1 minute
                        if (retry < 1 && retryDelay > TimeSpan.FromMinutes(5))
                            retryDelay = TimeSpan.FromMinutes(5);
                        else if (retry >= 1 && retry < 3 && retryDelay > TimeSpan.FromMinutes(2))
                            retryDelay = TimeSpan.FromMinutes(2);
                        else if (retry >= 3 && retryDelay > TimeSpan.FromMinutes(1))
                            retryDelay = TimeSpan.FromMinutes(1);

                        _delayedUntil[threadState] = DateTime.Now.Add(retryDelay);
                        if (Interlocked.Increment(ref _pausedThreadCount) == 1)
                            Interlocked.Increment(ref _serviceProtectionLimitHits);
                        ShowProgress(options, operationNames, meta);

                        loopState.RequestReduceThreadCount();
                        await Task.Delay(retryDelay, options.CancellationToken);
                        Interlocked.Decrement(ref _pausedThreadCount);
                        _delayedUntil.TryRemove(threadState, out _);
                        ShowProgress(options, operationNames, meta); 
                        _retryQueue.Enqueue(req);
                        return false;
                    }

                    loopState.IncrementSuccessfulExecution();

                    if (IsRetryableFault(ex?.Detail))
                    {
                        // Retry the request after a short delay
                        Thread.Sleep(TimeSpan.FromSeconds(2));
                        continue;
                    }

                    if (FilterErrors(context, req, ex.Detail))
                    {
                        if (ContinueOnError)
                            _fault = _fault ?? ex.Detail;
                        else
                            throw;
                    }

                    Interlocked.Increment(ref _errorCount);
                    return true;
                }
            }
        }

        private void ShowProgress(IQueryExecutionOptions options, OperationNames operationNames, EntityMetadata meta)
        {
            var progress = (double)_inProgressCount / _entityCount;
            var threadCountMessage = _threadCount < 2 ? "" : $" ({_threadCount:N0} threads)";
            var operationName = operationNames.InProgressUppercase;

            var delayedUntilValues = _delayedUntil.Values.ToArray();
            if (delayedUntilValues.Length > 0 && delayedUntilValues.Length == _threadCount)
            {
                operationName = operationNames.CompletedUppercase;
                threadCountMessage += $" (paused until {delayedUntilValues.Min().ToShortTimeString()} due to service protection limits)";
            }
            else if (delayedUntilValues.Length > 0)
            {
                threadCountMessage += $" ({delayedUntilValues.Length:N0} threads paused until {delayedUntilValues.Min().ToShortTimeString()} due to service protection limits)";
            }

            if (_successCount + _errorCount + 1 >= _inProgressCount)
                options.Progress(progress, $"{operationName} {GetDisplayName(0, meta)} {_inProgressCount:N0} of {_entityCount:N0}{threadCountMessage}...");
            else
                options.Progress(progress, $"{operationName} {GetDisplayName(0, meta)} {_successCount + _errorCount + 1:N0} - {_inProgressCount:N0} of {_entityCount:N0}{threadCountMessage}...");
        }

        private async Task<bool> ProcessBatch(ExecuteMultipleRequest req, OperationNames operationNames, EntityMetadata meta, IQueryExecutionOptions options, DataSource dataSource, IOrganizationService org, NodeExecutionContext context, Action<OrganizationResponse> responseHandler, ParallelThreadState threadState, ParallelLoopState loopState)
        {
            Interlocked.Add(ref _inProgressCount, req.Requests.Count);

            for (var retry = 0; !options.CancellationToken.IsCancellationRequested; retry++)
            {
                try
                {
                    ShowProgress(options, operationNames, meta);
                    var resp = ExecuteMultiple(dataSource, org, meta, req);

                    loopState.IncrementSuccessfulExecution();

                    if (responseHandler != null)
                    {
                        foreach (var item in resp.Responses)
                        {
                            if (item.Response != null)
                                responseHandler(item.Response);
                        }
                    }

                    var errorResponses = resp.Responses
                        .Where(r => r.Fault != null)
                        .ToList();

                    var nonRetryableErrorResponses = errorResponses
                        .Where(r => !IsRetryableFault(r.Fault))
                        .ToList();

                    Interlocked.Add(ref _successCount, req.Requests.Count - errorResponses.Count);
                    Interlocked.Add(ref _errorCount, nonRetryableErrorResponses.Count);

                    var error = errorResponses.FirstOrDefault(item => FilterErrors(context, req.Requests[item.RequestIndex], item.Fault) && !IsRetryableFault(item.Fault));

                    if (error != null)
                    {
                        _fault = _fault ?? error.Fault;

                        if (!ContinueOnError)
                            throw new FaultException<OrganizationServiceFault>(_fault, new FaultReason(_fault.Message));
                    }

                    if (ContinueOnError)
                    {
                        var retryableErrors = errorResponses.Where(item => IsRetryableFault(item.Fault)).ToList();

                        if (retryableErrors.Count > 0)
                        {
                            // Create a new ExecuteMultipleRequest with all the requests that haven't been processed yet
                            var retryReq = new ExecuteMultipleRequest
                            {
                                Requests = new OrganizationRequestCollection(),
                                Settings = new ExecuteMultipleSettings
                                {
                                    ContinueOnError = IgnoresSomeErrors,
                                    ReturnResponses = responseHandler != null
                                }
                            };

                            foreach (var errorItem in retryableErrors)
                                retryReq.Requests.Add(req.Requests[errorItem.RequestIndex]);

                            // Wait and retry
                            await Task.Delay(TimeSpan.FromSeconds(2), options.CancellationToken);
                            req = retryReq;
                            continue;
                        }
                    }
                    else
                    {
                        var firstRetryableError = errorResponses.FirstOrDefault(item => IsRetryableFault(item.Fault));

                        if (firstRetryableError != null)
                        {
                            // Create a new ExecuteMultipleRequest with all the requests that haven't been processed yet
                            var retryReq = new ExecuteMultipleRequest
                            {
                                Requests = new OrganizationRequestCollection(),
                                Settings = new ExecuteMultipleSettings
                                {
                                    ContinueOnError = IgnoresSomeErrors,
                                    ReturnResponses = responseHandler != null
                                }
                            };

                            for (var i = firstRetryableError.RequestIndex; i < req.Requests.Count; i++)
                                retryReq.Requests.Add(req.Requests[i]);

                            // Wait and retry
                            await Task.Delay(TimeSpan.FromSeconds(2), options.CancellationToken);
                            req = retryReq;
                            continue;
                        }
                    }

                    break;
                }
                catch (FaultException<OrganizationServiceFault> ex)
                {
                    if (!ex.IsThrottlingException(out var retryDelay))
                    {
                        loopState.IncrementSuccessfulExecution();
                        throw;
                    }

                    // Handle service protection limit retries ourselves to manage multi-threading
                    // Wait for the recommended retry time, then add the request to the queue for retrying
                    // Terminate this thread so we don't continue to overload the server
                    Interlocked.Add(ref _inProgressCount, -req.Requests.Count);

                    // The server can report too-long delays. Wait the full 5 minutes to start with,
                    // then reduce to 2 minutes then 1 minute
                    if (retry < 1 && retryDelay > TimeSpan.FromMinutes(5))
                        retryDelay = TimeSpan.FromMinutes(5);
                    else if (retry >= 1 && retry < 3 && retryDelay > TimeSpan.FromMinutes(2))
                        retryDelay = TimeSpan.FromMinutes(2);
                    else if (retry >= 3 && retryDelay > TimeSpan.FromMinutes(1))
                        retryDelay = TimeSpan.FromMinutes(1);

                    _delayedUntil[threadState] = DateTime.Now.Add(retryDelay);
                    if (Interlocked.Increment(ref _pausedThreadCount) == 1)
                        Interlocked.Increment(ref _serviceProtectionLimitHits);
                    ShowProgress(options, operationNames, meta);

                    loopState.RequestReduceThreadCount();
                    await Task.Delay(retryDelay, options.CancellationToken);
                    Interlocked.Decrement(ref _pausedThreadCount);
                    _delayedUntil.TryRemove(threadState, out _);
                    ShowProgress(options, operationNames, meta);
                    _retryQueue.Enqueue(req);
                    return false;
                }
            }

            return true;
        }

        protected virtual bool FilterErrors(NodeExecutionContext context, OrganizationRequest request, OrganizationServiceFault fault)
        {
            return true;
        }

        protected virtual ExecuteMultipleResponse ExecuteMultiple(DataSource dataSource, IOrganizationService org, EntityMetadata meta, ExecuteMultipleRequest req)
        {
            return (ExecuteMultipleResponse)dataSource.Execute(org, req);
        }

        public abstract object Clone();
    }
}
