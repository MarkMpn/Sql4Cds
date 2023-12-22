using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.SqlTypes;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.ServiceModel;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Metadata;
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
        [Description("The maximum number of operations that will be performed in parallel")]
        public abstract int MaxDOP { get; set; }

        /// <summary>
        /// The number of requests that will be submitted in a single batch
        /// </summary>
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

        /// <summary>
        /// Attempts to fold this node into its source to simplify the query
        /// </summary>
        /// <param name="context">The context in which the node is being built</param>
        /// <param name="hints">Any hints that can control the folding of this node</param>
        /// <returns>The node that should be used in place of this node</returns>
        public virtual IRootExecutionPlanNodeInternal[] FoldQuery(NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            if (Source is IDataExecutionPlanNodeInternal dataNode)
                Source = dataNode.FoldQuery(context, hints);
            else if (Source is IDataReaderExecutionPlanNode dataSetNode)
                Source = dataSetNode.FoldQuery(context, hints).Single();

            if (Source is AliasNode alias)
            {
                Source = alias.Source;
                Source.Parent = this;
                RenameSourceColumns(alias.ColumnSet.ToDictionary(col => alias.Alias + "." + col.OutputColumn, col => col.SourceColumn, StringComparer.OrdinalIgnoreCase));
            }

            MaxDOP = GetMaxDOP(context, hints);
            BatchSize = GetBatchSize(context, hints);
            BypassCustomPluginExecution = GetBypassPluginExecution(context, hints);
            ContinueOnError = GetContinueOnError(context, hints);

            return new[] { this };
        }

        private int GetMaxDOP(NodeCompilationContext context, IList<OptimizerHint> queryHints)
        {
            if (DataSource == null)
                return 1;

            if (!context.DataSources.TryGetValue(DataSource, out var dataSource))
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
                    throw new NotSupportedQueryFragmentException("BATCH_SIZE requires a positive integer value", batchSizeHint);

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

        /// <summary>
        /// Changes the name of source columns
        /// </summary>
        /// <param name="columnRenamings">A dictionary of old source column names to the corresponding new column names</param>
        protected abstract void RenameSourceColumns(IDictionary<string, string> columnRenamings);

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

                // Store the values under the column index as well as name for compatibility with INSERT ... SELECT ...
                var dataTable = new DataTable();
                var schemaTable = dataReader.GetSchemaTable();
                var columnTypes = new ColumnList();
                var targetDataSource = DataSource == null ? context.PrimaryDataSource : context.DataSources[DataSource];

                for (var i = 0; i < schemaTable.Rows.Count; i++)
                {
                    var colSchema = schemaTable.Rows[i];
                    var colName = (string)colSchema["ColumnName"];
                    var colType = (Type)colSchema["ProviderSpecificDataType"];
                    var colTypeName = (string)colSchema["DataTypeName"];
                    var colSize = (int)colSchema["ColumnSize"];
                    var precision = (short)colSchema["NumericPrecision"];
                    var scale = (short)colSchema["NumericScale"];

                    dataTable.Columns.Add(colName, colType);

                    DataTypeReference colSqlType;

                    switch (colTypeName)
                    {
                        case "binary": colSqlType = DataTypeHelpers.Binary(colSize); break;
                        case "varbinary": colSqlType = DataTypeHelpers.VarBinary(colSize); break;
                        case "char": colSqlType = DataTypeHelpers.Char(colSize, targetDataSource.DefaultCollation, CollationLabel.Implicit); break;
                        case "varchar": colSqlType = DataTypeHelpers.VarChar(colSize, targetDataSource.DefaultCollation, CollationLabel.Implicit); break;
                        case "nchar": colSqlType = DataTypeHelpers.NChar(colSize, targetDataSource.DefaultCollation, CollationLabel.Implicit); break;
                        case "nvarchar": colSqlType = DataTypeHelpers.NVarChar(colSize, targetDataSource.DefaultCollation, CollationLabel.Implicit); break;
                        case "datetime": colSqlType = DataTypeHelpers.DateTime; break;
                        case "smalldatetime": colSqlType = DataTypeHelpers.SmallDateTime; break;
                        case "date": colSqlType = DataTypeHelpers.Date; break;
                        case "time": colSqlType = DataTypeHelpers.Time(scale); break;
                        case "datetimeoffset": colSqlType = DataTypeHelpers.DateTimeOffset; break;
                        case "datetime2": colSqlType = DataTypeHelpers.DateTime2(scale); break;
                        case "decimal": colSqlType = DataTypeHelpers.Decimal(precision, scale); break;
                        case "numeric": colSqlType = DataTypeHelpers.Decimal(precision, scale); break;
                        case "float": colSqlType = DataTypeHelpers.Float; break;
                        case "real": colSqlType = DataTypeHelpers.Real; break;
                        case "bigint": colSqlType = DataTypeHelpers.BigInt; break;
                        case "int": colSqlType = DataTypeHelpers.Int; break;
                        case "smallint": colSqlType = DataTypeHelpers.SmallInt; break;
                        case "tinyint": colSqlType = DataTypeHelpers.TinyInt; break;
                        case "money": colSqlType = DataTypeHelpers.Money; break;
                        case "smallmoney": colSqlType = DataTypeHelpers.SmallMoney; break;
                        case "bit": colSqlType = DataTypeHelpers.Bit; break;
                        case "uniqueidentifier": colSqlType = DataTypeHelpers.UniqueIdentifier; break;
                        default: throw new NotSupportedException("Unhandled column type " + colTypeName);
                    }

                    columnTypes[colName] = new ColumnDefinition(colSqlType, true, false);
                    columnTypes[i.ToString()] = new ColumnDefinition(colSqlType, true, false);
                    dataTable.Columns[i].ExtendedProperties["SqlType"] = colSqlType;
                }
                
                dataTable.Load(dataReader);
                schema = new NodeSchema(
                    primaryKey: null,
                    schema: columnTypes,
                    aliases: null,
                    sortOrder: null);

                entities = dataTable.Rows
                    .Cast<DataRow>()
                    .Select(row =>
                    {
                        var entity = new Entity();

                        for (var i = 0; i < dataTable.Columns.Count; i++)
                        {
                            var value = row[i];

                            if (value is DateTime dt)
                            {
                                var sqlType = (DataTypeReference) dataTable.Columns[i].ExtendedProperties["SqlType"];
                                if (sqlType.IsSameAs(DataTypeHelpers.Date))
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

                            entity[dataTable.Columns[i].ColumnName] = value;
                            entity[i.ToString()] = value;
                        }

                        return entity;
                    })
                    .ToList();
            }
            else
            {
                throw new QueryExecutionException("Unexpected data source") { Node = this };
            }

            return entities;
        }

        /// <summary>
        /// Compiles methods to access the data required for the DML operation
        /// </summary>
        /// <param name="cache">The metadata cache</param>
        /// <param name="logicalName">The logical name of the entity that will be affected</param>
        /// <param name="mappings">The mappings of attribute name to source column</param>
        /// <param name="schema">The schema of data source</param>
        /// <param name="dateTimeKind">The time zone that datetime values are supplied in</param>
        /// <param name="entities">The records that are being mapped</param>
        /// <returns></returns>
        protected Dictionary<string, Func<Entity, object>> CompileColumnMappings(DataSource dataSource, string logicalName, IDictionary<string,string> mappings, INodeSchema schema, DateTimeKind dateTimeKind, List<Entity> entities)
        {
            var metadata = dataSource.Metadata[logicalName];
            var attributes = metadata.Attributes.ToDictionary(a => a.LogicalName, StringComparer.OrdinalIgnoreCase);

            var attributeAccessors = new Dictionary<string, Func<Entity, object>>();
            var entityParam = Expression.Parameter(typeof(Entity));

            foreach (var mapping in mappings)
            {
                var sourceColumnName = mapping.Value;
                var destAttributeName = mapping.Key;

                if (!schema.ContainsColumn(sourceColumnName, out sourceColumnName))
                    throw new QueryExecutionException($"Missing source column {mapping.Value}") { Node = this };

                // We might be using a virtual ___type attribute that has a different name in the metadata. We can safely
                // ignore these attributes - the attribute names have already been validated in the ExecutionPlanBuilder
                if (!attributes.TryGetValue(destAttributeName, out var attr) || attr.AttributeOf != null)
                    continue;

                var sourceSqlType = schema.Schema[sourceColumnName].Type;
                var destType = attr.GetAttributeType();
                var destSqlType = attr.IsPrimaryId == true ? DataTypeHelpers.UniqueIdentifier : attr.GetAttributeSqlType(dataSource, true);

                if (attr is LookupAttributeMetadata && metadata.IsIntersect == true)
                {
                    destType = typeof(Guid?);
                    destSqlType = DataTypeHelpers.UniqueIdentifier;
                }

                var expr = (Expression)Expression.Property(entityParam, typeof(Entity).GetCustomAttribute<DefaultMemberAttribute>().MemberName, Expression.Constant(sourceColumnName));
                var originalExpr = expr;

                if (sourceSqlType.IsSameAs(DataTypeHelpers.Int) && !SqlTypeConverter.CanChangeTypeExplicit(sourceSqlType, destSqlType) && entities.All(e => ((SqlInt32)e[sourceColumnName]).IsNull))
                {
                    // null literal is typed as int
                    expr = Expression.Constant(null, destType);
                    expr = Expr.Box(expr);
                }
                else
                {
                    // Unbox value as source SQL type
                    expr = Expression.Convert(expr, sourceSqlType.ToNetType(out _));

                    Expression convertedExpr;

                    if (attr is LookupAttributeMetadata lookupAttr && lookupAttr.AttributeType != AttributeTypeCode.PartyList && metadata.IsIntersect != true)
                    {
                        if (sourceSqlType.IsSameAs(DataTypeHelpers.EntityReference))
                        {
                            convertedExpr = expr;
                            expr = originalExpr;
                            convertedExpr = SqlTypeConverter.Convert(convertedExpr, typeof(EntityReference));
                        }
                        else if (sourceSqlType == DataTypeHelpers.ImplicitIntForNullLiteral)
                        {
                            expr = originalExpr;
                            convertedExpr = Expression.Constant(null, typeof(EntityReference));
                        }
                        else
                        {
                            Expression targetExpr;

                            if (lookupAttr.Targets.Length == 1)
                            {
                                targetExpr = Expression.Constant(lookupAttr.Targets[0]);
                            }
                            else
                            {
                                var sourceTargetColumnName = mappings[destAttributeName + "type"];
                                var sourceTargetType = schema.Schema[sourceTargetColumnName].Type;

                                targetExpr = Expression.Property(entityParam, typeof(Entity).GetCustomAttribute<DefaultMemberAttribute>().MemberName, Expression.Constant(sourceTargetColumnName));
                                targetExpr = Expression.Convert(targetExpr, sourceTargetType.ToNetType(out _));

                                if (targetExpr.Type == typeof(SqlInt32))
                                {
                                    // Using TDS Endpoint, ___type fields are returned as ObjectTypeCode values, not logical names
                                    targetExpr = Expr.Call(() => ObjectTypeCodeToLogicalName(Expr.Arg<SqlInt32>(), Expr.Arg<IAttributeMetadataCache>()), targetExpr, Expression.Constant(dataSource.Metadata));
                                }
                                else
                                {
                                    // Normally we want to specify the target type as a logical name
                                    var stringType = DataTypeHelpers.NVarChar(MetadataExtensions.EntityLogicalNameMaxLength, dataSource.DefaultCollation, CollationLabel.Implicit);
                                    targetExpr = SqlTypeConverter.Convert(targetExpr, sourceTargetType, stringType);
                                    targetExpr = SqlTypeConverter.Convert(targetExpr, typeof(string));
                                }
                            }

                            convertedExpr = SqlTypeConverter.Convert(expr, sourceSqlType, DataTypeHelpers.UniqueIdentifier);
                            convertedExpr = SqlTypeConverter.Convert(convertedExpr, typeof(Guid));
                            convertedExpr = Expression.New(
                                typeof(EntityReference).GetConstructor(new[] { typeof(string), typeof(Guid) }),
                                targetExpr,
                                convertedExpr
                            );
                        }

                        destType = typeof(EntityReference);
                    }
                    else
                    {
                        if (!sourceSqlType.IsSameAs(DataTypeHelpers.EntityReference) ||
                            !(attr is LookupAttributeMetadata partyListAttr) ||
                            partyListAttr.AttributeType != AttributeTypeCode.PartyList)
                        {
                            // Convert to destination SQL type - don't do this if we're converting from an EntityReference to a PartyList so
                            // we don't lose the entity name during the conversion via a string
                            expr = SqlTypeConverter.Convert(expr, sourceSqlType, destSqlType);
                        }

                        // Convert to final .NET SDK type
                        convertedExpr = SqlTypeConverter.Convert(expr, destType);
                        
                        if (attr is EnumAttributeMetadata && !(attr is MultiSelectPicklistAttributeMetadata))
                        {
                            convertedExpr = Expression.New(
                                typeof(OptionSetValue).GetConstructor(new[] { typeof(int) }),
                                Expression.Convert(convertedExpr, typeof(int))
                            );
                            destType = typeof(OptionSetValue);
                        }
                        else if (attr is MoneyAttributeMetadata)
                        {
                            convertedExpr = Expression.New(
                                typeof(Money).GetConstructor(new[] { typeof(decimal) }),
                                Expression.Convert(expr, typeof(decimal))
                            );
                            destType = typeof(Money);
                        }
                        else if (attr is DateTimeAttributeMetadata)
                        {
                            convertedExpr = Expression.Convert(
                                Expr.Call(() => DateTime.SpecifyKind(Expr.Arg<DateTime>(), Expr.Arg<DateTimeKind>()),
                                    expr,
                                    Expression.Constant(dateTimeKind)
                                ),
                                typeof(DateTime?)
                            );
                        }
                    }

                    // Check for null on the value BEFORE converting from the SQL to BCL type to avoid e.g. SqlDateTime.Null being converted to 1900-01-01
                    expr = Expression.Condition(
                        SqlTypeConverter.NullCheck(expr),
                        Expression.Constant(null, destType),
                        convertedExpr);

                    if (expr.Type.IsValueType)
                        expr = SqlTypeConverter.Convert(expr, typeof(object));
                }

                attributeAccessors[destAttributeName] = Expression.Lambda<Func<Entity, object>>(expr, entityParam).Compile();
            }

            return attributeAccessors;
        }

        private static string ObjectTypeCodeToLogicalName(SqlInt32 otc, IAttributeMetadataCache attributeMetadataCache)
        {
            if (otc.IsNull)
                throw new QueryExecutionException("Cannot convert null ObjectTypeCode to EntityReference");

            return attributeMetadataCache[otc.Value].LogicalName;
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
            var inProgressCount = 0;
            var count = 0;
            var errorCount = 0;
            var threadCount = 0;

#if NETCOREAPP
            var svc = dataSource.Connection as ServiceClient;
#else
            var svc = dataSource.Connection as CrmServiceClient;
#endif

            var maxDop = MaxDOP;

            if (!ParallelismHelper.CanParallelise(dataSource.Connection))
                maxDop = 1;

            if (maxDop == 1)
                svc = null;

            var useAffinityCookie = maxDop == 1 || entities.Count < 100;

            try
            {
                OrganizationServiceFault fault = null;

                using (UseParallelConnections())
                {
                    Parallel.ForEach(entities,
                        new ParallelOptions { MaxDegreeOfParallelism = maxDop },
                        () =>
                        {
                            var service = svc?.Clone() ?? dataSource.Connection;

#if NETCOREAPP
                            if (!useAffinityCookie && service is ServiceClient crmService)
                                crmService.EnableAffinityCookie = false;
#else
                            if (!useAffinityCookie && service is CrmServiceClient crmService)
                                crmService.EnableAffinityCookie = false;
#endif
                            Interlocked.Increment(ref threadCount);

                            return new { Service = service, EMR = default(ExecuteMultipleRequest) };
                        },
                        (entity, loopState, index, threadLocalState) =>
                        {
                            if (options.CancellationToken.IsCancellationRequested)
                            {
                                loopState.Stop();
                                return threadLocalState;
                            }

                            var request = requestGenerator(entity);

                            if (BypassCustomPluginExecution)
                                request.Parameters["BypassCustomPluginExecution"] = true;

                            if (BatchSize == 1)
                            {
                                var newCount = Interlocked.Increment(ref inProgressCount);
                                var progress = (double)newCount / entities.Count;

                                if (threadCount < 2)
                                    options.Progress(progress, $"{operationNames.InProgressUppercase} {newCount:N0} of {entities.Count:N0} {GetDisplayName(0, meta)} ({progress:P0})...");
                                else
                                    options.Progress(progress, $"{operationNames.InProgressUppercase} {newCount - threadCount + 1:N0}-{newCount:N0} of {entities.Count:N0} {GetDisplayName(0, meta)} ({progress:P0}, {threadCount:N0} threads)...");

                                try
                                {
                                    var response = dataSource.Execute(threadLocalState.Service, request);
                                    Interlocked.Increment(ref count);

                                    responseHandler?.Invoke(response);
                                }
                                catch (FaultException<OrganizationServiceFault> ex)
                                {
                                    if (FilterErrors(context, request, ex.Detail))
                                    {
                                        if (ContinueOnError)
                                            fault = fault ?? ex.Detail;
                                        else
                                            throw;
                                    }

                                    Interlocked.Increment(ref errorCount);
                                }
                            }
                            else
                            {
                                if (threadLocalState.EMR == null)
                                {
                                    threadLocalState = new
                                    {
                                        threadLocalState.Service,
                                        EMR = new ExecuteMultipleRequest
                                        {
                                            Requests = new OrganizationRequestCollection(),
                                            Settings = new ExecuteMultipleSettings
                                            {
                                                ContinueOnError = IgnoresSomeErrors,
                                                ReturnResponses = responseHandler != null
                                            }
                                        }
                                    };
                                }

                                threadLocalState.EMR.Requests.Add(request);

                                if (threadLocalState.EMR.Requests.Count == BatchSize)
                                {
                                    ProcessBatch(threadLocalState.EMR, threadCount, ref count, ref inProgressCount, ref errorCount, entities, operationNames, meta, options, dataSource, threadLocalState.Service, context, responseHandler, ref fault);

                                    threadLocalState = new { threadLocalState.Service, EMR = default(ExecuteMultipleRequest) };
                                }
                            }

                            return threadLocalState;
                        },
                        (threadLocalState) =>
                        {
                            if (threadLocalState.EMR != null)
                                ProcessBatch(threadLocalState.EMR, threadCount, ref count, ref inProgressCount, ref errorCount, entities, operationNames, meta, options, dataSource, threadLocalState.Service, context, responseHandler, ref fault);

                            Interlocked.Decrement(ref threadCount);

                            if (threadLocalState.Service != dataSource.Connection && threadLocalState.Service is IDisposable disposableClient)
                                disposableClient.Dispose();
                        });
                }

                if (fault != null)
                    throw new FaultException<OrganizationServiceFault>(fault, new FaultReason(fault.Message));
            }
            catch (Exception ex)
            {
                var originalEx = ex;

                if (ex is AggregateException agg && agg.InnerExceptions.Count == 1)
                    ex = agg.InnerException;

                if (count == 0)
                {
                    if (ex == originalEx)
                        throw;
                    else
                        throw ex;
                }

                throw new PartialSuccessException($"{count:N0} {GetDisplayName(count, meta)} {operationNames.CompletedLowercase}", ex);
            }

            recordsAffected = count;
            message = $"({count:N0} {GetDisplayName(count, meta)} {operationNames.CompletedLowercase})";
            context.ParameterValues["@@ROWCOUNT"] = (SqlInt32)count;
        }

        protected class BulkApiErrorDetail
        {
            public int RequestIndex { get; set; }
            public Guid Id { get; set; }
            public int StatusCode { get; set; }
        }

        private void ProcessBatch(ExecuteMultipleRequest req, int threadCount, ref int count, ref int inProgressCount, ref int errorCount, List<Entity> entities, OperationNames operationNames, EntityMetadata meta, IQueryExecutionOptions options, DataSource dataSource, IOrganizationService org, NodeExecutionContext context, Action<OrganizationResponse> responseHandler, ref OrganizationServiceFault fault)
        {
            var newCount = Interlocked.Add(ref inProgressCount, req.Requests.Count);
            var progress = (double)newCount / entities.Count;
            var threadCountMessage = threadCount < 2 ? "" : $" ({threadCount:N0} threads)";
            options.Progress(progress, $"{operationNames.InProgressUppercase} {GetDisplayName(0, meta)} {count + errorCount + 1:N0} - {newCount:N0} of {entities.Count:N0}{threadCountMessage}...");
            var resp = ExecuteMultiple(dataSource, org, meta, req);

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

            Interlocked.Add(ref count, req.Requests.Count - errorResponses.Count);
            Interlocked.Add(ref errorCount, errorResponses.Count);

            var error = errorResponses.FirstOrDefault(item => FilterErrors(context, req.Requests[item.RequestIndex], item.Fault));

            if (error != null)
            {
                fault = fault ?? error.Fault;

                if (!ContinueOnError)
                    throw new FaultException<OrganizationServiceFault>(fault, new FaultReason(fault.Message));
            }
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
