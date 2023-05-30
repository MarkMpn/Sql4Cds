using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.SqlTypes;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
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

        [Browsable(false)]
        public IExecutionPlanNodeInternal Source { get; set; }

        /// <summary>
        /// The instance that this node will be executed against
        /// </summary>
        [Category("Data Source")]
        [Description("The data source this query is executed against")]
        public virtual string DataSource { get; set; }

        /// <summary>
        /// The maximum degree of paralellism to apply to this operation
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
        /// Changes system settings to optimise for parallel connections
        /// </summary>
        /// <returns>An object to dispose of to reset the settings to their original values</returns>
        protected IDisposable UseParallelConnections() => new ParallelConnectionSettings();

        /// <summary>
        /// Executes the DML query and returns an appropriate log message
        /// </summary>
        /// <param name="context">The context in which the node is being executed</param>
        /// <param name="recordsAffected">The number of records that were affected by the query</param>
        /// <returns>A log message to display</returns>
        public abstract string Execute(NodeExecutionContext context, out int recordsAffected);

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

            return new[] { this };
        }

        private int GetMaxDOP(NodeCompilationContext context, IList<OptimizerHint> queryHints)
        {
            if (DataSource == null)
                return 1;

            if (!context.DataSources.TryGetValue(DataSource, out var dataSource))
                throw new NotSupportedQueryFragmentException("Unknown datasource");

            var org = dataSource.Connection;
            var recommendedMaxDop = 1;

#if NETCOREAPP
            var svc = org as ServiceClient;

            if (svc != null)
                recommendedMaxDop = svc.RecommendedDegreesOfParallelism;
#else
            var svc = org as CrmServiceClient;

            if (svc != null)
                recommendedMaxDop = svc.RecommendedDegreesOfParallelism;
#endif

            var maxDopHint = (queryHints ?? Array.Empty<OptimizerHint>())
                .OfType<LiteralOptimizerHint>()
                .Where(hint => hint.HintKind == OptimizerHintKind.MaxDop)
                .FirstOrDefault();

            if (maxDopHint != null)
            {
                if (!(maxDopHint.Value is IntegerLiteral maxDop) || !Int32.TryParse(maxDop.Value, out var value) || value < 0)
                    throw new NotSupportedQueryFragmentException("MAXDOP requires a positive integer value, or 0 to use recommended value", maxDopHint);

                if (value > 0)
                    return value;

                return recommendedMaxDop;
            }

            if (context.Options.MaxDegreeOfParallelism > 0)
                return context.Options.MaxDegreeOfParallelism;

            return recommendedMaxDop;
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

                    columnTypes[colName] = colSqlType;
                    columnTypes[i.ToString()] = colSqlType;
                    dataTable.Columns[i].ExtendedProperties["SqlType"] = colSqlType;
                }
                
                dataTable.Load(dataReader);
                schema = new NodeSchema(
                    primaryKey: null,
                    schema: columnTypes,
                    aliases: null,
                    notNullColumns: null,
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

                var sourceSqlType = schema.Schema[sourceColumnName];
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
                            expr = originalExpr;
                            convertedExpr = SqlTypeConverter.Convert(expr, sourceSqlType, sourceSqlType);
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
                                var sourceTargetType = schema.Schema[sourceTargetColumnName];
                                var stringType = DataTypeHelpers.NVarChar(MetadataExtensions.EntityLogicalNameMaxLength, dataSource.DefaultCollation, CollationLabel.Implicit);
                                targetExpr = Expression.Property(entityParam, typeof(Entity).GetCustomAttribute<DefaultMemberAttribute>().MemberName, Expression.Constant(sourceTargetColumnName));
                                targetExpr = SqlTypeConverter.Convert(targetExpr, sourceTargetType, stringType);
                                targetExpr = SqlTypeConverter.Convert(targetExpr, typeof(string));
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
                        // Convert to destination SQL type
                        expr = SqlTypeConverter.Convert(expr, sourceSqlType, destSqlType);

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
        /// <returns>The final log message</returns>
        protected string ExecuteDmlOperation(DataSource dataSource, IQueryExecutionOptions options, List<Entity> entities, EntityMetadata meta, Func<Entity,OrganizationRequest> requestGenerator, OperationNames operationNames, out int recordsAffected, IDictionary<string, object> parameterValues, Action<OrganizationResponse> responseHandler = null)
        {
            var inProgressCount = 0;
            var count = 0;

            var maxDop = MaxDOP;

#if NETCOREAPP
            var svc = dataSource.Connection as ServiceClient;

            if (maxDop <= 1 || svc == null || (svc.ActiveAuthenticationType != Microsoft.PowerPlatform.Dataverse.Client.AuthenticationType.OAuth && svc.ActiveAuthenticationType != Microsoft.PowerPlatform.Dataverse.Client.AuthenticationType.Certificate && svc.ActiveAuthenticationType != Microsoft.PowerPlatform.Dataverse.Client.AuthenticationType.ExternalTokenManagement && svc.ActiveAuthenticationType != Microsoft.PowerPlatform.Dataverse.Client.AuthenticationType.ClientSecret))
            {
                maxDop = 1;
                svc = null;
            }
#else
            var svc = dataSource.Connection as CrmServiceClient;

            if (maxDop <= 1 || svc == null || (svc.ActiveAuthenticationType != Microsoft.Xrm.Tooling.Connector.AuthenticationType.OAuth && svc.ActiveAuthenticationType != Microsoft.Xrm.Tooling.Connector.AuthenticationType.Certificate && svc.ActiveAuthenticationType != Microsoft.Xrm.Tooling.Connector.AuthenticationType.ExternalTokenManagement && svc.ActiveAuthenticationType != Microsoft.Xrm.Tooling.Connector.AuthenticationType.ClientSecret))
            {
                maxDop = 1;
                svc = null;
            }
#endif

            var useAffinityCookie = maxDop == 1 || entities.Count < 100;

            try
            {
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
                                options.Progress(progress, $"{operationNames.InProgressUppercase} {newCount:N0} of {entities.Count:N0} {GetDisplayName(0, meta)} ({progress:P0})...");
                                var response = dataSource.Execute(threadLocalState.Service, request);
                                Interlocked.Increment(ref count);

                                responseHandler?.Invoke(response);
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
                                                ContinueOnError = false,
                                                ReturnResponses = responseHandler != null
                                            }
                                        }
                                    };
                                }

                                threadLocalState.EMR.Requests.Add(request);

                                if (threadLocalState.EMR.Requests.Count == BatchSize)
                                {
                                    var newCount = Interlocked.Add(ref inProgressCount, threadLocalState.EMR.Requests.Count);
                                    var progress = (double)newCount / entities.Count;
                                    options.Progress(progress, $"{operationNames.InProgressUppercase} {GetDisplayName(0, meta)} {newCount + 1 - threadLocalState.EMR.Requests.Count:N0} - {newCount:N0} of {entities.Count:N0}...");
                                    var resp = ExecuteMultiple(dataSource, threadLocalState.Service, meta, threadLocalState.EMR);

                                    if (responseHandler != null)
                                    {
                                        foreach (var item in resp.Responses)
                                        {
                                            if (item.Response != null)
                                                responseHandler(item.Response);
                                        }
                                    }

                                    if (resp.IsFaulted)
                                    {
                                        var error = resp.Responses.First(r => r.Fault != null);
                                        Interlocked.Add(ref count, error.RequestIndex);
                                        throw new ApplicationException($"Error {operationNames.InProgressLowercase} {GetDisplayName(0, meta)} - " + error.Fault.Message);
                                    }
                                    else
                                    {
                                        Interlocked.Add(ref count, threadLocalState.EMR.Requests.Count);
                                    }

                                    threadLocalState = new { threadLocalState.Service, EMR = default(ExecuteMultipleRequest) };
                                }
                            }

                            return threadLocalState;
                        },
                        (threadLocalState) =>
                        {
                            if (threadLocalState.EMR != null)
                            {
                                var newCount = Interlocked.Add(ref inProgressCount, threadLocalState.EMR.Requests.Count);
                                var progress = (double)newCount / entities.Count;
                                options.Progress(progress, $"{operationNames.InProgressUppercase} {GetDisplayName(0, meta)} {newCount + 1 - threadLocalState.EMR.Requests.Count:N0} - {newCount:N0} of {entities.Count:N0}...");
                                var resp = ExecuteMultiple(dataSource, threadLocalState.Service, meta, threadLocalState.EMR);

                                if (responseHandler != null)
                                {
                                    foreach (var item in resp.Responses)
                                    {
                                        if (item.Response != null)
                                            responseHandler(item.Response);
                                    }
                                }

                                if (resp.IsFaulted)
                                {
                                    var error = resp.Responses.First(r => r.Fault != null);
                                    Interlocked.Add(ref count, error.RequestIndex);
                                    throw new ApplicationException($"Error {operationNames.InProgressLowercase} {GetDisplayName(0, meta)} - " + error.Fault.Message);
                                }
                                else
                                {
                                    Interlocked.Add(ref count, threadLocalState.EMR.Requests.Count);
                                }
                            }

                            if (threadLocalState.Service != dataSource.Connection && threadLocalState.Service is IDisposable disposableClient)
                                disposableClient.Dispose();
                        });
                }
            }
            catch (Exception ex)
            {
                if (count == 0)
                    throw;

                throw new PartialSuccessException($"{count:N0} {GetDisplayName(count, meta)} {operationNames.CompletedLowercase}", ex);
            }

            recordsAffected = count;
            parameterValues["@@ROWCOUNT"] = (SqlInt32)count;
            return $"{count:N0} {GetDisplayName(count, meta)} {operationNames.CompletedLowercase}";
        }

        protected virtual ExecuteMultipleResponse ExecuteMultiple(DataSource dataSource, IOrganizationService org, EntityMetadata meta, ExecuteMultipleRequest req)
        {
            return (ExecuteMultipleResponse)dataSource.Execute(org, req);
        }

        public abstract object Clone();
    }
}
