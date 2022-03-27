using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using Microsoft.Crm.Sdk.Messages;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using System.Threading;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using System.Data.SqlTypes;
#if NETCOREAPP
using Microsoft.PowerPlatform.Dataverse.Client;
#else
using Microsoft.Xrm.Tooling.Connector;
#endif
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine
{
    /// <summary>
    /// Provides access to Dataverse / Dynamics 365 data through a standard ADO.NET connection interface
    /// </summary>
    public class Sql4CdsConnection : DbConnection
    {
        private readonly Dictionary<string, DataSource> _dataSources;
        private readonly ChangeDatabaseOptionsWrapper _options;
        private readonly Dictionary<string, DataTypeReference> _globalVariableTypes;
        private readonly Dictionary<string, object> _globalVariableValues;

        /// <summary>
        /// Creates a new <see cref="Sql4CdsConnection"/> using the specified XRM connection string
        /// </summary>
        /// <param name="connectionString">The connection string to use to connect to the Dataverse / Dynamics 365 instance</param>
        public Sql4CdsConnection(string connectionString) : this(Connect(connectionString))
        {
            ConnectionString = connectionString;
        }

        /// <summary>
        /// Creates a new <see cref="Sql4CdsConnection"/> using the specified <see cref="IOrganizationService"/>
        /// </summary>
        /// <param name="svc">The list of <see cref="IOrganizationService"/>s to use</param>
        public Sql4CdsConnection(params IOrganizationService[] svc) : this(BuildDataSources(svc))
        {
        }

        /// <summary>
        /// Creates a new <see cref="Sql4CdsConnection"/> using the specified list of data sources
        /// </summary>
        /// <param name="dataSources">The list of data sources to use</param>
        public Sql4CdsConnection(params DataSource[] dataSources)
        {
            if (dataSources == null)
                throw new ArgumentNullException(nameof(dataSources));

            if (dataSources.Length == 0)
                throw new ArgumentOutOfRangeException("At least one data source must be supplied");

            var options = new DefaultQueryExecutionOptions(dataSources[0], CancellationToken.None);

            _dataSources = dataSources.ToDictionary(ds => ds.Name, StringComparer.OrdinalIgnoreCase);
            _options = new ChangeDatabaseOptionsWrapper(this, options);

            _globalVariableTypes = new Dictionary<string, DataTypeReference>(StringComparer.OrdinalIgnoreCase)
            {
                ["@@IDENTITY"] = typeof(SqlEntityReference).ToSqlType(),
                ["@@ROWCOUNT"] = typeof(SqlInt32).ToSqlType()
            };
            _globalVariableValues = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase)
            {
                ["@@IDENTITY"] = SqlEntityReference.Null,
                ["@@ROWCOUNT"] = (SqlInt32)0
            };
        }

        private static IOrganizationService Connect(string connectionString)
        {
#if NETCOREAPP
            var svc = new ServiceClient(connectionString);

            if (!svc.IsReady)
                throw new Sql4CdsException(svc.LastError);
#else
            var svc = new CrmServiceClient(connectionString);

            if (!svc.IsReady)
                throw new Sql4CdsException(svc.LastCrmException.Message, svc.LastCrmException);
#endif

            return svc;
        }

        private static DataSource[] BuildDataSources(IOrganizationService[] org)
        {
            var dataSources = new DataSource[org.Length];

            for (var i = 0; i < org.Length; i++)
                dataSources[i] = new DataSource(org[i]);

            return dataSources;
        }

        public event EventHandler<InfoMessageEventArgs> InfoMessage;

        internal void OnInfoMessage(IRootExecutionPlanNode node, string message)
        {
            var handler = InfoMessage;

            if (handler != null)
                handler(this, new InfoMessageEventArgs(node, message));
        }

        internal Dictionary<string, DataSource> DataSources => _dataSources;

        internal IQueryExecutionOptions Options => _options;

        /// <summary>
        /// Indicates if <see cref="SqlEntityReference"/> values should be returned as <see cref="Guid"/>s from the data reader
        /// </summary>
        public bool ReturnEntityReferenceAsGuid { get; set; }

        /// <summary>
        /// Indicates if the Dataverse TDS Endpoint should be used for query execution where possible
        /// </summary>
        public bool UseTDSEndpoint
        {
            get => _options.UseTDSEndpoint;
            set => _options.UseTDSEndpoint = value;
        }

        /// <summary>
        /// Indicates if an UPDATE statement cannot be executed unless it has a WHERE clause
        /// </summary>
        public bool BlockUpdateWithoutWhere
        {
            get => _options.BlockUpdateWithoutWhere;
            set => _options.BlockUpdateWithoutWhere = value;
        }

        /// <summary>
        /// Indicates if a DELETE statement cannot be execyted unless it has a WHERE clause
        /// </summary>
        public bool BlockDeleteWithoutWhere
        {
            get => _options.BlockDeleteWithoutWhere;
            set => _options.BlockDeleteWithoutWhere = value;
        }

        /// <summary>
        /// Indicates if DELETE queries should be executed with a bulk delete job
        /// </summary>
        public bool UseBulkDelete
        {
            get => _options.UseBulkDelete;
            set => _options.UseBulkDelete = value;
        }

        /// <summary>
        /// The number of records that should be inserted, updated or deleted in a single batch
        /// </summary>
        public int BatchSize
        {
            get => _options.BatchSize;
            set => _options.BatchSize = value;
        }

        /// <summary>
        /// Indicates if a <see cref="Microsoft.Crm.Sdk.Messages.RetrieveTotalRecordCountRequest"/> should be used for simple SELECT count(*) FROM table queries
        /// </summary>
        public bool UseRetrieveTotalRecordCount
        {
            get => _options.UseRetrieveTotalRecordCount;
            set => _options.UseRetrieveTotalRecordCount = value;
        }

        /// <summary>
        /// The maximum number of worker threads to use to execute DML queries
        /// </summary>
        public int MaxDegreeOfParallelism
        {
            get => _options.MaxDegreeOfParallelism;
            set => _options.MaxDegreeOfParallelism = value;
        }

        /// <summary>
        /// Indicates if date/time values should be interpreted in the local timezone or in UTC
        /// </summary>
        public bool UseLocalTimeZone
        {
            get => _options.UseLocalTimeZone;
            set => _options.UseLocalTimeZone = value;
        }

        /// <summary>
        /// Indicates if plugins should be bypassed when executing DML operations
        /// </summary>
        public bool BypassCustomPlugins
        {
            get => _options.BypassCustomPlugins;
            set => _options.BypassCustomPlugins = value;
        }

        /// <summary>
        /// Returns or sets a value indicating if SQL will be parsed using quoted identifiers
        /// </summary>
        public bool QuotedIdentifiers
        {
            get => _options.QuotedIdentifiers;
            set => _options.QuotedIdentifiers = value;
        }

        internal Dictionary<string, DataTypeReference> GlobalVariableTypes => _globalVariableTypes;

        internal Dictionary<string, object> GlobalVariableValues => _globalVariableValues;

        /// <summary>
        /// Triggered before one or more records are about to be deleted
        /// </summary>
        /// <remarks>
        /// Set the <see cref="CancelEventArgs.Cancel"/> property to <c>true</c> to prevent the deletion
        /// </remarks>
        public event EventHandler<ConfirmDmlStatementEventArgs> PreDelete;

        internal void OnPreDelete(ConfirmDmlStatementEventArgs args)
        {
            PreDelete?.Invoke(this, args);
        }

        /// <summary>
        /// Triggered before one or more records are about to be inserted
        /// </summary>
        /// <remarks>
        /// Set the <see cref="CancelEventArgs.Cancel"/> property to <c>true</c> to prevent the insertion
        /// </remarks>
        public event EventHandler<ConfirmDmlStatementEventArgs> PreInsert;

        internal void OnPreInsert(ConfirmDmlStatementEventArgs args)
        {
            PreInsert?.Invoke(this, args);
        }

        /// <summary>
        /// Triggered before one or more records are about to be updated
        /// </summary>
        /// <remarks>
        /// Set the <see cref="CancelEventArgs.Cancel"/> property to <c>true</c> to prevent the update
        /// </remarks>
        public event EventHandler<ConfirmDmlStatementEventArgs> PreUpdate;

        internal void OnPreUpdate(ConfirmDmlStatementEventArgs args)
        {
            PreUpdate?.Invoke(this, args);
        }

        /// <summary>
        /// Triggered before a page of data is about to be retrieved
        /// </summary>
        /// <remarks>
        /// Set the <see cref="CancelEventArgs.Cancel"/> property to <c>true</c> to prevent the retrieval
        /// </remarks>
        public event EventHandler<ConfirmRetrieveEventArgs> PreRetrieve;

        internal void OnPreRetrieve(ConfirmRetrieveEventArgs args)
        {
            PreRetrieve?.Invoke(this, args);
        }

        /// <summary>
        /// Triggered when there is some progress that can be reported to the user
        /// </summary>
        /// <remarks>
        /// This event does not signify that an operation has completed successfully. It can be triggered multiple times
        /// for the same operation. Use <see cref="Sql4CdsCommand.StatementCompleted"/> to receive a notification that a
        /// statement has completed successfully, or <see cref="InfoMessage"/> to receive messages that should be shown
        /// as part of the main output of a query.
        /// </remarks>
        public event EventHandler<ProgressEventArgs> Progress;

        internal void OnProgress(ProgressEventArgs args)
        {
            Progress?.Invoke(this, args);
        }

        public override string ConnectionString { get; set; }

        public override string Database => _options.PrimaryDataSource;

        public override string DataSource
        {
            get
            {
                var dataSource = _dataSources[Database];

#if NETCOREAPP
                if (dataSource.Connection is ServiceClient svc)
                    return svc.ConnectedOrgUriActual.Host;
#else
                if (dataSource.Connection is CrmServiceClient svc)
                    return svc.CrmConnectOrgUriActual.Host;
#endif

                return string.Empty;
            }
        }

        public override string ServerVersion
        {
            get
            {
                var dataSource = _dataSources[Database];

#if NETCOREAPP
                if (dataSource.Connection is ServiceClient svc)
                    return svc.ConnectedOrgVersion.ToString();
#else
                if (dataSource.Connection is CrmServiceClient svc)
                    return svc.ConnectedOrgVersion.ToString();
#endif

                var resp = (RetrieveVersionResponse) dataSource.Connection.Execute(new RetrieveVersionRequest());
                return resp.Version;
            }
        }

        public override ConnectionState State => ConnectionState.Open;

        public override void ChangeDatabase(string databaseName)
        {
            if (!_dataSources.ContainsKey(databaseName))
                throw new Sql4CdsException("Database is not in the list of connected databases");

            _options.PrimaryDataSource = databaseName;
        }

        public override void Close()
        {
        }

        public override void Open()
        {
        }

        protected override DbTransaction BeginDbTransaction(System.Data.IsolationLevel isolationLevel)
        {
            throw new NotSupportedException("Transactions are not supported");
        }

        protected override DbCommand CreateDbCommand()
        {
            return CreateCommand();
        }

        public new Sql4CdsCommand CreateCommand()
        {
            return new Sql4CdsCommand(this);
        }
    }
}
