﻿using System;
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
using Microsoft.ApplicationInsights;
using System.Reflection;
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
        private readonly IDictionary<string, DataSource> _dataSources;
        private readonly ChangeDatabaseOptionsWrapper _options;
        private readonly Dictionary<string, DataTypeReference> _globalVariableTypes;
        private readonly Dictionary<string, object> _globalVariableValues;
        private readonly TelemetryClient _ai;

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
        /// <param name="dataSources">The list of data sources to use, indexed by <see cref="DataSource.Name"/></param>
        public Sql4CdsConnection(IDictionary<string, DataSource> dataSources)
        {
            if (dataSources == null)
                throw new ArgumentNullException(nameof(dataSources));

            if (dataSources.Count == 0)
                throw new ArgumentOutOfRangeException("At least one data source must be supplied");

            var options = new DefaultQueryExecutionOptions(dataSources.First().Value, CancellationToken.None);

            _dataSources = dataSources;
            _options = new ChangeDatabaseOptionsWrapper(this, options);

            _globalVariableTypes = new Dictionary<string, DataTypeReference>(StringComparer.OrdinalIgnoreCase)
            {
                ["@@IDENTITY"] = DataTypeHelpers.EntityReference,
                ["@@ROWCOUNT"] = DataTypeHelpers.Int,
                ["@@SERVERNAME"] = DataTypeHelpers.NVarChar(100, _dataSources[_options.PrimaryDataSource].DefaultCollation, CollationLabel.CoercibleDefault),
                ["@@VERSION"] = DataTypeHelpers.NVarChar(Int32.MaxValue, _dataSources[_options.PrimaryDataSource].DefaultCollation, CollationLabel.CoercibleDefault),
                ["@@ERROR"] = DataTypeHelpers.Int,
            };
            _globalVariableValues = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase)
            {
                ["@@IDENTITY"] = SqlEntityReference.Null,
                ["@@ROWCOUNT"] = (SqlInt32)0,
                ["@@SERVERNAME"] = GetServerName(_dataSources[_options.PrimaryDataSource]),
                ["@@VERSION"] = GetVersion(_dataSources[_options.PrimaryDataSource]),
                ["@@ERROR"] = (SqlInt32)0,
            };

            _ai = new TelemetryClient(new Microsoft.ApplicationInsights.Extensibility.TelemetryConfiguration("79761278-a908-4575-afbf-2f4d82560da6"));

            var app = System.Reflection.Assembly.GetEntryAssembly();

            if (app != null)
                ApplicationName = System.IO.Path.GetFileNameWithoutExtension(app.Location);
            else
                ApplicationName = "SQL 4 CDS ADO.NET Provider";
        }

        private SqlString GetVersion(DataSource dataSource)
        {
            string orgVersion = null;

#if NETCOREAPP
            if (dataSource.Connection is ServiceClient svc)
                orgVersion = svc.ConnectedOrgVersion.ToString();
#else
            if (dataSource.Connection is CrmServiceClient svc)
                orgVersion = svc.ConnectedOrgVersion.ToString();
#endif

            if (orgVersion == null)
                orgVersion = ((RetrieveVersionResponse)dataSource.Execute(new RetrieveVersionRequest())).Version;

            var assembly = typeof(Sql4CdsConnection).Assembly;
            var assemblyVersion = assembly.GetName().Version;
            var assemblyCopyright = assembly
                .GetCustomAttributes(typeof(AssemblyCopyrightAttribute), false)
                .OfType<AssemblyCopyrightAttribute>()
                .FirstOrDefault()?
                .Copyright;
            var assemblyFilename = assembly.Location;
            var assemblyDate = System.IO.File.GetLastWriteTime(assemblyFilename);

            return $"Microsoft Dataverse - {orgVersion}\r\n\tSQL 4 CDS - {assemblyVersion}\r\n\t{assemblyDate:MMM dd yyyy HH:mm:ss}\r\n\t{assemblyCopyright}";
        }

        private SqlString GetServerName(DataSource dataSource)
        {
#if NETCOREAPP
            var svc = dataSource.Connection as ServiceClient;

            if (svc != null)
                return svc.ConnectedOrgUriActual.Host;
#else
            var svc = dataSource.Connection as CrmServiceClient;

            if (svc != null)
                return svc.CrmConnectOrgUriActual.Host;
#endif

            return dataSource.Name;
        }

        private static IOrganizationService Connect(string connectionString)
        {
#if NETCOREAPP
            var svc = new ServiceClient(connectionString);

            if (!svc.IsReady)
                throw new Sql4CdsException(svc.LastError, svc.LastException);
#else
            var svc = new CrmServiceClient(connectionString);

            if (!svc.IsReady)
                throw new Sql4CdsException(svc.LastCrmException.Message, svc.LastCrmException);
#endif

            return svc;
        }

        private static IDictionary<string, DataSource> BuildDataSources(IOrganizationService[] orgs)
        {
            var dataSources = new Dictionary<string, DataSource>(StringComparer.OrdinalIgnoreCase);

            foreach (var org in orgs)
            {
                var ds = new DataSource(org);
                dataSources[ds.Name] = ds;
            }

            return dataSources;
        }

        public event EventHandler<InfoMessageEventArgs> InfoMessage;

        internal void OnInfoMessage(IRootExecutionPlanNode node, Sql4CdsError message)
        {
            if (message.Class <= 10)
            {
                var handler = InfoMessage;

                if (handler != null)
                    handler(this, new InfoMessageEventArgs(node, message));
            }
            else
            {
                throw new Sql4CdsException(message);
            }
        }

        internal IDictionary<string, DataSource> DataSources => _dataSources;

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

        /// <summary>
        /// Returns or sets the name of the application using the connection
        /// </summary>
        public string ApplicationName { get; set; }

        /// <summary>
        /// Returns or sets how columns should be sorted within tables
        /// </summary>
        public ColumnOrdering ColumnOrdering
        {
            get => _options.ColumnOrdering;
            set => _options.ColumnOrdering = value;
        }

        internal Dictionary<string, DataTypeReference> GlobalVariableTypes => _globalVariableTypes;

        internal Dictionary<string, object> GlobalVariableValues => _globalVariableValues;

        internal TelemetryClient TelemetryClient => _ai;

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
                throw new Sql4CdsException(new Sql4CdsError(11, 0, 0, null, databaseName, 0, "Database is not in the list of connected databases", null));

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
            throw new Sql4CdsException(Sql4CdsError.NotSupported(null, "BEGIN TRAN"));
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
