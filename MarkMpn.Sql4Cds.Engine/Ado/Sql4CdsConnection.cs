using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using Microsoft.Crm.Sdk.Messages;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using System.Threading;
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
        /// <param name="svc">The <see cref="IOrganizationService"/> to use</param>
        public Sql4CdsConnection(IOrganizationService svc) : this(BuildDataSources(svc))
        {
        }

        /// <summary>
        /// Creates a new <see cref="Sql4CdsConnection"/> using the specified list of data sources
        /// </summary>
        /// <param name="dataSources">The list of data sources to use</param>
        public Sql4CdsConnection(IReadOnlyList<DataSource> dataSources) : this(dataSources, null)
        {
        }

        /// <summary>
        /// Creates a new <see cref="Sql4CdsConnection"/> using the specified list of data sources
        /// </summary>
        /// <param name="dataSources">The list of data sources to use</param>
        /// <param name="options">The options to control how queries will be executed</param>
        public Sql4CdsConnection(IReadOnlyList<DataSource> dataSources, IQueryExecutionOptions options)
        {
            if (dataSources == null)
                throw new ArgumentNullException(nameof(dataSources));

            if (dataSources.Count == 0)
                throw new ArgumentOutOfRangeException("At least one data source must be supplied");

            if (options == null)
                options = new DefaultQueryExecutionOptions(dataSources[0], CancellationToken.None);

            _dataSources = dataSources.ToDictionary(ds => ds.Name, StringComparer.OrdinalIgnoreCase);
            _options = new ChangeDatabaseOptionsWrapper(options);
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

        private static IReadOnlyList<DataSource> BuildDataSources(IOrganizationService org)
        {
            var metadata = new AttributeMetadataCache(org);
            var name = "local";

#if NETCOREAPP
            if (org is ServiceClient svc)
                name = svc.ConnectedOrgUniqueName;
#else
            if (org is CrmServiceClient svc)
                name = svc.ConnectedOrgUniqueName;
#endif

            return new []
            {
                new DataSource
                {
                    Connection = org,
                    Metadata = metadata,
                    Name = name,
                    TableSizeCache = new TableSizeCache(org, metadata)
                }
            };
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

        public bool ReturnEntityReferenceAsGuid { get; set; }

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

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
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
