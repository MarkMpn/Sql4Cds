using System;
using System.Collections.Generic;
using System.Text;
#if NETCOREAPP
using Microsoft.PowerPlatform.Dataverse.Client;
using Microsoft.PowerPlatform.Dataverse.Client.Extensions;
#else
using Microsoft.Xrm.Tooling.Connector;
#endif
using Microsoft.Xrm.Sdk.Metadata;
using Microsoft.Xrm.Sdk.Query;
using Microsoft.Crm.Sdk.Messages;
using System.Threading;

namespace MarkMpn.Sql4Cds.Engine
{
    class DefaultQueryExecutionOptions : IQueryExecutionOptions
    {
        private readonly Sql4CdsConnection _connection;
        private string _primaryDataSource;
        private Guid? _userId;

        public DefaultQueryExecutionOptions(Sql4CdsConnection connection, DataSource dataSource, CancellationToken cancellationToken)
        {
            _connection = connection;
            _primaryDataSource = dataSource.Name;
            CancellationToken = cancellationToken;

            PrimaryDataSourceChanged += (_, __) => _userId = null;
        }

        public CancellationToken CancellationToken { get; }

        public bool BlockUpdateWithoutWhere { get; set; }

        public bool BlockDeleteWithoutWhere { get; set; }

        public bool UseBulkDelete { get; set; }

        public int BatchSize { get; set; } = 100;

        public bool UseTDSEndpoint { get; set; } = true;

        public int MaxDegreeOfParallelism { get; set; } = 10;

        public bool ColumnComparisonAvailable { get; }

        public bool OrderByEntityNameAvailable { get; }

        public bool UseLocalTimeZone { get; set; }

        public List<JoinOperator> JoinOperatorsAvailable { get; }

        public bool BypassCustomPlugins { get; set; }

        public string PrimaryDataSource
        {
            get => _primaryDataSource;
            set
            {
                if (_primaryDataSource != value)
                {
                    _primaryDataSource = value;
                    OnPrimaryDataSourceChanged();
                }
            }
        }

        public Guid UserId
        {
            get
            {
                if (_userId == null)
                {
#if NETCOREAPP
                    if (_connection.Session.DataSources[PrimaryDataSource].Connection is ServiceClient svc)
                    {
                        _userId = svc.GetMyUserId();
                    }
#else
                    if (_connection.Session.DataSources[PrimaryDataSource].Connection is CrmServiceClient svc)
                    {
                        _userId = svc.GetMyCrmUserId();
                    }
#endif
                    else
                    {
                        var whoami = (WhoAmIResponse)_connection.Session.DataSources[PrimaryDataSource].Connection.Execute(new WhoAmIRequest());
                        _userId = whoami.UserId;
                    }
                }

                return _userId.Value;
            }
        }

        public bool QuotedIdentifiers { get; set; } = true;

        public ColumnOrdering ColumnOrdering { get; set; }

        public event EventHandler PrimaryDataSourceChanged;

        public void ConfirmDelete(ConfirmDmlStatementEventArgs e)
        {
            if (!e.Cancel)
                _connection.OnPreDelete(e);
        }

        public void ConfirmInsert(ConfirmDmlStatementEventArgs e)
        {
            if (!e.Cancel)
                _connection.OnPreInsert(e);
        }

        public void ConfirmUpdate(ConfirmDmlStatementEventArgs e)
        {
            if (!e.Cancel)
                _connection.OnPreUpdate(e);
        }

        public bool ContinueRetrieve(int count)
        {
            var args = new ConfirmRetrieveEventArgs(count);
            _connection.OnPreRetrieve(args);

            var cancelled = args.Cancel;

            return !cancelled;
        }

        public void Progress(double? progress, string message)
        {
            _connection.OnProgress(new ProgressEventArgs(progress, message));
        }

        protected virtual void OnPrimaryDataSourceChanged()
        {
            PrimaryDataSourceChanged?.Invoke(this, EventArgs.Empty);
        }
    }
}
