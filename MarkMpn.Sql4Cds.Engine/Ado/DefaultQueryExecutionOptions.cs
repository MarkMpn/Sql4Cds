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
        public DefaultQueryExecutionOptions(DataSource dataSource, CancellationToken cancellationToken)
        {
            PrimaryDataSource = dataSource.Name;
            CancellationToken = cancellationToken;

#if NETCOREAPP
            if (dataSource.Connection is ServiceClient svc)
            {
                UserId = svc.GetMyUserId();
            }
#else
            if (dataSource.Connection is CrmServiceClient svc)
            {
                UserId = svc.GetMyCrmUserId();
            }
#endif
            else
            {
                var whoami = (WhoAmIResponse)dataSource.Connection.Execute(new WhoAmIRequest());
                UserId = whoami.UserId;
            }

        }

        public CancellationToken CancellationToken { get; }

        public bool BlockUpdateWithoutWhere => false;

        public bool BlockDeleteWithoutWhere => false;

        public bool UseBulkDelete => false;

        public int BatchSize => 100;

        public bool UseTDSEndpoint => true;

        public int MaxDegreeOfParallelism => 10;

        public bool ColumnComparisonAvailable { get; }

        public bool OrderByEntityNameAvailable { get; }

        public bool UseLocalTimeZone => false;

        public List<JoinOperator> JoinOperatorsAvailable { get; }

        public bool BypassCustomPlugins => false;

        public string PrimaryDataSource { get; }

        public Guid UserId { get; }

        public bool QuotedIdentifiers => true;

        public ColumnOrdering ColumnOrdering => ColumnOrdering.Strict;

        public void ConfirmDelete(ConfirmDmlStatementEventArgs e)
        {
        }

        public void ConfirmInsert(ConfirmDmlStatementEventArgs e)
        {
        }

        public void ConfirmUpdate(ConfirmDmlStatementEventArgs e)
        {
        }

        public bool ContinueRetrieve(int count)
        {
            return true;
        }

        public void Progress(double? progress, string message)
        {
        }

        public void RetrievingNextPage()
        {
        }
    }
}
