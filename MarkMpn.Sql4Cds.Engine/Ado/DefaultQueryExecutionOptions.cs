using System;
using System.Collections.Generic;
using System.Text;
#if NETCOREAPP
using Microsoft.PowerPlatform.Dataverse.Client;
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

            Version version;

#if NETCOREAPP
            if (dataSource.Connection is ServiceClient svc)
            {
                UserId = svc.GetMyUserId();
                version = svc.ConnectedOrgVersion;
            }
#else
            if (dataSource.Connection is CrmServiceClient svc)
            {
                UserId = svc.GetMyCrmUserId();
                version = svc.ConnectedOrgVersion;
            }
#endif
            else
            {
                var whoami = (WhoAmIResponse)dataSource.Connection.Execute(new WhoAmIRequest());
                UserId = whoami.UserId;

                var ver = (RetrieveVersionResponse)dataSource.Connection.Execute(new RetrieveVersionRequest());
                version = new Version(ver.Version);
            }

            var joinOperators = new List<JoinOperator>
            {
                JoinOperator.Inner,
                JoinOperator.LeftOuter
            };

            if (version >= new Version("9.1.0.17461"))
            {
                // First documented in SDK Version 9.0.2.25: Updated for 9.1.0.17461 CDS release
                joinOperators.Add(JoinOperator.Any);
                joinOperators.Add(JoinOperator.Exists);
            }

            JoinOperatorsAvailable = joinOperators;
            ColumnComparisonAvailable = version >= new Version("9.1.0.19251");
        }

        public CancellationToken CancellationToken { get; }

        public bool BlockUpdateWithoutWhere => false;

        public bool BlockDeleteWithoutWhere => false;

        public bool UseBulkDelete => false;

        public int BatchSize => 100;

        public bool UseTDSEndpoint => true;

        public bool UseRetrieveTotalRecordCount => false;

        public int MaxDegreeOfParallelism => 10;

        public bool ColumnComparisonAvailable { get; }

        public bool UseLocalTimeZone => false;

        public List<JoinOperator> JoinOperatorsAvailable { get; }

        public bool BypassCustomPlugins => false;

        public string PrimaryDataSource { get; }

        public Guid UserId { get; }

        public bool QuotedIdentifiers => true;

        public bool ConfirmDelete(int count, EntityMetadata meta)
        {
            return true;
        }

        public bool ConfirmInsert(int count, EntityMetadata meta)
        {
            return true;
        }

        public bool ConfirmUpdate(int count, EntityMetadata meta)
        {
            return true;
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
