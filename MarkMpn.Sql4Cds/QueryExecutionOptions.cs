using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Windows.Forms;
using MarkMpn.Sql4Cds.Engine;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using McTools.Xrm.Connection;
using Microsoft.Crm.Sdk.Messages;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Metadata;
using Microsoft.Xrm.Sdk.Query;
using Microsoft.Xrm.Tooling.Connector;

namespace MarkMpn.Sql4Cds
{
    class QueryExecutionOptions : IQueryExecutionOptions
    {
        private readonly ConnectionDetail _con;
        private readonly IOrganizationService _org;
        private readonly BackgroundWorker _worker;
        private readonly Control _host;
        private readonly List<JoinOperator> _joinOperators;
        private int _localeId;
        private int _retrievedPages;
        private Guid? _userId;

        public QueryExecutionOptions(ConnectionDetail con, IOrganizationService org, BackgroundWorker worker, Control host)
        {
            _con = con;
            _org = org;
            _worker = worker;
            _host = host;
            _joinOperators = new List<JoinOperator>
            {
                JoinOperator.Inner,
                JoinOperator.LeftOuter
            };

            if (new Version(con.OrganizationVersion) >= new Version("9.1.0.17461"))
            {
                // First documented in SDK Version 9.0.2.25: Updated for 9.1.0.17461 CDS release
                _joinOperators.Add(JoinOperator.Any);
                _joinOperators.Add(JoinOperator.Exists);
            }

            UseTDSEndpoint = Settings.Instance.UseTSQLEndpoint;
        }

        public bool Cancelled => _worker.CancellationPending;

        public bool BlockUpdateWithoutWhere => Settings.Instance.BlockUpdateWithoutWhere;

        public bool BlockDeleteWithoutWhere => Settings.Instance.BlockDeleteWithoutWhere;

        public bool UseBulkDelete => Settings.Instance.UseBulkDelete;

        public bool ConfirmInsert(int count, EntityMetadata meta)
        {
            if (count > Settings.Instance.InsertWarnThreshold || BypassCustomPlugins)
            {
                var msg = $"Insert will affect {count:N0} {GetDisplayName(count, meta)}.";
                if (BypassCustomPlugins)
                    msg += "\r\n\r\nThis operation will bypass any custom plugins.";

                var result = MessageBox.Show(_host, msg + "\r\n\r\nDo you want to proceed?", "Confirm", MessageBoxButtons.YesNo, MessageBoxIcon.Warning, MessageBoxDefaultButton.Button2);

                if (result == DialogResult.No)
                    return false;
            }

            return true;
        }

        public bool ConfirmUpdate(int count, EntityMetadata meta)
        {
            if (count > Settings.Instance.UpdateWarnThreshold || BypassCustomPlugins)
            {
                var msg = $"Update will affect {count:N0} {GetDisplayName(count, meta)}.";
                if (BypassCustomPlugins)
                    msg += "\r\n\r\nThis operation will bypass any custom plugins.";

                var result = MessageBox.Show(_host, msg + "\r\n\r\nDo you want to proceed?", "Confirm", MessageBoxButtons.YesNo, MessageBoxIcon.Warning, MessageBoxDefaultButton.Button2);

                if (result == DialogResult.No)
                    return false;
            }

            return true;
        }

        public bool ConfirmDelete(int count, EntityMetadata meta)
        {
            if (count > Settings.Instance.DeleteWarnThreshold || BypassCustomPlugins)
            {
                var msg = $"Delete will affect {count:N0} {GetDisplayName(count, meta)}.";
                if (BypassCustomPlugins)
                    msg += "\r\n\r\nThis operation will bypass any custom plugins.";

                var result = MessageBox.Show(_host, msg + "\r\n\r\nDo you want to proceed?", "Confirm", MessageBoxButtons.YesNo, MessageBoxIcon.Warning, MessageBoxDefaultButton.Button2);

                if (result == DialogResult.No)
                    return false;
            }

            return true;
        }

        private string GetDisplayName(int count, EntityMetadata meta)
        {
            if (count == 1)
                return meta.DisplayName.UserLocalizedLabel?.Label ?? meta.LogicalName;

            return meta.DisplayCollectionName.UserLocalizedLabel?.Label ??
                meta.LogicalCollectionName ??
                meta.LogicalName;
        }

        public bool ContinueRetrieve(int count)
        {
            return Settings.Instance.SelectLimit == 0 || Settings.Instance.SelectLimit > count;
        }

        public void Progress(double? progress, string message)
        {
            _worker.ReportProgress(progress == null ? -1 : (int) (progress * 100), message);
        }

        public int BatchSize => Settings.Instance.BatchSize;

        public bool UseTDSEndpoint { get; set; }

        public bool UseRetrieveTotalRecordCount => Settings.Instance.UseRetrieveTotalRecordCount;

        public int MaxDegreeOfParallelism => Settings.Instance.MaxDegreeOfPaallelism;

        public bool ColumnComparisonAvailable => new Version(_con.OrganizationVersion) >= new Version("9.1.0.19251");

        public bool UseLocalTimeZone => Settings.Instance.ShowLocalTimes;

        public List<JoinOperator> JoinOperatorsAvailable => _joinOperators;

        public bool BypassCustomPlugins => Settings.Instance.BypassCustomPlugins;

        public void RetrievingNextPage()
        {
            _retrievedPages++;

            if (Settings.Instance.MaxRetrievesPerQuery != 0 && _retrievedPages > Settings.Instance.MaxRetrievesPerQuery)
                throw new QueryExecutionException($"Hit maximum retrieval limit. This limit is in place to protect against excessive API requests. Try restricting the data to retrieve with WHERE clauses or eliminating subqueries.\r\nYour limit of {Settings.Instance.MaxRetrievesPerQuery:N0} retrievals per query can be modified in Settings.");
        }

        public string PrimaryDataSource => _con.ConnectionName;

        public Guid UserId
        {
            get
            {
                if (_userId != null)
                    return _userId.Value;

                if (_org is CrmServiceClient svc && svc.CallerId != Guid.Empty)
                    _userId = svc.CallerId;
                else
                    _userId = ((WhoAmIResponse)_org.Execute(new WhoAmIRequest())).UserId;

                return _userId.Value;
            }
        }

        public void SyncUserId()
        {
            _userId = null;
        }
    }
}
