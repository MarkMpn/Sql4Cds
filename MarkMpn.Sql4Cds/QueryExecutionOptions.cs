using System.ComponentModel;
using System.Linq;
using System.Windows.Forms;
using MarkMpn.Sql4Cds.Engine;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Metadata;
using Microsoft.Xrm.Sdk.Query;

namespace MarkMpn.Sql4Cds
{
    class QueryExecutionOptions : IQueryExecutionOptions
    {
        private readonly IOrganizationService _org;
        private readonly BackgroundWorker _worker;
        private readonly Control _host;
        private int _localeId;

        public QueryExecutionOptions(IOrganizationService org, BackgroundWorker worker, Control host)
        {
            _org = org;
            _worker = worker;
            _host = host;
        }

        public bool Cancelled => _worker.CancellationPending;

        public bool BlockUpdateWithoutWhere => Settings.Instance.BlockUpdateWithoutWhere;

        public bool BlockDeleteWithoutWhere => Settings.Instance.BlockDeleteWithoutWhere;

        public bool UseBulkDelete => Settings.Instance.UseBulkDelete;

        public bool ConfirmUpdate(int count, EntityMetadata meta)
        {
            if (count > Settings.Instance.UpdateWarnThreshold)
            {
                var result = MessageBox.Show(_host, $"Update will affect {count:N0} {GetDisplayName(count, meta)}. Do you want to proceed?", "Confirm", MessageBoxButtons.YesNo, MessageBoxIcon.Warning, MessageBoxDefaultButton.Button2);

                if (result == DialogResult.No)
                    return false;
            }

            return true;
        }

        public bool ConfirmDelete(int count, EntityMetadata meta)
        {
            if (count > Settings.Instance.DeleteWarnThreshold)
            {
                var result = MessageBox.Show(_host, $"Delete will affect {count:N0} {GetDisplayName(count, meta)}. Do you want to proceed?", "Confirm", MessageBoxButtons.YesNo, MessageBoxIcon.Warning, MessageBoxDefaultButton.Button2);

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

        public bool UseTDSEndpoint => Settings.Instance.UseTSQLEndpoint;

        public bool UseRetrieveTotalRecordCount => Settings.Instance.UseRetrieveTotalRecordCount;

        public int LocaleId
        {
            get
            {
                if (_localeId != 0)
                    return _localeId;

                var qry = new QueryExpression("usersettings");
                qry.TopCount = 1;
                qry.ColumnSet = new ColumnSet("localeid");
                qry.Criteria.AddCondition("systemuserid", ConditionOperator.EqualUserId);
                var userLink = qry.AddLink("systemuser", "systemuserid", "systemuserid");
                var orgLink = userLink.AddLink("organization", "organizationid", "organizationid");
                orgLink.EntityAlias = "org";
                orgLink.Columns = new ColumnSet("localeid");
                var locale = _org.RetrieveMultiple(qry).Entities.Single();

                if (locale.Contains("localeid"))
                    _localeId = locale.GetAttributeValue<int>("localeid");
                else
                    _localeId = (int) locale.GetAttributeValue<AliasedValue>("org.localeid").Value;

                return _localeId;
            }
        }

        public int MaxDegreeOfParallelism => Settings.Instance.MaxDegreeOfPaallelism;

        public bool ColumnComparisonAvailable => true;
    }
}
