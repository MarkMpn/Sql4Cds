using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Threading;
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
    class QueryExecutionOptions : IDisposable
    {
        private readonly Control _host;
        private readonly BackgroundWorker _worker;
        private readonly Sql4CdsConnection _con;
        private readonly Sql4CdsCommand _cmd;
        private int _retrievedPages;

        public QueryExecutionOptions(Control host, BackgroundWorker worker, Sql4CdsConnection con, Sql4CdsCommand cmd)
        {
            _host = host;
            _worker = worker;
            _con = con;
            _cmd = cmd;
        }

        public void ApplySettings(bool execute)
        {
            _con.BlockDeleteWithoutWhere = Settings.Instance.BlockDeleteWithoutWhere;
            _con.BlockUpdateWithoutWhere = Settings.Instance.BlockUpdateWithoutWhere;
            _con.UseBulkDelete = Settings.Instance.UseBulkDelete;
            _con.BatchSize = Settings.Instance.BatchSize;
            _con.UseTDSEndpoint = Settings.Instance.UseTSQLEndpoint && (execute || !Settings.Instance.ShowFetchXMLInEstimatedExecutionPlans);
            _con.MaxDegreeOfParallelism = Settings.Instance.MaxDegreeOfPaallelism;
            _con.UseLocalTimeZone = Settings.Instance.ShowLocalTimes;
            _con.BypassCustomPlugins = Settings.Instance.BypassCustomPlugins;
            _con.QuotedIdentifiers = Settings.Instance.QuotedIdentifiers;
            _con.ColumnOrdering = Settings.Instance.ColumnOrdering;

            _con.PreInsert += ConfirmInsert;
            _con.PreUpdate += ConfirmUpdate;
            _con.PreDelete += ConfirmDelete;
            _con.PreRetrieve += ConfirmRetrieve;
            _con.Progress += Progress;

            _cmd.StatementCompleted += StatementCompleted;
        }

        private void ConfirmInsert(object sender, ConfirmDmlStatementEventArgs e)
        {
            e.Cancel |= !ConfirmInsert((Sql4CdsConnection)sender, e);
        }

        private bool ConfirmInsert(Sql4CdsConnection con, ConfirmDmlStatementEventArgs e)
        {
            if (e.Count > Settings.Instance.InsertWarnThreshold || e.BypassCustomPluginExecution)
            {
                var msg = $"Insert will affect {e.Count:N0} {GetDisplayName(e.Count, e.Metadata)}.";
                if (e.BypassCustomPluginExecution)
                    msg += "\r\n\r\nThis operation will bypass any custom plugins.";

                var result = MessageBox.Show(_host, msg + "\r\n\r\nDo you want to proceed?", "Confirm", MessageBoxButtons.YesNo, MessageBoxIcon.Warning, MessageBoxDefaultButton.Button2);

                if (result == DialogResult.No)
                    return false;
            }

            return true;
        }

        private void ConfirmUpdate(object sender, ConfirmDmlStatementEventArgs e)
        {
            e.Cancel |= !ConfirmUpdate((Sql4CdsConnection)sender, e);
        }

        private bool ConfirmUpdate(Sql4CdsConnection con, ConfirmDmlStatementEventArgs e)
        {
            if (e.Count > Settings.Instance.UpdateWarnThreshold || e.BypassCustomPluginExecution)
            {
                var msg = $"Update will affect {e.Count:N0} {GetDisplayName(e.Count, e.Metadata)}.";
                if (e.BypassCustomPluginExecution)
                    msg += "\r\n\r\nThis operation will bypass any custom plugins.";

                var result = MessageBox.Show(_host, msg + "\r\n\r\nDo you want to proceed?", "Confirm", MessageBoxButtons.YesNo, MessageBoxIcon.Warning, MessageBoxDefaultButton.Button2);

                if (result == DialogResult.No)
                    return false;
            }

            return true;
        }

        private void ConfirmDelete(object sender, ConfirmDmlStatementEventArgs e)
        {
            e.Cancel |= !ConfirmDelete((Sql4CdsConnection)sender, e);
        }

        private bool ConfirmDelete(Sql4CdsConnection con, ConfirmDmlStatementEventArgs e)
        {
            if (e.Count > Settings.Instance.DeleteWarnThreshold || e.BypassCustomPluginExecution)
            {
                var msg = $"Delete will affect {e.Count:N0} {GetDisplayName(e.Count, e.Metadata)}.";
                if (e.BypassCustomPluginExecution)
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

        private void ConfirmRetrieve(object sender, ConfirmRetrieveEventArgs e)
        {
            e.Cancel |= !ContinueRetrieve(e.Count);

            if (!e.Cancel)
                RetrievingNextPage();
        }

        private bool ContinueRetrieve(int count)
        {
            return Settings.Instance.SelectLimit == 0 || Settings.Instance.SelectLimit > count;
        }

        private void RetrievingNextPage()
        {
            _retrievedPages++;

            if (Settings.Instance.MaxRetrievesPerQuery != 0 && _retrievedPages > Settings.Instance.MaxRetrievesPerQuery)
                throw new QueryExecutionException($"Hit maximum retrieval limit. This limit is in place to protect against excessive API requests. Try restricting the data to retrieve with WHERE clauses or eliminating subqueries.\r\nYour limit of {Settings.Instance.MaxRetrievesPerQuery:N0} retrievals per query can be modified in Settings.");
        }

        private void StatementCompleted(object sender, StatementCompletedEventArgs e)
        {
            _retrievedPages = 0;
        }

        private void Progress(object sender, ProgressEventArgs e)
        {
            Progress(e.Progress, e.Message);
        }

        private void Progress(double? progress, string message)
        {
            _worker.ReportProgress(progress == null ? -1 : (int)(progress * 100), message);
        }

        public void Dispose()
        {
            _con.PreInsert -= ConfirmInsert;
            _con.PreUpdate -= ConfirmUpdate;
            _con.PreDelete -= ConfirmDelete;
            _con.PreRetrieve -= ConfirmRetrieve;
            _con.Progress -= Progress;

            _cmd.StatementCompleted -= StatementCompleted;
        }
    }
}
