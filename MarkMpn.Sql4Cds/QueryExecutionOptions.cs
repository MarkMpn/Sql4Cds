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
    class QueryExecutionOptions
    {
        private readonly Control _host;
        private readonly BackgroundWorker _worker;
        private int _retrievedPages;

        public QueryExecutionOptions(Control host, BackgroundWorker worker)
        {
            _host = host;
            _worker = worker;
        }

        public void ApplySettings(Sql4CdsConnection con, Sql4CdsCommand cmd, bool execute)
        {
            con.BlockDeleteWithoutWhere = Settings.Instance.BlockDeleteWithoutWhere;
            con.BlockUpdateWithoutWhere = Settings.Instance.BlockUpdateWithoutWhere;
            con.UseBulkDelete = Settings.Instance.UseBulkDelete;
            con.BatchSize = Settings.Instance.BatchSize;
            con.UseTDSEndpoint = Settings.Instance.UseTSQLEndpoint && (execute || !Settings.Instance.ShowFetchXMLInEstimatedExecutionPlans);
            con.UseRetrieveTotalRecordCount = Settings.Instance.UseRetrieveTotalRecordCount;
            con.MaxDegreeOfParallelism = Settings.Instance.MaxDegreeOfPaallelism;
            con.UseLocalTimeZone = Settings.Instance.ShowLocalTimes;
            con.BypassCustomPlugins = Settings.Instance.BypassCustomPlugins;
            con.QuotedIdentifiers = Settings.Instance.QuotedIdentifiers;

            con.PreInsert += ConfirmInsert;
            con.PreUpdate += ConfirmUpdate;
            con.PreDelete += ConfirmDelete;
            con.PreRetrieve += ConfirmRetrieve;
            con.Progress += Progress;

            cmd.StatementCompleted += StatementCompleted;
        }

        private void ConfirmInsert(object sender, ConfirmDmlStatementEventArgs e)
        {
            e.Cancel |= !ConfirmInsert((Sql4CdsConnection)sender, e.Count, e.Metadata);
        }

        private bool ConfirmInsert(Sql4CdsConnection con, int count, EntityMetadata meta)
        {
            if (count > Settings.Instance.InsertWarnThreshold || con.BypassCustomPlugins)
            {
                var msg = $"Insert will affect {count:N0} {GetDisplayName(count, meta)}.";
                if (con.BypassCustomPlugins)
                    msg += "\r\n\r\nThis operation will bypass any custom plugins.";

                var result = MessageBox.Show(_host, msg + "\r\n\r\nDo you want to proceed?", "Confirm", MessageBoxButtons.YesNo, MessageBoxIcon.Warning, MessageBoxDefaultButton.Button2);

                if (result == DialogResult.No)
                    return false;
            }

            return true;
        }

        private void ConfirmUpdate(object sender, ConfirmDmlStatementEventArgs e)
        {
            e.Cancel |= !ConfirmUpdate((Sql4CdsConnection)sender, e.Count, e.Metadata);
        }

        private bool ConfirmUpdate(Sql4CdsConnection con, int count, EntityMetadata meta)
        {
            if (count > Settings.Instance.UpdateWarnThreshold || con.BypassCustomPlugins)
            {
                var msg = $"Update will affect {count:N0} {GetDisplayName(count, meta)}.";
                if (con.BypassCustomPlugins)
                    msg += "\r\n\r\nThis operation will bypass any custom plugins.";

                var result = MessageBox.Show(_host, msg + "\r\n\r\nDo you want to proceed?", "Confirm", MessageBoxButtons.YesNo, MessageBoxIcon.Warning, MessageBoxDefaultButton.Button2);

                if (result == DialogResult.No)
                    return false;
            }

            return true;
        }

        private void ConfirmDelete(object sender, ConfirmDmlStatementEventArgs e)
        {
            e.Cancel |= !ConfirmDelete((Sql4CdsConnection)sender, e.Count, e.Metadata);
        }

        private bool ConfirmDelete(Sql4CdsConnection con, int count, EntityMetadata meta)
        {
            if (count > Settings.Instance.DeleteWarnThreshold || con.BypassCustomPlugins)
            {
                var msg = $"Delete will affect {count:N0} {GetDisplayName(count, meta)}.";
                if (con.BypassCustomPlugins)
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
    }
}
