using MarkMpn.Sql4Cds.Engine;
using Microsoft.Xrm.Sdk.Metadata;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace MarkMpn.Sql4Cds
{
    class QueryExecutionOptions : IQueryExecutionOptions
    {
        private readonly BackgroundWorker _worker;

        public QueryExecutionOptions(BackgroundWorker worker)
        {
            _worker = worker;
        }

        public bool Cancelled => _worker.CancellationPending;

        public bool BlockUpdateWithoutWhere => Settings.Instance.BlockUpdateWithoutWhere;

        public bool BlockDeleteWithoutWhere => Settings.Instance.BlockDeleteWithoutWhere;

        public bool UseBulkDelete => Settings.Instance.UseBulkDelete;

        public bool ConfirmUpdate(int count, EntityMetadata meta)
        {
            if (count > Settings.Instance.UpdateWarnThreshold)
            {
                var result = MessageBox.Show($"Update will affect {count:N0} {meta.DisplayCollectionName.UserLocalizedLabel.Label}. Do you want to proceed?", "Confirm", MessageBoxButtons.YesNo, MessageBoxIcon.Warning, MessageBoxDefaultButton.Button2);

                if (result == DialogResult.No)
                    return false;
            }

            return true;
        }

        public bool ConfirmDelete(int count, EntityMetadata meta)
        {
            if (count > Settings.Instance.DeleteWarnThreshold)
            {
                var result = MessageBox.Show($"Delete will affect {count:N0} {meta.DisplayCollectionName.UserLocalizedLabel.Label}. Do you want to proceed?", "Confirm", MessageBoxButtons.YesNo, MessageBoxIcon.Warning, MessageBoxDefaultButton.Button2);

                if (result == DialogResult.No)
                    return false;
            }

            return true;
        }

        public bool ContinueRetrieve(int count)
        {
            return Settings.Instance.SelectLimit == 0 || Settings.Instance.SelectLimit > count;
        }

        public void Progress(string message)
        {
            _worker.ReportProgress(-1, message);
        }

        public int BatchSize => Settings.Instance.BatchSize;
    }
}
