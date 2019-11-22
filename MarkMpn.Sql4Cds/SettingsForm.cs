using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace MarkMpn.Sql4Cds
{
    public partial class SettingsForm : Form
    {
        private Settings _settings;

        public SettingsForm(Settings settings)
        {
            InitializeComponent();

            selectLimitUpDown.Value = settings.SelectLimit;
            updateWarnThresholdUpDown.Value = settings.UpdateWarnThreshold;
            blockUpdateWithoutWhereCheckbox.Checked = settings.BlockUpdateWithoutWhere;
            deleteWarnThresholdUpDown.Value = settings.DeleteWarnThreshold;
            blockDeleteWithoutWhereCheckbox.Checked = settings.BlockDeleteWithoutWhere;
            batchSizeUpDown.Value = settings.BatchSize;
            bulkDeleteCheckbox.Checked = settings.UseBulkDelete;

            _settings = settings;
        }

        protected override void OnClosing(CancelEventArgs e)
        {
            base.OnClosing(e);

            if (!e.Cancel && DialogResult == DialogResult.OK)
            {
                _settings.SelectLimit = (int) selectLimitUpDown.Value;
                _settings.UpdateWarnThreshold = (int) updateWarnThresholdUpDown.Value;
                _settings.BlockUpdateWithoutWhere = blockUpdateWithoutWhereCheckbox.Checked;
                _settings.DeleteWarnThreshold = (int) deleteWarnThresholdUpDown.Value;
                _settings.BlockDeleteWithoutWhere = blockDeleteWithoutWhereCheckbox.Checked;
                _settings.BatchSize = (int) batchSizeUpDown.Value;
                _settings.UseBulkDelete = bulkDeleteCheckbox.Checked;
            }
        }
    }
}
