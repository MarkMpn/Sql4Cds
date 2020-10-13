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
        private readonly Settings _settings;

        public SettingsForm(Settings settings)
        {
            InitializeComponent();

            quotedIdentifiersCheckbox.Checked = settings.QuotedIdentifiers;
            selectLimitUpDown.Value = settings.SelectLimit;
            updateWarnThresholdUpDown.Value = settings.UpdateWarnThreshold;
            blockUpdateWithoutWhereCheckbox.Checked = settings.BlockUpdateWithoutWhere;
            deleteWarnThresholdUpDown.Value = settings.DeleteWarnThreshold;
            blockDeleteWithoutWhereCheckbox.Checked = settings.BlockDeleteWithoutWhere;
            batchSizeUpDown.Value = settings.BatchSize;
            bulkDeleteCheckbox.Checked = settings.UseBulkDelete;
            friendlyNamesComboBox.SelectedIndex = settings.ShowEntityReferenceNames ? 1 : 0;
            localTimesComboBox.SelectedIndex = settings.ShowLocalTimes ? 1 : 0;
            tsqlEndpointCheckBox.Checked = settings.UseTSQLEndpoint;
            retrieveTotalRecordCountCheckbox.Checked = settings.UseRetrieveTotalRecordCount;

            _settings = settings;
        }

        protected override void OnClosing(CancelEventArgs e)
        {
            base.OnClosing(e);

            if (!e.Cancel && DialogResult == DialogResult.OK)
            {
                _settings.QuotedIdentifiers = quotedIdentifiersCheckbox.Checked;
                _settings.SelectLimit = (int) selectLimitUpDown.Value;
                _settings.UpdateWarnThreshold = (int) updateWarnThresholdUpDown.Value;
                _settings.BlockUpdateWithoutWhere = blockUpdateWithoutWhereCheckbox.Checked;
                _settings.DeleteWarnThreshold = (int) deleteWarnThresholdUpDown.Value;
                _settings.BlockDeleteWithoutWhere = blockDeleteWithoutWhereCheckbox.Checked;
                _settings.BatchSize = (int) batchSizeUpDown.Value;
                _settings.UseBulkDelete = bulkDeleteCheckbox.Checked;
                _settings.ShowEntityReferenceNames = friendlyNamesComboBox.SelectedIndex == 1;
                _settings.ShowLocalTimes = localTimesComboBox.SelectedIndex == 1;
                _settings.UseTSQLEndpoint = tsqlEndpointCheckBox.Checked;
                _settings.UseRetrieveTotalRecordCount = retrieveTotalRecordCountCheckbox.Checked;
            }
        }
    }
}
