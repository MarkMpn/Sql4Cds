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
            retriveLimitUpDown.Value = settings.MaxRetrievesPerQuery;
            updateWarnThresholdUpDown.Value = settings.UpdateWarnThreshold;
            blockUpdateWithoutWhereCheckbox.Checked = settings.BlockUpdateWithoutWhere;
            deleteWarnThresholdUpDown.Value = settings.DeleteWarnThreshold;
            blockDeleteWithoutWhereCheckbox.Checked = settings.BlockDeleteWithoutWhere;
            batchSizeUpDown.Value = settings.BatchSize;
            bulkDeleteCheckbox.Checked = settings.UseBulkDelete;
            bypassCustomPluginsCheckBox.Checked = settings.BypassCustomPlugins;
            localTimesComboBox.SelectedIndex = settings.ShowLocalTimes ? 1 : 0;
            tsqlEndpointCheckBox.Checked = settings.UseTSQLEndpoint;
            retrieveTotalRecordCountCheckbox.Checked = settings.UseRetrieveTotalRecordCount;
            showTooltipsCheckbox.Checked = settings.ShowIntellisenseTooltips;
            maxDopUpDown.Value = settings.MaxDegreeOfPaallelism;
            autoSizeColumnsCheckBox.Checked = settings.AutoSizeColumns;

            _settings = settings;
        }

        protected override void OnClosing(CancelEventArgs e)
        {
            base.OnClosing(e);

            if (!e.Cancel && DialogResult == DialogResult.OK)
            {
                _settings.QuotedIdentifiers = quotedIdentifiersCheckbox.Checked;
                _settings.SelectLimit = (int) selectLimitUpDown.Value;
                _settings.MaxRetrievesPerQuery = (int) retriveLimitUpDown.Value;
                _settings.UpdateWarnThreshold = (int) updateWarnThresholdUpDown.Value;
                _settings.BlockUpdateWithoutWhere = blockUpdateWithoutWhereCheckbox.Checked;
                _settings.DeleteWarnThreshold = (int) deleteWarnThresholdUpDown.Value;
                _settings.BlockDeleteWithoutWhere = blockDeleteWithoutWhereCheckbox.Checked;
                _settings.BatchSize = (int) batchSizeUpDown.Value;
                _settings.UseBulkDelete = bulkDeleteCheckbox.Checked;
                _settings.BypassCustomPlugins = bypassCustomPluginsCheckBox.Checked;
                _settings.ShowLocalTimes = localTimesComboBox.SelectedIndex == 1;
                _settings.UseTSQLEndpoint = tsqlEndpointCheckBox.Checked;
                _settings.UseRetrieveTotalRecordCount = retrieveTotalRecordCountCheckbox.Checked;
                _settings.ShowIntellisenseTooltips = showTooltipsCheckbox.Checked;
                _settings.MaxDegreeOfPaallelism = (int) maxDopUpDown.Value;
                _settings.AutoSizeColumns = autoSizeColumnsCheckBox.Checked;
            }
        }
    }
}
