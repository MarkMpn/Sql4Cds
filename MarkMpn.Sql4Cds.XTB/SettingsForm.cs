using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using MarkMpn.Sql4Cds.Engine;
using ScintillaNET;

namespace MarkMpn.Sql4Cds.XTB
{
    public partial class SettingsForm : Form
    {
        private readonly Settings _settings;
        private readonly FetchXml2SqlOptions _fetchXml2SqlOptions;
        private readonly PluginControl _pluginControl;

        public SettingsForm(Settings settings, PluginControl plugin)
        {
            InitializeComponent();

            quotedIdentifiersCheckbox.Checked = settings.QuotedIdentifiers;
            selectLimitUpDown.Value = settings.SelectLimit;
            retriveLimitUpDown.Value = settings.MaxRetrievesPerQuery;
            insertWarnThresholdUpDown.Value = settings.InsertWarnThreshold;
            updateWarnThresholdUpDown.Value = settings.UpdateWarnThreshold;
            blockUpdateWithoutWhereCheckbox.Checked = settings.BlockUpdateWithoutWhere;
            deleteWarnThresholdUpDown.Value = settings.DeleteWarnThreshold;
            blockDeleteWithoutWhereCheckbox.Checked = settings.BlockDeleteWithoutWhere;
            batchSizeUpDown.Value = settings.BatchSize;
            bulkDeleteCheckbox.Checked = settings.UseBulkDelete;
            bypassCustomPluginsCheckBox.Checked = settings.BypassCustomPlugins;
            localTimesComboBox.SelectedIndex = settings.ShowLocalTimes ? 1 : 0;
            tsqlEndpointCheckBox.Checked = settings.UseTSQLEndpoint;
            showFetchXMLInEstimatedExecutionPlansCheckBox.Checked = settings.ShowFetchXMLInEstimatedExecutionPlans;
            showTooltipsCheckbox.Checked = settings.ShowIntellisenseTooltips;
            maxDopUpDown.Value = settings.MaxDegreeOfPaallelism;
            autoSizeColumnsCheckBox.Checked = settings.AutoSizeColumns;
            rememberSessionCheckbox.Checked = settings.RememberSession;
            localDateFormatCheckbox.Checked = settings.LocalFormatDates;
            simpleSqlRadioButton.Checked = !settings.UseNativeSqlConversion;
            nativeSqlRadioButton.Checked = settings.UseNativeSqlConversion;
            schemaColumnOrderingCheckbox.Checked = settings.ColumnOrdering == ColumnOrdering.Strict;
            openAiEndpointTextBox.Text = settings.OpenAIEndpoint;
            openAiKeyTextBox.Text = settings.OpenAIKey;
            assistantIdTextBox.Text = settings.AssistantID;

            SetSqlStyle(simpleSqlScintilla);
            SetSqlStyle(nativeSqlScintilla);

            _settings = settings;
            _fetchXml2SqlOptions = new FetchXml2SqlOptions
            {
                ConvertDateTimeToUtc = _settings.FetchXml2SqlOptions.ConvertDateTimeToUtc,
                ConvertFetchXmlOperatorsTo = _settings.FetchXml2SqlOptions.ConvertFetchXmlOperatorsTo,
                UseParametersForLiterals = _settings.FetchXml2SqlOptions.UseParametersForLiterals
            };
            _pluginControl = plugin;
        }

        private void SetSqlStyle(Scintilla scintilla)
        {
            scintilla.StyleResetDefault();
            scintilla.Styles[Style.Default].Font = "Courier New";
            scintilla.Styles[Style.Default].Size = 10;
            scintilla.StyleClearAll();

            scintilla.Lexer = Lexer.Sql;

            // Set the Styles
            scintilla.Styles[Style.LineNumber].ForeColor = Color.FromArgb(255, 128, 128, 128);  //Dark Gray
            scintilla.Styles[Style.LineNumber].BackColor = Color.FromArgb(255, 228, 228, 228);  //Light Gray
            scintilla.Styles[Style.Sql.Comment].ForeColor = Color.Green;
            scintilla.Styles[Style.Sql.CommentLine].ForeColor = Color.Green;
            scintilla.Styles[Style.Sql.CommentLineDoc].ForeColor = Color.Green;
            scintilla.Styles[Style.Sql.Number].ForeColor = Color.Maroon;
            scintilla.Styles[Style.Sql.Word].ForeColor = Color.Blue;
            scintilla.Styles[Style.Sql.Word2].ForeColor = Color.Fuchsia;
            scintilla.Styles[Style.Sql.User1].ForeColor = Color.Gray;
            scintilla.Styles[Style.Sql.User2].ForeColor = Color.FromArgb(255, 00, 128, 192);    //Medium Blue-Green
            scintilla.Styles[Style.Sql.String].ForeColor = Color.Red;
            scintilla.Styles[Style.Sql.Character].ForeColor = Color.Red;
            scintilla.Styles[Style.Sql.Operator].ForeColor = Color.Black;

            // Set keyword lists
            // Word = 0
            scintilla.SetKeywords(0, @"add alter as authorization backup begin bigint binary bit break browse bulk by cascade case catch check checkpoint close clustered column commit compute constraint containstable continue create current cursor cursor database date datetime datetime2 datetimeoffset dbcc deallocate decimal declare default delete deny desc disk distinct distributed double drop dump else end errlvl escape except exec execute exit external fetch file fillfactor float for foreign freetext freetexttable from full function goto grant group having hierarchyid holdlock identity identity_insert identitycol if image index insert int intersect into key kill lineno load merge money national nchar nocheck nocount nolock nonclustered ntext numeric nvarchar of off offsets on open opendatasource openquery openrowset openxml option order over percent plan precision primary print proc procedure public raiserror read readtext real reconfigure references replication restore restrict return revert revoke rollback rowcount rowguidcol rule save schema securityaudit select set setuser shutdown smalldatetime smallint smallmoney sql_variant statistics table table tablesample text textsize then time timestamp tinyint to top tran transaction trigger truncate try union unique uniqueidentifier update updatetext use user values varbinary varchar varying view waitfor when where while with writetext xml go ");
            // Word2 = 1
            scintilla.SetKeywords(1, @"ascii cast char charindex ceiling coalesce collate contains convert current_date current_time current_timestamp current_user floor isnull max min nullif object_id session_user substring system_user tsequal ");
            // User1 = 4
            scintilla.SetKeywords(4, @"all and any between cross exists in inner is join left like not null or outer pivot right some unpivot ( ) * ");
            // User2 = 5
            scintilla.SetKeywords(5, @"sys objects sysobjects ");

            scintilla.ReadOnly = true;
        }

        protected override void OnClosing(CancelEventArgs e)
        {
            base.OnClosing(e);

            if (!e.Cancel && DialogResult == DialogResult.OK)
            {
                _settings.QuotedIdentifiers = quotedIdentifiersCheckbox.Checked;
                _settings.SelectLimit = (int) selectLimitUpDown.Value;
                _settings.MaxRetrievesPerQuery = (int) retriveLimitUpDown.Value;
                _settings.InsertWarnThreshold = (int) insertWarnThresholdUpDown.Value;
                _settings.UpdateWarnThreshold = (int) updateWarnThresholdUpDown.Value;
                _settings.BlockUpdateWithoutWhere = blockUpdateWithoutWhereCheckbox.Checked;
                _settings.DeleteWarnThreshold = (int) deleteWarnThresholdUpDown.Value;
                _settings.BlockDeleteWithoutWhere = blockDeleteWithoutWhereCheckbox.Checked;
                _settings.BatchSize = (int) batchSizeUpDown.Value;
                _settings.UseBulkDelete = bulkDeleteCheckbox.Checked;
                _settings.BypassCustomPlugins = bypassCustomPluginsCheckBox.Checked;
                _settings.ShowLocalTimes = localTimesComboBox.SelectedIndex == 1;
                _settings.UseTSQLEndpoint = tsqlEndpointCheckBox.Checked;
                _settings.ShowFetchXMLInEstimatedExecutionPlans = showFetchXMLInEstimatedExecutionPlansCheckBox.Checked;
                _settings.ShowIntellisenseTooltips = showTooltipsCheckbox.Checked;
                _settings.MaxDegreeOfPaallelism = (int) maxDopUpDown.Value;
                _settings.AutoSizeColumns = autoSizeColumnsCheckBox.Checked;
                _settings.RememberSession = rememberSessionCheckbox.Checked;
                _settings.LocalFormatDates = localDateFormatCheckbox.Checked;
                _settings.UseNativeSqlConversion = nativeSqlRadioButton.Checked;
                _settings.FetchXml2SqlOptions = _fetchXml2SqlOptions;
                _settings.ColumnOrdering = schemaColumnOrderingCheckbox.Checked ? ColumnOrdering.Strict : ColumnOrdering.Alphabetical;
                _settings.OpenAIEndpoint = openAiEndpointTextBox.Text;
                _settings.OpenAIKey = openAiKeyTextBox.Text;
                _settings.AssistantID = assistantIdTextBox.Text;
            }
        }

        private void helpIcon_Click(object sender, EventArgs e)
        {
            var url = (string)((Control)sender).Tag;
            const string token = "WT.mc_id=DX-MVP-5004203";

            var anchor = url.IndexOf('#');
            var query = url.IndexOf('?');

            if (anchor == -1)
            {
                if (query == -1)
                    url += "?" + token;
                else
                    url += "&" + token;
            }
            else
            {
                if (query == -1)
                    url = url.Insert(anchor, "?" + token);
                else
                    url = url.Insert(anchor, "&" + token);
            }

            System.Diagnostics.Process.Start(url);
        }

        private void tsqlEndpointCheckBox_CheckedChanged(object sender, EventArgs e)
        {
            showFetchXMLInEstimatedExecutionPlansCheckBox.Enabled = tsqlEndpointCheckBox.Checked;

            if (!tsqlEndpointCheckBox.Checked)
                showFetchXMLInEstimatedExecutionPlansCheckBox.Checked = true;
        }

        private void fetchXml2SqlConversionAdvancedLinkLabel_LinkClicked(object sender, LinkLabelLinkClickedEventArgs e)
        {
            using (var form = new FetchXml2SqlSettingsForm(_fetchXml2SqlOptions))
            {
                form.ShowDialog(this);
            }
        }

        private void resetToolWindowsButton_Click(object sender, EventArgs e)
        {
            _pluginControl.ResetDockLayout();
        }
    }
}
