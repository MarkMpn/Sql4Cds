using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Diagnostics;
using System.IO;
using System.Windows.Forms;
using System.Xml.Serialization;
using MarkMpn.Sql4Cds.Engine;
using McTools.Xrm.Connection;
using Microsoft.ApplicationInsights;
using WeifenLuo.WinFormsUI.Docking;
using XrmToolBox.Extensibility;
using XrmToolBox.Extensibility.Interfaces;

namespace MarkMpn.Sql4Cds
{
    public partial class PluginControl : MultipleConnectionsPluginControlBase, IMessageBusHost, IGitHubPlugin, IHelpPlugin
    {
        private readonly IDictionary<ConnectionDetail, AttributeMetadataCache> _metadata;
        private readonly TelemetryClient _ai;
        private readonly ObjectExplorer _objectExplorer;

        public PluginControl()
        {
            InitializeComponent();
            dockPanel.Theme = new VS2015LightTheme();
            _metadata = new Dictionary<ConnectionDetail, AttributeMetadataCache>();
            _objectExplorer = new ObjectExplorer(_metadata, WorkAsync, con => CreateQuery(con, "", null));
            _objectExplorer.Show(dockPanel, DockState.DockLeft);
            _objectExplorer.CloseButtonVisible = false;
            _ai = new TelemetryClient(new Microsoft.ApplicationInsights.Extensibility.TelemetryConfiguration("79761278-a908-4575-afbf-2f4d82560da6"));

            TabIcon = Properties.Resources.SQL4CDS_Icon_16;
            PluginIcon = System.Drawing.Icon.FromHandle(Properties.Resources.SQL4CDS_Icon_16.GetHicon());
        }

        protected override void OnConnectionUpdated(ConnectionUpdatedEventArgs e)
        {
            AddConnection(e.ConnectionDetail);

            if (dockPanel.ActiveDocument == null)
                tsbNewQuery_Click(this, EventArgs.Empty);
            
            base.OnConnectionUpdated(e);
        }

        protected override void ConnectionDetailsUpdated(NotifyCollectionChangedEventArgs e)
        {
            if (e.Action == NotifyCollectionChangedAction.Add)
            {
                foreach (var con in e.NewItems)
                    AddConnection((ConnectionDetail) con);
            }
        }

        private void AddConnection(ConnectionDetail con)
        {
            _metadata[con] = new AttributeMetadataCache(con.ServiceClient);
            _objectExplorer.AddConnection(con);

            // Start loading the entity list in the background
            EntityCache.TryGetEntities(con.ServiceClient, out _);
        }

        private void PluginControl_Load(object sender, EventArgs e)
        {
            // Loads or creates the settings for the plugin
            if (!SettingsManager.Instance.TryLoad(GetType(), out Settings settings))
                settings = new Settings();

            Settings.Instance = settings;

            tsbIncludeFetchXml.Checked = settings.IncludeFetchXml;
        }

        private void tsbExecute_Click(object sender, EventArgs e)
        {
            if (dockPanel.ActiveDocument == null)
                return;

            var query = (SqlQueryControl)dockPanel.ActiveDocument;
            query.Execute(true, tsbIncludeFetchXml.Checked);
        }

        private void tsbPreviewFetchXml_Click(object sender, EventArgs e)
        {
            if (dockPanel.ActiveDocument == null)
                return;

            var query = (SqlQueryControl)dockPanel.ActiveDocument;
            query.Execute(false, true);
        }

        private void tsbConnect_Click(object sender, EventArgs e)
        {
            AddAdditionalOrganization();
        }

        private void tsbNewQuery_Click(object sender, EventArgs e)
        {
            if (_objectExplorer.SelectedConnection == null)
                return;
            
            CreateQuery(_objectExplorer.SelectedConnection, "", null);
        }

        private SqlQueryControl CreateQuery(ConnectionDetail con, string sql, string sourcePlugin)
        { 
            var query = new SqlQueryControl(con, _metadata[con], _ai, SendOutgoingMessage, sourcePlugin, msg => LogError(msg));
            query.InsertText(sql);
            query.CancellableChanged += SyncStopButton;
            query.BusyChanged += SyncExecuteButton;

            query.Show(dockPanel, WeifenLuo.WinFormsUI.Docking.DockState.Document);
            query.SetFocus();

            return query;
        }

        private void tsbFormat_Click(object sender, EventArgs e)
        {
            if (dockPanel.ActiveDocument == null)
                return;

            var query = (SqlQueryControl)dockPanel.ActiveDocument;
            query.Format();
        }

        private void tsbSettings_Click(object sender, EventArgs e)
        {
            using (var form = new SettingsForm(Settings.Instance))
            {
                if (form.ShowDialog(this) == DialogResult.OK)
                    SettingsManager.Instance.Save(GetType(), Settings.Instance);
            }
        }

        private void tslAboutLink_Click(object sender, EventArgs e)
        {
            Process.Start("https://markcarrington.dev/sql-4-cds/");
        }

        public event EventHandler<MessageBusEventArgs> OnOutgoingMessage;

        public void OnIncomingMessage(MessageBusEventArgs message)
        {
            _ai.TrackEvent("Incoming message", new Dictionary<string, string> { ["SourcePlugin"] = message.SourcePlugin, ["Source"] = "XrmToolBox" });

            var param = message.TargetArgument as IDictionary<string, object>;

            if (param == null)
            {
                var xml = message.TargetArgument as string;
                param = new Dictionary<string, object>();
                param["FetchXml"] = xml;
                param["ConvertOnly"] = false;
            }

            if (_objectExplorer.SelectedConnection == null)
                return;

            var con = _objectExplorer.SelectedConnection;
            var metadata = _metadata[con];

            var fetch = DeserializeFetchXml((string)param["FetchXml"]);
            var options = new FetchXml2SqlOptions();

            if ((bool)param["ConvertOnly"])
                options.ConvertFetchXmlOperatorsTo = FetchXmlOperatorConversion.SqlCalculations;

            _ai.TrackEvent("Convert", new Dictionary<string, string> { ["QueryType"] = "FetchXML", ["Source"] = "XrmToolBox" });

            var sql = FetchXml2Sql.Convert(con.ServiceClient, metadata, fetch, options, out _);

            if ((bool)param["ConvertOnly"])
            {
                param["Sql"] = sql;
                OnOutgoingMessage(this, new MessageBusEventArgs(message.SourcePlugin) { TargetArgument = null });
            }
            else
            {
                CreateQuery(con, "-- Imported from " + message.SourcePlugin + "\r\n\r\n" + sql, message.SourcePlugin == "FetchXML Builder" ? null : message.SourcePlugin);
            }
        }

        private Engine.FetchXml.FetchType DeserializeFetchXml(string xml)
        {
            var serializer = new XmlSerializer(typeof(Engine.FetchXml.FetchType));

            using (var reader = new StringReader(xml))
            {
                return (Engine.FetchXml.FetchType) serializer.Deserialize(reader);
            }
        }

        public void SendOutgoingMessage(MessageBusEventArgs args)
        {
            _ai.TrackEvent("Outgoing message", new Dictionary<string, string> { ["TargetPlugin"] = args.TargetPlugin, ["Source"] = "XrmToolBox" });
            OnOutgoingMessage(this, args);
        }

        private void tsbOpen_Click(object sender, EventArgs e)
        {
            if (_objectExplorer.SelectedConnection == null)
                return;

            using (var open = new OpenFileDialog())
            {
                open.Filter = "SQL Scripts (*.sql)|*.sql";

                if (open.ShowDialog() != DialogResult.OK)
                    return;

                var query = CreateQuery(_objectExplorer.SelectedConnection, File.ReadAllText(open.FileName), null);
                query.Filename = open.FileName;
            }
        }

        private void tsbSave_Click(object sender, EventArgs e)
        {
            if (dockPanel.ActiveDocument == null)
                return;

            ((SqlQueryControl)dockPanel.ActiveDocument).Save();
        }

        private void dockPanel_ActiveDocumentChanged(object sender, EventArgs e)
        {
            tsbSave.Enabled = dockPanel.ActiveDocument != null;
            SyncStopButton(sender, e);
            SyncExecuteButton(sender, e);
        }

        protected override bool ProcessCmdKey(ref Message msg, Keys keyData)
        {
            if (keyData == (Keys.Control | Keys.S))
                tsbSave.PerformClick();
            else if (keyData == (Keys.Control | Keys.O))
                tsbOpen.PerformClick();
            else if (keyData == (Keys.Control | Keys.N))
                tsbNewQuery.PerformClick();
            else if (keyData == (Keys.Control | Keys.L))
                tsbPreviewFetchXml.PerformClick();
            else if (keyData == (Keys.Control | Keys.M))
                tsbIncludeFetchXml.PerformClick();
            else if (keyData == Keys.F5)
                tsbExecute.PerformClick();
            else
                return base.ProcessCmdKey(ref msg, keyData);

            return true;
        }

        private void SyncStopButton(object sender, EventArgs e)
        {
            if (dockPanel.ActiveDocument == null)
            {
                tsbStop.Enabled = false;
                return;
            }

            tsbStop.Enabled = ((SqlQueryControl)dockPanel.ActiveDocument).Cancellable;
        }

        private void SyncExecuteButton(object sender, EventArgs e)
        {
            if (dockPanel.ActiveDocument == null)
            {
                tsbExecute.Enabled = false;
                return;
            }

            tsbExecute.Enabled = !((SqlQueryControl)dockPanel.ActiveDocument).Busy;
        }

        private void tsbStop_Click(object sender, EventArgs e)
        {
            if (dockPanel.ActiveDocument == null)
                return;

            ((SqlQueryControl)dockPanel.ActiveDocument).Cancel();
        }

        string IGitHubPlugin.UserName => "MarkMpn";

        string IGitHubPlugin.RepositoryName => "Sql4Cds";

        string IHelpPlugin.HelpUrl => "https://markcarrington.dev/sql-4-cds/";

        private void tsbIncludeFetchXml_Click(object sender, EventArgs e)
        {
            Settings.Instance.IncludeFetchXml = tsbIncludeFetchXml.Checked;
            SettingsManager.Instance.Save(GetType(), Settings.Instance);
        }
    }
}