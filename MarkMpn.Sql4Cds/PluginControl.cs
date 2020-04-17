using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Diagnostics;
using System.IO;
using System.Threading;
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
        private int _metadataLoadingTasks;

        public PluginControl()
        {
            InitializeComponent();
            dockPanel.Theme = new VS2015LightTheme();
            _metadata = new Dictionary<ConnectionDetail, AttributeMetadataCache>();
            _objectExplorer = new ObjectExplorer(_metadata, WorkAsync);
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

            _metadata[con].MetadataLoading += MetadataLoading;
            //_metadata[con].LoadAllAsync();
        }

        private void MetadataLoading(object sender, MetadataLoadingEventArgs e)
        {
            if (Interlocked.Increment(ref _metadataLoadingTasks) == 1)
                progressBar.Style = ProgressBarStyle.Marquee;

            e.Task.ContinueWith(t =>
            {
                if (Interlocked.Decrement(ref _metadataLoadingTasks) == 0)
                {
                    if (InvokeRequired)
                        Invoke((Action)(() => { progressBar.Style = ProgressBarStyle.Blocks; }));
                    else
                        progressBar.Style = ProgressBarStyle.Blocks;
                }
            });
        }

        private void PluginControl_Load(object sender, EventArgs e)
        {
            // Loads or creates the settings for the plugin
            if (!SettingsManager.Instance.TryLoad(GetType(), out Settings settings))
                settings = new Settings();

            Settings.Instance = settings;
        }

        private void tsbClose_Click(object sender, EventArgs e)
        {
            CloseTool();
        }

        private void tsbExecute_Click(object sender, EventArgs e)
        {
            if (dockPanel.ActiveDocument == null)
                return;

            var query = (SqlQueryControl)dockPanel.ActiveDocument;
            query.Execute(true);
        }

        private void tsbPreviewFetchXml_Click(object sender, EventArgs e)
        {
            if (dockPanel.ActiveDocument == null)
                return;

            var query = (SqlQueryControl)dockPanel.ActiveDocument;
            query.Execute(false);
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
            var query = new SqlQueryControl(con, _metadata[con], _ai, WorkAsyncWithCancel, msg => SetWorkingMessage(msg), ExecuteMethod, SendOutgoingMessage, sourcePlugin);
            query.InsertText(sql);

            query.Show(dockPanel, WeifenLuo.WinFormsUI.Docking.DockState.Document);
            query.SetFocus();

            return query;
        }

        private void WorkAsyncWithCancel(WorkAsyncInfo info)
        {
            WorkAsync(new WorkAsyncInfo
            {
                AsyncArgument = info.AsyncArgument,
                Host = info.Host,
                IsCancelable = info.IsCancelable,
                Message = info.Message,
                MessageHeight = info.MessageHeight,
                MessageWidth = info.MessageWidth,
                PostWorkCallBack = (args) =>
                {
                    if (InvokeRequired)
                        Invoke((Action) (() => { tsbStop.Enabled = false; }));
                    else
                        tsbStop.Enabled = false;

                    info.PostWorkCallBack(args);
                },
                ProgressChanged = info.ProgressChanged,
                Work = (worker, args) =>
                {
                    if (InvokeRequired)
                        Invoke((Action)(() => { tsbStop.Enabled = true; }));
                    else
                        tsbStop.Enabled = true;

                    info.Work(worker, args);
                }
            });
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
            _ai.TrackEvent("Incoming message", new Dictionary<string, string> { ["SourcePlugin"] = message.SourcePlugin });

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
            var sql = FetchXml2Sql.Convert(metadata, fetch);

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
            _ai.TrackEvent("Outgoing message", new Dictionary<string, string> { ["TargetPlugin"] = args.TargetPlugin });
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
        }

        protected override bool ProcessCmdKey(ref Message msg, Keys keyData)
        {
            if (keyData == (Keys.Control | Keys.S))
                tsbSave.PerformClick();
            else if (keyData == (Keys.Control | Keys.O))
                tsbOpen.PerformClick();
            else if (keyData == (Keys.Control | Keys.N))
                tsbNewQuery.PerformClick();
            else
                return base.ProcessCmdKey(ref msg, keyData);

            return true;
        }

        private void tsbStop_Click(object sender, EventArgs e)
        {
            CancelWorker();
        }

        string IGitHubPlugin.UserName => "MarkMpn";

        string IGitHubPlugin.RepositoryName => "Sql4Cds";

        string IHelpPlugin.HelpUrl => "https://markcarrington.dev/sql-4-cds/";
    }
}