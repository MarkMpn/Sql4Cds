﻿using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Windows.Forms;
using System.Xml.Serialization;
using MarkMpn.Sql4Cds.Engine;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using McTools.Xrm.Connection;
using Microsoft.ApplicationInsights;
using WeifenLuo.WinFormsUI.Docking;
using XrmToolBox.Extensibility;
using XrmToolBox.Extensibility.Interfaces;

namespace MarkMpn.Sql4Cds
{
    public partial class PluginControl : MultipleConnectionsPluginControlBase, IMessageBusHost, IGitHubPlugin, IHelpPlugin, ISettingsPlugin, IPayPalPlugin
    {
        private readonly IDictionary<ConnectionDetail, SharedMetadataCache> _metadata;
        private readonly IDictionary<ConnectionDetail, TableSizeCache> _tableSize;
        private readonly TelemetryClient _ai;
        private readonly ObjectExplorer _objectExplorer;
        private readonly PropertiesWindow _properties;
        private bool _connectObjectExplorer;

        public PluginControl()
        {
            InitializeComponent();
            dockPanel.Theme = new VS2015LightTheme();
            _metadata = new Dictionary<ConnectionDetail, SharedMetadataCache>();
            _tableSize = new Dictionary<ConnectionDetail, TableSizeCache>();
            _objectExplorer = new ObjectExplorer(_metadata, WorkAsync, con => CreateQuery(con, ""), () => { _connectObjectExplorer = true; AddAdditionalOrganization(); });
            _objectExplorer.Show(dockPanel, DockState.DockLeft);
            _objectExplorer.CloseButtonVisible = false;
            _properties = new PropertiesWindow();
            _properties.Show(dockPanel, DockState.DockRightAutoHide);
            _properties.SelectedObjectChanged += OnSelectedObjectChanged;
            _ai = new TelemetryClient(new Microsoft.ApplicationInsights.Extensibility.TelemetryConfiguration("79761278-a908-4575-afbf-2f4d82560da6"));
            _connectObjectExplorer = true;

            TabIcon = Properties.Resources.SQL4CDS_Icon_16;
            PluginIcon = System.Drawing.Icon.FromHandle(Properties.Resources.SQL4CDS_Icon_16.GetHicon());
        }

        protected override void OnConnectionUpdated(ConnectionUpdatedEventArgs e)
        {
            AddConnection(e.ConnectionDetail);

            if (dockPanel.ActiveDocument is SqlQueryControl query && !_connectObjectExplorer)
                query.ChangeConnection(e.ConnectionDetail, _metadata[e.ConnectionDetail], _tableSize[e.ConnectionDetail]);

            base.OnConnectionUpdated(e);
        }

        protected override void ConnectionDetailsUpdated(NotifyCollectionChangedEventArgs e)
        {
            if (e.Action == NotifyCollectionChangedAction.Add)
            {
                foreach (ConnectionDetail con in e.NewItems)
                {
                    AddConnection(con);

                    if (!_connectObjectExplorer && dockPanel.ActiveDocument is SqlQueryControl query)
                        query.ChangeConnection(con, _metadata[con], _tableSize[con]);
                }
            }
        }

        private void AddConnection(ConnectionDetail con)
        {
            var svc = con.ServiceClient;

            if (!_metadata.ContainsKey(con))
                _metadata[con] = new SharedMetadataCache(con);
            
            if (!_tableSize.ContainsKey(con))
                _tableSize[con] = new TableSizeCache(svc, _metadata[con]);

            if (_connectObjectExplorer)
                _objectExplorer.AddConnection(con);

            // Start loading the entity list in the background
            EntityCache.TryGetEntities(con.MetadataCacheLoader, svc, out _);
        }

        private void PluginControl_Load(object sender, EventArgs e)
        {
            // Loads or creates the settings for the plugin
            if (!SettingsManager.Instance.TryLoad(GetType(), out Settings settings))
                settings = new Settings();

            Settings.Instance = settings;

            tsbIncludeFetchXml.Checked = settings.IncludeFetchXml;

            if (settings.RememberSession && settings.Session != null)
            {
                foreach (var doc in dockPanel.Documents.OfType<Form>().ToArray())
                    doc.Close();

                foreach (var tab in settings.Session)
                {
                    IDocumentWindow query = null;

                    switch (tab.Type)
                    {
                        case "SQL":
                            query = CreateQuery(_objectExplorer.SelectedConnection, null);
                            break;

                        case "FetchXML":
                            query = CreateFetchXML(null);
                            break;

                        default:
                            continue;
                    }

                    try
                    {
                        query.RestoreSessionDetails(tab);
                    }
                    catch
                    {
                        ((Form)query).Close();
                    }
                }
            }
            else
            {
                tsbNewQuery_Click(this, EventArgs.Empty);
            }
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
            _connectObjectExplorer = false;
            AddAdditionalOrganization();
        }

        private void tsbChangeConnection_Click(object sender, EventArgs e)
        {
            _connectObjectExplorer = false;
            AddAdditionalOrganization();
        }

        private void tsbNewQuery_Click(object sender, EventArgs e)
        {
            if (_objectExplorer.SelectedConnection == null)
                return;
            
            CreateQuery(_objectExplorer.SelectedConnection, "");
        }

        private SqlQueryControl CreateQuery(ConnectionDetail con, string sql)
        {
            var query = new SqlQueryControl(con, con == null ? null : _metadata[con], con == null ? null : _tableSize[con], _ai, xml => CreateFetchXML(xml), msg => LogError(msg), _properties);
            query.InsertText(sql);
            query.CancellableChanged += SyncStopButton;
            query.BusyChanged += SyncExecuteButton;

            query.Show(dockPanel, DockState.Document);
            query.SetFocus();

            return query;
        }

        private FetchXmlControl CreateFetchXML(string xml)
        {
            var query = new FetchXmlControl();
            query.FetchXml = xml;

            query.Show(dockPanel, DockState.Document);
            query.SetFocus();

            return query;
        }

        private void tsbFormat_Click(object sender, EventArgs e)
        {
            if (dockPanel.ActiveDocument == null)
                return;

            var query = (IDocumentWindow)dockPanel.ActiveDocument;
            query.Format();
        }

        private void tsbSettings_Click(object sender, EventArgs e)
        {
            ShowSettings();
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
                CreateQuery(con, "-- Imported from " + message.SourcePlugin + "\r\n\r\n" + sql);
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

        private void tsbOpen_Click(object sender, EventArgs e)
        {
            if (_objectExplorer.SelectedConnection == null)
                return;

            using (var open = new OpenFileDialog())
            {
                open.Filter = "SQL Scripts (*.sql)|*.sql";

                if (open.ShowDialog() != DialogResult.OK)
                    return;

                var query = CreateQuery(_objectExplorer.SelectedConnection, File.ReadAllText(open.FileName));
                query.Filename = open.FileName;
            }
        }

        private void tsbSave_Click(object sender, EventArgs e)
        {
            if (dockPanel.ActiveDocument == null)
                return;

            ((IDocumentWindow)dockPanel.ActiveDocument).Save();
        }

        private void OnSelectedObjectChanged(object sender, EventArgs e)
        {
            tsbFetchXMLBuilder.Enabled = dockPanel.ActiveDocument is FetchXmlControl || _properties.SelectedObject is IFetchXmlExecutionPlanNode;
        }

        private void dockPanel_ActiveDocumentChanged(object sender, EventArgs e)
        {
            var doc = (IDocumentWindow)dockPanel.ActiveDocument;
            var sql = doc as SqlQueryControl;
            var xml = doc as FetchXmlControl;
            tsbSave.Enabled = doc != null;
            tsbConnect.Enabled = sql != null && sql.Connection == null;
            tsbChangeConnection.Enabled = sql != null && sql.Connection != null;
            tsbFetchXMLBuilder.Enabled = xml != null;
            tsbFormat.Enabled = doc != null;
            SyncStopButton(sender, e);
            SyncExecuteButton(sender, e);

            if (sql != null)
                _properties.SelectedObject = sql.Connection;
            else
                _properties.SelectedObject = null;
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
            else if (keyData == Keys.F5 || keyData == (Keys.Control | Keys.E))
                tsbExecute.PerformClick();
            else if (keyData == Keys.F4)
                dockPanel.ActiveAutoHideContent = _properties;
            else
                return base.ProcessCmdKey(ref msg, keyData);

            return true;
        }

        private void SyncStopButton(object sender, EventArgs e)
        {
            if (!(dockPanel.ActiveDocument is SqlQueryControl query))
            {
                tsbStop.Enabled = false;
                return;
            }

            tsbStop.Enabled = query.Cancellable;
        }

        private void SyncExecuteButton(object sender, EventArgs e)
        {
            if (!(dockPanel.ActiveDocument is SqlQueryControl query))
            {
                tsbExecute.Enabled = false;
                tsbPreviewFetchXml.Enabled = false;
                return;
            }

            tsbExecute.Enabled = query.Connection != null && !query.Busy;
            tsbPreviewFetchXml.Enabled = query.Connection != null && !query.Busy;
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

        string IPayPalPlugin.DonationDescription => "SQL 4 CDS Donation";

        string IPayPalPlugin.EmailAccount => "donate@markcarrington.dev";

        private void tsbIncludeFetchXml_Click(object sender, EventArgs e)
        {
            Settings.Instance.IncludeFetchXml = tsbIncludeFetchXml.Checked;
            SettingsManager.Instance.Save(GetType(), Settings.Instance);
        }

        public void ShowSettings()
        {
            using (var form = new SettingsForm(Settings.Instance))
            {
                if (form.ShowDialog(this) == DialogResult.OK)
                    SaveSettings();
            }
        }

        private void SaveSettings()
        {
            if (Settings.Instance.RememberSession)
                Settings.Instance.Session = dockPanel.Documents.OfType<IDocumentWindow>().Select(query => query.GetSessionDetails()).ToArray();
            else
                Settings.Instance.Session = null;

            SettingsManager.Instance.Save(GetType(), Settings.Instance);
        }

        public override void ClosingPlugin(PluginCloseInfo info)
        {
            if (Settings.Instance.RememberSession)
                SaveSettings();

            base.ClosingPlugin(info);
        }

        private void saveSessionTimer_Tick(object sender, EventArgs e)
        {
            if (Settings.Instance.RememberSession)
                SaveSettings();
        }

        private void tsbFetchXMLBuilder_Click(object sender, EventArgs e)
        {
            string fetchXml;

            if (dockPanel.ActiveDocument is FetchXmlControl xml)
                fetchXml = xml.FetchXml;
            else
                fetchXml = ((IFetchXmlExecutionPlanNode) _properties.SelectedObject).FetchXmlString;

            var args = new MessageBusEventArgs("FetchXML Builder") { TargetArgument = fetchXml };
            _ai.TrackEvent("Outgoing message", new Dictionary<string, string> { ["TargetPlugin"] = args.TargetPlugin, ["Source"] = "XrmToolBox" });
            OnOutgoingMessage(this, args);
        }
    }
}