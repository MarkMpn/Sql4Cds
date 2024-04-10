﻿using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Windows.Forms;
using System.Xml.Serialization;
using MarkMpn.Sql4Cds.Engine;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using McTools.Xrm.Connection;
using Microsoft.ApplicationInsights;
using Microsoft.Xrm.Sdk;
using WeifenLuo.WinFormsUI.Docking;
using XrmToolBox.Extensibility;
using XrmToolBox.Extensibility.Interfaces;
using static System.Windows.Forms.VisualStyles.VisualStyleElement;

namespace MarkMpn.Sql4Cds.XTB
{
    public partial class PluginControl : PluginControlBase, IMessageBusHost, IGitHubPlugin, IHelpPlugin, ISettingsPlugin, IPayPalPlugin, INoHighlightingPlugin
    {
        private readonly object _plugin;
        private readonly IDictionary<string, DataSource> _dataSources;
        private readonly TelemetryClient _ai;
        private readonly ObjectExplorer _objectExplorer;
        private readonly PropertiesWindow _properties;

        public PluginControl(object plugin)
        {
            InitializeComponent();
            _plugin = plugin;
            dockPanel.Theme = new VS2015LightTheme();

            // Loads or creates the settings for the plugin
            if (!SettingsManager.Instance.TryLoad(_plugin.GetType(), out Settings settings))
                settings = new Settings();

            Settings.Instance = settings;

            _dataSources = new Dictionary<string, DataSource>(StringComparer.OrdinalIgnoreCase);
            _objectExplorer = new ObjectExplorer(_dataSources, WorkAsync, con => CreateQuery(con, ""), ConnectObjectExplorer);
            _objectExplorer.CloseButtonVisible = false;
            _properties = new PropertiesWindow();
            _properties.SelectedObjectChanged += OnSelectedObjectChanged;
            _ai = new TelemetryClient(new Microsoft.ApplicationInsights.Extensibility.TelemetryConfiguration("79761278-a908-4575-afbf-2f4d82560da6"));
            tscbConnection.Items.Add("Add Connection...");
            
            TabIcon = Properties.Resources.SQL4CDS_Icon_16;
            PluginIcon = System.Drawing.Icon.FromHandle(Properties.Resources.SQL4CDS_Icon_16.GetHicon());
        }

        private void ConnectObjectExplorer()
        {
            var args = new RequestConnectionEventArgs { ActionName = "ConnectObjectExplorer", Control = this };
            OnConnectionRequested(this, args);
        }

        public override void UpdateConnection(IOrganizationService newService, ConnectionDetail detail, string actionName, object parameter)
        {
            AddConnection(detail);
            ConnectionDetail = detail;

            if (actionName == "ChangeConnection")
            {
                if (dockPanel.ActiveDocument is SqlQueryControl query)
                {
                    query.ChangeConnection(detail);
                    tscbConnection.Text = detail.ConnectionName;
                }

                if (!tscbConnection.Items.Contains(detail))
                    tscbConnection.Items.Insert(tscbConnection.Items.Count - 1, detail);
            }
            else if (actionName == "ConnectObjectExplorer")
            {
                _objectExplorer.AddConnection(detail);

                if (!tscbConnection.Items.Contains(detail))
                    tscbConnection.Items.Insert(tscbConnection.Items.Count - 1, detail);
            }
            else if (actionName == "ConnectObjectExplorerAndChangeConnection")
            {
                _objectExplorer.AddConnection(detail);

                if (!tscbConnection.Items.Contains(detail))
                    tscbConnection.Items.Insert(tscbConnection.Items.Count - 1, detail);

                if (dockPanel.ActiveDocument is SqlQueryControl query)
                {
                    query.ChangeConnection(detail);
                    tscbConnection.Text = detail.ConnectionName;
                }
            }
            else if (String.IsNullOrEmpty(actionName))
            {
                _objectExplorer.AddConnection(detail);

                if (!tscbConnection.Items.Contains(detail))
                    tscbConnection.Items.Insert(tscbConnection.Items.Count - 1, detail);
            }
            else
            {
                base.UpdateConnection(newService, detail, actionName, parameter);
            }
        }

        private void AddConnection(ConnectionDetail con)
        {
            if (!_dataSources.ContainsKey(con.ConnectionName))
            {
                var metadata = new SharedMetadataCache(con, GetNewServiceClient(con));
                var tableSize = new TableSizeCache(GetNewServiceClient(con), metadata);
                var messages = new MessageCache(GetNewServiceClient(con), metadata);

                _dataSources[con.ConnectionName] = new XtbDataSource(con, GetNewServiceClient, metadata, tableSize, messages);
            }

            // Start loading the entity list in the background
            EntityCache.TryGetEntities(con.MetadataCacheLoader, GetNewServiceClient(con), out _);
        }

        private IOrganizationService GetNewServiceClient(ConnectionDetail con)
        {
            var svc = con.ServiceClient;

            if (con.IsFromSdkLoginCtrl)
                return svc;

            if (svc.ActiveAuthenticationType == Microsoft.Xrm.Tooling.Connector.AuthenticationType.OAuth)
                return svc.Clone();

            try
            {
                return con.GetCrmServiceClient(true);
            }
            catch
            {
                return svc;
            }
        }

        private void PluginControl_Load(object sender, EventArgs e)
        {
            tsbIncludeFetchXml.Checked = Settings.Instance.IncludeFetchXml;

            if (Settings.Instance.DockLayout != null)
            {
                using (var stream = new MemoryStream(Encoding.UTF8.GetBytes(Settings.Instance.DockLayout)))
                {
                    var contentIndex = 0;

                    dockPanel.LoadFromXml(stream, (name) =>
                    {
                        if (name == _objectExplorer.GetType().FullName)
                            return _objectExplorer;

                        if (name == _properties.GetType().FullName)
                            return _properties;

                        if (!Settings.Instance.RememberSession || Settings.Instance.Session == null)
                            return null;

                        IDocumentWindow query = null;

                        if (name == typeof(SqlQueryControl).FullName)
                            query = CreateQuery(_objectExplorer.SelectedConnection, null);
                        else if (name == typeof(FetchXmlControl).FullName)
                            query = CreateFetchXML(null);
                        else if (name == typeof(MQueryControl).FullName)
                            query = CreateM(null);
                        else
                            return null;

                        try
                        {
                            query.RestoreSessionDetails(Settings.Instance.Session[contentIndex++]);
                            return (IDockContent)query;
                        }
                        catch
                        {
                            ((Form)query).Close();
                            return null;
                        }
                    });
                }
            }
            else
            {
                _objectExplorer.Show(dockPanel, DockState.DockLeft);
                _properties.Show(dockPanel, DockState.DockRightAutoHide);

                if (Settings.Instance.RememberSession && Settings.Instance.Session != null)
                {
                    foreach (var tab in Settings.Instance.Session)
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

                            case "M":
                                query = CreateM(null);
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
            }

            if (!dockPanel.Contents.OfType<IDocumentWindow>().Any())
                CreateQuery(_objectExplorer.SelectedConnection, null);
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
            var args = new RequestConnectionEventArgs { ActionName = "ChangeConnection", Control = this };
            OnConnectionRequested(this, args);
        }

        private void tsbChangeConnection_Click(object sender, EventArgs e)
        {
            var args = new RequestConnectionEventArgs { ActionName = "ChangeConnection", Control = this };
            OnConnectionRequested(this, args);
        }

        private void tsbNewQuery_Click(object sender, EventArgs e)
        {
            if (_objectExplorer.SelectedConnection == null)
                return;
            
            CreateQuery(_objectExplorer.SelectedConnection, "");
        }

        private SqlQueryControl CreateQuery(ConnectionDetail con, string sql)
        {
            var query = new SqlQueryControl(con, _dataSources, _ai, xml => CreateFetchXML(xml), msg => LogError(msg), _properties);
            query.InsertText(sql);
            query.CancellableChanged += SyncStopButton;
            query.BusyChanged += SyncExecuteButton;

            query.TabPageContextMenuStrip = tabContextMenuStrip;
            query.Show(dockPanel, DockState.Document);
            query.SetFocus();

            return query;
        }

        private FetchXmlControl CreateFetchXML(string xml)
        {
            var query = new FetchXmlControl();
            query.Content = xml;

            query.TabPageContextMenuStrip = tabContextMenuStrip;
            query.Show(dockPanel, DockState.Document);
            query.SetFocus();

            return query;
        }

        private MQueryControl CreateM(string m)
        {
            var query = new MQueryControl();
            query.Content = m;

            query.TabPageContextMenuStrip = tabContextMenuStrip;
            query.Show(dockPanel, DockState.Document);
            query.SetFocus();

            return query;
        }

        private void tsbFormat_Click(object sender, EventArgs e)
        {
            if (!(dockPanel.ActiveDocument is IFormatableDocumentWindow doc))
                return;

            doc.Format();
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
                var str = message.TargetArgument as string;
                param = new Dictionary<string, object>();

                if (str.StartsWith("<"))
                    param["FetchXml"] = str;
                else
                    param["SQL"] = str;

                param["ConvertOnly"] = false;
            }

            if (_objectExplorer.SelectedConnection == null)
                return;

            var con = _objectExplorer.SelectedConnection;
            var metadata = _dataSources[con.ConnectionName].Metadata;

            if (param.TryGetValue("FetchXml", out var xml) && xml is string xmlStr && !String.IsNullOrEmpty(xmlStr))
            {
                var options = new FetchXml2SqlOptions
                {
                    ConvertDateTimeToUtc = Settings.Instance.FetchXml2SqlOptions.ConvertDateTimeToUtc,
                    ConvertFetchXmlOperatorsTo = Settings.Instance.FetchXml2SqlOptions.ConvertFetchXmlOperatorsTo,
                    UseParametersForLiterals = Settings.Instance.FetchXml2SqlOptions.UseParametersForLiterals
                };

                if ((bool)param["ConvertOnly"])
                    options.ConvertFetchXmlOperatorsTo = FetchXmlOperatorConversion.SqlCalculations;

                _ai.TrackEvent("Convert", new Dictionary<string, string> { ["QueryType"] = "FetchXML", ["Source"] = "XrmToolBox" });

                string sql;

                if (Settings.Instance.UseNativeSqlConversion)
                {
                    try
                    {
                        var convertReq = new OrganizationRequest("FetchXMLToSQL")
                        {
                            ["FetchXml"] = xmlStr,
                            ["SubqueryCompatible"] = true
                        };
                        var convertResp = con.ServiceClient.Execute(convertReq);
                        sql = (string) convertResp["Response"];
                    }
                    catch
                    {
                        var fetch = DeserializeFetchXml(xmlStr);
                        sql = FetchXml2Sql.Convert(con.ServiceClient, metadata, fetch, options, out _);
                    }
                }
                else
                {
                    var fetch = DeserializeFetchXml(xmlStr);
                    sql = FetchXml2Sql.Convert(con.ServiceClient, metadata, fetch, options, out _);
                }

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
            else if (param.TryGetValue("SQL", out var sql) && sql is string sqlStr && !String.IsNullOrEmpty(sqlStr))
            {
                CreateQuery(con, "-- Imported from " + message.SourcePlugin + "\r\n\r\n" + sqlStr);
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

                var query = CreateQuery(_objectExplorer.SelectedConnection, "");
                query.Open(open.FileName);
            }
        }

        private void tsbSave_Click(object sender, EventArgs e)
        {
            if (!(dockPanel.ActiveDocument is ISaveableDocumentWindow doc))
                return;

            Save(doc, doc.Filename);
        }

        private void saveAsToolStripMenuItem_Click(object sender, EventArgs e)
        {
            if (!(dockPanel.ActiveDocument is ISaveableDocumentWindow doc))
                return;

            Save(doc, null);
        }

        private void saveAllToolStripMenuItem_Click(object sender, EventArgs e)
        {
            var unsavedDocuments = dockPanel.Contents
                .OfType<ISaveableDocumentWindow>()
                .Where(query => query.Modified)
                .ToArray();

            foreach (var doc in unsavedDocuments)
            {
                ((IDockContent)doc).DockHandler.Show();
                if (!Save(doc, doc.Filename))
                    break;
            }
        }

        private bool Save(ISaveableDocumentWindow doc, string filename)
        {
            if (filename == null)
            {
                using (var save = new SaveFileDialog())
                {
                    save.Filter = doc.Filter;
                    save.FileName = doc.Filename ?? doc.DisplayName;

                    if (save.ShowDialog() != DialogResult.OK)
                        return false;

                    filename = save.FileName;
                }
            }

            doc.Save(filename);
            return true;
        }

        private void OnSelectedObjectChanged(object sender, EventArgs e)
        {
            tsbFetchXMLBuilder.Enabled = dockPanel.ActiveDocument is FetchXmlControl || _properties.SelectedObject is IFetchXmlExecutionPlanNode;
        }

        private void dockPanel_ActiveDocumentChanged(object sender, EventArgs e)
        {
            var doc = (IDocumentWindow)dockPanel.ActiveDocument;
            var saveable = doc as ISaveableDocumentWindow;
            var formatable = doc as IFormatableDocumentWindow;
            var sql = doc as SqlQueryControl;
            var xml = doc as FetchXmlControl;
            tsbSave.Enabled = saveable != null;
            tsbConnect.Enabled = sql != null && sql.Connection == null;
            tsbChangeConnection.Enabled = sql != null && sql.Connection != null;
            tsbFetchXMLBuilder.Enabled = xml != null;
            tsbFormat.Enabled = formatable != null;
            SyncStopButton(sender, e);
            SyncExecuteButton(sender, e);

            if (sql != null)
            {
                _properties.Connections = sql.DataSources;
                _properties.SelectObject(new ConnectionPropertiesWrapper(sql.Connection), false);
            }
            else
            {
                _properties.SelectObject(null, false);
            }
        }

        protected override bool ProcessCmdKey(ref System.Windows.Forms.Message msg, Keys keyData)
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
            else if (keyData == (Keys.Control | Keys.U))
                tscbConnection.Focus();
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
            if (InvokeRequired)
            {
                Invoke(new EventHandler(SyncStopButton), sender, e);
                return;
            }

            if (!(dockPanel.ActiveDocument is SqlQueryControl query))
            {
                tsbStop.Enabled = false;
                return;
            }

            tsbStop.Enabled = query.Cancellable;
        }

        private void SyncExecuteButton(object sender, EventArgs e)
        {
            if (InvokeRequired)
            {
                Invoke(new EventHandler(SyncExecuteButton), sender, e);
                return;
            }

            if (!(dockPanel.ActiveDocument is SqlQueryControl query))
            {
                tscbConnection.Enabled = false;
                tsbExecute.Enabled = false;
                tsbPreviewFetchXml.Enabled = false;
                tsbConvertToFetchXMLSplitButton.Enabled = false;
                return;
            }

            tscbConnection.Enabled = !query.Busy;
            tscbConnection.Text = query.Connection?.ConnectionName;
            tsbExecute.Enabled = query.Connection != null && !query.Busy;
            tsbPreviewFetchXml.Enabled = query.Connection != null && !query.Busy;
            tsbConvertToFetchXMLSplitButton.Enabled = query.Connection != null && !query.Busy;
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
            SettingsManager.Instance.Save(_plugin.GetType(), Settings.Instance);
        }

        public void ShowSettings()
        {
            using (var form = new SettingsForm(Settings.Instance, this))
            {
                if (form.ShowDialog(this) == DialogResult.OK)
                    SaveSettings();
            }
        }

        private void SaveSettings()
        {
            if (Settings.Instance.RememberSession)
            {
                Settings.Instance.Session = dockPanel.Contents.OfType<IDocumentWindow>().Select(query => query.GetSessionDetails()).ToArray();

                using (var memoryStream = new MemoryStream())
                {
                    dockPanel.SaveAsXml(memoryStream, Encoding.UTF8);
                    Settings.Instance.DockLayout = Encoding.UTF8.GetString(memoryStream.ToArray());
                }
            }
            else
            {
                Settings.Instance.Session = null;
            }

            SettingsManager.Instance.Save(_plugin.GetType(), Settings.Instance);
        }

        public override void ClosingPlugin(PluginCloseInfo info)
        {
            if (!ConfirmBulkClose(dockPanel.Contents.OfType<IDocumentWindow>().Cast<IDockContent>().ToArray(), info.Silent))
            {
                info.Cancel = true;
                return;
            }

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
                fetchXml = xml.Content;
            else
                fetchXml = ((IFetchXmlExecutionPlanNode) _properties.SelectedObject).FetchXmlString;

            var args = new MessageBusEventArgs("FetchXML Builder") { TargetArgument = fetchXml };
            _ai.TrackEvent("Outgoing message", new Dictionary<string, string> { ["TargetPlugin"] = args.TargetPlugin, ["Source"] = "XrmToolBox" });
            OnOutgoingMessage(this, args);
        }

        private void powerBIMToolStripMenuItem_Click(object sender, EventArgs e)
        {
            if (!(dockPanel.ActiveDocument is SqlQueryControl sql))
                return;

            _ai.TrackEvent("Convert", new Dictionary<string, string> { ["QueryType"] = "M", ["Source"] = "XrmToolBox" });

            var m = $@"/*
Query converted to M format by SQL 4 CDS
To use in Power BI:
1. Click New Source
2. Click Blank Query
3. Click Advanced Editor
4. Copy & paste in this query
*/

let
  Source = CommonDataService.Database(""{new Uri(sql.Connection.OriginalUrl).Host}""),
  DataverseSQL = Value.NativeQuery(Source, ""{sql.Sql.Replace("\"", "\"\"").Replace("\r\n", " ").Trim()}"", null, [EnableFolding=true])
in
  DataverseSQL";
            CreateM(m);
        }

        private void fetchXMLToolStripMenuItem_Click(object sender, EventArgs e)
        {
            if (!(dockPanel.ActiveDocument is SqlQueryControl sql))
                return;

            using (var con = new Sql4CdsConnection(sql.DataSources))
            using (var cmd = con.CreateCommand())
            using (var options = new QueryExecutionOptions(this, null, con, cmd))
            {
                options.ApplySettings(false);
                con.UseTDSEndpoint = false;
                cmd.CommandText = sql.Sql;

                try
                {
                    var queries = cmd.GeneratePlan(false);

                    foreach (var query in queries)
                    {
                        _ai.TrackEvent("Convert", new Dictionary<string, string> { ["QueryType"] = query.GetType().Name, ["Source"] = "XrmToolBox" });

                        var sb = new StringBuilder();
                        sb.Append("<!--\r\nCreated from query:\r\n\r\n");
                        sb.Append(query.Sql);

                        var nodes = GetAllNodes(query).ToList();
                        var fetchXmlNodes = nodes.OfType<IFetchXmlExecutionPlanNode>().ToList();

                        if (fetchXmlNodes.Count == 0)
                            continue;

                        if (nodes.Count > fetchXmlNodes.Count)
                        {
                            sb.Append("\r\n\r\n‼ WARNING ‼\r\n");
                            sb.Append("This query requires additional processing. This FetchXML gives the required data, but needs additional processing to format it in the same way as returned by the TDS Endpoint or SQL 4 CDS.\r\n\r\n");
                            sb.Append("See the estimated execution plan to see what extra processing is performed by SQL 4 CDS");
                        }

                        sb.Append("\r\n\r\n-->");

                        foreach (var fetchXml in nodes.OfType<IFetchXmlExecutionPlanNode>())
                        {
                            sb.Append("\r\n\r\n");
                            sb.Append(fetchXml.FetchXmlString);
                        }

                        CreateFetchXML(sb.ToString());
                    }
                }
                catch (NotSupportedQueryFragmentException ex)
                {
                    MessageBox.Show(this, "The query could not be converted to FetchXML: " + ex.Message, "Query Not Supported", MessageBoxButtons.OK, MessageBoxIcon.Error);
                }
                catch (QueryParseException ex)
                {
                    MessageBox.Show(this, "The query could not be parsed: " + ex.Message, "Query Parsing Error", MessageBoxButtons.OK, MessageBoxIcon.Error);
                }
            }
        }

        private IEnumerable<IExecutionPlanNode> GetAllNodes(IExecutionPlanNode node)
        {
            foreach (var source in node.GetSources())
            {
                yield return source;

                foreach (var subSource in GetAllNodes(source))
                    yield return subSource;
            }
        }

        private void tscbConnection_SelectedIndexChanged(object sender, EventArgs e)
        {
            if (dockPanel.ActiveDocument is SqlQueryControl query && tscbConnection.SelectedItem != null)
            {
                if (tscbConnection.SelectedItem is ConnectionDetail conn)
                {
                    query.ChangeConnection(conn);
                    query.SetFocus();
                }
                else
                {
                    var args = new RequestConnectionEventArgs { ActionName = "ConnectObjectExplorerAndChangeConnection", Control = this };
                    OnConnectionRequested(this, args);
                }
            }
        }

        private void tabContextMenuStrip_Opening(object sender, System.ComponentModel.CancelEventArgs e)
        {
            var tabIndex = 0;

            while (tabIndex < dockPanel.ActiveDocumentPane.DisplayingContents.Count && dockPanel.ActiveDocumentPane.DisplayingContents[tabIndex] != dockPanel.ActiveDocument)
                tabIndex++;

            closeOthersToolStripMenuItem.Enabled = dockPanel.DocumentsCount > 1;
            closeToTheRightToolStripMenuItem.Enabled = tabIndex < dockPanel.ActiveDocumentPane.DisplayingContents.Count - 1;
            copyFullPathToolStripMenuItem.Enabled = !String.IsNullOrEmpty((dockPanel.ActiveDocument as ISaveableDocumentWindow)?.Filename);
            openContainingFolderToolStripMenuItem.Enabled = copyFullPathToolStripMenuItem.Enabled;
        }

        private void closeToolStripMenuItem_Click(object sender, EventArgs e)
        {
            dockPanel.ActiveDocument.DockHandler.Close();
        }

        private void closeOthersToolStripMenuItem_Click(object sender, EventArgs e)
        {
            var others = dockPanel.Documents.Except(new[] { dockPanel.ActiveDocument }).ToArray();

            if (!ConfirmBulkClose(others, false))
                return;

            foreach (var doc in others)
                doc.DockHandler.Close();
        }

        private void closeToTheRightToolStripMenuItem_Click(object sender, EventArgs e)
        {
            var right = new List<IDockContent>();
            var foundCurrent = false;

            for (var i = 0; i < dockPanel.ActiveDocumentPane.DisplayingContents.Count; i++)
            {
                if (dockPanel.ActiveDocumentPane.DisplayingContents[i] == dockPanel.ActiveDocument)
                    foundCurrent = true;
                else if (foundCurrent)
                    right.Add(dockPanel.ActiveDocumentPane.DisplayingContents[i]);
            }

            if (!ConfirmBulkClose(right.ToArray(), false))
                return;

            foreach (var doc in right)
                doc.DockHandler.Close();
        }

        private void closeSavedToolStripMenuItem_Click(object sender, EventArgs e)
        {
            var saved = dockPanel.Documents
                .OfType<ISaveableDocumentWindow>()
                .Where(doc => !doc.Modified)
                .Cast<IDockContent>()
                .ToList();

            foreach (var doc in saved)
                doc.DockHandler.Close();
        }

        private void closeAllToolStripMenuItem_Click(object sender, EventArgs e)
        {
            if (!ConfirmBulkClose(dockPanel.DocumentsToArray(), false))
                return;

            foreach (var doc in dockPanel.DocumentsToArray())
                doc.DockHandler.Close();
        }

        private bool ConfirmBulkClose(IDockContent[] documents, bool silent)
        {
            if (!silent)
            {
                var busy = documents
                    .OfType<SqlQueryControl>()
                    .Any(query => query.Busy);

                if (busy)
                {
                    MessageBox.Show(this, "Query is still executing. Please wait for the query to finish before closing this tab.", "Query Running", MessageBoxButtons.OK, MessageBoxIcon.Error);
                    return false;
                }
            }

            var unsavedDocuments = dockPanel.Contents
                .OfType<ISaveableDocumentWindow>()
                .Where(query => query.Modified)
                .ToArray();

            if (unsavedDocuments.Length > 0 && !Settings.Instance.RememberSession)
            {
                using (var form = new ConfirmCloseForm(unsavedDocuments.Select(query => query.DisplayName).ToArray(), !silent))
                {
                    switch (form.ShowDialog())
                    {
                        case DialogResult.Yes:
                            foreach (var doc in unsavedDocuments)
                            {
                                var filename = doc.Filename;

                                if (filename == null)
                                {
                                    using (var save = new SaveFileDialog())
                                    {
                                        save.Filter = doc.Filter;

                                        if (save.ShowDialog() != DialogResult.OK)
                                            return false;

                                        filename = save.FileName;
                                    }
                                }

                                doc.Save(filename);
                            }
                            break;

                        case DialogResult.Cancel:
                            return false;
                    }
                }
            }

            return true;
        }

        private void copyFullPathToolStripMenuItem_Click(object sender, EventArgs e)
        {
            var saveable = (ISaveableDocumentWindow)dockPanel.ActiveDocument;
            Clipboard.SetText(saveable.Filename);
        }

        private void openContainingFolderToolStripMenuItem_Click(object sender, EventArgs e)
        {
            var saveable = (ISaveableDocumentWindow)dockPanel.ActiveDocument;
            Process.Start("explorer.exe", $"/select,\"{saveable.Filename}\"");
        }

        internal void ResetDockLayout()
        {
            _objectExplorer.Show(dockPanel, DockState.DockLeft);
            _properties.Show(dockPanel, DockState.DockRightAutoHide);
        }
    }
}