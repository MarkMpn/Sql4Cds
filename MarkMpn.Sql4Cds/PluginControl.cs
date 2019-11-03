using System;
using System.Drawing;
using System.Data;
using System.Linq;
using System.Windows.Forms;
using XrmToolBox.Extensibility;
using McTools.Xrm.Connection;
using Microsoft.Xrm.Sdk.Messages;
using System.Collections.Specialized;
using Microsoft.Xrm.Sdk.Metadata;
using Microsoft.Xrm.Sdk;
using System.Diagnostics;
using XrmToolBox.Extensibility.Interfaces;
using System.Xml;
using System.IO;
using System.Xml.Serialization;
using System.Collections.Generic;

namespace MarkMpn.Sql4Cds
{
    public partial class PluginControl : MultipleConnectionsPluginControlBase, IMessageBusHost
    {
        private readonly IDictionary<ConnectionDetail, AttributeMetadataCache> _metadata;
        private ObjectExplorer _objectExplorer;

        public PluginControl()
        {
            InitializeComponent();
            _metadata = new Dictionary<ConnectionDetail, AttributeMetadataCache>();
            _objectExplorer = new ObjectExplorer(_metadata, WorkAsync);
            _objectExplorer.Show(dockPanel, WeifenLuo.WinFormsUI.Docking.DockState.DockLeft);
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
        }

        private void MyPluginControl_Load(object sender, EventArgs e)
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

        private void CreateQuery(ConnectionDetail con, string sql, string sourcePlugin)
        { 
            var query = new SqlQueryControl(con, _metadata[con], WorkAsync, msg => SetWorkingMessage(msg), ExecuteMethod, SendOutgoingMessage, sourcePlugin);
            query.InsertText(sql);

            query.Show(dockPanel, WeifenLuo.WinFormsUI.Docking.DockState.Document);
            query.SetFocus();
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
                CreateQuery(con, sql, message.SourcePlugin == "FetchXML Builder" ? null : message.SourcePlugin);
            }
        }

        private FetchXml.FetchType DeserializeFetchXml(string xml)
        {
            var serializer = new XmlSerializer(typeof(FetchXml.FetchType));

            using (var reader = new StringReader(xml))
            {
                return (FetchXml.FetchType) serializer.Deserialize(reader);
            }
        }

        public void SendOutgoingMessage(MessageBusEventArgs args)
        {
            OnOutgoingMessage(this, args);
        }
    }
}