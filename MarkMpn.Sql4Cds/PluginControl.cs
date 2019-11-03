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
        private IDictionary<ConnectionDetail, AttributeMetadataCache> _metadata;

        public PluginControl()
        {
            InitializeComponent();
            _metadata = new Dictionary<ConnectionDetail, AttributeMetadataCache>();
        }

        protected override void OnConnectionUpdated(ConnectionUpdatedEventArgs e)
        {
            AddConnection(e.ConnectionDetail);

            if (tabControl.TabPages.Count == 0)
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
            var conNode = treeView.Nodes.Add(con.ConnectionName);
            conNode.Tag = con;
            SetIcon(conNode, "Environment");
            var entitiesNode = conNode.Nodes.Add("Entities");
            SetIcon(entitiesNode, "Folder");
            AddVirtualChildNodes(entitiesNode, LoadEntities);
            treeView.SelectedNode = conNode;
        }

        private void SetIcon(TreeNode node, string imageKey)
        {
            node.ImageKey = imageKey;
            node.StateImageKey = imageKey;
            node.SelectedImageKey = imageKey;
        }

        private ConnectionDetail GetService(TreeNode node)
        {
            while (node.Parent != null)
                node = node.Parent;

            var con = (ConnectionDetail)node.Tag;

            return con;
        }

        private TreeNode[] LoadEntities(TreeNode parent)
        {
            var metadata = (RetrieveAllEntitiesResponse) GetService(parent).ServiceClient.Execute(new RetrieveAllEntitiesRequest
            {
                EntityFilters = EntityFilters.Entity
            });

            return metadata.EntityMetadata
                .OrderBy(e => e.LogicalName)
                .Select(e =>
                {
                    var node = new TreeNode(e.LogicalName);
                    node.Tag = e;
                    SetIcon(node, "Entity");
                    var attrsNode = node.Nodes.Add("Attributes");
                    SetIcon(attrsNode, "Folder");
                    AddVirtualChildNodes(attrsNode, LoadAttributes);
                    var relsNode = node.Nodes.Add("Relationships");
                    SetIcon(relsNode, "Folder");
                    AddVirtualChildNodes(relsNode, LoadRelationships);
                    return node;
                })
                .ToArray();
        }

        private TreeNode[] LoadAttributes(TreeNode parent)
        {
            var logicalName = parent.Parent.Text;

            var metadata = _metadata[GetService(parent)][logicalName];

            return metadata.Attributes
                .OrderBy(a => a.LogicalName)
                .Select(a =>
                {
                    var node = new TreeNode(a.LogicalName);
                    node.Tag = a;
                    SetIcon(node, GetIconType(a));
                    return node;
                })
                .ToArray();
        }

        private TreeNode[] LoadRelationships(TreeNode parent)
        {
            var logicalName = parent.Parent.Text;

            var metadata = (RetrieveEntityResponse)GetService(parent).ServiceClient.Execute(new RetrieveEntityRequest
            {
                LogicalName = logicalName,
                EntityFilters = EntityFilters.Relationships
            });

            return metadata.EntityMetadata.OneToManyRelationships.Select(r =>
                {
                    var node = new TreeNode(r.SchemaName);
                    node.Tag = r;
                    SetIcon(node, "OneToMany");
                    return node;
                })
                .Union(metadata.EntityMetadata.ManyToOneRelationships.Select(r =>
                {
                    var node = new TreeNode(r.SchemaName);
                    node.Tag = r;
                    SetIcon(node, "ManyToOne");
                    return node;
                }))
                .Union(metadata.EntityMetadata.ManyToManyRelationships.Select(r =>
                {
                    var node = new TreeNode(r.SchemaName);
                    node.Tag = r;
                    SetIcon(node, "ManyToMany");
                    return node;
                }))
                .OrderBy(node => node.Text)
                .ToArray();
        }

        private string GetIconType(AttributeMetadata a)
        {
            switch (a.AttributeType.Value)
            {
                case AttributeTypeCode.BigInt:
                case AttributeTypeCode.Integer:
                    return "Integer";

                case AttributeTypeCode.Boolean:
                case AttributeTypeCode.Picklist:
                case AttributeTypeCode.State:
                case AttributeTypeCode.Status:
                    return "OptionSet";

                case AttributeTypeCode.Customer:
                case AttributeTypeCode.Owner:
                case AttributeTypeCode.PartyList:
                    return "Owner";

                case AttributeTypeCode.DateTime:
                    return "DateTime";

                case AttributeTypeCode.Decimal:
                    return "Decimal";

                case AttributeTypeCode.Double:
                    return "Double";

                case AttributeTypeCode.Lookup:
                    return "Lookup";

                case AttributeTypeCode.Memo:
                    return "Multiline";

                case AttributeTypeCode.Money:
                    return "Currency";

                case AttributeTypeCode.String:
                case AttributeTypeCode.Virtual:
                    return "Text";

                case AttributeTypeCode.Uniqueidentifier:
                    return "UniqueIdentifier";

                default:
                    return null;
            }
        }

        private void AddVirtualChildNodes(TreeNode node, Func<TreeNode,TreeNode[]> loader)
        {
            var child = node.Nodes.Add("Loading...");
            child.ForeColor = SystemColors.HotTrack;
            SetIcon(child, "Loading");
            node.Collapse();
            child.Tag = loader;
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
            if (tabControl.TabPages.Count == 0)
                return;

            var query = (SqlQueryControl)tabControl.SelectedTab.Controls[0];
            query.Execute(true);
        }

        private void tsbPreviewFetchXml_Click(object sender, EventArgs e)
        {
            if (tabControl.TabPages.Count == 0)
                return;

            var query = (SqlQueryControl)tabControl.SelectedTab.Controls[0];
            query.Execute(false);
        }

        class LoaderParam
        {
            public Func<TreeNode,TreeNode[]> Loader;
            public TreeNode Parent;
        }

        private void treeView_BeforeExpand(object sender, TreeViewCancelEventArgs e)
        {
            if (e.Action.HasFlag(TreeViewAction.Expand) && e.Node.Nodes.Count == 1 && e.Node.Nodes[0].Tag is Func<TreeNode,TreeNode[]> loader)
            {
                LoadChildNodes(new LoaderParam { Loader = loader, Parent = e.Node });
            }
        }

        private void LoadChildNodes(LoaderParam loader)
        {
            WorkAsync(new WorkAsyncInfo
            {
                Message = "Loading...",
                Work = (worker, args) =>
                {
                    args.Result = loader.Loader(loader.Parent);
                },
                PostWorkCallBack = (args) =>
                {
                    if (args.Error != null)
                    {
                        MessageBox.Show(args.Error.ToString(), "Error", MessageBoxButtons.OK, MessageBoxIcon.Error);
                    }
                    var result = args.Result as TreeNode[];
                    if (result != null)
                    {
                        loader.Parent.TreeView.BeginUpdate();

                        foreach (var child in result)
                            loader.Parent.Nodes.Add(child);

                        loader.Parent.Nodes.RemoveAt(0);

                        loader.Parent.TreeView.EndUpdate();
                    }
                }
            });
        }

        private void tsbConnect_Click(object sender, EventArgs e)
        {
            AddAdditionalOrganization();
        }

        private void tsbNewQuery_Click(object sender, EventArgs e)
        {
            if (treeView.SelectedNode == null)
                return;

            var node = treeView.SelectedNode;
            while (node.Parent != null)
                node = node.Parent;
            
            CreateQuery((ConnectionDetail)node.Tag, "", null);
        }

        private void CreateQuery(ConnectionDetail con, string sql, string sourcePlugin)
        { 
            var query = new SqlQueryControl(con.ServiceClient, _metadata[con], WorkAsync, msg => SetWorkingMessage(msg), ExecuteMethod, SendOutgoingMessage, sourcePlugin);
            query.InsertText(sql);
            var tabPage = new TabPage(con.ConnectionName);
            tabPage.Controls.Add(query);
            query.Dock = DockStyle.Fill;
            tabControl.TabPages.Add(tabPage);
            tabControl.SelectedTab = tabPage;
            query.SetFocus();
        }

        private void treeView_NodeMouseDoubleClick(object sender, TreeNodeMouseClickEventArgs e)
        {
            if (!(e.Node.Tag is EntityMetadata || e.Node.Tag is AttributeMetadata || e.Node.Tag is RelationshipMetadataBase))
                return;

            if (tabControl.TabPages.Count == 0)
                return;

            var query = (SqlQueryControl)tabControl.SelectedTab.Controls[0];

            if (e.Node.Tag is OneToManyRelationshipMetadata oneToMany)
            {
                var join = $@"
{oneToMany.ReferencingEntity}
INNER JOIN {oneToMany.ReferencedEntity}
    ON {oneToMany.ReferencingEntity}.{oneToMany.ReferencingAttribute} = {oneToMany.ReferencedEntity}.{oneToMany.ReferencedAttribute}";

                query.InsertText(join);
            }
            else if (e.Node.Tag is ManyToManyRelationshipMetadata manyToMany)
            {
                var con = GetService(e.Node);
                var entity1 = (RetrieveEntityResponse)con.ServiceClient.Execute(new RetrieveEntityRequest
                {
                    LogicalName = manyToMany.Entity1LogicalName,
                    EntityFilters = EntityFilters.Entity
                });
                var entity2 = (RetrieveEntityResponse)con.ServiceClient.Execute(new RetrieveEntityRequest
                {
                    LogicalName = manyToMany.Entity2LogicalName,
                    EntityFilters = EntityFilters.Entity
                });

                var join = $@"
{manyToMany.Entity1LogicalName}
INNER JOIN {manyToMany.IntersectEntityName}
    ON {manyToMany.Entity1LogicalName}.{entity1.EntityMetadata.PrimaryIdAttribute} = {manyToMany.IntersectEntityName}.{manyToMany.Entity1IntersectAttribute}
INNER JOIN {manyToMany.Entity2LogicalName}
    ON {manyToMany.Entity2LogicalName}.{entity2.EntityMetadata.PrimaryIdAttribute} = {manyToMany.IntersectEntityName}.{manyToMany.Entity2IntersectAttribute}";

                query.InsertText(join);
            }
            else
            {
                query.InsertText(e.Node.Text);
            }
        }

        private void tsbFormat_Click(object sender, EventArgs e)
        {
            if (tabControl.TabPages.Count == 0)
                return;

            var query = (SqlQueryControl)tabControl.SelectedTab.Controls[0];
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

            if (treeView.SelectedNode == null)
                return;

            var node = treeView.SelectedNode;
            while (node.Parent != null)
                node = node.Parent;

            var con = (ConnectionDetail)node.Tag;
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