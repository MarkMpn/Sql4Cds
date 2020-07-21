using System;
using System.Collections.Generic;
using System.Drawing;
using System.Data;
using System.Linq;
using System.Windows.Forms;
using MarkMpn.Sql4Cds.Engine;
using McTools.Xrm.Connection;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Metadata;
using XrmToolBox.Extensibility;

namespace MarkMpn.Sql4Cds
{
    public partial class ObjectExplorer : WeifenLuo.WinFormsUI.Docking.DockContent
    {
        private readonly IDictionary<ConnectionDetail, AttributeMetadataCache> _metadata;

        class LoaderParam
        {
            public Func<TreeNode, TreeNode[]> Loader;
            public TreeNode Parent;
        }

        public ObjectExplorer(IDictionary<ConnectionDetail, AttributeMetadataCache> metadata, Action<WorkAsyncInfo> workAsync)
        {
            InitializeComponent();

            _metadata = metadata;
            WorkAsync = workAsync;
        }

        public Action<WorkAsyncInfo> WorkAsync { get; }

        private ConnectionDetail GetService(TreeNode node)
        {
            while (node.Parent != null)
                node = node.Parent;

            var con = (ConnectionDetail)node.Tag;

            return con;
        }

        public ConnectionDetail SelectedConnection
        {
            get
            {
                if (treeView.SelectedNode == null)
                    return null;

                return GetService(treeView.SelectedNode);
            }
        }

        private void SetIcon(TreeNode node, string imageKey)
        {
            node.ImageKey = imageKey;
            node.StateImageKey = imageKey;
            node.SelectedImageKey = imageKey;
        }

        public IEnumerable<Image> GetImages()
        {
            return imageList.Images.OfType<Image>();
        }

        private TreeNode[] LoadEntities(TreeNode parent)
        {
            var metadata = EntityCache.GetEntities(GetService(parent).ServiceClient);

            return metadata
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

        public void AddConnection(ConnectionDetail con)
        {
            var conNode = treeView.Nodes.Add(con.ConnectionName);
            conNode.Tag = con;
            SetIcon(conNode, "Environment");
            var entitiesNode = conNode.Nodes.Add("Entities");
            SetIcon(entitiesNode, "Folder");
            AddVirtualChildNodes(entitiesNode, LoadEntities);
            treeView.SelectedNode = conNode;

            if (new Uri(con.OrganizationServiceUrl).Host.EndsWith(".dynamics.com") &&
                new Version(con.OrganizationVersion) >= new Version("9.1.0.17437"))
            {
                var tsqlNode = conNode.Nodes.Add("T-SQL Endpoint");

                if (TSqlEndpoint.IsEnabled(con.ServiceClient))
                {
                    if (!String.IsNullOrEmpty(con.ServiceClient.CurrentAccessToken))
                    {
                        tsqlNode.ImageIndex = 21;
                        tsqlNode.SelectedImageIndex = 21;
                    }
                    else
                    {
                        tsqlNode.Text += " (Unavailable - OAuth authentication required)";
                        tsqlNode.ImageIndex = 22;
                        tsqlNode.SelectedImageIndex = 22;
                    }
                }
                else
                {
                    tsqlNode.Text += " (Disabled)";
                    tsqlNode.ImageIndex = 20;
                    tsqlNode.SelectedImageIndex = 20;
                }

                tsqlNode.ContextMenuStrip = contextMenuStrip;
            }

            conNode.Expand();
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

        private void AddVirtualChildNodes(TreeNode node, Func<TreeNode, TreeNode[]> loader)
        {
            var child = node.Nodes.Add("Loading...");
            child.ForeColor = SystemColors.HotTrack;
            SetIcon(child, "Loading");
            node.Collapse();
            child.Tag = loader;
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

        private void treeView_BeforeExpand(object sender, TreeViewCancelEventArgs e)
        {
            if (e.Action.HasFlag(TreeViewAction.Expand) && e.Node.Nodes.Count == 1 && e.Node.Nodes[0].Tag is Func<TreeNode, TreeNode[]> loader)
            {
                LoadChildNodes(new LoaderParam { Loader = loader, Parent = e.Node });
            }
        }

        private void treeView_NodeMouseDoubleClick(object sender, TreeNodeMouseClickEventArgs e)
        {
            if (!(e.Node.Tag is EntityMetadata || e.Node.Tag is AttributeMetadata || e.Node.Tag is RelationshipMetadataBase))
                return;

            if (DockPanel.ActiveDocument == null)
                return;

            var query = (SqlQueryControl)DockPanel.ActiveDocument;

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

        private void contextMenuStrip_Opening(object sender, System.ComponentModel.CancelEventArgs e)
        {
            enableTSQLToolStripMenuItem.Enabled = treeView.SelectedNode.ImageIndex == 20;
            disableTSQLToolStripMenuItem.Enabled = treeView.SelectedNode.ImageIndex != 20;
        }

        private void enableTSQLToolStripMenuItem_Click(object sender, EventArgs e)
        {
            var con = GetService(treeView.SelectedNode);
            var node = treeView.SelectedNode;

            WorkAsync(new WorkAsyncInfo
            {
                Message = "Enabling...",
                Work = (worker, args) =>
                {
                    TSqlEndpoint.Enable(con.ServiceClient);
                },
                PostWorkCallBack = (args) =>
                {
                    if (args.Error != null)
                    {
                        MessageBox.Show("Error enabling T-SQL Endpoint:\r\n\r\n" + args.Error.Message, "Error", MessageBoxButtons.OK, MessageBoxIcon.Error);
                        return;
                    }

                    node.Text = "T-SQL Endpoint";

                    if (!String.IsNullOrEmpty(con.ServiceClient.CurrentAccessToken))
                    {
                        node.ImageIndex = 21;
                        node.SelectedImageIndex = 21;
                    }
                    else
                    {
                        node.ImageIndex = 21;
                        node.SelectedImageIndex = 21;
                        node.Text += " (Unavailable - OAuth authentication required)";
                    }
                }
            });
        }

        private void disableTSQLToolStripMenuItem_Click(object sender, EventArgs e)
        {
            var con = GetService(treeView.SelectedNode);
            var node = treeView.SelectedNode;

            WorkAsync(new WorkAsyncInfo
            {
                Message = "Disabling...",
                Work = (worker, args) =>
                {
                    TSqlEndpoint.Disable(con.ServiceClient);
                },
                PostWorkCallBack = (args) =>
                {
                    if (args.Error != null)
                    {
                        MessageBox.Show("Error disabling T-SQL Endpoint:\r\n\r\n" + args.Error.Message, "Error", MessageBoxButtons.OK, MessageBoxIcon.Error);
                        return;
                    }

                    node.Text = "T-SQL Endpoint (Disabled)";
                    node.ImageIndex = 20;
                    node.SelectedImageIndex = 20;
                }
            });
        }

        private void treeView_NodeMouseClick(object sender, TreeNodeMouseClickEventArgs e)
        {
            treeView.SelectedNode = e.Node;
        }
    }
}
