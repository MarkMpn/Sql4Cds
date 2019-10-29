namespace MarkMpn.Sql4Cds
{
    partial class PluginControl
    {
        /// <summary> 
        /// Variable nécessaire au concepteur.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary> 
        /// Nettoyage des ressources utilisées.
        /// </summary>
        /// <param name="disposing">true si les ressources managées doivent être supprimées ; sinon, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Code généré par le Concepteur de composants

        /// <summary> 
        /// Méthode requise pour la prise en charge du concepteur - ne modifiez pas 
        /// le contenu de cette méthode avec l'éditeur de code.
        /// </summary>
        private void InitializeComponent()
        {
            this.components = new System.ComponentModel.Container();
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(PluginControl));
            this.toolStripMenu = new System.Windows.Forms.ToolStrip();
            this.tsbConnect = new System.Windows.Forms.ToolStripButton();
            this.toolStripSeparator = new System.Windows.Forms.ToolStripSeparator();
            this.tsbNewQuery = new System.Windows.Forms.ToolStripButton();
            this.tsbExecute = new System.Windows.Forms.ToolStripButton();
            this.tssSeparator1 = new System.Windows.Forms.ToolStripSeparator();
            this.tsbFormat = new System.Windows.Forms.ToolStripButton();
            this.toolStripSeparator1 = new System.Windows.Forms.ToolStripSeparator();
            this.tsbSettings = new System.Windows.Forms.ToolStripButton();
            this.tsbClose = new System.Windows.Forms.ToolStripButton();
            this.tslAboutLink = new System.Windows.Forms.ToolStripLabel();
            this.splitContainer = new System.Windows.Forms.SplitContainer();
            this.treeView = new System.Windows.Forms.TreeView();
            this.imageList = new System.Windows.Forms.ImageList(this.components);
            this.tabControl = new System.Windows.Forms.TabControl();
            this.tsbPreviewFetchXml = new System.Windows.Forms.ToolStripButton();
            this.toolStripMenu.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)(this.splitContainer)).BeginInit();
            this.splitContainer.Panel1.SuspendLayout();
            this.splitContainer.Panel2.SuspendLayout();
            this.splitContainer.SuspendLayout();
            this.SuspendLayout();
            // 
            // toolStripMenu
            // 
            this.toolStripMenu.ImageScalingSize = new System.Drawing.Size(24, 24);
            this.toolStripMenu.Items.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.tsbConnect,
            this.toolStripSeparator,
            this.tsbNewQuery,
            this.tsbExecute,
            this.tsbPreviewFetchXml,
            this.tssSeparator1,
            this.tsbFormat,
            this.toolStripSeparator1,
            this.tsbSettings,
            this.tsbClose,
            this.tslAboutLink});
            this.toolStripMenu.Location = new System.Drawing.Point(0, 0);
            this.toolStripMenu.Name = "toolStripMenu";
            this.toolStripMenu.Padding = new System.Windows.Forms.Padding(0, 0, 2, 0);
            this.toolStripMenu.Size = new System.Drawing.Size(861, 31);
            this.toolStripMenu.TabIndex = 4;
            this.toolStripMenu.Text = "toolStrip1";
            // 
            // tsbConnect
            // 
            this.tsbConnect.Image = ((System.Drawing.Image)(resources.GetObject("tsbConnect.Image")));
            this.tsbConnect.ImageTransparentColor = System.Drawing.Color.White;
            this.tsbConnect.Name = "tsbConnect";
            this.tsbConnect.Size = new System.Drawing.Size(80, 28);
            this.tsbConnect.Text = "Connect";
            this.tsbConnect.ToolTipText = "Connect to Environment";
            this.tsbConnect.Click += new System.EventHandler(this.tsbConnect_Click);
            // 
            // toolStripSeparator
            // 
            this.toolStripSeparator.Name = "toolStripSeparator";
            this.toolStripSeparator.Size = new System.Drawing.Size(6, 31);
            // 
            // tsbNewQuery
            // 
            this.tsbNewQuery.Image = ((System.Drawing.Image)(resources.GetObject("tsbNewQuery.Image")));
            this.tsbNewQuery.ImageTransparentColor = System.Drawing.Color.White;
            this.tsbNewQuery.Name = "tsbNewQuery";
            this.tsbNewQuery.Size = new System.Drawing.Size(94, 28);
            this.tsbNewQuery.Text = "New Query";
            this.tsbNewQuery.ToolTipText = "Create New Query Tab";
            this.tsbNewQuery.Click += new System.EventHandler(this.tsbNewQuery_Click);
            // 
            // tsbExecute
            // 
            this.tsbExecute.Image = ((System.Drawing.Image)(resources.GetObject("tsbExecute.Image")));
            this.tsbExecute.ImageTransparentColor = System.Drawing.Color.White;
            this.tsbExecute.Name = "tsbExecute";
            this.tsbExecute.Size = new System.Drawing.Size(75, 28);
            this.tsbExecute.Text = "Execute";
            this.tsbExecute.ToolTipText = "Execute Selected Query";
            this.tsbExecute.Click += new System.EventHandler(this.tsbExecute_Click);
            // 
            // tssSeparator1
            // 
            this.tssSeparator1.Name = "tssSeparator1";
            this.tssSeparator1.Size = new System.Drawing.Size(6, 31);
            // 
            // tsbFormat
            // 
            this.tsbFormat.Image = ((System.Drawing.Image)(resources.GetObject("tsbFormat.Image")));
            this.tsbFormat.ImageTransparentColor = System.Drawing.Color.Magenta;
            this.tsbFormat.Name = "tsbFormat";
            this.tsbFormat.Size = new System.Drawing.Size(73, 28);
            this.tsbFormat.Text = "Format";
            this.tsbFormat.ToolTipText = "Reformat SQL Query";
            this.tsbFormat.Click += new System.EventHandler(this.tsbFormat_Click);
            // 
            // toolStripSeparator1
            // 
            this.toolStripSeparator1.Name = "toolStripSeparator1";
            this.toolStripSeparator1.Size = new System.Drawing.Size(6, 31);
            // 
            // tsbSettings
            // 
            this.tsbSettings.DisplayStyle = System.Windows.Forms.ToolStripItemDisplayStyle.Text;
            this.tsbSettings.Image = ((System.Drawing.Image)(resources.GetObject("tsbSettings.Image")));
            this.tsbSettings.ImageTransparentColor = System.Drawing.Color.Magenta;
            this.tsbSettings.Name = "tsbSettings";
            this.tsbSettings.Size = new System.Drawing.Size(53, 28);
            this.tsbSettings.Text = "Settings";
            this.tsbSettings.Click += new System.EventHandler(this.tsbSettings_Click);
            // 
            // tsbClose
            // 
            this.tsbClose.DisplayStyle = System.Windows.Forms.ToolStripItemDisplayStyle.Text;
            this.tsbClose.Name = "tsbClose";
            this.tsbClose.Size = new System.Drawing.Size(40, 28);
            this.tsbClose.Text = "Close";
            this.tsbClose.Click += new System.EventHandler(this.tsbClose_Click);
            // 
            // tslAboutLink
            // 
            this.tslAboutLink.Alignment = System.Windows.Forms.ToolStripItemAlignment.Right;
            this.tslAboutLink.IsLink = true;
            this.tslAboutLink.Name = "tslAboutLink";
            this.tslAboutLink.Size = new System.Drawing.Size(168, 28);
            this.tslAboutLink.Text = "SQL 4 CDS by Mark Carrington";
            this.tslAboutLink.Click += new System.EventHandler(this.tslAboutLink_Click);
            // 
            // splitContainer
            // 
            this.splitContainer.Dock = System.Windows.Forms.DockStyle.Fill;
            this.splitContainer.Location = new System.Drawing.Point(0, 31);
            this.splitContainer.Margin = new System.Windows.Forms.Padding(2);
            this.splitContainer.Name = "splitContainer";
            // 
            // splitContainer.Panel1
            // 
            this.splitContainer.Panel1.Controls.Add(this.treeView);
            // 
            // splitContainer.Panel2
            // 
            this.splitContainer.Panel2.Controls.Add(this.tabControl);
            this.splitContainer.Size = new System.Drawing.Size(861, 472);
            this.splitContainer.SplitterDistance = 287;
            this.splitContainer.SplitterWidth = 2;
            this.splitContainer.TabIndex = 5;
            // 
            // treeView
            // 
            this.treeView.Dock = System.Windows.Forms.DockStyle.Fill;
            this.treeView.ImageIndex = 0;
            this.treeView.ImageList = this.imageList;
            this.treeView.Location = new System.Drawing.Point(0, 0);
            this.treeView.Margin = new System.Windows.Forms.Padding(2);
            this.treeView.Name = "treeView";
            this.treeView.SelectedImageIndex = 0;
            this.treeView.Size = new System.Drawing.Size(287, 472);
            this.treeView.TabIndex = 0;
            this.treeView.BeforeExpand += new System.Windows.Forms.TreeViewCancelEventHandler(this.treeView_BeforeExpand);
            this.treeView.NodeMouseDoubleClick += new System.Windows.Forms.TreeNodeMouseClickEventHandler(this.treeView_NodeMouseDoubleClick);
            // 
            // imageList
            // 
            this.imageList.ImageStream = ((System.Windows.Forms.ImageListStreamer)(resources.GetObject("imageList.ImageStream")));
            this.imageList.TransparentColor = System.Drawing.Color.Transparent;
            this.imageList.Images.SetKeyName(0, "Currency");
            this.imageList.Images.SetKeyName(1, "DateOnly");
            this.imageList.Images.SetKeyName(2, "DateTime");
            this.imageList.Images.SetKeyName(3, "Decimal");
            this.imageList.Images.SetKeyName(4, "Entity");
            this.imageList.Images.SetKeyName(5, "Environment");
            this.imageList.Images.SetKeyName(6, "Float");
            this.imageList.Images.SetKeyName(7, "Image");
            this.imageList.Images.SetKeyName(8, "Integer");
            this.imageList.Images.SetKeyName(9, "Lookup");
            this.imageList.Images.SetKeyName(10, "Multiline");
            this.imageList.Images.SetKeyName(11, "OptionSet");
            this.imageList.Images.SetKeyName(12, "Owner");
            this.imageList.Images.SetKeyName(13, "Text");
            this.imageList.Images.SetKeyName(14, "UniqueIdentifier");
            this.imageList.Images.SetKeyName(15, "Folder");
            this.imageList.Images.SetKeyName(16, "Loading");
            this.imageList.Images.SetKeyName(17, "ManyToMany");
            this.imageList.Images.SetKeyName(18, "ManyToOne");
            this.imageList.Images.SetKeyName(19, "OneToMany");
            // 
            // tabControl
            // 
            this.tabControl.Dock = System.Windows.Forms.DockStyle.Fill;
            this.tabControl.Location = new System.Drawing.Point(0, 0);
            this.tabControl.Margin = new System.Windows.Forms.Padding(2);
            this.tabControl.Name = "tabControl";
            this.tabControl.SelectedIndex = 0;
            this.tabControl.Size = new System.Drawing.Size(572, 472);
            this.tabControl.TabIndex = 0;
            // 
            // tsbPreviewFetchXml
            // 
            this.tsbPreviewFetchXml.DisplayStyle = System.Windows.Forms.ToolStripItemDisplayStyle.Image;
            this.tsbPreviewFetchXml.Image = ((System.Drawing.Image)(resources.GetObject("tsbPreviewFetchXml.Image")));
            this.tsbPreviewFetchXml.ImageTransparentColor = System.Drawing.Color.Magenta;
            this.tsbPreviewFetchXml.Name = "tsbPreviewFetchXml";
            this.tsbPreviewFetchXml.Size = new System.Drawing.Size(28, 28);
            this.tsbPreviewFetchXml.Text = "Preview FetchXml";
            this.tsbPreviewFetchXml.ToolTipText = "Display FetchXml Without Executing Query";
            this.tsbPreviewFetchXml.Click += new System.EventHandler(this.tsbPreviewFetchXml_Click);
            // 
            // PluginControl
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.Controls.Add(this.splitContainer);
            this.Controls.Add(this.toolStripMenu);
            this.Margin = new System.Windows.Forms.Padding(2, 3, 2, 3);
            this.Name = "PluginControl";
            this.Size = new System.Drawing.Size(861, 503);
            this.Load += new System.EventHandler(this.MyPluginControl_Load);
            this.toolStripMenu.ResumeLayout(false);
            this.toolStripMenu.PerformLayout();
            this.splitContainer.Panel1.ResumeLayout(false);
            this.splitContainer.Panel2.ResumeLayout(false);
            ((System.ComponentModel.ISupportInitialize)(this.splitContainer)).EndInit();
            this.splitContainer.ResumeLayout(false);
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion
        private System.Windows.Forms.ToolStrip toolStripMenu;
        private System.Windows.Forms.ToolStripButton tsbClose;
        private System.Windows.Forms.ToolStripSeparator tssSeparator1;
        private System.Windows.Forms.SplitContainer splitContainer;
        private System.Windows.Forms.TreeView treeView;
        private System.Windows.Forms.ToolStripButton tsbExecute;
        private System.Windows.Forms.ToolStripButton tsbConnect;
        private System.Windows.Forms.ToolStripSeparator toolStripSeparator;
        private System.Windows.Forms.ToolStripButton tsbNewQuery;
        private System.Windows.Forms.TabControl tabControl;
        private System.Windows.Forms.ImageList imageList;
        private System.Windows.Forms.ToolStripLabel tslAboutLink;
        private System.Windows.Forms.ToolStripButton tsbFormat;
        private System.Windows.Forms.ToolStripSeparator toolStripSeparator1;
        private System.Windows.Forms.ToolStripButton tsbSettings;
        private System.Windows.Forms.ToolStripButton tsbPreviewFetchXml;
    }
}
