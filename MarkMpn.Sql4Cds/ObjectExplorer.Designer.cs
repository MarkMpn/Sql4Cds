namespace MarkMpn.Sql4Cds
{
    partial class ObjectExplorer
    {
        /// <summary> 
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary> 
        /// Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Component Designer generated code

        /// <summary> 
        /// Required method for Designer support - do not modify 
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            this.components = new System.ComponentModel.Container();
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(ObjectExplorer));
            this.treeView = new System.Windows.Forms.TreeView();
            this.imageList = new System.Windows.Forms.ImageList(this.components);
            this.contextMenuStrip = new System.Windows.Forms.ContextMenuStrip(this.components);
            this.enableTSQLToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.disableTSQLToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.contextMenuStrip.SuspendLayout();
            this.SuspendLayout();
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
            this.treeView.Size = new System.Drawing.Size(284, 261);
            this.treeView.TabIndex = 1;
            this.treeView.BeforeExpand += new System.Windows.Forms.TreeViewCancelEventHandler(this.treeView_BeforeExpand);
            this.treeView.NodeMouseClick += new System.Windows.Forms.TreeNodeMouseClickEventHandler(this.treeView_NodeMouseClick);
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
            this.imageList.Images.SetKeyName(20, "DatabaseStop_16x.png");
            this.imageList.Images.SetKeyName(21, "DatabaseRun_16x.png");
            this.imageList.Images.SetKeyName(22, "DatabaseWarning_16x.png");
            // 
            // contextMenuStrip
            // 
            this.contextMenuStrip.Items.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.enableTSQLToolStripMenuItem,
            this.disableTSQLToolStripMenuItem});
            this.contextMenuStrip.Name = "contextMenuStrip";
            this.contextMenuStrip.Size = new System.Drawing.Size(113, 48);
            this.contextMenuStrip.Opening += new System.ComponentModel.CancelEventHandler(this.contextMenuStrip_Opening);
            // 
            // enableTSQLToolStripMenuItem
            // 
            this.enableTSQLToolStripMenuItem.Name = "enableTSQLToolStripMenuItem";
            this.enableTSQLToolStripMenuItem.Size = new System.Drawing.Size(112, 22);
            this.enableTSQLToolStripMenuItem.Text = "Enable";
            this.enableTSQLToolStripMenuItem.Click += new System.EventHandler(this.enableTSQLToolStripMenuItem_Click);
            // 
            // disableTSQLToolStripMenuItem
            // 
            this.disableTSQLToolStripMenuItem.Name = "disableTSQLToolStripMenuItem";
            this.disableTSQLToolStripMenuItem.Size = new System.Drawing.Size(112, 22);
            this.disableTSQLToolStripMenuItem.Text = "Disable";
            this.disableTSQLToolStripMenuItem.Click += new System.EventHandler(this.disableTSQLToolStripMenuItem_Click);
            // 
            // ObjectExplorer
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(284, 261);
            this.Controls.Add(this.treeView);
            this.Name = "ObjectExplorer";
            this.Text = "Object Explorer";
            this.contextMenuStrip.ResumeLayout(false);
            this.ResumeLayout(false);

        }

        #endregion

        private System.Windows.Forms.TreeView treeView;
        private System.Windows.Forms.ImageList imageList;
        private System.Windows.Forms.ContextMenuStrip contextMenuStrip;
        private System.Windows.Forms.ToolStripMenuItem enableTSQLToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem disableTSQLToolStripMenuItem;
    }
}
