namespace MarkMpn.Sql4Cds.XTB
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
            this.tsqlContextMenuStrip = new System.Windows.Forms.ContextMenuStrip(this.components);
            this.enableTSQLToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.disableTSQLToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.serverContextMenuStrip = new System.Windows.Forms.ContextMenuStrip(this.components);
            this.disconnectToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.toolStripMenuItem1 = new System.Windows.Forms.ToolStripSeparator();
            this.newQueryToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.toolStripSeparator1 = new System.Windows.Forms.ToolStripSeparator();
            this.refreshToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.toolStrip1 = new System.Windows.Forms.ToolStrip();
            this.tsbConnect = new System.Windows.Forms.ToolStripButton();
            this.tsbDisconnect = new System.Windows.Forms.ToolStripButton();
            this.tableContextMenuStrip = new System.Windows.Forms.ContextMenuStrip(this.components);
            this.selectTop1000RowsToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.scriptTableasToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.scriptAsTypeContextMenuStrip = new System.Windows.Forms.ContextMenuStrip(this.components);
            this.selectToToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.scriptAsTargetsContextMenuStrip = new System.Windows.Forms.ContextMenuStrip(this.components);
            this.newQueryEditorWindowToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.toolStripMenuItem2 = new System.Windows.Forms.ToolStripSeparator();
            this.fileToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.clipboardToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.executeToToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.insertToToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.updateToToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.deleteToToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.toolStripMenuItem5 = new System.Windows.Forms.ToolStripSeparator();
            this.toolStripMenuItem3 = new System.Windows.Forms.ToolStripMenuItem();
            this.toolStripMenuItem4 = new System.Windows.Forms.ToolStripMenuItem();
            this.functionContextMenuStrip = new System.Windows.Forms.ContextMenuStrip(this.components);
            this.procedureContextMenuStrip = new System.Windows.Forms.ContextMenuStrip(this.components);
            this.tsqlContextMenuStrip.SuspendLayout();
            this.serverContextMenuStrip.SuspendLayout();
            this.toolStrip1.SuspendLayout();
            this.tableContextMenuStrip.SuspendLayout();
            this.scriptAsTypeContextMenuStrip.SuspendLayout();
            this.scriptAsTargetsContextMenuStrip.SuspendLayout();
            this.functionContextMenuStrip.SuspendLayout();
            this.procedureContextMenuStrip.SuspendLayout();
            this.SuspendLayout();
            // 
            // treeView
            // 
            this.treeView.Dock = System.Windows.Forms.DockStyle.Fill;
            this.treeView.ImageIndex = 0;
            this.treeView.ImageList = this.imageList;
            this.treeView.Location = new System.Drawing.Point(0, 25);
            this.treeView.Margin = new System.Windows.Forms.Padding(2);
            this.treeView.Name = "treeView";
            this.treeView.SelectedImageIndex = 0;
            this.treeView.Size = new System.Drawing.Size(284, 236);
            this.treeView.TabIndex = 1;
            this.treeView.BeforeExpand += new System.Windows.Forms.TreeViewCancelEventHandler(this.treeView_BeforeExpand);
            this.treeView.AfterSelect += new System.Windows.Forms.TreeViewEventHandler(this.treeView_AfterSelect);
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
            this.imageList.Images.SetKeyName(23, "Method_16x.png");
            this.imageList.Images.SetKeyName(24, "Aggregate_16x.png");
            this.imageList.Images.SetKeyName(25, "TableFunction_16x.png");
            this.imageList.Images.SetKeyName(26, "StoredProcedureScript_16x.png");
            this.imageList.Images.SetKeyName(27, "Event.png");
            // 
            // tsqlContextMenuStrip
            // 
            this.tsqlContextMenuStrip.ImageScalingSize = new System.Drawing.Size(32, 32);
            this.tsqlContextMenuStrip.Items.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.enableTSQLToolStripMenuItem,
            this.disableTSQLToolStripMenuItem});
            this.tsqlContextMenuStrip.Name = "contextMenuStrip";
            this.tsqlContextMenuStrip.Size = new System.Drawing.Size(113, 48);
            this.tsqlContextMenuStrip.Opening += new System.ComponentModel.CancelEventHandler(this.contextMenuStrip_Opening);
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
            // serverContextMenuStrip
            // 
            this.serverContextMenuStrip.ImageScalingSize = new System.Drawing.Size(32, 32);
            this.serverContextMenuStrip.Items.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.disconnectToolStripMenuItem,
            this.toolStripMenuItem1,
            this.newQueryToolStripMenuItem,
            this.toolStripSeparator1,
            this.refreshToolStripMenuItem});
            this.serverContextMenuStrip.Name = "serverContextMenuStrip";
            this.serverContextMenuStrip.Size = new System.Drawing.Size(134, 82);
            this.serverContextMenuStrip.Opening += new System.ComponentModel.CancelEventHandler(this.serverContextMenuStrip_Opening);
            // 
            // disconnectToolStripMenuItem
            // 
            this.disconnectToolStripMenuItem.Name = "disconnectToolStripMenuItem";
            this.disconnectToolStripMenuItem.Size = new System.Drawing.Size(133, 22);
            this.disconnectToolStripMenuItem.Text = "Disconnect";
            this.disconnectToolStripMenuItem.Click += new System.EventHandler(this.disconnectToolStripMenuItem_Click);
            // 
            // toolStripMenuItem1
            // 
            this.toolStripMenuItem1.Name = "toolStripMenuItem1";
            this.toolStripMenuItem1.Size = new System.Drawing.Size(130, 6);
            // 
            // newQueryToolStripMenuItem
            // 
            this.newQueryToolStripMenuItem.Name = "newQueryToolStripMenuItem";
            this.newQueryToolStripMenuItem.Size = new System.Drawing.Size(133, 22);
            this.newQueryToolStripMenuItem.Text = "New Query";
            this.newQueryToolStripMenuItem.Click += new System.EventHandler(this.newQueryToolStripMenuItem_Click);
            // 
            // toolStripSeparator1
            // 
            this.toolStripSeparator1.Name = "toolStripSeparator1";
            this.toolStripSeparator1.Size = new System.Drawing.Size(130, 6);
            // 
            // refreshToolStripMenuItem
            // 
            this.refreshToolStripMenuItem.Name = "refreshToolStripMenuItem";
            this.refreshToolStripMenuItem.Size = new System.Drawing.Size(133, 22);
            this.refreshToolStripMenuItem.Text = "Refresh";
            this.refreshToolStripMenuItem.Click += new System.EventHandler(this.refreshToolStripMenuItem_Click);
            // 
            // toolStrip1
            // 
            this.toolStrip1.GripStyle = System.Windows.Forms.ToolStripGripStyle.Hidden;
            this.toolStrip1.Items.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.tsbConnect,
            this.tsbDisconnect});
            this.toolStrip1.Location = new System.Drawing.Point(0, 0);
            this.toolStrip1.Name = "toolStrip1";
            this.toolStrip1.Size = new System.Drawing.Size(284, 25);
            this.toolStrip1.TabIndex = 2;
            this.toolStrip1.Text = "toolStrip1";
            // 
            // tsbConnect
            // 
            this.tsbConnect.DisplayStyle = System.Windows.Forms.ToolStripItemDisplayStyle.Image;
            this.tsbConnect.Image = global::MarkMpn.Sql4Cds.XTB.Properties.Resources.ConnectFilled_grey_16x;
            this.tsbConnect.ImageTransparentColor = System.Drawing.Color.Magenta;
            this.tsbConnect.Name = "tsbConnect";
            this.tsbConnect.Size = new System.Drawing.Size(23, 22);
            this.tsbConnect.Text = "Connect";
            this.tsbConnect.Click += new System.EventHandler(this.tsbConnect_Click);
            // 
            // tsbDisconnect
            // 
            this.tsbDisconnect.DisplayStyle = System.Windows.Forms.ToolStripItemDisplayStyle.Image;
            this.tsbDisconnect.Enabled = false;
            this.tsbDisconnect.Image = ((System.Drawing.Image)(resources.GetObject("tsbDisconnect.Image")));
            this.tsbDisconnect.ImageTransparentColor = System.Drawing.Color.Magenta;
            this.tsbDisconnect.Name = "tsbDisconnect";
            this.tsbDisconnect.Size = new System.Drawing.Size(23, 22);
            this.tsbDisconnect.Text = "Disconnect";
            this.tsbDisconnect.Click += new System.EventHandler(this.disconnectToolStripMenuItem_Click);
            // 
            // tableContextMenuStrip
            // 
            this.tableContextMenuStrip.Items.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.selectTop1000RowsToolStripMenuItem,
            this.scriptTableasToolStripMenuItem});
            this.tableContextMenuStrip.Name = "tableContextMenuStrip";
            this.tableContextMenuStrip.Size = new System.Drawing.Size(187, 48);
            this.tableContextMenuStrip.Opening += new System.ComponentModel.CancelEventHandler(this.tableContextMenuStrip_Opening);
            // 
            // selectTop1000RowsToolStripMenuItem
            // 
            this.selectTop1000RowsToolStripMenuItem.Name = "selectTop1000RowsToolStripMenuItem";
            this.selectTop1000RowsToolStripMenuItem.Size = new System.Drawing.Size(186, 22);
            this.selectTop1000RowsToolStripMenuItem.Text = "Select Top 1000 Rows";
            this.selectTop1000RowsToolStripMenuItem.Click += new System.EventHandler(this.selectTop1000RowsToolStripMenuItem_Click);
            // 
            // scriptTableasToolStripMenuItem
            // 
            this.scriptTableasToolStripMenuItem.DropDown = this.scriptAsTypeContextMenuStrip;
            this.scriptTableasToolStripMenuItem.Name = "scriptTableasToolStripMenuItem";
            this.scriptTableasToolStripMenuItem.Size = new System.Drawing.Size(186, 22);
            this.scriptTableasToolStripMenuItem.Text = "Script Table as";
            // 
            // scriptAsTypeContextMenuStrip
            // 
            this.scriptAsTypeContextMenuStrip.Items.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.selectToToolStripMenuItem,
            this.insertToToolStripMenuItem,
            this.updateToToolStripMenuItem,
            this.deleteToToolStripMenuItem,
            this.toolStripMenuItem5,
            this.executeToToolStripMenuItem});
            this.scriptAsTypeContextMenuStrip.Name = "tableContextMenuStrip";
            this.scriptAsTypeContextMenuStrip.OwnerItem = this.toolStripMenuItem4;
            this.scriptAsTypeContextMenuStrip.Size = new System.Drawing.Size(139, 120);
            // 
            // selectToToolStripMenuItem
            // 
            this.selectToToolStripMenuItem.DropDown = this.scriptAsTargetsContextMenuStrip;
            this.selectToToolStripMenuItem.Name = "selectToToolStripMenuItem";
            this.selectToToolStripMenuItem.Size = new System.Drawing.Size(138, 22);
            this.selectToToolStripMenuItem.Text = "SELECT To";
            this.selectToToolStripMenuItem.DropDownOpening += new System.EventHandler(this.scriptTableAsTargetsContextMenuStripOpening);
            // 
            // scriptAsTargetsContextMenuStrip
            // 
            this.scriptAsTargetsContextMenuStrip.Items.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.newQueryEditorWindowToolStripMenuItem,
            this.toolStripMenuItem2,
            this.fileToolStripMenuItem,
            this.clipboardToolStripMenuItem});
            this.scriptAsTargetsContextMenuStrip.Name = "scriptTableAsTargetsContextMenuStrip";
            this.scriptAsTargetsContextMenuStrip.OwnerItem = this.deleteToToolStripMenuItem;
            this.scriptAsTargetsContextMenuStrip.Size = new System.Drawing.Size(215, 76);
            // 
            // newQueryEditorWindowToolStripMenuItem
            // 
            this.newQueryEditorWindowToolStripMenuItem.Image = global::MarkMpn.Sql4Cds.XTB.Properties.Resources.ScriptManager;
            this.newQueryEditorWindowToolStripMenuItem.Name = "newQueryEditorWindowToolStripMenuItem";
            this.newQueryEditorWindowToolStripMenuItem.Size = new System.Drawing.Size(214, 22);
            this.newQueryEditorWindowToolStripMenuItem.Text = "New Query Editor Window";
            this.newQueryEditorWindowToolStripMenuItem.Click += new System.EventHandler(this.newQueryEditorWindowToolStripMenuItem_Click);
            // 
            // toolStripMenuItem2
            // 
            this.toolStripMenuItem2.Name = "toolStripMenuItem2";
            this.toolStripMenuItem2.Size = new System.Drawing.Size(211, 6);
            // 
            // fileToolStripMenuItem
            // 
            this.fileToolStripMenuItem.Image = global::MarkMpn.Sql4Cds.XTB.Properties.Resources.ExportScript;
            this.fileToolStripMenuItem.Name = "fileToolStripMenuItem";
            this.fileToolStripMenuItem.Size = new System.Drawing.Size(214, 22);
            this.fileToolStripMenuItem.Text = "File...";
            this.fileToolStripMenuItem.Click += new System.EventHandler(this.fileToolStripMenuItem_Click);
            // 
            // clipboardToolStripMenuItem
            // 
            this.clipboardToolStripMenuItem.Image = global::MarkMpn.Sql4Cds.XTB.Properties.Resources.Copy;
            this.clipboardToolStripMenuItem.Name = "clipboardToolStripMenuItem";
            this.clipboardToolStripMenuItem.Size = new System.Drawing.Size(214, 22);
            this.clipboardToolStripMenuItem.Text = "Clipboard";
            this.clipboardToolStripMenuItem.Click += new System.EventHandler(this.clipboardToolStripMenuItem_Click);
            // 
            // executeToToolStripMenuItem
            // 
            this.executeToToolStripMenuItem.DropDown = this.scriptAsTargetsContextMenuStrip;
            this.executeToToolStripMenuItem.Name = "executeToToolStripMenuItem";
            this.executeToToolStripMenuItem.Size = new System.Drawing.Size(138, 22);
            this.executeToToolStripMenuItem.Text = "EXECUTE To";
            this.executeToToolStripMenuItem.DropDownOpening += new System.EventHandler(this.scriptTableAsTargetsContextMenuStripOpening);
            // 
            // insertToToolStripMenuItem
            // 
            this.insertToToolStripMenuItem.DropDown = this.scriptAsTargetsContextMenuStrip;
            this.insertToToolStripMenuItem.Name = "insertToToolStripMenuItem";
            this.insertToToolStripMenuItem.Size = new System.Drawing.Size(138, 22);
            this.insertToToolStripMenuItem.Text = "INSERT To";
            this.insertToToolStripMenuItem.DropDownOpening += new System.EventHandler(this.scriptTableAsTargetsContextMenuStripOpening);
            // 
            // updateToToolStripMenuItem
            // 
            this.updateToToolStripMenuItem.DropDown = this.scriptAsTargetsContextMenuStrip;
            this.updateToToolStripMenuItem.Name = "updateToToolStripMenuItem";
            this.updateToToolStripMenuItem.Size = new System.Drawing.Size(138, 22);
            this.updateToToolStripMenuItem.Text = "UPDATE To";
            this.updateToToolStripMenuItem.DropDownOpening += new System.EventHandler(this.scriptTableAsTargetsContextMenuStripOpening);
            // 
            // deleteToToolStripMenuItem
            // 
            this.deleteToToolStripMenuItem.DropDown = this.scriptAsTargetsContextMenuStrip;
            this.deleteToToolStripMenuItem.Name = "deleteToToolStripMenuItem";
            this.deleteToToolStripMenuItem.Size = new System.Drawing.Size(138, 22);
            this.deleteToToolStripMenuItem.Text = "DELETE To";
            this.deleteToToolStripMenuItem.DropDownOpening += new System.EventHandler(this.scriptTableAsTargetsContextMenuStripOpening);
            // 
            // toolStripMenuItem5
            // 
            this.toolStripMenuItem5.Name = "toolStripMenuItem5";
            this.toolStripMenuItem5.Size = new System.Drawing.Size(135, 6);
            // 
            // toolStripMenuItem3
            // 
            this.toolStripMenuItem3.DropDown = this.scriptAsTypeContextMenuStrip;
            this.toolStripMenuItem3.Name = "toolStripMenuItem3";
            this.toolStripMenuItem3.Size = new System.Drawing.Size(212, 22);
            this.toolStripMenuItem3.Text = "Script Stored Procedure as";
            // 
            // toolStripMenuItem4
            // 
            this.toolStripMenuItem4.DropDown = this.scriptAsTypeContextMenuStrip;
            this.toolStripMenuItem4.Name = "toolStripMenuItem4";
            this.toolStripMenuItem4.Size = new System.Drawing.Size(168, 22);
            this.toolStripMenuItem4.Text = "Script Function as";
            // 
            // functionContextMenuStrip
            // 
            this.functionContextMenuStrip.Items.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.toolStripMenuItem4});
            this.functionContextMenuStrip.Name = "tableContextMenuStrip";
            this.functionContextMenuStrip.Size = new System.Drawing.Size(169, 26);
            this.functionContextMenuStrip.Opening += new System.ComponentModel.CancelEventHandler(this.functionContextMenuStrip_Opening);
            // 
            // procedureContextMenuStrip
            // 
            this.procedureContextMenuStrip.Items.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.toolStripMenuItem3});
            this.procedureContextMenuStrip.Name = "tableContextMenuStrip";
            this.procedureContextMenuStrip.Size = new System.Drawing.Size(213, 26);
            this.procedureContextMenuStrip.Opening += new System.ComponentModel.CancelEventHandler(this.procedureContextMenuStrip_Opening);
            // 
            // ObjectExplorer
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(284, 261);
            this.Controls.Add(this.treeView);
            this.Controls.Add(this.toolStrip1);
            this.HideOnClose = true;
            this.Name = "ObjectExplorer";
            this.Text = "Object Explorer";
            this.tsqlContextMenuStrip.ResumeLayout(false);
            this.serverContextMenuStrip.ResumeLayout(false);
            this.toolStrip1.ResumeLayout(false);
            this.toolStrip1.PerformLayout();
            this.tableContextMenuStrip.ResumeLayout(false);
            this.scriptAsTypeContextMenuStrip.ResumeLayout(false);
            this.scriptAsTargetsContextMenuStrip.ResumeLayout(false);
            this.functionContextMenuStrip.ResumeLayout(false);
            this.procedureContextMenuStrip.ResumeLayout(false);
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.TreeView treeView;
        private System.Windows.Forms.ImageList imageList;
        private System.Windows.Forms.ContextMenuStrip tsqlContextMenuStrip;
        private System.Windows.Forms.ToolStripMenuItem enableTSQLToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem disableTSQLToolStripMenuItem;
        private System.Windows.Forms.ContextMenuStrip serverContextMenuStrip;
        private System.Windows.Forms.ToolStripMenuItem newQueryToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem disconnectToolStripMenuItem;
        private System.Windows.Forms.ToolStripSeparator toolStripMenuItem1;
        private System.Windows.Forms.ToolStripMenuItem refreshToolStripMenuItem;
        private System.Windows.Forms.ToolStripSeparator toolStripSeparator1;
        private System.Windows.Forms.ToolStrip toolStrip1;
        private System.Windows.Forms.ToolStripButton tsbConnect;
        private System.Windows.Forms.ToolStripButton tsbDisconnect;
        private System.Windows.Forms.ContextMenuStrip tableContextMenuStrip;
        private System.Windows.Forms.ToolStripMenuItem selectTop1000RowsToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem scriptTableasToolStripMenuItem;
        private System.Windows.Forms.ContextMenuStrip scriptAsTargetsContextMenuStrip;
        private System.Windows.Forms.ToolStripMenuItem newQueryEditorWindowToolStripMenuItem;
        private System.Windows.Forms.ToolStripSeparator toolStripMenuItem2;
        private System.Windows.Forms.ToolStripMenuItem fileToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem clipboardToolStripMenuItem;
        private System.Windows.Forms.ContextMenuStrip scriptAsTypeContextMenuStrip;
        private System.Windows.Forms.ToolStripMenuItem selectToToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem insertToToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem updateToToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem deleteToToolStripMenuItem;
        private System.Windows.Forms.ToolStripSeparator toolStripMenuItem5;
        private System.Windows.Forms.ToolStripMenuItem executeToToolStripMenuItem;
        private System.Windows.Forms.ContextMenuStrip functionContextMenuStrip;
        private System.Windows.Forms.ToolStripMenuItem toolStripMenuItem4;
        private System.Windows.Forms.ContextMenuStrip procedureContextMenuStrip;
        private System.Windows.Forms.ToolStripMenuItem toolStripMenuItem3;
    }
}
