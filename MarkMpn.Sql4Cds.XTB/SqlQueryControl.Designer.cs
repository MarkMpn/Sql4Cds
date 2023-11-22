namespace MarkMpn.Sql4Cds.XTB
{
    partial class SqlQueryControl
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
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(SqlQueryControl));
            this.splitContainer = new System.Windows.Forms.SplitContainer();
            this.tabControl = new System.Windows.Forms.TabControl();
            this.resultsTabPage = new System.Windows.Forms.TabPage();
            this.resultsFlowLayoutPanel = new System.Windows.Forms.FlowLayoutPanel();
            this.fetchXmlTabPage = new System.Windows.Forms.TabPage();
            this.fetchXMLFlowLayoutPanel = new System.Windows.Forms.FlowLayoutPanel();
            this.messagesTabPage = new System.Windows.Forms.TabPage();
            this.imageList = new System.Windows.Forms.ImageList(this.components);
            this.gridContextMenuStrip = new System.Windows.Forms.ContextMenuStrip(this.components);
            this.copyToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.copyWithHeadersToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.toolStripMenuItem1 = new System.Windows.Forms.ToolStripSeparator();
            this.openRecordToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.copyRecordUrlToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.createSELECTStatementToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.statusStrip = new System.Windows.Forms.StatusStrip();
            this.toolStripStatusLabel = new System.Windows.Forms.ToolStripStatusLabel();
            this.hostLabel = new System.Windows.Forms.ToolStripStatusLabel();
            this.usernameDropDownButton = new System.Windows.Forms.ToolStripDropDownButton();
            this.impersonateMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.revertToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.orgNameLabel = new System.Windows.Forms.ToolStripStatusLabel();
            this.timerLabel = new System.Windows.Forms.ToolStripStatusLabel();
            this.rowsLabel = new System.Windows.Forms.ToolStripStatusLabel();
            this.backgroundWorker = new System.ComponentModel.BackgroundWorker();
            this.timer = new System.Windows.Forms.Timer(this.components);
            this.environmentHighlightLabel = new System.Windows.Forms.Label();
            ((System.ComponentModel.ISupportInitialize)(this.splitContainer)).BeginInit();
            this.splitContainer.Panel1.SuspendLayout();
            this.splitContainer.Panel2.SuspendLayout();
            this.splitContainer.SuspendLayout();
            this.tabControl.SuspendLayout();
            this.resultsTabPage.SuspendLayout();
            this.fetchXmlTabPage.SuspendLayout();
            this.gridContextMenuStrip.SuspendLayout();
            this.statusStrip.SuspendLayout();
            this.SuspendLayout();
            // 
            // splitContainer
            // 
            this.splitContainer.Dock = System.Windows.Forms.DockStyle.Fill;
            this.splitContainer.Location = new System.Drawing.Point(0, 0);
            this.splitContainer.Margin = new System.Windows.Forms.Padding(2);
            this.splitContainer.Name = "splitContainer";
            this.splitContainer.Orientation = System.Windows.Forms.Orientation.Horizontal;
            // 
            // splitContainer.Panel1
            // 
            this.splitContainer.Panel1.Controls.Add(this.environmentHighlightLabel);
            // 
            // splitContainer.Panel2
            // 
            this.splitContainer.Panel2.AutoScroll = true;
            this.splitContainer.Panel2.Controls.Add(this.tabControl);
            this.splitContainer.Panel2Collapsed = true;
            this.splitContainer.Size = new System.Drawing.Size(595, 452);
            this.splitContainer.SplitterDistance = 190;
            this.splitContainer.SplitterWidth = 2;
            this.splitContainer.TabIndex = 0;
            // 
            // tabControl
            // 
            this.tabControl.Controls.Add(this.resultsTabPage);
            this.tabControl.Controls.Add(this.fetchXmlTabPage);
            this.tabControl.Controls.Add(this.messagesTabPage);
            this.tabControl.Dock = System.Windows.Forms.DockStyle.Fill;
            this.tabControl.ImageList = this.imageList;
            this.tabControl.Location = new System.Drawing.Point(0, 0);
            this.tabControl.Name = "tabControl";
            this.tabControl.SelectedIndex = 0;
            this.tabControl.Size = new System.Drawing.Size(150, 46);
            this.tabControl.TabIndex = 0;
            // 
            // resultsTabPage
            // 
            this.resultsTabPage.Controls.Add(this.resultsFlowLayoutPanel);
            this.resultsTabPage.ImageIndex = 0;
            this.resultsTabPage.Location = new System.Drawing.Point(4, 23);
            this.resultsTabPage.Name = "resultsTabPage";
            this.resultsTabPage.Size = new System.Drawing.Size(142, 19);
            this.resultsTabPage.TabIndex = 0;
            this.resultsTabPage.Text = "Results";
            this.resultsTabPage.UseVisualStyleBackColor = true;
            // 
            // resultsFlowLayoutPanel
            // 
            this.resultsFlowLayoutPanel.AutoScroll = true;
            this.resultsFlowLayoutPanel.Dock = System.Windows.Forms.DockStyle.Fill;
            this.resultsFlowLayoutPanel.FlowDirection = System.Windows.Forms.FlowDirection.TopDown;
            this.resultsFlowLayoutPanel.Location = new System.Drawing.Point(0, 0);
            this.resultsFlowLayoutPanel.Margin = new System.Windows.Forms.Padding(0);
            this.resultsFlowLayoutPanel.Name = "resultsFlowLayoutPanel";
            this.resultsFlowLayoutPanel.Size = new System.Drawing.Size(142, 19);
            this.resultsFlowLayoutPanel.TabIndex = 2;
            this.resultsFlowLayoutPanel.WrapContents = false;
            this.resultsFlowLayoutPanel.ClientSizeChanged += new System.EventHandler(this.ResizeLayoutPanel);
            // 
            // fetchXmlTabPage
            // 
            this.fetchXmlTabPage.Controls.Add(this.fetchXMLFlowLayoutPanel);
            this.fetchXmlTabPage.ImageIndex = 1;
            this.fetchXmlTabPage.Location = new System.Drawing.Point(4, 23);
            this.fetchXmlTabPage.Name = "fetchXmlTabPage";
            this.fetchXmlTabPage.Size = new System.Drawing.Size(142, 19);
            this.fetchXmlTabPage.TabIndex = 2;
            this.fetchXmlTabPage.Text = "Execution Plan";
            this.fetchXmlTabPage.UseVisualStyleBackColor = true;
            // 
            // fetchXMLFlowLayoutPanel
            // 
            this.fetchXMLFlowLayoutPanel.AutoScroll = true;
            this.fetchXMLFlowLayoutPanel.Dock = System.Windows.Forms.DockStyle.Fill;
            this.fetchXMLFlowLayoutPanel.FlowDirection = System.Windows.Forms.FlowDirection.TopDown;
            this.fetchXMLFlowLayoutPanel.Location = new System.Drawing.Point(0, 0);
            this.fetchXMLFlowLayoutPanel.Margin = new System.Windows.Forms.Padding(0);
            this.fetchXMLFlowLayoutPanel.Name = "fetchXMLFlowLayoutPanel";
            this.fetchXMLFlowLayoutPanel.Size = new System.Drawing.Size(142, 19);
            this.fetchXMLFlowLayoutPanel.TabIndex = 1;
            this.fetchXMLFlowLayoutPanel.WrapContents = false;
            this.fetchXMLFlowLayoutPanel.ClientSizeChanged += new System.EventHandler(this.ResizeLayoutPanel);
            // 
            // messagesTabPage
            // 
            this.messagesTabPage.ImageIndex = 2;
            this.messagesTabPage.Location = new System.Drawing.Point(4, 23);
            this.messagesTabPage.Name = "messagesTabPage";
            this.messagesTabPage.Padding = new System.Windows.Forms.Padding(3);
            this.messagesTabPage.Size = new System.Drawing.Size(142, 19);
            this.messagesTabPage.TabIndex = 1;
            this.messagesTabPage.Text = "Messages";
            this.messagesTabPage.UseVisualStyleBackColor = true;
            // 
            // imageList
            // 
            this.imageList.ImageStream = ((System.Windows.Forms.ImageListStreamer)(resources.GetObject("imageList.ImageStream")));
            this.imageList.TransparentColor = System.Drawing.Color.Transparent;
            this.imageList.Images.SetKeyName(0, "Table_16x.png");
            this.imageList.Images.SetKeyName(1, "ExecutionPlan_16x.png");
            this.imageList.Images.SetKeyName(2, "ServerReport_16x.png");
            // 
            // gridContextMenuStrip
            // 
            this.gridContextMenuStrip.Items.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.copyToolStripMenuItem,
            this.copyWithHeadersToolStripMenuItem,
            this.toolStripMenuItem1,
            this.openRecordToolStripMenuItem,
            this.copyRecordUrlToolStripMenuItem,
            this.createSELECTStatementToolStripMenuItem});
            this.gridContextMenuStrip.Name = "gridContextMenuStrip";
            this.gridContextMenuStrip.Size = new System.Drawing.Size(207, 142);
            this.gridContextMenuStrip.Opening += new System.ComponentModel.CancelEventHandler(this.gridContextMenuStrip_Opening);
            // 
            // copyToolStripMenuItem
            // 
            this.copyToolStripMenuItem.Name = "copyToolStripMenuItem";
            this.copyToolStripMenuItem.Size = new System.Drawing.Size(206, 22);
            this.copyToolStripMenuItem.Text = "Copy";
            this.copyToolStripMenuItem.Click += new System.EventHandler(this.copyToolStripMenuItem_Click);
            // 
            // copyWithHeadersToolStripMenuItem
            // 
            this.copyWithHeadersToolStripMenuItem.Name = "copyWithHeadersToolStripMenuItem";
            this.copyWithHeadersToolStripMenuItem.Size = new System.Drawing.Size(206, 22);
            this.copyWithHeadersToolStripMenuItem.Text = "Copy with Headers";
            this.copyWithHeadersToolStripMenuItem.Click += new System.EventHandler(this.copyWithHeadersToolStripMenuItem_Click);
            // 
            // toolStripMenuItem1
            // 
            this.toolStripMenuItem1.Name = "toolStripMenuItem1";
            this.toolStripMenuItem1.Size = new System.Drawing.Size(203, 6);
            // 
            // openRecordToolStripMenuItem
            // 
            this.openRecordToolStripMenuItem.Enabled = false;
            this.openRecordToolStripMenuItem.Name = "openRecordToolStripMenuItem";
            this.openRecordToolStripMenuItem.Size = new System.Drawing.Size(206, 22);
            this.openRecordToolStripMenuItem.Text = "Open Record";
            this.openRecordToolStripMenuItem.Click += new System.EventHandler(this.openRecordToolStripMenuItem_Click);
            // 
            // copyRecordUrlToolStripMenuItem
            // 
            this.copyRecordUrlToolStripMenuItem.Enabled = false;
            this.copyRecordUrlToolStripMenuItem.Name = "copyRecordUrlToolStripMenuItem";
            this.copyRecordUrlToolStripMenuItem.Size = new System.Drawing.Size(206, 22);
            this.copyRecordUrlToolStripMenuItem.Text = "Copy Record Url";
            this.copyRecordUrlToolStripMenuItem.Click += new System.EventHandler(this.copyRecordUrlToolStripMenuItem_Click);
            // 
            // createSELECTStatementToolStripMenuItem
            // 
            this.createSELECTStatementToolStripMenuItem.Enabled = false;
            this.createSELECTStatementToolStripMenuItem.Name = "createSELECTStatementToolStripMenuItem";
            this.createSELECTStatementToolStripMenuItem.Size = new System.Drawing.Size(206, 22);
            this.createSELECTStatementToolStripMenuItem.Text = "Create SELECT Statement";
            this.createSELECTStatementToolStripMenuItem.Click += new System.EventHandler(this.createSELECTStatementToolStripMenuItem_Click);
            // 
            // statusStrip
            // 
            this.statusStrip.BackColor = System.Drawing.Color.Khaki;
            this.statusStrip.Items.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.toolStripStatusLabel,
            this.hostLabel,
            this.usernameDropDownButton,
            this.orgNameLabel,
            this.timerLabel,
            this.rowsLabel});
            this.statusStrip.Location = new System.Drawing.Point(0, 452);
            this.statusStrip.Name = "statusStrip";
            this.statusStrip.ShowItemToolTips = true;
            this.statusStrip.Size = new System.Drawing.Size(595, 22);
            this.statusStrip.SizingGrip = false;
            this.statusStrip.TabIndex = 1;
            this.statusStrip.Text = "statusStrip1";
            // 
            // toolStripStatusLabel
            // 
            this.toolStripStatusLabel.Image = global::MarkMpn.Sql4Cds.XTB.Properties.Resources.ConnectFilled_grey_16x;
            this.toolStripStatusLabel.ImageAlign = System.Drawing.ContentAlignment.MiddleLeft;
            this.toolStripStatusLabel.Name = "toolStripStatusLabel";
            this.toolStripStatusLabel.Size = new System.Drawing.Size(130, 17);
            this.toolStripStatusLabel.Spring = true;
            this.toolStripStatusLabel.Text = "Connected";
            this.toolStripStatusLabel.TextAlign = System.Drawing.ContentAlignment.MiddleLeft;
            this.toolStripStatusLabel.ToolTipText = "Connection status";
            // 
            // hostLabel
            // 
            this.hostLabel.Image = global::MarkMpn.Sql4Cds.XTB.Properties.Resources.timeline_lock_on_16x;
            this.hostLabel.Name = "hostLabel";
            this.hostLabel.Size = new System.Drawing.Size(164, 17);
            this.hostLabel.Text = "orgxxx.crm.dynamics.com";
            this.hostLabel.ToolTipText = "Server URL";
            // 
            // usernameDropDownButton
            // 
            this.usernameDropDownButton.DropDownItems.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.impersonateMenuItem,
            this.revertToolStripMenuItem});
            this.usernameDropDownButton.ImageTransparentColor = System.Drawing.Color.Magenta;
            this.usernameDropDownButton.Name = "usernameDropDownButton";
            this.usernameDropDownButton.Size = new System.Drawing.Size(153, 20);
            this.usernameDropDownButton.Text = "username@contoso.com";
            this.usernameDropDownButton.ToolTipText = "Impersonate other user";
            // 
            // impersonateMenuItem
            // 
            this.impersonateMenuItem.Name = "impersonateMenuItem";
            this.impersonateMenuItem.Size = new System.Drawing.Size(140, 22);
            this.impersonateMenuItem.Text = "Impersonate";
            this.impersonateMenuItem.Click += new System.EventHandler(this.impersonateMenuItem_Click);
            // 
            // revertToolStripMenuItem
            // 
            this.revertToolStripMenuItem.Enabled = false;
            this.revertToolStripMenuItem.Name = "revertToolStripMenuItem";
            this.revertToolStripMenuItem.Size = new System.Drawing.Size(140, 22);
            this.revertToolStripMenuItem.Text = "Revert";
            this.revertToolStripMenuItem.Click += new System.EventHandler(this.revertToolStripMenuItem_Click);
            // 
            // orgNameLabel
            // 
            this.orgNameLabel.Name = "orgNameLabel";
            this.orgNameLabel.Size = new System.Drawing.Size(43, 17);
            this.orgNameLabel.Text = "orgxxx";
            this.orgNameLabel.ToolTipText = "Organization Name";
            // 
            // timerLabel
            // 
            this.timerLabel.Name = "timerLabel";
            this.timerLabel.Size = new System.Drawing.Size(49, 17);
            this.timerLabel.Text = "00:00:00";
            this.timerLabel.ToolTipText = "Query execution time";
            // 
            // rowsLabel
            // 
            this.rowsLabel.Name = "rowsLabel";
            this.rowsLabel.Size = new System.Drawing.Size(41, 17);
            this.rowsLabel.Text = "0 rows";
            this.rowsLabel.ToolTipText = "Number of rows returned";
            // 
            // backgroundWorker
            // 
            this.backgroundWorker.WorkerReportsProgress = true;
            this.backgroundWorker.WorkerSupportsCancellation = true;
            this.backgroundWorker.DoWork += new System.ComponentModel.DoWorkEventHandler(this.backgroundWorker_DoWork);
            this.backgroundWorker.ProgressChanged += new System.ComponentModel.ProgressChangedEventHandler(this.backgroundWorker_ProgressChanged);
            this.backgroundWorker.RunWorkerCompleted += new System.ComponentModel.RunWorkerCompletedEventHandler(this.backgroundWorker_RunWorkerCompleted);
            // 
            // timer
            // 
            this.timer.Interval = 1000;
            this.timer.Tick += new System.EventHandler(this.timer_Tick);
            // 
            // environmentHighlightLabel
            // 
            this.environmentHighlightLabel.BackColor = System.Drawing.Color.Red;
            this.environmentHighlightLabel.Dock = System.Windows.Forms.DockStyle.Top;
            this.environmentHighlightLabel.Font = new System.Drawing.Font("Microsoft Sans Serif", 8.25F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.environmentHighlightLabel.ForeColor = System.Drawing.Color.White;
            this.environmentHighlightLabel.Location = new System.Drawing.Point(0, 0);
            this.environmentHighlightLabel.Name = "environmentHighlightLabel";
            this.environmentHighlightLabel.Size = new System.Drawing.Size(595, 23);
            this.environmentHighlightLabel.TabIndex = 0;
            this.environmentHighlightLabel.Text = "Environment Name";
            this.environmentHighlightLabel.TextAlign = System.Drawing.ContentAlignment.MiddleCenter;
            // 
            // SqlQueryControl
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(595, 474);
            this.Controls.Add(this.splitContainer);
            this.Controls.Add(this.statusStrip);
            this.Margin = new System.Windows.Forms.Padding(2);
            this.Name = "SqlQueryControl";
            this.splitContainer.Panel1.ResumeLayout(false);
            this.splitContainer.Panel2.ResumeLayout(false);
            ((System.ComponentModel.ISupportInitialize)(this.splitContainer)).EndInit();
            this.splitContainer.ResumeLayout(false);
            this.tabControl.ResumeLayout(false);
            this.resultsTabPage.ResumeLayout(false);
            this.fetchXmlTabPage.ResumeLayout(false);
            this.gridContextMenuStrip.ResumeLayout(false);
            this.statusStrip.ResumeLayout(false);
            this.statusStrip.PerformLayout();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.SplitContainer splitContainer;
        private System.Windows.Forms.ContextMenuStrip gridContextMenuStrip;
        private System.Windows.Forms.ToolStripMenuItem copyToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem copyWithHeadersToolStripMenuItem;
        private System.Windows.Forms.StatusStrip statusStrip;
        private System.Windows.Forms.ToolStripStatusLabel toolStripStatusLabel;
        private System.Windows.Forms.ToolStripStatusLabel hostLabel;
        private System.Windows.Forms.ToolStripStatusLabel timerLabel;
        private System.Windows.Forms.ToolStripStatusLabel rowsLabel;
        private System.Windows.Forms.ToolStripStatusLabel orgNameLabel;
        private System.Windows.Forms.TabControl tabControl;
        private System.Windows.Forms.TabPage resultsTabPage;
        private System.Windows.Forms.TabPage fetchXmlTabPage;
        private System.Windows.Forms.TabPage messagesTabPage;
        private System.Windows.Forms.ImageList imageList;
        private System.ComponentModel.BackgroundWorker backgroundWorker;
        private System.Windows.Forms.Timer timer;
        private System.Windows.Forms.FlowLayoutPanel fetchXMLFlowLayoutPanel;
        private System.Windows.Forms.FlowLayoutPanel resultsFlowLayoutPanel;
        private System.Windows.Forms.ToolStripDropDownButton usernameDropDownButton;
        private System.Windows.Forms.ToolStripMenuItem impersonateMenuItem;
        private System.Windows.Forms.ToolStripMenuItem revertToolStripMenuItem;
        private System.Windows.Forms.ToolStripSeparator toolStripMenuItem1;
        private System.Windows.Forms.ToolStripMenuItem openRecordToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem createSELECTStatementToolStripMenuItem;
        private System.Windows.Forms.Label environmentHighlightLabel;
        private System.Windows.Forms.ToolStripMenuItem copyRecordUrlToolStripMenuItem;
    }
}
