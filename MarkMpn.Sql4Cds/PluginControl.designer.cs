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
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(PluginControl));
            this.toolStripMenu = new System.Windows.Forms.ToolStrip();
            this.tsbConnect = new System.Windows.Forms.ToolStripButton();
            this.toolStripSeparator = new System.Windows.Forms.ToolStripSeparator();
            this.tsbNewQuery = new System.Windows.Forms.ToolStripButton();
            this.tsbOpen = new System.Windows.Forms.ToolStripButton();
            this.tsbSave = new System.Windows.Forms.ToolStripButton();
            this.toolStripSeparator2 = new System.Windows.Forms.ToolStripSeparator();
            this.tsbExecute = new System.Windows.Forms.ToolStripButton();
            this.tsbStop = new System.Windows.Forms.ToolStripButton();
            this.tsbPreviewFetchXml = new System.Windows.Forms.ToolStripButton();
            this.toolStripSeparator3 = new System.Windows.Forms.ToolStripSeparator();
            this.tsbIncludeFetchXml = new System.Windows.Forms.ToolStripButton();
            this.tssSeparator1 = new System.Windows.Forms.ToolStripSeparator();
            this.tsbFormat = new System.Windows.Forms.ToolStripButton();
            this.tsbSettings = new System.Windows.Forms.ToolStripButton();
            this.tslAboutLink = new System.Windows.Forms.ToolStripLabel();
            this.dockPanel = new WeifenLuo.WinFormsUI.Docking.DockPanel();
            this.toolStripMenu.SuspendLayout();
            this.SuspendLayout();
            // 
            // toolStripMenu
            // 
            this.toolStripMenu.Items.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.tsbConnect,
            this.toolStripSeparator,
            this.tsbNewQuery,
            this.tsbOpen,
            this.tsbSave,
            this.toolStripSeparator2,
            this.tsbExecute,
            this.tsbStop,
            this.tsbPreviewFetchXml,
            this.toolStripSeparator3,
            this.tsbIncludeFetchXml,
            this.tssSeparator1,
            this.tsbFormat,
            this.tsbSettings,
            this.tslAboutLink});
            this.toolStripMenu.Location = new System.Drawing.Point(0, 0);
            this.toolStripMenu.Name = "toolStripMenu";
            this.toolStripMenu.Padding = new System.Windows.Forms.Padding(0, 0, 2, 0);
            this.toolStripMenu.Size = new System.Drawing.Size(861, 25);
            this.toolStripMenu.TabIndex = 4;
            this.toolStripMenu.Text = "toolStrip1";
            // 
            // tsbConnect
            // 
            this.tsbConnect.Image = ((System.Drawing.Image)(resources.GetObject("tsbConnect.Image")));
            this.tsbConnect.ImageTransparentColor = System.Drawing.Color.White;
            this.tsbConnect.Name = "tsbConnect";
            this.tsbConnect.Size = new System.Drawing.Size(72, 22);
            this.tsbConnect.Text = "Connect";
            this.tsbConnect.ToolTipText = "Connect to Environment";
            this.tsbConnect.Click += new System.EventHandler(this.tsbConnect_Click);
            // 
            // toolStripSeparator
            // 
            this.toolStripSeparator.Name = "toolStripSeparator";
            this.toolStripSeparator.Size = new System.Drawing.Size(6, 25);
            // 
            // tsbNewQuery
            // 
            this.tsbNewQuery.Image = ((System.Drawing.Image)(resources.GetObject("tsbNewQuery.Image")));
            this.tsbNewQuery.ImageTransparentColor = System.Drawing.Color.White;
            this.tsbNewQuery.Name = "tsbNewQuery";
            this.tsbNewQuery.Size = new System.Drawing.Size(86, 22);
            this.tsbNewQuery.Text = "New Query";
            this.tsbNewQuery.ToolTipText = "Create New Query Tab";
            this.tsbNewQuery.Click += new System.EventHandler(this.tsbNewQuery_Click);
            // 
            // tsbOpen
            // 
            this.tsbOpen.DisplayStyle = System.Windows.Forms.ToolStripItemDisplayStyle.Image;
            this.tsbOpen.Image = ((System.Drawing.Image)(resources.GetObject("tsbOpen.Image")));
            this.tsbOpen.ImageTransparentColor = System.Drawing.Color.Magenta;
            this.tsbOpen.Name = "tsbOpen";
            this.tsbOpen.Size = new System.Drawing.Size(23, 22);
            this.tsbOpen.Text = "Open";
            this.tsbOpen.ToolTipText = "Open File";
            this.tsbOpen.Click += new System.EventHandler(this.tsbOpen_Click);
            // 
            // tsbSave
            // 
            this.tsbSave.DisplayStyle = System.Windows.Forms.ToolStripItemDisplayStyle.Image;
            this.tsbSave.Enabled = false;
            this.tsbSave.Image = ((System.Drawing.Image)(resources.GetObject("tsbSave.Image")));
            this.tsbSave.ImageTransparentColor = System.Drawing.Color.Magenta;
            this.tsbSave.Name = "tsbSave";
            this.tsbSave.Size = new System.Drawing.Size(23, 22);
            this.tsbSave.Text = "Save";
            this.tsbSave.ToolTipText = "Save File";
            this.tsbSave.Click += new System.EventHandler(this.tsbSave_Click);
            // 
            // toolStripSeparator2
            // 
            this.toolStripSeparator2.Name = "toolStripSeparator2";
            this.toolStripSeparator2.Size = new System.Drawing.Size(6, 25);
            // 
            // tsbExecute
            // 
            this.tsbExecute.Image = ((System.Drawing.Image)(resources.GetObject("tsbExecute.Image")));
            this.tsbExecute.ImageTransparentColor = System.Drawing.Color.White;
            this.tsbExecute.Name = "tsbExecute";
            this.tsbExecute.Size = new System.Drawing.Size(68, 22);
            this.tsbExecute.Text = "Execute";
            this.tsbExecute.ToolTipText = "Execute Selected Query";
            this.tsbExecute.Click += new System.EventHandler(this.tsbExecute_Click);
            // 
            // tsbStop
            // 
            this.tsbStop.DisplayStyle = System.Windows.Forms.ToolStripItemDisplayStyle.Image;
            this.tsbStop.Enabled = false;
            this.tsbStop.Image = ((System.Drawing.Image)(resources.GetObject("tsbStop.Image")));
            this.tsbStop.ImageTransparentColor = System.Drawing.Color.Magenta;
            this.tsbStop.Name = "tsbStop";
            this.tsbStop.Size = new System.Drawing.Size(23, 22);
            this.tsbStop.Text = "Stop";
            this.tsbStop.Click += new System.EventHandler(this.tsbStop_Click);
            // 
            // tsbPreviewFetchXml
            // 
            this.tsbPreviewFetchXml.DisplayStyle = System.Windows.Forms.ToolStripItemDisplayStyle.Image;
            this.tsbPreviewFetchXml.Image = ((System.Drawing.Image)(resources.GetObject("tsbPreviewFetchXml.Image")));
            this.tsbPreviewFetchXml.ImageTransparentColor = System.Drawing.Color.Magenta;
            this.tsbPreviewFetchXml.Name = "tsbPreviewFetchXml";
            this.tsbPreviewFetchXml.Size = new System.Drawing.Size(23, 22);
            this.tsbPreviewFetchXml.Text = "Preview FetchXML";
            this.tsbPreviewFetchXml.ToolTipText = "Display FetchXML Without Executing Query";
            this.tsbPreviewFetchXml.Click += new System.EventHandler(this.tsbPreviewFetchXml_Click);
            // 
            // toolStripSeparator3
            // 
            this.toolStripSeparator3.Name = "toolStripSeparator3";
            this.toolStripSeparator3.Size = new System.Drawing.Size(6, 25);
            // 
            // tsbIncludeFetchXml
            // 
            this.tsbIncludeFetchXml.CheckOnClick = true;
            this.tsbIncludeFetchXml.DisplayStyle = System.Windows.Forms.ToolStripItemDisplayStyle.Image;
            this.tsbIncludeFetchXml.Image = global::MarkMpn.Sql4Cds.Properties.Resources.ExecutionPlan_16x;
            this.tsbIncludeFetchXml.ImageTransparentColor = System.Drawing.Color.Magenta;
            this.tsbIncludeFetchXml.Name = "tsbIncludeFetchXml";
            this.tsbIncludeFetchXml.Size = new System.Drawing.Size(23, 22);
            this.tsbIncludeFetchXml.Text = "Include FetchXML";
            this.tsbIncludeFetchXml.ToolTipText = "Display FetchXML when executing query";
            this.tsbIncludeFetchXml.Click += new System.EventHandler(this.tsbIncludeFetchXml_Click);
            // 
            // tssSeparator1
            // 
            this.tssSeparator1.Name = "tssSeparator1";
            this.tssSeparator1.Size = new System.Drawing.Size(6, 25);
            // 
            // tsbFormat
            // 
            this.tsbFormat.Image = ((System.Drawing.Image)(resources.GetObject("tsbFormat.Image")));
            this.tsbFormat.ImageTransparentColor = System.Drawing.Color.Magenta;
            this.tsbFormat.Name = "tsbFormat";
            this.tsbFormat.Size = new System.Drawing.Size(65, 22);
            this.tsbFormat.Text = "Format";
            this.tsbFormat.ToolTipText = "Reformat SQL Query";
            this.tsbFormat.Click += new System.EventHandler(this.tsbFormat_Click);
            // 
            // tsbSettings
            // 
            this.tsbSettings.Image = global::MarkMpn.Sql4Cds.Properties.Resources.Settings_16x;
            this.tsbSettings.ImageTransparentColor = System.Drawing.Color.Magenta;
            this.tsbSettings.Name = "tsbSettings";
            this.tsbSettings.Size = new System.Drawing.Size(69, 22);
            this.tsbSettings.Text = "Settings";
            this.tsbSettings.Click += new System.EventHandler(this.tsbSettings_Click);
            // 
            // tslAboutLink
            // 
            this.tslAboutLink.Alignment = System.Windows.Forms.ToolStripItemAlignment.Right;
            this.tslAboutLink.Image = global::MarkMpn.Sql4Cds.Properties.Resources.SQL4CDS_Icon_16;
            this.tslAboutLink.IsLink = true;
            this.tslAboutLink.Name = "tslAboutLink";
            this.tslAboutLink.Size = new System.Drawing.Size(184, 22);
            this.tslAboutLink.Text = "SQL 4 CDS by Mark Carrington";
            this.tslAboutLink.Click += new System.EventHandler(this.tslAboutLink_Click);
            // 
            // dockPanel
            // 
            this.dockPanel.Dock = System.Windows.Forms.DockStyle.Fill;
            this.dockPanel.DocumentStyle = WeifenLuo.WinFormsUI.Docking.DocumentStyle.DockingWindow;
            this.dockPanel.Location = new System.Drawing.Point(0, 25);
            this.dockPanel.Name = "dockPanel";
            this.dockPanel.Size = new System.Drawing.Size(861, 478);
            this.dockPanel.TabIndex = 5;
            this.dockPanel.ActiveDocumentChanged += new System.EventHandler(this.dockPanel_ActiveDocumentChanged);
            // 
            // PluginControl
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.Controls.Add(this.dockPanel);
            this.Controls.Add(this.toolStripMenu);
            this.Margin = new System.Windows.Forms.Padding(2, 3, 2, 3);
            this.Name = "PluginControl";
            this.Size = new System.Drawing.Size(861, 503);
            this.Load += new System.EventHandler(this.PluginControl_Load);
            this.toolStripMenu.ResumeLayout(false);
            this.toolStripMenu.PerformLayout();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion
        private System.Windows.Forms.ToolStrip toolStripMenu;
        private System.Windows.Forms.ToolStripSeparator tssSeparator1;
        private System.Windows.Forms.ToolStripButton tsbExecute;
        private System.Windows.Forms.ToolStripButton tsbConnect;
        private System.Windows.Forms.ToolStripSeparator toolStripSeparator;
        private System.Windows.Forms.ToolStripButton tsbNewQuery;
        private System.Windows.Forms.ToolStripLabel tslAboutLink;
        private System.Windows.Forms.ToolStripButton tsbFormat;
        private System.Windows.Forms.ToolStripButton tsbSettings;
        private System.Windows.Forms.ToolStripButton tsbPreviewFetchXml;
        private WeifenLuo.WinFormsUI.Docking.DockPanel dockPanel;
        private System.Windows.Forms.ToolStripButton tsbOpen;
        private System.Windows.Forms.ToolStripButton tsbSave;
        private System.Windows.Forms.ToolStripSeparator toolStripSeparator2;
        private System.Windows.Forms.ToolStripButton tsbStop;
        private System.Windows.Forms.ToolStripSeparator toolStripSeparator3;
        private System.Windows.Forms.ToolStripButton tsbIncludeFetchXml;
    }
}
