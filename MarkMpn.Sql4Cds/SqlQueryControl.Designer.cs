namespace MarkMpn.Sql4Cds
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
            this.splitContainer = new System.Windows.Forms.SplitContainer();
            this.gridContextMenuStrip = new System.Windows.Forms.ContextMenuStrip(this.components);
            this.copyToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.copyWithHeadersToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.openRecordToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.createSELECTQueryToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            ((System.ComponentModel.ISupportInitialize)(this.splitContainer)).BeginInit();
            this.splitContainer.SuspendLayout();
            this.gridContextMenuStrip.SuspendLayout();
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
            // splitContainer.Panel2
            // 
            this.splitContainer.Panel2.AutoScroll = true;
            this.splitContainer.Size = new System.Drawing.Size(595, 474);
            this.splitContainer.SplitterDistance = 200;
            this.splitContainer.SplitterWidth = 2;
            this.splitContainer.TabIndex = 0;
            // 
            // gridContextMenuStrip
            // 
            this.gridContextMenuStrip.Items.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.copyToolStripMenuItem,
            this.copyWithHeadersToolStripMenuItem,
            this.openRecordToolStripMenuItem,
            this.createSELECTQueryToolStripMenuItem});
            this.gridContextMenuStrip.Name = "gridContextMenuStrip";
            this.gridContextMenuStrip.Size = new System.Drawing.Size(183, 114);
            this.gridContextMenuStrip.Opening += new System.ComponentModel.CancelEventHandler(this.gridContextMenuStrip_Opening);
            // 
            // copyToolStripMenuItem
            // 
            this.copyToolStripMenuItem.Name = "copyToolStripMenuItem";
            this.copyToolStripMenuItem.Size = new System.Drawing.Size(182, 22);
            this.copyToolStripMenuItem.Text = "Copy";
            this.copyToolStripMenuItem.Click += new System.EventHandler(this.copyToolStripMenuItem_Click);
            // 
            // copyWithHeadersToolStripMenuItem
            // 
            this.copyWithHeadersToolStripMenuItem.Name = "copyWithHeadersToolStripMenuItem";
            this.copyWithHeadersToolStripMenuItem.Size = new System.Drawing.Size(182, 22);
            this.copyWithHeadersToolStripMenuItem.Text = "Copy with Headers";
            this.copyWithHeadersToolStripMenuItem.Click += new System.EventHandler(this.copyWithHeadersToolStripMenuItem_Click);
            // 
            // openRecordToolStripMenuItem
            // 
            this.openRecordToolStripMenuItem.Name = "openRecordToolStripMenuItem";
            this.openRecordToolStripMenuItem.Size = new System.Drawing.Size(182, 22);
            this.openRecordToolStripMenuItem.Text = "Open Record";
            this.openRecordToolStripMenuItem.Click += new System.EventHandler(this.openRecordToolStripMenuItem_Click);
            // 
            // createSELECTQueryToolStripMenuItem
            // 
            this.createSELECTQueryToolStripMenuItem.Name = "createSELECTQueryToolStripMenuItem";
            this.createSELECTQueryToolStripMenuItem.Size = new System.Drawing.Size(182, 22);
            this.createSELECTQueryToolStripMenuItem.Text = "Create SELECT query";
            this.createSELECTQueryToolStripMenuItem.Click += new System.EventHandler(this.createSELECTQueryToolStripMenuItem_Click);
            // 
            // SqlQueryControl
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(595, 474);
            this.Controls.Add(this.splitContainer);
            this.Margin = new System.Windows.Forms.Padding(2);
            this.Name = "SqlQueryControl";
            ((System.ComponentModel.ISupportInitialize)(this.splitContainer)).EndInit();
            this.splitContainer.ResumeLayout(false);
            this.gridContextMenuStrip.ResumeLayout(false);
            this.ResumeLayout(false);

        }

        #endregion

        private System.Windows.Forms.SplitContainer splitContainer;
        private System.Windows.Forms.ContextMenuStrip gridContextMenuStrip;
        private System.Windows.Forms.ToolStripMenuItem copyToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem copyWithHeadersToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem openRecordToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem createSELECTQueryToolStripMenuItem;
    }
}
