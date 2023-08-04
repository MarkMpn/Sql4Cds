
namespace MarkMpn.Sql4Cds
{
    partial class FindReplace
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
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(FindReplace));
            this.toolStrip1 = new System.Windows.Forms.ToolStrip();
            this.showReplaceToolStripButton = new System.Windows.Forms.ToolStripButton();
            this.findToolStripComboBox = new System.Windows.Forms.ToolStripComboBox();
            this.findToolStripSplitButton = new System.Windows.Forms.ToolStripSplitButton();
            this.findNextToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.findPreviousToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.closeToolStripButton = new System.Windows.Forms.ToolStripButton();
            this.replaceSpacerToolStripLabel = new System.Windows.Forms.ToolStripLabel();
            this.replaceToolStripComboBox = new System.Windows.Forms.ToolStripComboBox();
            this.replaceToolStripButton = new System.Windows.Forms.ToolStripButton();
            this.replaceAllToolStripButton = new System.Windows.Forms.ToolStripButton();
            this.optionsSpacerToolStripLabel = new System.Windows.Forms.ToolStripLabel();
            this.matchCaseToolStripButton = new System.Windows.Forms.ToolStripButton();
            this.wholeWordToolStripButton = new System.Windows.Forms.ToolStripButton();
            this.regexToolStripButton = new System.Windows.Forms.ToolStripButton();
            this.panel1 = new System.Windows.Forms.Panel();
            this.toolStrip1.SuspendLayout();
            this.SuspendLayout();
            // 
            // toolStrip1
            // 
            this.toolStrip1.Items.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.showReplaceToolStripButton,
            this.findToolStripComboBox,
            this.findToolStripSplitButton,
            this.closeToolStripButton,
            this.replaceSpacerToolStripLabel,
            this.replaceToolStripComboBox,
            this.replaceToolStripButton,
            this.replaceAllToolStripButton,
            this.optionsSpacerToolStripLabel,
            this.matchCaseToolStripButton,
            this.wholeWordToolStripButton,
            this.regexToolStripButton});
            this.toolStrip1.LayoutStyle = System.Windows.Forms.ToolStripLayoutStyle.Flow;
            this.toolStrip1.Location = new System.Drawing.Point(0, 0);
            this.toolStrip1.Name = "toolStrip1";
            this.toolStrip1.Size = new System.Drawing.Size(338, 46);
            this.toolStrip1.TabIndex = 0;
            this.toolStrip1.Text = "toolStrip1";
            // 
            // showReplaceToolStripButton
            // 
            this.showReplaceToolStripButton.CheckOnClick = true;
            this.showReplaceToolStripButton.DisplayStyle = System.Windows.Forms.ToolStripItemDisplayStyle.Image;
            this.showReplaceToolStripButton.Image = ((System.Drawing.Image)(resources.GetObject("showReplaceToolStripButton.Image")));
            this.showReplaceToolStripButton.ImageTransparentColor = System.Drawing.Color.Magenta;
            this.showReplaceToolStripButton.Name = "showReplaceToolStripButton";
            this.showReplaceToolStripButton.Size = new System.Drawing.Size(23, 20);
            this.showReplaceToolStripButton.Text = "toolStripButton2";
            this.showReplaceToolStripButton.ToolTipText = "Toggle between find and replace modes";
            this.showReplaceToolStripButton.Click += new System.EventHandler(this.showReplaceToolStripButton_Click);
            // 
            // findToolStripComboBox
            // 
            this.findToolStripComboBox.AutoSize = false;
            this.findToolStripComboBox.Name = "findToolStripComboBox";
            this.findToolStripComboBox.Size = new System.Drawing.Size(121, 23);
            this.findToolStripComboBox.Text = "Find...";
            this.findToolStripComboBox.KeyPress += new System.Windows.Forms.KeyPressEventHandler(this.findToolStripComboBox_KeyPress);
            // 
            // findToolStripSplitButton
            // 
            this.findToolStripSplitButton.DisplayStyle = System.Windows.Forms.ToolStripItemDisplayStyle.Image;
            this.findToolStripSplitButton.DropDownItems.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.findNextToolStripMenuItem,
            this.findPreviousToolStripMenuItem});
            this.findToolStripSplitButton.Image = ((System.Drawing.Image)(resources.GetObject("findToolStripSplitButton.Image")));
            this.findToolStripSplitButton.ImageTransparentColor = System.Drawing.Color.Magenta;
            this.findToolStripSplitButton.Name = "findToolStripSplitButton";
            this.findToolStripSplitButton.Size = new System.Drawing.Size(32, 20);
            this.findToolStripSplitButton.Text = "Find Next";
            this.findToolStripSplitButton.ToolTipText = "Find Next (F3)";
            this.findToolStripSplitButton.ButtonClick += new System.EventHandler(this.findToolStripSplitButton_ButtonClick);
            // 
            // findNextToolStripMenuItem
            // 
            this.findNextToolStripMenuItem.Image = ((System.Drawing.Image)(resources.GetObject("findNextToolStripMenuItem.Image")));
            this.findNextToolStripMenuItem.Name = "findNextToolStripMenuItem";
            this.findNextToolStripMenuItem.ShortcutKeys = System.Windows.Forms.Keys.F3;
            this.findNextToolStripMenuItem.Size = new System.Drawing.Size(196, 22);
            this.findNextToolStripMenuItem.Text = "Find Next";
            this.findNextToolStripMenuItem.Click += new System.EventHandler(this.findToolStripSplitButton_ButtonClick);
            // 
            // findPreviousToolStripMenuItem
            // 
            this.findPreviousToolStripMenuItem.Image = ((System.Drawing.Image)(resources.GetObject("findPreviousToolStripMenuItem.Image")));
            this.findPreviousToolStripMenuItem.Name = "findPreviousToolStripMenuItem";
            this.findPreviousToolStripMenuItem.ShortcutKeys = ((System.Windows.Forms.Keys)((System.Windows.Forms.Keys.Shift | System.Windows.Forms.Keys.F3)));
            this.findPreviousToolStripMenuItem.Size = new System.Drawing.Size(196, 22);
            this.findPreviousToolStripMenuItem.Text = "Find Previous";
            this.findPreviousToolStripMenuItem.Click += new System.EventHandler(this.findPreviousToolStripMenuItem_Click);
            // 
            // closeToolStripButton
            // 
            this.closeToolStripButton.DisplayStyle = System.Windows.Forms.ToolStripItemDisplayStyle.Image;
            this.closeToolStripButton.Image = ((System.Drawing.Image)(resources.GetObject("closeToolStripButton.Image")));
            this.closeToolStripButton.ImageTransparentColor = System.Drawing.Color.Magenta;
            this.closeToolStripButton.Name = "closeToolStripButton";
            this.closeToolStripButton.Size = new System.Drawing.Size(23, 20);
            this.closeToolStripButton.Text = "Close";
            this.closeToolStripButton.Click += new System.EventHandler(this.closeToolStripButton_Click);
            // 
            // replaceSpacerToolStripLabel
            // 
            this.replaceSpacerToolStripLabel.AutoSize = false;
            this.replaceSpacerToolStripLabel.BackColor = System.Drawing.SystemColors.Control;
            this.replaceSpacerToolStripLabel.Name = "replaceSpacerToolStripLabel";
            this.replaceSpacerToolStripLabel.Size = new System.Drawing.Size(23, 15);
            // 
            // replaceToolStripComboBox
            // 
            this.replaceToolStripComboBox.AutoSize = false;
            this.replaceToolStripComboBox.Name = "replaceToolStripComboBox";
            this.replaceToolStripComboBox.Size = new System.Drawing.Size(121, 23);
            this.replaceToolStripComboBox.Text = "Replace...";
            // 
            // replaceToolStripButton
            // 
            this.replaceToolStripButton.DisplayStyle = System.Windows.Forms.ToolStripItemDisplayStyle.Image;
            this.replaceToolStripButton.Image = ((System.Drawing.Image)(resources.GetObject("replaceToolStripButton.Image")));
            this.replaceToolStripButton.ImageTransparentColor = System.Drawing.Color.Magenta;
            this.replaceToolStripButton.Name = "replaceToolStripButton";
            this.replaceToolStripButton.Size = new System.Drawing.Size(23, 20);
            this.replaceToolStripButton.Text = "Replace Next";
            this.replaceToolStripButton.Click += new System.EventHandler(this.replaceToolStripButton_Click);
            // 
            // replaceAllToolStripButton
            // 
            this.replaceAllToolStripButton.DisplayStyle = System.Windows.Forms.ToolStripItemDisplayStyle.Image;
            this.replaceAllToolStripButton.Image = ((System.Drawing.Image)(resources.GetObject("replaceAllToolStripButton.Image")));
            this.replaceAllToolStripButton.ImageTransparentColor = System.Drawing.Color.Magenta;
            this.replaceAllToolStripButton.Name = "replaceAllToolStripButton";
            this.replaceAllToolStripButton.Size = new System.Drawing.Size(23, 20);
            this.replaceAllToolStripButton.Text = "Replace All";
            this.replaceAllToolStripButton.Click += new System.EventHandler(this.replaceAllToolStripButton_Click);
            // 
            // optionsSpacerToolStripLabel
            // 
            this.optionsSpacerToolStripLabel.AutoSize = false;
            this.optionsSpacerToolStripLabel.BackColor = System.Drawing.SystemColors.Control;
            this.optionsSpacerToolStripLabel.Name = "optionsSpacerToolStripLabel";
            this.optionsSpacerToolStripLabel.Size = new System.Drawing.Size(23, 15);
            // 
            // matchCaseToolStripButton
            // 
            this.matchCaseToolStripButton.CheckOnClick = true;
            this.matchCaseToolStripButton.DisplayStyle = System.Windows.Forms.ToolStripItemDisplayStyle.Image;
            this.matchCaseToolStripButton.Image = ((System.Drawing.Image)(resources.GetObject("matchCaseToolStripButton.Image")));
            this.matchCaseToolStripButton.ImageTransparentColor = System.Drawing.Color.Magenta;
            this.matchCaseToolStripButton.Name = "matchCaseToolStripButton";
            this.matchCaseToolStripButton.Size = new System.Drawing.Size(23, 20);
            this.matchCaseToolStripButton.Text = "Case Sensitive";
            // 
            // wholeWordToolStripButton
            // 
            this.wholeWordToolStripButton.CheckOnClick = true;
            this.wholeWordToolStripButton.DisplayStyle = System.Windows.Forms.ToolStripItemDisplayStyle.Image;
            this.wholeWordToolStripButton.Image = ((System.Drawing.Image)(resources.GetObject("wholeWordToolStripButton.Image")));
            this.wholeWordToolStripButton.ImageTransparentColor = System.Drawing.Color.Magenta;
            this.wholeWordToolStripButton.Name = "wholeWordToolStripButton";
            this.wholeWordToolStripButton.Size = new System.Drawing.Size(23, 20);
            this.wholeWordToolStripButton.Text = "Match Whole Word";
            // 
            // regexToolStripButton
            // 
            this.regexToolStripButton.CheckOnClick = true;
            this.regexToolStripButton.DisplayStyle = System.Windows.Forms.ToolStripItemDisplayStyle.Image;
            this.regexToolStripButton.Image = ((System.Drawing.Image)(resources.GetObject("regexToolStripButton.Image")));
            this.regexToolStripButton.ImageTransparentColor = System.Drawing.Color.Magenta;
            this.regexToolStripButton.Name = "regexToolStripButton";
            this.regexToolStripButton.Size = new System.Drawing.Size(23, 20);
            this.regexToolStripButton.Text = "Regular Expression";
            // 
            // panel1
            // 
            this.panel1.BackColor = System.Drawing.SystemColors.Highlight;
            this.panel1.Dock = System.Windows.Forms.DockStyle.Bottom;
            this.panel1.Location = new System.Drawing.Point(0, 74);
            this.panel1.Name = "panel1";
            this.panel1.Size = new System.Drawing.Size(338, 4);
            this.panel1.TabIndex = 1;
            // 
            // FindReplace
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.Controls.Add(this.panel1);
            this.Controls.Add(this.toolStrip1);
            this.Name = "FindReplace";
            this.Size = new System.Drawing.Size(338, 78);
            this.toolStrip1.ResumeLayout(false);
            this.toolStrip1.PerformLayout();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.ToolStrip toolStrip1;
        private System.Windows.Forms.ToolStripButton showReplaceToolStripButton;
        private System.Windows.Forms.ToolStripComboBox findToolStripComboBox;
        private System.Windows.Forms.ToolStripSplitButton findToolStripSplitButton;
        private System.Windows.Forms.ToolStripButton closeToolStripButton;
        private System.Windows.Forms.ToolStripComboBox replaceToolStripComboBox;
        private System.Windows.Forms.ToolStripButton replaceToolStripButton;
        private System.Windows.Forms.ToolStripButton replaceAllToolStripButton;
        private System.Windows.Forms.ToolStripButton matchCaseToolStripButton;
        private System.Windows.Forms.ToolStripButton wholeWordToolStripButton;
        private System.Windows.Forms.ToolStripButton regexToolStripButton;
        private System.Windows.Forms.ToolStripLabel replaceSpacerToolStripLabel;
        private System.Windows.Forms.ToolStripLabel optionsSpacerToolStripLabel;
        private System.Windows.Forms.ToolStripMenuItem findNextToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem findPreviousToolStripMenuItem;
        private System.Windows.Forms.Panel panel1;
    }
}
