namespace MarkMpn.Sql4Cds
{
    partial class SettingsForm
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

        #region Windows Form Designer generated code

        /// <summary>
        /// Required method for Designer support - do not modify
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(SettingsForm));
            this.topPanel = new System.Windows.Forms.Panel();
            this.pictureBox = new System.Windows.Forms.PictureBox();
            this.label1 = new System.Windows.Forms.Label();
            this.panel2 = new System.Windows.Forms.Panel();
            this.cancelButton = new System.Windows.Forms.Button();
            this.okButton = new System.Windows.Forms.Button();
            this.label2 = new System.Windows.Forms.Label();
            this.selectLimitUpDown = new System.Windows.Forms.NumericUpDown();
            this.label3 = new System.Windows.Forms.Label();
            this.label4 = new System.Windows.Forms.Label();
            this.updateWarnThresholdUpDown = new System.Windows.Forms.NumericUpDown();
            this.label5 = new System.Windows.Forms.Label();
            this.label6 = new System.Windows.Forms.Label();
            this.deleteWarnThresholdUpDown = new System.Windows.Forms.NumericUpDown();
            this.label7 = new System.Windows.Forms.Label();
            this.blockUpdateWithoutWhereCheckbox = new System.Windows.Forms.CheckBox();
            this.blockDeleteWithoutWhereCheckbox = new System.Windows.Forms.CheckBox();
            this.bulkDeleteCheckbox = new System.Windows.Forms.CheckBox();
            this.label8 = new System.Windows.Forms.Label();
            this.batchSizeUpDown = new System.Windows.Forms.NumericUpDown();
            this.label9 = new System.Windows.Forms.Label();
            this.groupBox1 = new System.Windows.Forms.GroupBox();
            this.label12 = new System.Windows.Forms.Label();
            this.maxDopUpDown = new System.Windows.Forms.NumericUpDown();
            this.label13 = new System.Windows.Forms.Label();
            this.retrieveTotalRecordCountCheckbox = new System.Windows.Forms.CheckBox();
            this.tsqlEndpointCheckBox = new System.Windows.Forms.CheckBox();
            this.groupBox2 = new System.Windows.Forms.GroupBox();
            this.autoSizeColumnsCheckBox = new System.Windows.Forms.CheckBox();
            this.localTimesComboBox = new System.Windows.Forms.ComboBox();
            this.label11 = new System.Windows.Forms.Label();
            this.groupBox3 = new System.Windows.Forms.GroupBox();
            this.quotedIdentifiersCheckbox = new System.Windows.Forms.CheckBox();
            this.groupBox4 = new System.Windows.Forms.GroupBox();
            this.showTooltipsCheckbox = new System.Windows.Forms.CheckBox();
            this.topPanel.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)(this.pictureBox)).BeginInit();
            this.panel2.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)(this.selectLimitUpDown)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.updateWarnThresholdUpDown)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.deleteWarnThresholdUpDown)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.batchSizeUpDown)).BeginInit();
            this.groupBox1.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)(this.maxDopUpDown)).BeginInit();
            this.groupBox2.SuspendLayout();
            this.groupBox3.SuspendLayout();
            this.groupBox4.SuspendLayout();
            this.SuspendLayout();
            // 
            // topPanel
            // 
            this.topPanel.BackColor = System.Drawing.Color.FromArgb(((int)(((byte)(7)))), ((int)(((byte)(14)))), ((int)(((byte)(22)))));
            this.topPanel.Controls.Add(this.pictureBox);
            this.topPanel.Controls.Add(this.label1);
            this.topPanel.Dock = System.Windows.Forms.DockStyle.Top;
            this.topPanel.Location = new System.Drawing.Point(0, 0);
            this.topPanel.Margin = new System.Windows.Forms.Padding(2);
            this.topPanel.Name = "topPanel";
            this.topPanel.Size = new System.Drawing.Size(400, 52);
            this.topPanel.TabIndex = 0;
            // 
            // pictureBox
            // 
            this.pictureBox.BackColor = System.Drawing.Color.FromArgb(((int)(((byte)(7)))), ((int)(((byte)(14)))), ((int)(((byte)(22)))));
            this.pictureBox.Image = ((System.Drawing.Image)(resources.GetObject("pictureBox.Image")));
            this.pictureBox.Location = new System.Drawing.Point(6, 6);
            this.pictureBox.Margin = new System.Windows.Forms.Padding(2);
            this.pictureBox.Name = "pictureBox";
            this.pictureBox.Size = new System.Drawing.Size(40, 40);
            this.pictureBox.SizeMode = System.Windows.Forms.PictureBoxSizeMode.StretchImage;
            this.pictureBox.TabIndex = 0;
            this.pictureBox.TabStop = false;
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Font = new System.Drawing.Font("Tahoma", 10F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label1.ForeColor = System.Drawing.Color.White;
            this.label1.Location = new System.Drawing.Point(49, 17);
            this.label1.Margin = new System.Windows.Forms.Padding(2, 0, 2, 0);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(143, 17);
            this.label1.TabIndex = 1;
            this.label1.Text = "SQL 4 CDS Settings";
            // 
            // panel2
            // 
            this.panel2.Controls.Add(this.cancelButton);
            this.panel2.Controls.Add(this.okButton);
            this.panel2.Dock = System.Windows.Forms.DockStyle.Bottom;
            this.panel2.Location = new System.Drawing.Point(0, 517);
            this.panel2.Margin = new System.Windows.Forms.Padding(2);
            this.panel2.Name = "panel2";
            this.panel2.Size = new System.Drawing.Size(400, 45);
            this.panel2.TabIndex = 0;
            // 
            // cancelButton
            // 
            this.cancelButton.DialogResult = System.Windows.Forms.DialogResult.Cancel;
            this.cancelButton.Location = new System.Drawing.Point(313, 10);
            this.cancelButton.Name = "cancelButton";
            this.cancelButton.Size = new System.Drawing.Size(75, 23);
            this.cancelButton.TabIndex = 1;
            this.cancelButton.Text = "Cancel";
            this.cancelButton.UseVisualStyleBackColor = true;
            // 
            // okButton
            // 
            this.okButton.DialogResult = System.Windows.Forms.DialogResult.OK;
            this.okButton.Location = new System.Drawing.Point(232, 10);
            this.okButton.Name = "okButton";
            this.okButton.Size = new System.Drawing.Size(75, 23);
            this.okButton.TabIndex = 0;
            this.okButton.Text = "OK";
            this.okButton.UseVisualStyleBackColor = true;
            // 
            // label2
            // 
            this.label2.AutoSize = true;
            this.label2.Location = new System.Drawing.Point(10, 20);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(73, 13);
            this.label2.TabIndex = 1;
            this.label2.Text = "Limit results to";
            // 
            // selectLimitUpDown
            // 
            this.selectLimitUpDown.Location = new System.Drawing.Point(88, 18);
            this.selectLimitUpDown.Maximum = new decimal(new int[] {
            100000,
            0,
            0,
            0});
            this.selectLimitUpDown.Name = "selectLimitUpDown";
            this.selectLimitUpDown.Size = new System.Drawing.Size(102, 20);
            this.selectLimitUpDown.TabIndex = 2;
            // 
            // label3
            // 
            this.label3.AutoSize = true;
            this.label3.Location = new System.Drawing.Point(196, 20);
            this.label3.Name = "label3";
            this.label3.Size = new System.Drawing.Size(116, 13);
            this.label3.TabIndex = 3;
            this.label3.Text = "records (0 for unlimited)";
            // 
            // label4
            // 
            this.label4.AutoSize = true;
            this.label4.Location = new System.Drawing.Point(280, 46);
            this.label4.Name = "label4";
            this.label4.Size = new System.Drawing.Size(42, 13);
            this.label4.TabIndex = 6;
            this.label4.Text = "records";
            // 
            // updateWarnThresholdUpDown
            // 
            this.updateWarnThresholdUpDown.Location = new System.Drawing.Point(172, 44);
            this.updateWarnThresholdUpDown.Maximum = new decimal(new int[] {
            100000,
            0,
            0,
            0});
            this.updateWarnThresholdUpDown.Name = "updateWarnThresholdUpDown";
            this.updateWarnThresholdUpDown.Size = new System.Drawing.Size(102, 20);
            this.updateWarnThresholdUpDown.TabIndex = 5;
            // 
            // label5
            // 
            this.label5.AutoSize = true;
            this.label5.Location = new System.Drawing.Point(10, 46);
            this.label5.Name = "label5";
            this.label5.Size = new System.Drawing.Size(156, 13);
            this.label5.TabIndex = 4;
            this.label5.Text = "Warn when updating more than";
            // 
            // label6
            // 
            this.label6.AutoSize = true;
            this.label6.Location = new System.Drawing.Point(280, 95);
            this.label6.Name = "label6";
            this.label6.Size = new System.Drawing.Size(42, 13);
            this.label6.TabIndex = 9;
            this.label6.Text = "records";
            // 
            // deleteWarnThresholdUpDown
            // 
            this.deleteWarnThresholdUpDown.Location = new System.Drawing.Point(172, 93);
            this.deleteWarnThresholdUpDown.Maximum = new decimal(new int[] {
            100000,
            0,
            0,
            0});
            this.deleteWarnThresholdUpDown.Name = "deleteWarnThresholdUpDown";
            this.deleteWarnThresholdUpDown.Size = new System.Drawing.Size(102, 20);
            this.deleteWarnThresholdUpDown.TabIndex = 8;
            // 
            // label7
            // 
            this.label7.AutoSize = true;
            this.label7.Location = new System.Drawing.Point(10, 95);
            this.label7.Name = "label7";
            this.label7.Size = new System.Drawing.Size(152, 13);
            this.label7.TabIndex = 7;
            this.label7.Text = "Warn when deleting more than";
            // 
            // blockUpdateWithoutWhereCheckbox
            // 
            this.blockUpdateWithoutWhereCheckbox.AutoSize = true;
            this.blockUpdateWithoutWhereCheckbox.Location = new System.Drawing.Point(12, 70);
            this.blockUpdateWithoutWhereCheckbox.Name = "blockUpdateWithoutWhereCheckbox";
            this.blockUpdateWithoutWhereCheckbox.Size = new System.Drawing.Size(191, 17);
            this.blockUpdateWithoutWhereCheckbox.TabIndex = 10;
            this.blockUpdateWithoutWhereCheckbox.Text = "Prevent UPDATE without WHERE";
            this.blockUpdateWithoutWhereCheckbox.UseVisualStyleBackColor = true;
            // 
            // blockDeleteWithoutWhereCheckbox
            // 
            this.blockDeleteWithoutWhereCheckbox.AutoSize = true;
            this.blockDeleteWithoutWhereCheckbox.Location = new System.Drawing.Point(12, 119);
            this.blockDeleteWithoutWhereCheckbox.Name = "blockDeleteWithoutWhereCheckbox";
            this.blockDeleteWithoutWhereCheckbox.Size = new System.Drawing.Size(189, 17);
            this.blockDeleteWithoutWhereCheckbox.TabIndex = 11;
            this.blockDeleteWithoutWhereCheckbox.Text = "Prevent DELETE without WHERE";
            this.blockDeleteWithoutWhereCheckbox.UseVisualStyleBackColor = true;
            // 
            // bulkDeleteCheckbox
            // 
            this.bulkDeleteCheckbox.AutoSize = true;
            this.bulkDeleteCheckbox.Location = new System.Drawing.Point(13, 194);
            this.bulkDeleteCheckbox.Name = "bulkDeleteCheckbox";
            this.bulkDeleteCheckbox.Size = new System.Drawing.Size(152, 17);
            this.bulkDeleteCheckbox.TabIndex = 12;
            this.bulkDeleteCheckbox.Text = "Use bulk delete operations";
            this.bulkDeleteCheckbox.UseVisualStyleBackColor = true;
            // 
            // label8
            // 
            this.label8.AutoSize = true;
            this.label8.Location = new System.Drawing.Point(335, 144);
            this.label8.Name = "label8";
            this.label8.Size = new System.Drawing.Size(42, 13);
            this.label8.TabIndex = 15;
            this.label8.Text = "records";
            // 
            // batchSizeUpDown
            // 
            this.batchSizeUpDown.Location = new System.Drawing.Point(227, 142);
            this.batchSizeUpDown.Maximum = new decimal(new int[] {
            1000,
            0,
            0,
            0});
            this.batchSizeUpDown.Minimum = new decimal(new int[] {
            1,
            0,
            0,
            0});
            this.batchSizeUpDown.Name = "batchSizeUpDown";
            this.batchSizeUpDown.Size = new System.Drawing.Size(102, 20);
            this.batchSizeUpDown.TabIndex = 14;
            this.batchSizeUpDown.Value = new decimal(new int[] {
            1,
            0,
            0,
            0});
            // 
            // label9
            // 
            this.label9.AutoSize = true;
            this.label9.Location = new System.Drawing.Point(10, 144);
            this.label9.Name = "label9";
            this.label9.Size = new System.Drawing.Size(211, 13);
            this.label9.TabIndex = 13;
            this.label9.Text = "Insert/Update/Delete records in batches of";
            // 
            // groupBox1
            // 
            this.groupBox1.Controls.Add(this.label12);
            this.groupBox1.Controls.Add(this.localTimesComboBox);
            this.groupBox1.Controls.Add(this.label11);
            this.groupBox1.Controls.Add(this.maxDopUpDown);
            this.groupBox1.Controls.Add(this.label13);
            this.groupBox1.Controls.Add(this.retrieveTotalRecordCountCheckbox);
            this.groupBox1.Controls.Add(this.tsqlEndpointCheckBox);
            this.groupBox1.Controls.Add(this.label2);
            this.groupBox1.Controls.Add(this.label8);
            this.groupBox1.Controls.Add(this.selectLimitUpDown);
            this.groupBox1.Controls.Add(this.batchSizeUpDown);
            this.groupBox1.Controls.Add(this.label3);
            this.groupBox1.Controls.Add(this.label9);
            this.groupBox1.Controls.Add(this.label5);
            this.groupBox1.Controls.Add(this.bulkDeleteCheckbox);
            this.groupBox1.Controls.Add(this.updateWarnThresholdUpDown);
            this.groupBox1.Controls.Add(this.blockDeleteWithoutWhereCheckbox);
            this.groupBox1.Controls.Add(this.label4);
            this.groupBox1.Controls.Add(this.blockUpdateWithoutWhereCheckbox);
            this.groupBox1.Controls.Add(this.label7);
            this.groupBox1.Controls.Add(this.label6);
            this.groupBox1.Controls.Add(this.deleteWarnThresholdUpDown);
            this.groupBox1.Location = new System.Drawing.Point(11, 125);
            this.groupBox1.Margin = new System.Windows.Forms.Padding(2);
            this.groupBox1.Name = "groupBox1";
            this.groupBox1.Padding = new System.Windows.Forms.Padding(2);
            this.groupBox1.Size = new System.Drawing.Size(382, 290);
            this.groupBox1.TabIndex = 16;
            this.groupBox1.TabStop = false;
            this.groupBox1.Text = "Query Execution";
            // 
            // label12
            // 
            this.label12.AutoSize = true;
            this.label12.Location = new System.Drawing.Point(177, 170);
            this.label12.Name = "label12";
            this.label12.Size = new System.Drawing.Size(155, 13);
            this.label12.TabIndex = 20;
            this.label12.Text = "worker threads for DML queries";
            // 
            // maxDopUpDown
            // 
            this.maxDopUpDown.Location = new System.Drawing.Point(69, 168);
            this.maxDopUpDown.Minimum = new decimal(new int[] {
            1,
            0,
            0,
            0});
            this.maxDopUpDown.Name = "maxDopUpDown";
            this.maxDopUpDown.Size = new System.Drawing.Size(102, 20);
            this.maxDopUpDown.TabIndex = 19;
            this.maxDopUpDown.Value = new decimal(new int[] {
            10,
            0,
            0,
            0});
            // 
            // label13
            // 
            this.label13.AutoSize = true;
            this.label13.Location = new System.Drawing.Point(10, 170);
            this.label13.Name = "label13";
            this.label13.Size = new System.Drawing.Size(53, 13);
            this.label13.TabIndex = 18;
            this.label13.Text = "Use up to";
            // 
            // retrieveTotalRecordCountCheckbox
            // 
            this.retrieveTotalRecordCountCheckbox.AutoSize = true;
            this.retrieveTotalRecordCountCheckbox.Location = new System.Drawing.Point(13, 240);
            this.retrieveTotalRecordCountCheckbox.Name = "retrieveTotalRecordCountCheckbox";
            this.retrieveTotalRecordCountCheckbox.Size = new System.Drawing.Size(286, 17);
            this.retrieveTotalRecordCountCheckbox.TabIndex = 17;
            this.retrieveTotalRecordCountCheckbox.Text = "Use RetrieveTotalRecordCount request where possible";
            this.retrieveTotalRecordCountCheckbox.UseVisualStyleBackColor = true;
            // 
            // tsqlEndpointCheckBox
            // 
            this.tsqlEndpointCheckBox.AutoSize = true;
            this.tsqlEndpointCheckBox.Location = new System.Drawing.Point(13, 217);
            this.tsqlEndpointCheckBox.Name = "tsqlEndpointCheckBox";
            this.tsqlEndpointCheckBox.Size = new System.Drawing.Size(235, 17);
            this.tsqlEndpointCheckBox.TabIndex = 16;
            this.tsqlEndpointCheckBox.Text = "Use TDS Endpoint where possible (Preview)";
            this.tsqlEndpointCheckBox.UseVisualStyleBackColor = true;
            // 
            // groupBox2
            // 
            this.groupBox2.Controls.Add(this.autoSizeColumnsCheckBox);
            this.groupBox2.Location = new System.Drawing.Point(11, 419);
            this.groupBox2.Margin = new System.Windows.Forms.Padding(2);
            this.groupBox2.Name = "groupBox2";
            this.groupBox2.Padding = new System.Windows.Forms.Padding(2);
            this.groupBox2.Size = new System.Drawing.Size(382, 43);
            this.groupBox2.TabIndex = 17;
            this.groupBox2.TabStop = false;
            this.groupBox2.Text = "Results";
            // 
            // autoSizeColumnsCheckBox
            // 
            this.autoSizeColumnsCheckBox.AutoSize = true;
            this.autoSizeColumnsCheckBox.Location = new System.Drawing.Point(12, 18);
            this.autoSizeColumnsCheckBox.Name = "autoSizeColumnsCheckBox";
            this.autoSizeColumnsCheckBox.Size = new System.Drawing.Size(158, 17);
            this.autoSizeColumnsCheckBox.TabIndex = 18;
            this.autoSizeColumnsCheckBox.Text = "Auto-size columns to fit data";
            this.autoSizeColumnsCheckBox.UseVisualStyleBackColor = true;
            // 
            // localTimesComboBox
            // 
            this.localTimesComboBox.DropDownStyle = System.Windows.Forms.ComboBoxStyle.DropDownList;
            this.localTimesComboBox.FormattingEnabled = true;
            this.localTimesComboBox.Items.AddRange(new object[] {
            "UTC times",
            "Local times"});
            this.localTimesComboBox.Location = new System.Drawing.Point(142, 262);
            this.localTimesComboBox.Margin = new System.Windows.Forms.Padding(2);
            this.localTimesComboBox.Name = "localTimesComboBox";
            this.localTimesComboBox.Size = new System.Drawing.Size(203, 21);
            this.localTimesComboBox.TabIndex = 3;
            // 
            // label11
            // 
            this.label11.AutoSize = true;
            this.label11.Location = new System.Drawing.Point(10, 265);
            this.label11.Margin = new System.Windows.Forms.Padding(2, 0, 2, 0);
            this.label11.Name = "label11";
            this.label11.Size = new System.Drawing.Size(128, 13);
            this.label11.TabIndex = 2;
            this.label11.Text = "Treat date/time values as";
            // 
            // groupBox3
            // 
            this.groupBox3.Controls.Add(this.quotedIdentifiersCheckbox);
            this.groupBox3.Location = new System.Drawing.Point(11, 65);
            this.groupBox3.Name = "groupBox3";
            this.groupBox3.Size = new System.Drawing.Size(382, 55);
            this.groupBox3.TabIndex = 18;
            this.groupBox3.TabStop = false;
            this.groupBox3.Text = "SQL Language";
            // 
            // quotedIdentifiersCheckbox
            // 
            this.quotedIdentifiersCheckbox.AutoSize = true;
            this.quotedIdentifiersCheckbox.Location = new System.Drawing.Point(12, 28);
            this.quotedIdentifiersCheckbox.Name = "quotedIdentifiersCheckbox";
            this.quotedIdentifiersCheckbox.Size = new System.Drawing.Size(109, 17);
            this.quotedIdentifiersCheckbox.TabIndex = 0;
            this.quotedIdentifiersCheckbox.Text = "Quoted Identifiers";
            this.quotedIdentifiersCheckbox.UseVisualStyleBackColor = true;
            // 
            // groupBox4
            // 
            this.groupBox4.Controls.Add(this.showTooltipsCheckbox);
            this.groupBox4.Location = new System.Drawing.Point(11, 467);
            this.groupBox4.Name = "groupBox4";
            this.groupBox4.Size = new System.Drawing.Size(382, 43);
            this.groupBox4.TabIndex = 19;
            this.groupBox4.TabStop = false;
            this.groupBox4.Text = "Intellisense";
            // 
            // showTooltipsCheckbox
            // 
            this.showTooltipsCheckbox.AutoSize = true;
            this.showTooltipsCheckbox.Location = new System.Drawing.Point(12, 19);
            this.showTooltipsCheckbox.Name = "showTooltipsCheckbox";
            this.showTooltipsCheckbox.Size = new System.Drawing.Size(89, 17);
            this.showTooltipsCheckbox.TabIndex = 0;
            this.showTooltipsCheckbox.Text = "Show tooltips";
            this.showTooltipsCheckbox.UseVisualStyleBackColor = true;
            // 
            // SettingsForm
            // 
            this.AcceptButton = this.okButton;
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.CancelButton = this.cancelButton;
            this.ClientSize = new System.Drawing.Size(400, 562);
            this.Controls.Add(this.groupBox4);
            this.Controls.Add(this.groupBox3);
            this.Controls.Add(this.groupBox2);
            this.Controls.Add(this.groupBox1);
            this.Controls.Add(this.panel2);
            this.Controls.Add(this.topPanel);
            this.FormBorderStyle = System.Windows.Forms.FormBorderStyle.FixedDialog;
            this.Margin = new System.Windows.Forms.Padding(2);
            this.MaximizeBox = false;
            this.MinimizeBox = false;
            this.Name = "SettingsForm";
            this.ShowIcon = false;
            this.ShowInTaskbar = false;
            this.StartPosition = System.Windows.Forms.FormStartPosition.CenterParent;
            this.Text = "SQL 4 CDS Settings";
            this.topPanel.ResumeLayout(false);
            this.topPanel.PerformLayout();
            ((System.ComponentModel.ISupportInitialize)(this.pictureBox)).EndInit();
            this.panel2.ResumeLayout(false);
            ((System.ComponentModel.ISupportInitialize)(this.selectLimitUpDown)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.updateWarnThresholdUpDown)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.deleteWarnThresholdUpDown)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.batchSizeUpDown)).EndInit();
            this.groupBox1.ResumeLayout(false);
            this.groupBox1.PerformLayout();
            ((System.ComponentModel.ISupportInitialize)(this.maxDopUpDown)).EndInit();
            this.groupBox2.ResumeLayout(false);
            this.groupBox2.PerformLayout();
            this.groupBox3.ResumeLayout(false);
            this.groupBox3.PerformLayout();
            this.groupBox4.ResumeLayout(false);
            this.groupBox4.PerformLayout();
            this.ResumeLayout(false);

        }

        #endregion

        private System.Windows.Forms.Panel topPanel;
        private System.Windows.Forms.Panel panel2;
        private System.Windows.Forms.PictureBox pictureBox;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.Button cancelButton;
        private System.Windows.Forms.Button okButton;
        private System.Windows.Forms.Label label2;
        private System.Windows.Forms.NumericUpDown selectLimitUpDown;
        private System.Windows.Forms.Label label3;
        private System.Windows.Forms.Label label4;
        private System.Windows.Forms.NumericUpDown updateWarnThresholdUpDown;
        private System.Windows.Forms.Label label5;
        private System.Windows.Forms.Label label6;
        private System.Windows.Forms.NumericUpDown deleteWarnThresholdUpDown;
        private System.Windows.Forms.Label label7;
        private System.Windows.Forms.CheckBox blockUpdateWithoutWhereCheckbox;
        private System.Windows.Forms.CheckBox blockDeleteWithoutWhereCheckbox;
        private System.Windows.Forms.CheckBox bulkDeleteCheckbox;
        private System.Windows.Forms.Label label8;
        private System.Windows.Forms.NumericUpDown batchSizeUpDown;
        private System.Windows.Forms.Label label9;
        private System.Windows.Forms.GroupBox groupBox1;
        private System.Windows.Forms.GroupBox groupBox2;
        private System.Windows.Forms.ComboBox localTimesComboBox;
        private System.Windows.Forms.Label label11;
        private System.Windows.Forms.GroupBox groupBox3;
        private System.Windows.Forms.CheckBox quotedIdentifiersCheckbox;
        private System.Windows.Forms.CheckBox tsqlEndpointCheckBox;
        private System.Windows.Forms.CheckBox retrieveTotalRecordCountCheckbox;
        private System.Windows.Forms.GroupBox groupBox4;
        private System.Windows.Forms.CheckBox showTooltipsCheckbox;
        private System.Windows.Forms.Label label12;
        private System.Windows.Forms.NumericUpDown maxDopUpDown;
        private System.Windows.Forms.Label label13;
        private System.Windows.Forms.CheckBox autoSizeColumnsCheckBox;
    }
}