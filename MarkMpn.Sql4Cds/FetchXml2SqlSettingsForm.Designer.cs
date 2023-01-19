
namespace MarkMpn.Sql4Cds
{
    partial class FetchXml2SqlSettingsForm
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
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(FetchXml2SqlSettingsForm));
            this.panel2 = new System.Windows.Forms.Panel();
            this.cancelButton = new System.Windows.Forms.Button();
            this.okButton = new System.Windows.Forms.Button();
            this.label1 = new System.Windows.Forms.Label();
            this.operatorConversionComboBox = new System.Windows.Forms.ComboBox();
            this.useParametersCheckBox = new System.Windows.Forms.CheckBox();
            this.useUTCCheckBox = new System.Windows.Forms.CheckBox();
            this.fetchXmlScintilla = new ScintillaNET.Scintilla();
            this.sqlScintilla = new ScintillaNET.Scintilla();
            this.panel2.SuspendLayout();
            this.SuspendLayout();
            // 
            // panel2
            // 
            this.panel2.Controls.Add(this.cancelButton);
            this.panel2.Controls.Add(this.okButton);
            this.panel2.Dock = System.Windows.Forms.DockStyle.Bottom;
            this.panel2.Location = new System.Drawing.Point(0, 352);
            this.panel2.Margin = new System.Windows.Forms.Padding(2);
            this.panel2.Name = "panel2";
            this.panel2.Size = new System.Drawing.Size(370, 45);
            this.panel2.TabIndex = 3;
            // 
            // cancelButton
            // 
            this.cancelButton.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Right)));
            this.cancelButton.DialogResult = System.Windows.Forms.DialogResult.Cancel;
            this.cancelButton.Location = new System.Drawing.Point(279, 10);
            this.cancelButton.Name = "cancelButton";
            this.cancelButton.Size = new System.Drawing.Size(75, 23);
            this.cancelButton.TabIndex = 1;
            this.cancelButton.Text = "Cancel";
            this.cancelButton.UseVisualStyleBackColor = true;
            // 
            // okButton
            // 
            this.okButton.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Right)));
            this.okButton.DialogResult = System.Windows.Forms.DialogResult.OK;
            this.okButton.Location = new System.Drawing.Point(198, 10);
            this.okButton.Name = "okButton";
            this.okButton.Size = new System.Drawing.Size(75, 23);
            this.okButton.TabIndex = 0;
            this.okButton.Text = "OK";
            this.okButton.UseVisualStyleBackColor = true;
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Location = new System.Drawing.Point(10, 9);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(263, 13);
            this.label1.TabIndex = 4;
            this.label1.Text = "Convert advanced FetchXML comparison operators to";
            // 
            // operatorConversionComboBox
            // 
            this.operatorConversionComboBox.DropDownStyle = System.Windows.Forms.ComboBoxStyle.DropDownList;
            this.operatorConversionComboBox.FormattingEnabled = true;
            this.operatorConversionComboBox.Items.AddRange(new object[] {
            "Functions",
            "Literals",
            "SQL Calculations"});
            this.operatorConversionComboBox.Location = new System.Drawing.Point(12, 25);
            this.operatorConversionComboBox.Name = "operatorConversionComboBox";
            this.operatorConversionComboBox.Size = new System.Drawing.Size(158, 21);
            this.operatorConversionComboBox.TabIndex = 5;
            this.operatorConversionComboBox.SelectedIndexChanged += new System.EventHandler(this.operatorConversionComboBox_SelectedIndexChanged);
            // 
            // useParametersCheckBox
            // 
            this.useParametersCheckBox.AutoSize = true;
            this.useParametersCheckBox.Location = new System.Drawing.Point(13, 61);
            this.useParametersCheckBox.Name = "useParametersCheckBox";
            this.useParametersCheckBox.Size = new System.Drawing.Size(147, 17);
            this.useParametersCheckBox.TabIndex = 6;
            this.useParametersCheckBox.Text = "Use parameters for literals";
            this.useParametersCheckBox.UseVisualStyleBackColor = true;
            this.useParametersCheckBox.CheckedChanged += new System.EventHandler(this.useParametersCheckBox_CheckedChanged);
            // 
            // useUTCCheckBox
            // 
            this.useUTCCheckBox.AutoSize = true;
            this.useUTCCheckBox.Location = new System.Drawing.Point(13, 84);
            this.useUTCCheckBox.Name = "useUTCCheckBox";
            this.useUTCCheckBox.Size = new System.Drawing.Size(182, 17);
            this.useUTCCheckBox.TabIndex = 7;
            this.useUTCCheckBox.Text = "Convert date/time values to UTC";
            this.useUTCCheckBox.UseVisualStyleBackColor = true;
            this.useUTCCheckBox.CheckedChanged += new System.EventHandler(this.useUTCCheckBox_CheckedChanged);
            // 
            // fetchXmlScintilla
            // 
            this.fetchXmlScintilla.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.fetchXmlScintilla.Location = new System.Drawing.Point(13, 107);
            this.fetchXmlScintilla.Name = "fetchXmlScintilla";
            this.fetchXmlScintilla.Size = new System.Drawing.Size(345, 116);
            this.fetchXmlScintilla.TabIndex = 8;
            this.fetchXmlScintilla.Text = resources.GetString("fetchXmlScintilla.Text");
            // 
            // sqlScintilla
            // 
            this.sqlScintilla.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.sqlScintilla.Location = new System.Drawing.Point(9, 229);
            this.sqlScintilla.Name = "sqlScintilla";
            this.sqlScintilla.Size = new System.Drawing.Size(345, 118);
            this.sqlScintilla.TabIndex = 9;
            this.sqlScintilla.Text = "SELECT name,\r\n       telephone1,\r\n       ownerid\r\nFROM   account;";
            // 
            // FetchXml2SqlSettingsForm
            // 
            this.AcceptButton = this.okButton;
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.CancelButton = this.cancelButton;
            this.ClientSize = new System.Drawing.Size(370, 397);
            this.Controls.Add(this.sqlScintilla);
            this.Controls.Add(this.fetchXmlScintilla);
            this.Controls.Add(this.useUTCCheckBox);
            this.Controls.Add(this.useParametersCheckBox);
            this.Controls.Add(this.operatorConversionComboBox);
            this.Controls.Add(this.label1);
            this.Controls.Add(this.panel2);
            this.FormBorderStyle = System.Windows.Forms.FormBorderStyle.FixedDialog;
            this.Name = "FetchXml2SqlSettingsForm";
            this.StartPosition = System.Windows.Forms.FormStartPosition.CenterParent;
            this.Text = "FetchXML to SQL Conversion Advanced Settings";
            this.panel2.ResumeLayout(false);
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Panel panel2;
        private System.Windows.Forms.Button cancelButton;
        private System.Windows.Forms.Button okButton;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.ComboBox operatorConversionComboBox;
        private System.Windows.Forms.CheckBox useParametersCheckBox;
        private System.Windows.Forms.CheckBox useUTCCheckBox;
        private ScintillaNET.Scintilla fetchXmlScintilla;
        private ScintillaNET.Scintilla sqlScintilla;
    }
}