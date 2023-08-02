namespace xrmtb.XrmToolBox.Controls.Controls
{
    partial class CDSLookupDialogForm
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
            this.components = new System.ComponentModel.Container();
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(CDSLookupDialogForm));
            this.panel1 = new System.Windows.Forms.Panel();
            this.btnFilter = new System.Windows.Forms.Button();
            this.txtFilter = new System.Windows.Forms.TextBox();
            this.label3 = new System.Windows.Forms.Label();
            this.label2 = new System.Windows.Forms.Label();
            this.cmbEntity = new System.Windows.Forms.ComboBox();
            this.label1 = new System.Windows.Forms.Label();
            this.panel2 = new System.Windows.Forms.Panel();
            this.btnRemoveValue = new System.Windows.Forms.Button();
            this.btnCancel = new System.Windows.Forms.Button();
            this.btnOk = new System.Windows.Forms.Button();
            this.panel3 = new System.Windows.Forms.Panel();
            this.btnClearSelection = new System.Windows.Forms.Button();
            this.btnAddSelection = new System.Windows.Forms.Button();
            this.btnRemoveSelection = new System.Windows.Forms.Button();
            this.splitGrids = new System.Windows.Forms.SplitContainer();
            this.timerLoadData = new System.Windows.Forms.Timer(this.components);
            this.gridResults = new xrmtb.XrmToolBox.Controls.CRMGridView();
            this.gridSelection = new xrmtb.XrmToolBox.Controls.CRMGridView();
            this.cmbView = new xrmtb.XrmToolBox.Controls.Controls.CDSDataComboBox();
            this.panel1.SuspendLayout();
            this.panel2.SuspendLayout();
            this.panel3.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)(this.splitGrids)).BeginInit();
            this.splitGrids.Panel1.SuspendLayout();
            this.splitGrids.Panel2.SuspendLayout();
            this.splitGrids.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)(this.gridResults)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.gridSelection)).BeginInit();
            this.SuspendLayout();
            // 
            // panel1
            // 
            this.panel1.Controls.Add(this.btnFilter);
            this.panel1.Controls.Add(this.txtFilter);
            this.panel1.Controls.Add(this.label3);
            this.panel1.Controls.Add(this.cmbView);
            this.panel1.Controls.Add(this.label2);
            this.panel1.Controls.Add(this.cmbEntity);
            this.panel1.Controls.Add(this.label1);
            this.panel1.Dock = System.Windows.Forms.DockStyle.Top;
            this.panel1.Location = new System.Drawing.Point(0, 0);
            this.panel1.Name = "panel1";
            this.panel1.Size = new System.Drawing.Size(533, 110);
            this.panel1.TabIndex = 0;
            // 
            // btnFilter
            // 
            this.btnFilter.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Right)));
            this.btnFilter.Image = ((System.Drawing.Image)(resources.GetObject("btnFilter.Image")));
            this.btnFilter.Location = new System.Drawing.Point(496, 73);
            this.btnFilter.Name = "btnFilter";
            this.btnFilter.Size = new System.Drawing.Size(26, 22);
            this.btnFilter.TabIndex = 6;
            this.btnFilter.UseVisualStyleBackColor = true;
            this.btnFilter.Click += new System.EventHandler(this.btnFilter_Click);
            // 
            // txtFilter
            // 
            this.txtFilter.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.txtFilter.Location = new System.Drawing.Point(100, 74);
            this.txtFilter.Name = "txtFilter";
            this.txtFilter.Size = new System.Drawing.Size(390, 20);
            this.txtFilter.TabIndex = 5;
            this.txtFilter.Enter += new System.EventHandler(this.txtFilter_Enter);
            this.txtFilter.Leave += new System.EventHandler(this.txtFilter_Leave);
            // 
            // label3
            // 
            this.label3.AutoSize = true;
            this.label3.Location = new System.Drawing.Point(12, 77);
            this.label3.Name = "label3";
            this.label3.Size = new System.Drawing.Size(29, 13);
            this.label3.TabIndex = 4;
            this.label3.Text = "Filter";
            // 
            // label2
            // 
            this.label2.AutoSize = true;
            this.label2.Location = new System.Drawing.Point(12, 49);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(30, 13);
            this.label2.TabIndex = 2;
            this.label2.Text = "View";
            // 
            // cmbEntity
            // 
            this.cmbEntity.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.cmbEntity.BackColor = System.Drawing.SystemColors.Window;
            this.cmbEntity.DropDownStyle = System.Windows.Forms.ComboBoxStyle.DropDownList;
            this.cmbEntity.FormattingEnabled = true;
            this.cmbEntity.Location = new System.Drawing.Point(100, 19);
            this.cmbEntity.Name = "cmbEntity";
            this.cmbEntity.Size = new System.Drawing.Size(421, 21);
            this.cmbEntity.TabIndex = 1;
            this.cmbEntity.SelectedIndexChanged += new System.EventHandler(this.cmbEntity_SelectedIndexChanged);
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Location = new System.Drawing.Point(12, 22);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(33, 13);
            this.label1.TabIndex = 0;
            this.label1.Text = "Entity";
            // 
            // panel2
            // 
            this.panel2.Controls.Add(this.btnRemoveValue);
            this.panel2.Controls.Add(this.btnCancel);
            this.panel2.Controls.Add(this.btnOk);
            this.panel2.Dock = System.Windows.Forms.DockStyle.Bottom;
            this.panel2.Location = new System.Drawing.Point(0, 501);
            this.panel2.Name = "panel2";
            this.panel2.Size = new System.Drawing.Size(533, 37);
            this.panel2.TabIndex = 1;
            // 
            // btnRemoveValue
            // 
            this.btnRemoveValue.DialogResult = System.Windows.Forms.DialogResult.Abort;
            this.btnRemoveValue.Location = new System.Drawing.Point(12, 6);
            this.btnRemoveValue.Name = "btnRemoveValue";
            this.btnRemoveValue.Size = new System.Drawing.Size(106, 23);
            this.btnRemoveValue.TabIndex = 2;
            this.btnRemoveValue.Text = "Remove value";
            this.btnRemoveValue.UseVisualStyleBackColor = true;
            // 
            // btnCancel
            // 
            this.btnCancel.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Right)));
            this.btnCancel.DialogResult = System.Windows.Forms.DialogResult.Cancel;
            this.btnCancel.Location = new System.Drawing.Point(446, 6);
            this.btnCancel.Name = "btnCancel";
            this.btnCancel.Size = new System.Drawing.Size(75, 23);
            this.btnCancel.TabIndex = 1;
            this.btnCancel.Text = "Cancel";
            this.btnCancel.UseVisualStyleBackColor = true;
            // 
            // btnOk
            // 
            this.btnOk.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Right)));
            this.btnOk.DialogResult = System.Windows.Forms.DialogResult.OK;
            this.btnOk.Location = new System.Drawing.Point(365, 6);
            this.btnOk.Name = "btnOk";
            this.btnOk.Size = new System.Drawing.Size(75, 23);
            this.btnOk.TabIndex = 0;
            this.btnOk.Text = "Ok";
            this.btnOk.UseVisualStyleBackColor = true;
            // 
            // panel3
            // 
            this.panel3.Controls.Add(this.btnClearSelection);
            this.panel3.Controls.Add(this.btnAddSelection);
            this.panel3.Controls.Add(this.btnRemoveSelection);
            this.panel3.Dock = System.Windows.Forms.DockStyle.Left;
            this.panel3.Location = new System.Drawing.Point(0, 0);
            this.panel3.Name = "panel3";
            this.panel3.Size = new System.Drawing.Size(99, 118);
            this.panel3.TabIndex = 2;
            // 
            // btnClearSelection
            // 
            this.btnClearSelection.Location = new System.Drawing.Point(12, 64);
            this.btnClearSelection.Name = "btnClearSelection";
            this.btnClearSelection.Size = new System.Drawing.Size(75, 23);
            this.btnClearSelection.TabIndex = 2;
            this.btnClearSelection.Text = "Clear";
            this.btnClearSelection.UseVisualStyleBackColor = true;
            this.btnClearSelection.Click += new System.EventHandler(this.btnClearSelection_Click);
            // 
            // btnAddSelection
            // 
            this.btnAddSelection.Enabled = false;
            this.btnAddSelection.Location = new System.Drawing.Point(12, 6);
            this.btnAddSelection.Name = "btnAddSelection";
            this.btnAddSelection.Size = new System.Drawing.Size(75, 23);
            this.btnAddSelection.TabIndex = 0;
            this.btnAddSelection.Text = "Add";
            this.btnAddSelection.UseVisualStyleBackColor = true;
            this.btnAddSelection.Click += new System.EventHandler(this.btnAddSelection_Click);
            // 
            // btnRemoveSelection
            // 
            this.btnRemoveSelection.Enabled = false;
            this.btnRemoveSelection.Location = new System.Drawing.Point(12, 35);
            this.btnRemoveSelection.Name = "btnRemoveSelection";
            this.btnRemoveSelection.Size = new System.Drawing.Size(75, 23);
            this.btnRemoveSelection.TabIndex = 1;
            this.btnRemoveSelection.Text = "Remove";
            this.btnRemoveSelection.UseVisualStyleBackColor = true;
            this.btnRemoveSelection.Click += new System.EventHandler(this.btnRemoveSelection_Click);
            // 
            // splitGrids
            // 
            this.splitGrids.Dock = System.Windows.Forms.DockStyle.Fill;
            this.splitGrids.FixedPanel = System.Windows.Forms.FixedPanel.Panel2;
            this.splitGrids.Location = new System.Drawing.Point(0, 110);
            this.splitGrids.Name = "splitGrids";
            this.splitGrids.Orientation = System.Windows.Forms.Orientation.Horizontal;
            // 
            // splitGrids.Panel1
            // 
            this.splitGrids.Panel1.Controls.Add(this.gridResults);
            this.splitGrids.Panel1.Padding = new System.Windows.Forms.Padding(10, 0, 10, 0);
            // 
            // splitGrids.Panel2
            // 
            this.splitGrids.Panel2.Controls.Add(this.gridSelection);
            this.splitGrids.Panel2.Controls.Add(this.panel3);
            this.splitGrids.Panel2.Padding = new System.Windows.Forms.Padding(0, 0, 10, 0);
            this.splitGrids.Size = new System.Drawing.Size(533, 391);
            this.splitGrids.SplitterDistance = 269;
            this.splitGrids.TabIndex = 4;
            // 
            // timerLoadData
            // 
            this.timerLoadData.Tick += new System.EventHandler(this.timerLoadData_Tick);
            // 
            // gridResults
            // 
            this.gridResults.AllowUserToOrderColumns = true;
            this.gridResults.AllowUserToResizeRows = false;
            this.gridResults.AutoSizeColumnsMode = System.Windows.Forms.DataGridViewAutoSizeColumnsMode.AllCells;
            this.gridResults.BackgroundColor = System.Drawing.SystemColors.Window;
            this.gridResults.ColumnHeadersHeightSizeMode = System.Windows.Forms.DataGridViewColumnHeadersHeightSizeMode.AutoSize;
            this.gridResults.ColumnOrder = "";
            this.gridResults.Dock = System.Windows.Forms.DockStyle.Fill;
            this.gridResults.FilterColumns = "";
            this.gridResults.Location = new System.Drawing.Point(10, 0);
            this.gridResults.MultiSelect = false;
            this.gridResults.Name = "gridResults";
            this.gridResults.OrganizationService = null;
            this.gridResults.RowHeadersVisible = false;
            this.gridResults.SelectionMode = System.Windows.Forms.DataGridViewSelectionMode.FullRowSelect;
            this.gridResults.ShowFriendlyNames = true;
            this.gridResults.ShowIdColumn = false;
            this.gridResults.ShowIndexColumn = false;
            this.gridResults.ShowLocalTimes = true;
            this.gridResults.Size = new System.Drawing.Size(513, 269);
            this.gridResults.TabIndex = 2;
            this.gridResults.RecordDoubleClick += new xrmtb.XrmToolBox.Controls.CRMRecordEventHandler(this.gridResults_RecordDoubleClick);
            this.gridResults.SelectionChanged += new System.EventHandler(this.gridResults_SelectionChanged);
            // 
            // gridSelection
            // 
            this.gridSelection.AllowUserToOrderColumns = true;
            this.gridSelection.AllowUserToResizeRows = false;
            this.gridSelection.AutoSizeColumnsMode = System.Windows.Forms.DataGridViewAutoSizeColumnsMode.AllCells;
            this.gridSelection.BackgroundColor = System.Drawing.SystemColors.Window;
            this.gridSelection.ColumnHeadersHeightSizeMode = System.Windows.Forms.DataGridViewColumnHeadersHeightSizeMode.AutoSize;
            this.gridSelection.ColumnHeadersVisible = false;
            this.gridSelection.ColumnOrder = "";
            this.gridSelection.Dock = System.Windows.Forms.DockStyle.Fill;
            this.gridSelection.FilterColumns = "";
            this.gridSelection.Location = new System.Drawing.Point(99, 0);
            this.gridSelection.Name = "gridSelection";
            this.gridSelection.OrganizationService = null;
            this.gridSelection.RowHeadersVisible = false;
            this.gridSelection.SelectionMode = System.Windows.Forms.DataGridViewSelectionMode.FullRowSelect;
            this.gridSelection.ShowFriendlyNames = true;
            this.gridSelection.ShowIdColumn = false;
            this.gridSelection.ShowIndexColumn = false;
            this.gridSelection.ShowLocalTimes = true;
            this.gridSelection.Size = new System.Drawing.Size(424, 118);
            this.gridSelection.TabIndex = 3;
            this.gridSelection.DataSourceChanged += new System.EventHandler(this.gridSelection_DataSourceChanged);
            this.gridSelection.SelectionChanged += new System.EventHandler(this.gridSelection_SelectionChanged);
            // 
            // cmbView
            // 
            this.cmbView.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.cmbView.BackColor = System.Drawing.SystemColors.Window;
            this.cmbView.DisplayFormat = "";
            this.cmbView.DropDownStyle = System.Windows.Forms.ComboBoxStyle.DropDownList;
            this.cmbView.FormattingEnabled = true;
            this.cmbView.Location = new System.Drawing.Point(100, 46);
            this.cmbView.Name = "cmbView";
            this.cmbView.OrganizationService = null;
            this.cmbView.Size = new System.Drawing.Size(421, 21);
            this.cmbView.TabIndex = 3;
            this.cmbView.SelectedIndexChanged += new System.EventHandler(this.cmbView_SelectedIndexChanged);
            // 
            // CDSLookupDialogForm
            // 
            this.AcceptButton = this.btnOk;
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.BackColor = System.Drawing.SystemColors.Window;
            this.CancelButton = this.btnCancel;
            this.ClientSize = new System.Drawing.Size(533, 538);
            this.Controls.Add(this.splitGrids);
            this.Controls.Add(this.panel2);
            this.Controls.Add(this.panel1);
            this.MaximizeBox = false;
            this.MinimizeBox = false;
            this.Name = "CDSLookupDialogForm";
            this.StartPosition = System.Windows.Forms.FormStartPosition.CenterParent;
            this.panel1.ResumeLayout(false);
            this.panel1.PerformLayout();
            this.panel2.ResumeLayout(false);
            this.panel3.ResumeLayout(false);
            this.splitGrids.Panel1.ResumeLayout(false);
            this.splitGrids.Panel2.ResumeLayout(false);
            ((System.ComponentModel.ISupportInitialize)(this.splitGrids)).EndInit();
            this.splitGrids.ResumeLayout(false);
            ((System.ComponentModel.ISupportInitialize)(this.gridResults)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.gridSelection)).EndInit();
            this.ResumeLayout(false);

        }

        #endregion

        private System.Windows.Forms.Panel panel1;
        private System.Windows.Forms.Panel panel2;
        private System.Windows.Forms.Button btnFilter;
        private System.Windows.Forms.TextBox txtFilter;
        private System.Windows.Forms.Label label3;
        private CDSDataComboBox cmbView;
        private System.Windows.Forms.Label label2;
        private System.Windows.Forms.ComboBox cmbEntity;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.Button btnCancel;
        private System.Windows.Forms.Button btnOk;
        internal CRMGridView gridResults;
        internal CRMGridView gridSelection;
        private System.Windows.Forms.Panel panel3;
        private System.Windows.Forms.Button btnClearSelection;
        private System.Windows.Forms.Button btnAddSelection;
        private System.Windows.Forms.Button btnRemoveSelection;
        internal System.Windows.Forms.SplitContainer splitGrids;
        private System.Windows.Forms.Button btnRemoveValue;
        private System.Windows.Forms.Timer timerLoadData;
    }
}