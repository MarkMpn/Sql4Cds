using Microsoft.Xrm.Sdk;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.ServiceModel;
using System.Windows.Forms;
using System.Xml;
using xrmtb.XrmToolBox.Controls.Helper;

namespace xrmtb.XrmToolBox.Controls.Controls
{
    public partial class CDSLookupDialogForm : Form
    {
        #region Private Fields

        private const int Error_QuickFindQueryRecordLimit = -2147164124;
        private Dictionary<string, List<Entity>> entityviews;
        private IOrganizationService service;
        private bool includePersonalViews;

        #endregion Private Fields

        #region Public Constructors

        public CDSLookupDialogForm(IOrganizationService service, string[] logicalNames, bool multiSelect, bool friendlyNames, bool includePersonalViews, string title)
        {
            InitializeComponent();
            SetMulti(multiSelect);
            this.includePersonalViews = includePersonalViews;
            gridResults.ShowFriendlyNames = friendlyNames;
            gridSelection.ShowFriendlyNames = friendlyNames;
            Text = title;
            SetService(service);
            SetLogicalNames(logicalNames);
        }

        #endregion Public Constructors

        #region Internal Properties

        internal IOrganizationService Service
        {
            get => service;
            set
            {
                service = value;
                cmbView.OrganizationService = value;
                gridResults.OrganizationService = value;
                gridSelection.OrganizationService = value;
            }
        }

        #endregion Internal Properties

        #region Internal Methods

        internal Entity[] GetSelectedRecords()
        {
            if (gridResults.MultiSelect)
            {
                if (gridSelection.GetDataSource<IEnumerable<Entity>>() is IEnumerable<Entity> current)
                {
                    return current.ToArray();
                }
                return new Entity[] { };
            }
            else
            {
                return gridResults.SelectedCellRecords?.Take(1).ToArray();
            }
        }

        #endregion Internal Methods

        #region Private Methods

        private void LoadData()
        {
            if (!(cmbEntity.SelectedItem is EntityMetadataProxy entity))
            {
                gridResults.DataSource = null;
                return;
            }
            if (!(cmbView.SelectedEntity is Entity view) ||
                !view.Contains(Savedquery.Fetchxml) ||
                string.IsNullOrWhiteSpace(view.GetAttributeValue<string>(Savedquery.Fetchxml).Trim()))
            {
                gridResults.DataSource = null;
                return;
            }
            txtFilter.Enabled = view.GetAttributeValue<int>(Savedquery.QueryType) == 4;
            if (!txtFilter.Enabled && !string.IsNullOrWhiteSpace(txtFilter.Text))
            {
                txtFilter.Text = string.Empty;
            }
            try
            {
                Cursor = Cursors.WaitCursor;
                gridResults.DataSource = service.ExecuteQuickFind(entity.Metadata.LogicalName, view, txtFilter.Text);
            }
            catch (FaultException<OrganizationServiceFault> ex)
            {
                if (ex.Detail.ErrorCode == Error_QuickFindQueryRecordLimit)
                {
                    if (view.TryGetAttributeValue<bool?>(Savedquery.Isquickfindquery, out bool? isqf) && isqf == true)
                    {
                        Cursor = Cursors.Arrow;
                        MessageBox.Show("The environment contains too many records to use the Quick Find view.\nPlease select another view.", "Loading data", MessageBoxButtons.OK, MessageBoxIcon.Error);
                        if (cmbView.DataSource is IEnumerable<Entity> views)
                        {
                            views = views.Except(views.Where(v => v.TryGetAttributeValue<bool?>(Savedquery.Isquickfindquery, out bool? isqfq) && isqfq == true));
                            cmbView.DataSource = views;
                        }
                    }
                }
            }
            finally
            {
                Cursor = Cursors.Arrow;
            }
            var layout = new XmlDocument();
            layout.LoadXml(view["layoutxml"].ToString());
            gridResults.ColumnOrder = String.Join(",", layout.SelectNodes("//cell/@name").OfType<XmlAttribute>().Select(a => a.Value));
            gridResults.ShowAllColumnsInColumnOrder = true;
            gridResults.ShowColumnsNotInColumnOrder = false;
            gridSelection.ColumnOrder = gridResults.ColumnOrder;
            gridSelection.ShowAllColumnsInColumnOrder = true;
            gridSelection.ShowColumnsNotInColumnOrder = false;
        }

        private void SetLogicalNames(string[] logicalNames)
        {
            cmbEntity.Items.Clear();
            if (logicalNames != null)
            {
                cmbEntity.Items.AddRange(logicalNames
                    .Where(l => !string.IsNullOrWhiteSpace(l))
                    .Select(l => service.GetEntity(l))
                    .Where(m => m != null)
                    .Select(m => new EntityMetadataProxy(m))
                    .ToArray());
            }
            cmbEntity.SelectedIndex = cmbEntity.Items.Count > 0 ? 0 : -1;
            cmbEntity.Enabled = cmbEntity.Items.Count > 1;
        }

        private void SetMulti(bool multiSelect)
        {
            splitGrids.Panel2Collapsed = !multiSelect;
            gridResults.MultiSelect = multiSelect;
        }

        private void SetService(IOrganizationService service)
        {
            Service = service;
            entityviews = new Dictionary<string, List<Entity>>();
        }

        private void SetViews(EntityMetadataProxy entity)
        {
            if (entity == null)
            {
                cmbView.DataSource = null;
                return;
            }
            var logicalname = entity.Metadata.LogicalName;
            if (!entityviews.ContainsKey(logicalname))
            {
                var views = new List<Entity>();
                if (service.RetrieveSystemViews(logicalname, true) is EntityCollection qfviews)
                {
                    views.AddRange(qfviews.Entities);
                }
                if (service.RetrieveSystemViews(logicalname, false) is EntityCollection otherviews)
                {
                    views.AddRange(otherviews.Entities);
                }
                if (includePersonalViews && service.RetrievePersonalViews(logicalname) is EntityCollection userviews && userviews.Entities.Count > 0)
                {
                    var separator = new Entity(UserQuery.EntityName);
                    separator.Attributes[UserQuery.PrimaryName] = "-- Personal Views --";
                    views.Add(separator);
                    views.AddRange(userviews.Entities);
                }
                entityviews.Add(logicalname, views);
            }
            if (entityviews.ContainsKey(logicalname))
            {
                cmbView.DataSource = entityviews[logicalname];
            }
        }

        #endregion Private Methods

        #region Private Event Handlers

        private void btnAddSelection_Click(object sender, EventArgs e)
        {
            if (gridResults.SelectedRowRecords is IEnumerable<Entity> selected)
            {
                var current = GetSelectedRecords().ToList();
                current.AddRange(selected);
                gridSelection.DataSource = current.Distinct();
            }
        }

        private void btnClearSelection_Click(object sender, EventArgs e)
        {
            gridSelection.DataSource = null;
        }

        private void btnFilter_Click(object sender, EventArgs e)
        {
            LoadData();
        }

        private void btnRemoveSelection_Click(object sender, EventArgs e)
        {
            if (gridSelection.SelectedRowRecords is IEnumerable<Entity> selected)
            {
                var current = GetSelectedRecords().ToList();
                selected.ToList().ForEach(s => current.Remove(s));
                gridSelection.DataSource = current;
            }
        }

        private void cmbEntity_SelectedIndexChanged(object sender, EventArgs e)
        {
            SetViews(cmbEntity.SelectedItem as EntityMetadataProxy);
        }

        private void cmbView_SelectedIndexChanged(object sender, EventArgs e)
        {
            timerLoadData.Start();
        }

        private void gridResults_SelectionChanged(object sender, EventArgs e)
        {
            btnAddSelection.Enabled = gridResults.SelectedRowRecords?.Count() > 0;
        }

        private void gridSelection_DataSourceChanged(object sender, EventArgs e)
        {
            btnClearSelection.Enabled = gridSelection.Rows.Count > 0;
        }

        private void gridResults_RecordDoubleClick(object sender, CRMRecordEventArgs e)
        {
            if (gridResults.MultiSelect)
            {
                btnAddSelection_Click(sender, e);
            }
            else
            {
                DialogResult = DialogResult.OK;
            }
        }

        private void gridSelection_SelectionChanged(object sender, EventArgs e)
        {
            btnRemoveSelection.Enabled = gridSelection.SelectedRowRecords?.Count() > 0;
        }

        private void timerLoadData_Tick(object sender, EventArgs e)
        {
            timerLoadData.Stop();
            LoadData();
        }

        private void txtFilter_Enter(object sender, EventArgs e)
        {
            AcceptButton = btnFilter;
        }

        private void txtFilter_Leave(object sender, EventArgs e)
        {
            AcceptButton = btnOk;
        }

        #endregion Private Event Handlers
    }
}