using MarkMpn.Sql4Cds.Engine;
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
        private IAttributeMetadataCache metadata;
        private bool includePersonalViews;

        #endregion Private Fields

        #region Public Constructors

        public CDSLookupDialogForm(IOrganizationService service, IAttributeMetadataCache metadata, string[] logicalNames, bool friendlyNames, bool includePersonalViews)
        {
            InitializeComponent();
            this.includePersonalViews = includePersonalViews;
            this.metadata = metadata;
            gridResults.ShowFriendlyNames = friendlyNames;
            gridResults.Metadata = metadata;
            cmbView.Metadata = metadata;
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
            }
        }

        #endregion Internal Properties

        #region Internal Methods

        internal Entity[] GetSelectedRecords()
        {
            return gridResults.SelectedCellRecords?.Take(1).ToArray();
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

            var layout = new XmlDocument();
            layout.LoadXml(view["layoutxml"].ToString());
            gridResults.ColumnOrder = String.Join(",", layout.SelectNodes("//cell/@name").OfType<XmlAttribute>().Select(a => a.Value));
            gridResults.ShowAllColumnsInColumnOrder = true;
            gridResults.ShowColumnsNotInColumnOrder = false;

            try
            {
                Cursor = Cursors.WaitCursor;
                gridResults.DataSource = service.ExecuteQuickFind(metadata, entity.Metadata.LogicalName, view, txtFilter.Text);
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
        }

        private void SetLogicalNames(string[] logicalNames)
        {
            cmbEntity.Items.Clear();
            if (logicalNames != null)
            {
                cmbEntity.Items.AddRange(logicalNames
                    .Where(l => !string.IsNullOrWhiteSpace(l))
                    .Select(l => { metadata.TryGetValue(l, out var entityMetadata); return entityMetadata; })
                    .Where(m => m != null)
                    .Select(m => new EntityMetadataProxy(m))
                    .ToArray());
            }
            cmbEntity.SelectedIndex = cmbEntity.Items.Count > 0 ? 0 : -1;
            cmbEntity.Enabled = cmbEntity.Items.Count > 1;
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
                    views.AddRange(qfviews.Entities.OrderBy(e => e.GetAttributeValue<string>(Savedquery.PrimaryName)));
                }
                if (service.RetrieveSystemViews(logicalname, false) is EntityCollection otherviews)
                {
                    views.AddRange(otherviews.Entities.OrderBy(e => e.GetAttributeValue<string>(Savedquery.PrimaryName)));
                }
                if (includePersonalViews && service.RetrievePersonalViews(logicalname) is EntityCollection userviews && userviews.Entities.Count > 0)
                {
                    var separator = new Entity(UserQuery.EntityName);
                    separator.Attributes[UserQuery.PrimaryName] = "-- Personal Views --";
                    views.Add(separator);
                    views.AddRange(userviews.Entities.OrderBy(e => e.GetAttributeValue<string>(UserQuery.PrimaryName)));
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
            }
        }

        private void btnFilter_Click(object sender, EventArgs e)
        {
            LoadData();
        }

        private void cmbEntity_SelectedIndexChanged(object sender, EventArgs e)
        {
            SetViews(cmbEntity.SelectedItem as EntityMetadataProxy);
        }

        private void cmbView_SelectedIndexChanged(object sender, EventArgs e)
        {
            timerLoadData.Start();
        }

        private void gridResults_RecordDoubleClick(object sender, CRMRecordEventArgs e)
        {
            DialogResult = DialogResult.OK;
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