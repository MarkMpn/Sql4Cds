using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Windows.Forms;
using xrmtb.XrmToolBox.Controls.Helper;

namespace xrmtb.XrmToolBox.Controls.Controls
{
    public delegate void ProgressUpdate(string message);
    public delegate void RetrieveComplete(int itemCount, Entity FirstItem);

    public partial class CDSDataComboBox : ComboBox
    {
        #region Private properties
        private string displayFormat = string.Empty;
        private IEnumerable<Entity> entities;
        private IOrganizationService organizationService;
        #endregion

        #region Public Constructors

        public CDSDataComboBox()
        {
            InitializeComponent();
        }

        #endregion Public Constructors

        #region Public Properties

        [Category("Data")]
        [Description("Indicates the source of data (EntityCollection) for the CDSDataComboBox control.")]
        public new object DataSource
        {
            get
            {
                if (entities != null)
                {
                    return entities;
                }
                return base.DataSource;
            }
            set
            {
                IEnumerable<Entity> newEntities = null;
                if (value is EntityCollection entityCollection)
                {
                    newEntities = entityCollection.Entities;
                }
                else if (value is IEnumerable<Entity> entities)
                {
                    newEntities = entities;
                }
                if (newEntities != null)
                {
                    entities = newEntities;
                    Refresh();
                }
            }
        }

        [Category("Data")]
        [DisplayName("Display Format")]
        [Description("Single attribute from datasource to display for items, or use {{attributename}} syntax freely.")]
        public string DisplayFormat
        {
            get { return displayFormat; }
            set
            {
                if (value != displayFormat)
                {
                    displayFormat = value;
                    Refresh();
                }
            }
        }

        [Browsable(false)]
        public IOrganizationService OrganizationService
        {
            get { return organizationService; }
            set
            {
                organizationService = value;
                Refresh();
            }
        }

        [Browsable(false)]
        public Entity SelectedEntity => (SelectedItem is EntityWrapper item) ? item.Entity : null;

        #endregion Public Properties

        #region Public Methods

        public override void Refresh()
        {
            SuspendLayout();
            var selected = SelectedEntity;
            var ds = entities?.Select(e => new EntityWrapper(e, displayFormat, organizationService)).ToArray();
            base.DataSource = ds;
            base.Refresh();
            if (selected != null && ds.FirstOrDefault(e => e.Entity.Id.Equals(selected.Id)) is EntityWrapper newselected)
            {
                SelectedItem = newselected;
            }
            ResumeLayout();
        }

        public void RetrieveMultiple(QueryBase query, ProgressUpdate progressCallback, RetrieveComplete completeCallback)
        {
            if (this.OrganizationService == null)
            {
                throw new InvalidOperationException("The Service reference must be set before calling RetrieveMultiple.");
            }

            try
            {
                var worker = new BackgroundWorker();
                worker.DoWork += (w, e) =>
                {
                    var queryExp = e.Argument as QueryBase;

                    BeginInvoke(progressCallback, "Begin Retrieve Multiple");

                    var fetchReq = new RetrieveMultipleRequest
                    {
                        Query = queryExp
                    };

                    var records = OrganizationService.RetrieveMultiple(query);

                    BeginInvoke(progressCallback, "End Retrieve Multiple");

                    e.Result = records;
                };

                worker.RunWorkerCompleted += (s, e) =>
                {
                    var records = e.Result as EntityCollection;

                    BeginInvoke(progressCallback, $"Retrieve Multiple - records returned: {records.Entities.Count}");

                    DataSource = records;

                    // make the final callback
                    BeginInvoke(completeCallback, entities?.Count(), SelectedEntity);
                };

                // kick off the worker thread!
                worker.RunWorkerAsync(query);
            }
            catch (System.ServiceModel.FaultException ex)
            {
            }
        }

        public void RetrieveMultiple(string fetchXml, ProgressUpdate progressCallback, RetrieveComplete completeCallback)
        {
            RetrieveMultiple(new FetchExpression(fetchXml), progressCallback, completeCallback);
        }

        #endregion Public Methods
    }
}
