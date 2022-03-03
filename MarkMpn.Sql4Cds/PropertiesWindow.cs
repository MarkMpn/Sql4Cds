using System;
using System.Collections.Generic;
using MarkMpn.Sql4Cds.Engine;

namespace MarkMpn.Sql4Cds
{
    public partial class PropertiesWindow : WeifenLuo.WinFormsUI.Docking.DockContent
    {
        private object _obj;

        public PropertiesWindow()
        {
            InitializeComponent();
        }

        internal IDictionary<string, DataSource> Connections { get; set; }

        public void SelectObject(object value, bool estimated)
        {
            _obj = value;

            if (value != null)
                value = new ExecutionPlanNodeTypeDescriptor(value, estimated, dataSourceName =>
                {
                    if (!Connections.TryGetValue(dataSourceName, out var dataSource))
                        return null;

                    return new ConnectionPropertiesWrapper(dataSource.ConnectionDetail);
                });

            propertyGrid.SelectedObject = value;
            SelectedObjectChanged?.Invoke(this, EventArgs.Empty);
        }

        public object SelectedObject
        {
            get { return _obj; }
        }

        public event EventHandler SelectedObjectChanged;
    }
}
