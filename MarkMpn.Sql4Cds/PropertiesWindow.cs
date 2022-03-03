using System;
using System.Collections.Generic;
using System.Drawing;
using System.Data;
using System.Linq;
using System.ComponentModel;
using System.Globalization;
using System.Reflection;
using System.Runtime.Serialization;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using McTools.Xrm.Connection;
using MarkMpn.Sql4Cds.Controls;

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

        public object SelectedObject
        {
            get { return _obj; }
            set
            {
                _obj = value;

                if (value != null)
                    value = new ExecutionPlanNodeTypeDescriptor(value, dataSourceName =>
                    {
                        if (!Connections.TryGetValue(dataSourceName, out var dataSource))
                            return null;

                        return new ConnectionPropertiesWrapper(dataSource.ConnectionDetail);
                    });

                propertyGrid.SelectedObject = value;
                SelectedObjectChanged?.Invoke(this, EventArgs.Empty);
            }
        }

        public event EventHandler SelectedObjectChanged;
    }
}
