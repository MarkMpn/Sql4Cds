using System;
using System.Collections.Generic;
using System.Drawing;
using System.Data;
using System.Linq;
using System.Windows.Forms;
using MarkMpn.Sql4Cds.Engine;
using McTools.Xrm.Connection;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Metadata;
using XrmToolBox.Extensibility;
using Microsoft.Xrm.Sdk.Metadata.Query;
using System.ComponentModel;

namespace MarkMpn.Sql4Cds
{
    public partial class PropertiesWindow : WeifenLuo.WinFormsUI.Docking.DockContent
    {
        public PropertiesWindow()
        {
            InitializeComponent();
        }

        public void SelectObject(object obj)
        {
            if (obj != null)
                TypeDescriptor.AddAttributes(obj, new ReadOnlyAttribute(true));

            propertyGrid.SelectedObject = obj;
        }
    }
}
