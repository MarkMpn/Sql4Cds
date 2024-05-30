using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using Microsoft.VisualStudio.Extensibility.DebuggerVisualizers;
using Microsoft.VisualStudio.PlatformUI;
//using Microsoft.Web.WebView2.Core;
using System.Buffers;
using System.Diagnostics;
using System.Text;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Forms;

namespace MarkMpn.Sql4Cds.DebugVisualizer.DebuggerSide
{
    public partial class QueryPlanUserControl : System.Windows.Controls.UserControl
    {
        private readonly IRootExecutionPlanNode _plan;

        public QueryPlanUserControl(IRootExecutionPlanNode plan)
        {
            InitializeComponent();
            DataContext = this;

            _plan = plan;
            this.Loaded += QueryPlanUserControl_Loaded;
        }

        private void QueryPlanUserControl_Loaded(object sender, RoutedEventArgs e)
        {
            // Create the interop host control.
            System.Windows.Forms.Integration.WindowsFormsHost host =
                new System.Windows.Forms.Integration.WindowsFormsHost();

            // Create the control.
            var control = new MarkMpn.Sql4Cds.Controls.ExecutionPlanView { Plan = _plan };
            
            // Assign the MaskedTextBox control as the host control's child.
            host.Child = control;

            // Add the interop host control to the Grid
            // control's collection of child controls.
            this.grid.Children.Add(host);
        }
    }
}
