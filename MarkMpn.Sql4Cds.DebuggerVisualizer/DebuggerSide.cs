using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using MarkMpn.Sql4Cds.Controls;
using MarkMpn.Sql4Cds.DebuggerVisualizer;
using MarkMpn.Sql4Cds.DebuggerVisualizer.Debugee;
using MarkMpn.Sql4Cds.Engine;
using Microsoft.VisualStudio.DebuggerVisualizers;

[assembly: System.Diagnostics.DebuggerVisualizer(
    typeof(DebuggerSide),
    typeof(ExecutionPlanObjectSource),
    TargetTypeName = "MarkMpn.Sql4Cds.Engine.ExecutionPlan.AssignVariablesNode, MarkMpn.Sql4Cds.Engine",
    Description = "SQL 4 CDS Execution Plan Visualizer")]
[assembly: System.Diagnostics.DebuggerVisualizer(
    typeof(DebuggerSide),
    typeof(ExecutionPlanObjectSource),
    TargetTypeName = "MarkMpn.Sql4Cds.Engine.ExecutionPlan.BulkDeleteJobNode, MarkMpn.Sql4Cds.Engine",
    Description = "SQL 4 CDS Execution Plan Visualizer")]
[assembly: System.Diagnostics.DebuggerVisualizer(
    typeof(DebuggerSide),
    typeof(ExecutionPlanObjectSource),
    TargetTypeName = "MarkMpn.Sql4Cds.Engine.ExecutionPlan.ConditionalNode, MarkMpn.Sql4Cds.Engine",
    Description = "SQL 4 CDS Execution Plan Visualizer")]
[assembly: System.Diagnostics.DebuggerVisualizer(
    typeof(DebuggerSide),
    typeof(ExecutionPlanObjectSource),
    TargetTypeName = "MarkMpn.Sql4Cds.Engine.ExecutionPlan.ContinueBreakNode, MarkMpn.Sql4Cds.Engine",
    Description = "SQL 4 CDS Execution Plan Visualizer")]
[assembly: System.Diagnostics.DebuggerVisualizer(
    typeof(DebuggerSide),
    typeof(ExecutionPlanObjectSource),
    TargetTypeName = "MarkMpn.Sql4Cds.Engine.ExecutionPlan.DeclareVariablesNode, MarkMpn.Sql4Cds.Engine",
    Description = "SQL 4 CDS Execution Plan Visualizer")]
[assembly: System.Diagnostics.DebuggerVisualizer(
    typeof(DebuggerSide),
    typeof(ExecutionPlanObjectSource),
    TargetTypeName = "MarkMpn.Sql4Cds.Engine.ExecutionPlan.DeleteNode, MarkMpn.Sql4Cds.Engine",
    Description = "SQL 4 CDS Execution Plan Visualizer")]
[assembly: System.Diagnostics.DebuggerVisualizer(
    typeof(DebuggerSide),
    typeof(ExecutionPlanObjectSource),
    TargetTypeName = "MarkMpn.Sql4Cds.Engine.ExecutionPlan.ExecuteAsNode, MarkMpn.Sql4Cds.Engine",
    Description = "SQL 4 CDS Execution Plan Visualizer")]
[assembly: System.Diagnostics.DebuggerVisualizer(
    typeof(DebuggerSide),
    typeof(ExecutionPlanObjectSource),
    TargetTypeName = "MarkMpn.Sql4Cds.Engine.ExecutionPlan.ExecuteMessageNode, MarkMpn.Sql4Cds.Engine",
    Description = "SQL 4 CDS Execution Plan Visualizer")]
[assembly: System.Diagnostics.DebuggerVisualizer(
    typeof(DebuggerSide),
    typeof(ExecutionPlanObjectSource),
    TargetTypeName = "MarkMpn.Sql4Cds.Engine.ExecutionPlan.GotoLabelNode, MarkMpn.Sql4Cds.Engine",
    Description = "SQL 4 CDS Execution Plan Visualizer")]
[assembly: System.Diagnostics.DebuggerVisualizer(
    typeof(DebuggerSide),
    typeof(ExecutionPlanObjectSource),
    TargetTypeName = "MarkMpn.Sql4Cds.Engine.ExecutionPlan.GoToNode, MarkMpn.Sql4Cds.Engine",
    Description = "SQL 4 CDS Execution Plan Visualizer")]
[assembly: System.Diagnostics.DebuggerVisualizer(
    typeof(DebuggerSide),
    typeof(ExecutionPlanObjectSource),
    TargetTypeName = "MarkMpn.Sql4Cds.Engine.ExecutionPlan.InsertNode, MarkMpn.Sql4Cds.Engine",
    Description = "SQL 4 CDS Execution Plan Visualizer")]
[assembly: System.Diagnostics.DebuggerVisualizer(
    typeof(DebuggerSide),
    typeof(ExecutionPlanObjectSource),
    TargetTypeName = "MarkMpn.Sql4Cds.Engine.ExecutionPlan.PrintNode, MarkMpn.Sql4Cds.Engine",
    Description = "SQL 4 CDS Execution Plan Visualizer")]
[assembly: System.Diagnostics.DebuggerVisualizer(
    typeof(DebuggerSide),
    typeof(ExecutionPlanObjectSource),
    TargetTypeName = "MarkMpn.Sql4Cds.Engine.ExecutionPlan.RaiseErrorNode, MarkMpn.Sql4Cds.Engine",
    Description = "SQL 4 CDS Execution Plan Visualizer")]
[assembly: System.Diagnostics.DebuggerVisualizer(
    typeof(DebuggerSide),
    typeof(ExecutionPlanObjectSource),
    TargetTypeName = "MarkMpn.Sql4Cds.Engine.ExecutionPlan.RevertNode, MarkMpn.Sql4Cds.Engine",
    Description = "SQL 4 CDS Execution Plan Visualizer")]
[assembly: System.Diagnostics.DebuggerVisualizer(
    typeof(DebuggerSide),
    typeof(ExecutionPlanObjectSource),
    TargetTypeName = "MarkMpn.Sql4Cds.Engine.ExecutionPlan.SelectNode, MarkMpn.Sql4Cds.Engine",
    Description = "SQL 4 CDS Execution Plan Visualizer")]
[assembly: System.Diagnostics.DebuggerVisualizer(
    typeof(DebuggerSide),
    typeof(ExecutionPlanObjectSource),
    TargetTypeName = "MarkMpn.Sql4Cds.Engine.ExecutionPlan.SqlNode, MarkMpn.Sql4Cds.Engine",
    Description = "SQL 4 CDS Execution Plan Visualizer")]
[assembly: System.Diagnostics.DebuggerVisualizer(
    typeof(DebuggerSide),
    typeof(ExecutionPlanObjectSource),
    TargetTypeName = "MarkMpn.Sql4Cds.Engine.ExecutionPlan.ThrowNode, MarkMpn.Sql4Cds.Engine",
    Description = "SQL 4 CDS Execution Plan Visualizer")]
[assembly: System.Diagnostics.DebuggerVisualizer(
    typeof(DebuggerSide),
    typeof(ExecutionPlanObjectSource),
    TargetTypeName = "MarkMpn.Sql4Cds.Engine.ExecutionPlan.BeginTryNode, MarkMpn.Sql4Cds.Engine",
    Description = "SQL 4 CDS Execution Plan Visualizer")]
[assembly: System.Diagnostics.DebuggerVisualizer(
    typeof(DebuggerSide),
    typeof(ExecutionPlanObjectSource),
    TargetTypeName = "MarkMpn.Sql4Cds.Engine.ExecutionPlan.EndTryNode, MarkMpn.Sql4Cds.Engine",
    Description = "SQL 4 CDS Execution Plan Visualizer")]
[assembly: System.Diagnostics.DebuggerVisualizer(
    typeof(DebuggerSide),
    typeof(ExecutionPlanObjectSource),
    TargetTypeName = "MarkMpn.Sql4Cds.Engine.ExecutionPlan.BeginCatchNode, MarkMpn.Sql4Cds.Engine",
    Description = "SQL 4 CDS Execution Plan Visualizer")]
[assembly: System.Diagnostics.DebuggerVisualizer(
    typeof(DebuggerSide),
    typeof(ExecutionPlanObjectSource),
    TargetTypeName = "MarkMpn.Sql4Cds.Engine.ExecutionPlan.EndCatchNode, MarkMpn.Sql4Cds.Engine",
    Description = "SQL 4 CDS Execution Plan Visualizer")]
[assembly: System.Diagnostics.DebuggerVisualizer(
    typeof(DebuggerSide),
    typeof(ExecutionPlanObjectSource),
    TargetTypeName = "MarkMpn.Sql4Cds.Engine.ExecutionPlan.UpdateNode, MarkMpn.Sql4Cds.Engine",
    Description = "SQL 4 CDS Execution Plan Visualizer")]
[assembly: System.Diagnostics.DebuggerVisualizer(
    typeof(DebuggerSide),
    typeof(ExecutionPlanObjectSource),
    TargetTypeName = "MarkMpn.Sql4Cds.Engine.ExecutionPlan.WaitForNode, MarkMpn.Sql4Cds.Engine",
    Description = "SQL 4 CDS Execution Plan Visualizer")]

namespace MarkMpn.Sql4Cds.DebuggerVisualizer
{
    public class DebuggerSide : DialogDebuggerVisualizer
    {
        public DebuggerSide() : base(FormatterPolicy.NewtonsoftJson)
        {
        }

        protected override void Show(IDialogVisualizerService windowService, IVisualizerObjectProvider objectProvider)
        {
            var provider = (IVisualizerObjectProvider3)objectProvider;

            using (var stream = provider.GetData())
            using (var reader = new StreamReader(stream))
            {
                var json = reader.ReadToEnd();
                var plan = ExecutionPlanSerializer.Deserialize(json);

                using (var form = new Form())
                {
                    form.Text = plan.Sql;
                    form.Width = 600;
                    form.Height = 400;

                    var splitter = new SplitContainer
                    {
                        Dock = DockStyle.Fill,
                        FixedPanel = FixedPanel.Panel2,
                    };
                    form.Controls.Add(splitter);

                    var planView = new ExecutionPlanView
                    {
                        Plan = plan,
                        Dock = DockStyle.Fill
                    };
                    splitter.Panel1.Controls.Add(planView);

                    var properties = new PropertyGrid
                    {
                        Dock = DockStyle.Fill
                    };
                    splitter.Panel2.Controls.Add(properties);
                    splitter.SplitterDistance = form.Width - 200;

                    planView.NodeSelected += (_, __) =>
                    {
                        var obj = (object) planView.Selected;

                        if (obj != null)
                            obj = new ExecutionPlanNodeTypeDescriptor(obj, true, dataSourceName => dataSourceName);

                        properties.SelectedObject = obj;
                    };

                    var statusBar = new StatusStrip();
                    form.Controls.Add(statusBar);

                    windowService.ShowDialog(form);
                }
            }
        }
    }
}
