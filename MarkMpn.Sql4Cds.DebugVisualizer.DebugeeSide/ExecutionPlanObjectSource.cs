using System.Text;
using MarkMpn.Sql4Cds.Engine;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using Microsoft.VisualStudio.DebuggerVisualizers;

namespace MarkMpn.Sql4Cds.DebugVisualizer.DebugeeSide
{
    public class ExecutionPlanObjectSource : VisualizerObjectSource
    {
        public override void GetData(object target, Stream outgoingData)
        {
            if (target is IRootExecutionPlanNode root)
            {
                WritePlan(outgoingData, root);
            }
            else if (target is IExecutionPlanNode node)
            {
                WritePlan(outgoingData, new UnknownRootNode { Source = node });
            }
            else if (target is Sql4CdsCommand cmd)
            {
                if (cmd.Plan != null)
                    WritePlan(outgoingData, cmd.Plan.First());
                else
                    WritePlan(outgoingData, cmd.GeneratePlan(false).First());
            }
        }

        private void WritePlan(Stream outgoingData, IRootExecutionPlanNode source)
        {
            var json = ExecutionPlanSerializer.Serialize(source);
            SerializeAsJson(outgoingData, new SerializedPlan { Plan = json });
        }
    }
}
