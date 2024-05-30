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
                WritePlan(outgoingData, root);
            else
                WritePlan(outgoingData, new UnknownRootNode { Source = (IExecutionPlanNode)target });
        }

        private void WritePlan(Stream outgoingData, IRootExecutionPlanNode source)
        {
            var json = ExecutionPlanSerializer.Serialize(source);
            SerializeAsJson(outgoingData, new SerializedPlan { Plan = json });
        }
    }
}
