using System;
using System.IO;
using System.Text;
using MarkMpn.Sql4Cds.Engine;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using Microsoft.VisualStudio.DebuggerVisualizers;
using Newtonsoft.Json;

namespace MarkMpn.Sql4Cds.DebuggerVisualizer.Debugee
{
    public class ExecutionPlanObjectSource : VisualizerObjectSource
    {
        public override void GetData(object target, Stream outgoingData)
        {
            using (var writer = new StreamWriter(outgoingData, Encoding.UTF8, 1024, true))
            {
                writer.Write(ExecutionPlanSerializer.Serialize((IRootExecutionPlanNode)target));
            }
        }
    }
}
