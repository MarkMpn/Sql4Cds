using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;

namespace MarkMpn.Sql4Cds.DebugVisualizer.DebugeeSide
{
    internal class UnknownRootNode : IRootExecutionPlanNode
    {
        public string Sql { get; set; }
        public int Index { get; set; }
        public int Length { get; set; }
        public int LineNumber { get; set; }

        public IExecutionPlanNode Parent => null;

        public int ExecutionCount => 0;

        public TimeSpan Duration => TimeSpan.Zero;

        public IExecutionPlanNode Source { get; set; }

        public IEnumerable<IExecutionPlanNode> GetSources()
        {
            yield return Source;
        }

        public override string ToString()
        {
            return "< Unknown Root >";
        }
    }
}
