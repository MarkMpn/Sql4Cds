using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    interface ISingleSourceExecutionPlanNode : IExecutionPlanNode
    {
        IDataExecutionPlanNode Source { get; set; }
    }
}
