using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// An execution plan node that has a single source node
    /// </summary>
    interface ISingleSourceExecutionPlanNode : IExecutionPlanNodeInternal
    {
        /// <summary>
        /// The node that produces the data for this node
        /// </summary>
        IDataExecutionPlanNodeInternal Source { get; set; }
    }
}
