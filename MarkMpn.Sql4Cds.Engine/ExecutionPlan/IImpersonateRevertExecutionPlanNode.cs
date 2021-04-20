using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// Indicates that the node can change the user that future queries are executed as
    /// </summary>
    public interface IImpersonateRevertExecutionPlanNode : IDmlQueryExecutionPlanNode
    {
    }
}
