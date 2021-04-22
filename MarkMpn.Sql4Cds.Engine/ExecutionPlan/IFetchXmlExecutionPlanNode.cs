using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// Identifies an <see cref="IExecutionPlanNode"/> that uses FetchXML to get its data
    /// </summary>
    public interface IFetchXmlExecutionPlanNode : IExecutionPlanNode
    {
        /// <summary>
        /// Returns the FetchXML the node will use
        /// </summary>
        string FetchXmlString { get; }
    }
}
