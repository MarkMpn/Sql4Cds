using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// Describes a node which moves execution to another node
    /// </summary>
    interface IGoToNode : IRootExecutionPlanNodeInternal
    {
        /// <summary>
        /// Checks which nodes should be executed next
        /// </summary>
        /// <param name="context">The context in which the node is being executed</param>
        /// <returns>The label which should be executed next</returns>
        string Execute(NodeExecutionContext context);
    }
}
