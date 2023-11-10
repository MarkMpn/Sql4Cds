using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// A <see cref="IRootExecutionPlanNode"/> that modifies data
    /// </summary>
    internal interface IDmlQueryExecutionPlanNode : IRootExecutionPlanNodeInternal
    {
        /// <summary>
        /// Executes the execution plan
        /// </summary>
        /// <param name="context">The context in which the node is being executed</param>
        /// <param name="recordsAffected">The number of records that were affected by the query</param>
        void Execute(NodeExecutionContext context, out int recordsAffected);
    }
}
