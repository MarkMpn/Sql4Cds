using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// Describes a node which moves execution to another node
    /// </summary>
    interface IControlOfFlowNode : IRootExecutionPlanNodeInternal
    {
        /// <summary>
        /// Checks which nodes should be executed next
        /// </summary>
        /// <param name="dataSources">The data sources that can be accessed by the query</param>
        /// <param name="options">The options which describe how the query should be executed</param>
        /// <param name="parameterTypes">The types of any parameters available to the query</param>
        /// <param name="parameterValues">The values of any parameters available to the query</param>
        /// <param name="rerun">Indicates if this node should be executed again before moving on to the next statement in the batch</param>
        /// <returns>The nodes which should be executed next. If <c>null</c>, the entire node is finished and control should move on to the next statement in the batch</returns>
        IRootExecutionPlanNodeInternal[] Execute(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, DataTypeReference> parameterTypes, IDictionary<string, object> parameterValues, out bool rerun);
    }
}
