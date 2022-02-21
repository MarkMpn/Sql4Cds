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
        /// <param name="dataSources">The data sources that can be accessed by the query</param>
        /// <param name="options">The options which describe how the query should be executed</param>
        /// <param name="parameterTypes">The types of any parameters available to the query</param>
        /// <param name="parameterValues">The values of any parameters available to the query</param>
        /// <returns>The label which should be executed next</returns>
        string Execute(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, DataTypeReference> parameterTypes, IDictionary<string, object> parameterValues);
    }
}
