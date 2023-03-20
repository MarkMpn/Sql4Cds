using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// A <see cref="IRootExecutionPlanNode"/> that produces a <see cref="IDataReader"/>
    /// </summary>
    internal interface IDataReaderExecutionPlanNode : IRootExecutionPlanNodeInternal
    {
        /// <summary>
        /// Executes the execution plan
        /// </summary>
        /// <param name="context">The context in which the node is being executed</param>
        /// <param name="behavior">Additional options to control how the command should be executed</param>
        /// <returns>A <see cref="IDataReader"/> that contains the results of the query</returns>
        DbDataReader Execute(NodeExecutionContext context, CommandBehavior behavior);
    }
}
