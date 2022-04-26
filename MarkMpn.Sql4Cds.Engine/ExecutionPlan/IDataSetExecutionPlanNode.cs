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
        /// <param name="dataSources">The data sources that can be used in the query</param>
        /// <param name="options">The options that control how the query should be executed</param>
        /// <param name="parameterTypes">The types of the parameters that are available to the query</param>
        /// <param name="parameterValues">The values of the parameters that are available to the query</param>
        /// <param name="behavior">Additional options to control how the command should be executed</param>
        /// <returns>A <see cref="IDataReader"/> that contains the results of the query</returns>
        DbDataReader Execute(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, DataTypeReference> parameterTypes, IDictionary<string, object> parameterValues, CommandBehavior behavior);
    }
}
