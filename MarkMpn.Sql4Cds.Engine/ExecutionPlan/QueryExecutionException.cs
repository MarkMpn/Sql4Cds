using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// An exception that is generated during the execution of a query
    /// </summary>
    public class QueryExecutionException : ApplicationException
    {
        public QueryExecutionException(string message) : base(message)
        {
        }

        public QueryExecutionException(string message, Exception innerException) : base(message, innerException)
        {
        }

        /// <summary>
        /// The node in the query that generated the exception
        /// </summary>
        public IExecutionPlanNode Node { get; set; }
    }
}
