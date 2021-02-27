using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    public class QueryExecutionException : ApplicationException
    {
        public QueryExecutionException(string message) : base(message)
        {
        }

        public QueryExecutionException(string message, Exception innerException) : base(message, innerException)
        {
        }

        public IExecutionPlanNode Node { get; set; }
    }
}
