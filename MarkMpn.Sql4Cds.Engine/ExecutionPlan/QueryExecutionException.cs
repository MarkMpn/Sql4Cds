using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    class QueryExecutionException : ApplicationException
    {
        public QueryExecutionException(TSqlFragment fragment, string message) : base(message)
        {
        }
    }
}
