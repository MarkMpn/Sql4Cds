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
    public class QueryExecutionException : ApplicationException, ISql4CdsErrorException
    {
        private readonly Sql4CdsError _error;

        public QueryExecutionException(string message) : base(message)
        {
        }

        public QueryExecutionException(string message, Exception innerException) : base(message, innerException)
        {
            if (innerException is ISql4CdsErrorException ex)
                _error = ex.Error;
        }

        public QueryExecutionException(Sql4CdsError error) : this(error.Message)
        {
            _error = error;
        }

        public QueryExecutionException(Sql4CdsError error, Exception innerException) : this(error.Message, innerException)
        {
            _error = error;
        }

        /// <summary>
        /// The node in the query that generated the exception
        /// </summary>
        public IExecutionPlanNode Node { get; set; }

        public Sql4CdsError Error => _error;
    }
}
