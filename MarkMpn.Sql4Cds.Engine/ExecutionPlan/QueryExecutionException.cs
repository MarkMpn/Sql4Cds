using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceModel;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// An exception that is generated during the execution of a query
    /// </summary>
    public class QueryExecutionException : ApplicationException, ISql4CdsErrorException
    {
        public QueryExecutionException(string message) : base(message)
        {
            Errors = new[] { new Sql4CdsError(16, 10337, message) };
        }

        public QueryExecutionException(string message, Exception innerException) : base(message, innerException)
        {
            if (innerException is ISql4CdsErrorException ex)
            {
                Errors = ex.Errors;
            }
            else if (innerException is FaultException<OrganizationServiceFault> faultEx)
            {
                Errors = new[] { new Sql4CdsError(16, FaultCodeToSqlError(faultEx.Detail.ErrorCode), message) };
            }
            else if (innerException is AggregateException aggregateException)
            {
                var errors = new List<Sql4CdsError>();

                foreach (var innerEx in aggregateException.InnerExceptions)
                {
                    if (innerEx is ISql4CdsErrorException innerSqlEx)
                        errors.AddRange(innerSqlEx.Errors);
                    else if (innerEx is FaultException<OrganizationServiceFault> innerFaultEx)
                        errors.Add(new Sql4CdsError(16, FaultCodeToSqlError(innerFaultEx.Detail.ErrorCode), innerFaultEx.Message));
            else
                        errors.Add(new Sql4CdsError(16, 10337, innerEx.Message));
                }

                Errors = errors;
            }
            else
            {
                Errors = new[] { new Sql4CdsError(16, 10337, message) };
            }
        }

        public QueryExecutionException(Sql4CdsError error) : this(error.Message)
        {
            Errors = new[] { error };
        }

        public QueryExecutionException(Sql4CdsError error, Exception innerException) : this(error.Message, innerException)
        {
            Errors = new[] { error };
        }

        /// <summary>
        /// The node in the query that generated the exception
        /// </summary>
        public IExecutionPlanNode Node { get; set; }

        public IReadOnlyList<Sql4CdsError> Errors { get; }
    }
}
