using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceModel;
using System.Text;
using System.Text.RegularExpressions;
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
                Errors = new[] { new Sql4CdsError(16, FaultCodeToSqlError(faultEx.Detail), message) };
            }
            else if (innerException is AggregateException aggregateException)
            {
                var errors = new List<Sql4CdsError>();

                foreach (var innerEx in aggregateException.InnerExceptions)
                {
                    if (innerEx is ISql4CdsErrorException innerSqlEx)
                        errors.AddRange(innerSqlEx.Errors);
                    else if (innerEx is FaultException<OrganizationServiceFault> innerFaultEx)
                        errors.Add(new Sql4CdsError(16, FaultCodeToSqlError(innerFaultEx.Detail), innerFaultEx.Message));
                    else
                        errors.Add(Sql4CdsError.InternalError(innerEx.Message));
                }

                Errors = errors;
            }
            else
            {
                Errors = new[] { Sql4CdsError.InternalError(message) };
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

        private static int FaultCodeToSqlError(OrganizationServiceFault fault)
        {
            switch (fault.ErrorCode)
            {
                case -2146892759: return 2628;
                case -2147204303:
                case -2147204288: return 8152;
                case -2147217098: return 8115;
                case -2147204304:
                case -2146892679:
                case -2147204324:
                case -2146892644:
                case -2146892643:
                case -2147158398:
                case -2147204305:
                case -2147086331:
                case -2147187710:
                case -2147204326:
                case -2147182263:
                case -2147220309:
                case -2147220948:
                case -2147213282:
                case -2147086332:
                case -2147187954: return 547;
                case 409: // Elastic tables use HTTP status codes instead of the standard web service error codes
                case -2147220950:
                case -2147188216:
                case -2147220937: return 2627;
            }

            if (fault.ErrorDetails.TryGetValue("ApiExceptionHttpStatusCode", out var httpStatusCode) &&
                httpStatusCode is int httpError)
            {
                switch (httpError)
                {
                    case 409: return 2627;
                }
            }

            var match = Regex.Match(fault.Message, "Sql Number: (\\d+)");
            if (match.Success)
                return Int32.Parse(match.Groups[1].Value);

            return 10337;
        }
    }
}
