namespace MarkMpn.Sql4Cds.LanguageServer.QueryExecution.Contracts
{
    /// <summary>
    /// Parameters for the query execution plan request
    /// </summary>
    public class QueryExecutionPlanResult
    {
        /// <summary>
        /// The requested execution plan. Optional, can be set to null to indicate an error
        /// </summary>
        public ExecutionPlan ExecutionPlan { get; set; }
    }
}
