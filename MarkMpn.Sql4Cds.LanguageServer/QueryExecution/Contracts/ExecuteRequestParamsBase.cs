namespace MarkMpn.Sql4Cds.LanguageServer.QueryExecution.Contracts
{
    public abstract class ExecuteRequestParamsBase
    {
        /// <summary>
        /// URI for the editor that is asking for the query execute
        /// </summary>
        public string OwnerUri { get; set; }

        public string ExecutionId { get; set; }

        /// <summary>
        /// Execution plan options
        /// </summary>
        public ExecutionPlanOptions ExecutionPlanOptions { get; set; }

        /// <summary>
        /// Flag to get full column schema via additional queries.
        /// </summary>
        public bool GetFullColumnSchema { get; set; }

        /// <summary>
        /// Return any result sets as fixed-width text messages instead of result sets that can be shown in a grid
        /// </summary>
        public bool ResultsToText { get; set; }
    }
}
