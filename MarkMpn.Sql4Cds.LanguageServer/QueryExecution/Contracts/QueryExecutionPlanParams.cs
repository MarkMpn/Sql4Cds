namespace MarkMpn.Sql4Cds.LanguageServer.QueryExecution.Contracts
{
    /// <summary>
    /// Parameters for query execution plan request
    /// </summary>
    public class QueryExecutionPlanParams
    {
        /// <summary>
        /// URI for the file that owns the query to look up the results for
        /// </summary>
        public string OwnerUri { get; set; }

        /// <summary>
        /// Index of the batch to get the results from
        /// </summary>
        public int BatchIndex { get; set; }

        /// <summary>
        /// Index of the result set to get the results from
        /// </summary>
        public int ResultSetIndex { get; set; }

    }
}
