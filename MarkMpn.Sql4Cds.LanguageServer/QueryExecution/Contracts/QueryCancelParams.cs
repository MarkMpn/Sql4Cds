namespace MarkMpn.Sql4Cds.LanguageServer.QueryExecution.Contracts
{
    /// <summary>
    /// Parameters for the query cancellation request
    /// </summary>
    public class QueryCancelParams
    {
        public string OwnerUri { get; set; }
    }
}
