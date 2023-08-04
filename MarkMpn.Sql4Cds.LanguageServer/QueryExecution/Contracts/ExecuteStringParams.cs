namespace MarkMpn.Sql4Cds.LanguageServer.QueryExecution.Contracts
{
    public class ExecuteStringParams : ExecuteRequestParamsBase
    {
        /// <summary>
        /// The query to execute
        /// </summary>
        public string Query { get; set; }
    }
}
