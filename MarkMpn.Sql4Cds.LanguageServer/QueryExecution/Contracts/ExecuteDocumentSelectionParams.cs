namespace MarkMpn.Sql4Cds.LanguageServer.QueryExecution.Contracts
{
    public class ExecuteDocumentSelectionParams : ExecuteRequestParamsBase
    {
        /// <summary>
        /// The selection from the document
        /// </summary>
        public SelectionData QuerySelection { get; set; }
    }
}
