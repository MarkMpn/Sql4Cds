namespace MarkMpn.Sql4Cds.LanguageServer.QueryExecution.Contracts
{
    /// <summary> 
    /// Incoming execution plan options from the extension
    /// </summary>
    public struct ExecutionPlanOptions
    {

        /// <summary>
        /// Setting to return the actual execution plan as XML
        /// </summary>
        public bool IncludeActualExecutionPlanXml { get; set; }

        /// <summary>
        /// Setting to return the estimated execution plan as XML
        /// </summary>
        public bool IncludeEstimatedExecutionPlanXml { get; set; }
    }
}
