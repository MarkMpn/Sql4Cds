using Microsoft.VisualStudio.LanguageServer.Protocol;

namespace MarkMpn.Sql4Cds.LanguageServer.QueryExecution.Contracts
{
    /// <summary>
    /// Parameters to return when a result set is completed.
    /// </summary>
    public class ResultSetCompleteEventParams : ResultSetEventParams
    {
    }

    public class ResultSetCompleteEvent
    {
        public static string MethodName { get; } = "query/resultSetComplete";

        public static readonly LspNotification<ResultSetCompleteEventParams> Type = new LspNotification<ResultSetCompleteEventParams>(MethodName);
    }
}
