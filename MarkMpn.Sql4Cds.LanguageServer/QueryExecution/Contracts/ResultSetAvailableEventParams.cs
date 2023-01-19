using Microsoft.VisualStudio.LanguageServer.Protocol;

namespace MarkMpn.Sql4Cds.LanguageServer.QueryExecution.Contracts
{
    /// <summary>
    /// Parameters to return when a result set is available.
    /// </summary>
    public class ResultSetAvailableEventParams : ResultSetEventParams
    {
    }

    public class ResultSetAvailableEvent
    {
        public static string MethodName { get; } = "query/resultSetAvailable";

        public static readonly LspNotification<ResultSetAvailableEventParams> Type = new LspNotification<ResultSetAvailableEventParams>(MethodName);
    }
}
