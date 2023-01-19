using Microsoft.VisualStudio.LanguageServer.Protocol;

namespace MarkMpn.Sql4Cds.LanguageServer.QueryExecution.Contracts
{
    public class QueryCancelRequest
    {
        public const string MessageName = "query/cancel";

        public static readonly LspRequest<QueryCancelParams, QueryCancelResult> Type = new LspRequest<QueryCancelParams, QueryCancelResult>(MessageName);
    }
}
