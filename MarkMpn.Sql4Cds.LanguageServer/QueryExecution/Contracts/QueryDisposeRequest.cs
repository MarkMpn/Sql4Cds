using Microsoft.VisualStudio.LanguageServer.Protocol;

namespace MarkMpn.Sql4Cds.LanguageServer.QueryExecution.Contracts
{
    public class QueryDisposeRequest
    {
        public const string MessageName = "query/dispose";
        public static readonly LspRequest<QueryDisposeParams, QueryDisposeResult> Type = new LspRequest<QueryDisposeParams, QueryDisposeResult>(MessageName);
    }
}
