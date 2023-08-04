using Microsoft.VisualStudio.LanguageServer.Protocol;

namespace MarkMpn.Sql4Cds.LanguageServer.QueryExecution.Contracts
{
    public class ExecuteStringRequest
    {
        public const string MessageName = "query/executeString";

        public static readonly LspRequest<ExecuteStringParams, ExecuteRequestResult> Type = new LspRequest<ExecuteStringParams, ExecuteRequestResult>(MessageName);
    }
}
