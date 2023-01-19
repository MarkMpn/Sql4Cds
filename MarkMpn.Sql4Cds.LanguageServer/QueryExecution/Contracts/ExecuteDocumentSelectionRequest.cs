using Microsoft.VisualStudio.LanguageServer.Protocol;

namespace MarkMpn.Sql4Cds.LanguageServer.QueryExecution.Contracts
{
    public class ExecuteDocumentSelectionRequest
    {
        public const string MessageName = "query/executeDocumentSelection";

        public static readonly LspRequest<ExecuteDocumentSelectionParams, ExecuteRequestResult> Type = new LspRequest<ExecuteDocumentSelectionParams, ExecuteRequestResult>(MessageName);
    }
}
