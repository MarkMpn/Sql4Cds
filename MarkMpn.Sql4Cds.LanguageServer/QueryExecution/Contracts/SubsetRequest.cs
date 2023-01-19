using Microsoft.VisualStudio.LanguageServer.Protocol;

namespace MarkMpn.Sql4Cds.LanguageServer.QueryExecution.Contracts
{
    public class SubsetRequest
    {
        public const string MessageName = "query/subset";

        public static readonly LspRequest<SubsetParams, SubsetResult> Type = new LspRequest<SubsetParams, SubsetResult>(MessageName);
    }
}
