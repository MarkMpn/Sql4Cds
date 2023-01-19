using Microsoft.VisualStudio.LanguageServer.Protocol;

namespace MarkMpn.Sql4Cds.LanguageServer.Admin.Contracts
{
    public class MetadataListRequest
    {
        public const string MessageName = "metadata/list";

        public static readonly LspRequest<MetadataQueryParams, MetadataQueryResult> Type = new LspRequest<MetadataQueryParams, MetadataQueryResult>(MessageName);
    }
}
