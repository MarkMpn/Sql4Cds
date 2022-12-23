using Microsoft.VisualStudio.LanguageServer.Protocol;

namespace MarkMpn.Sql4Cds.LanguageServer.Admin.Contracts
{
    /// <summary>
    /// Get database info request mapping
    /// </summary>
    public class GetDatabaseInfoRequest
    {
        public const string MessageName = "admin/getdatabaseinfo";

        public static readonly LspRequest<GetDatabaseInfoParams, GetDatabaseInfoResponse> Type = new LspRequest<GetDatabaseInfoParams, GetDatabaseInfoResponse>(MessageName);
    }
}
