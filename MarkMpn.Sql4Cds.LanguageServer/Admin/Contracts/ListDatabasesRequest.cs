using Microsoft.VisualStudio.LanguageServer.Protocol;

namespace MarkMpn.Sql4Cds.LanguageServer.Admin.Contracts
{
    /// <summary>
    /// List databases request mapping entry 
    /// </summary>
    public class ListDatabasesRequest
    {
        public const string MessageName = "connection/listdatabases";

        public static readonly LspRequest<ListDatabasesParams, ListDatabasesResponse> Type = new LspRequest<ListDatabasesParams, ListDatabasesResponse>(MessageName);
    }
}
