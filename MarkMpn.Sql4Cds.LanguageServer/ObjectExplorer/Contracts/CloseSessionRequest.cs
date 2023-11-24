using MarkMpn.Sql4Cds.LanguageServer.Connection.Contracts;
using Microsoft.VisualStudio.LanguageServer.Protocol;

namespace MarkMpn.Sql4Cds.LanguageServer.ObjectExplorer.Contracts
{
    /// <summary>
    /// Closes an Object Explorer tree session for a specific connection.
    /// </summary>
    public class CloseSessionRequest
    {
        public const string MessageName = "objectexplorer/closesession";

        public static readonly LspRequest<CloseSessionParams, CloseSessionResponse> Type = new LspRequest<CloseSessionParams, CloseSessionResponse>(MessageName);
    }
}
