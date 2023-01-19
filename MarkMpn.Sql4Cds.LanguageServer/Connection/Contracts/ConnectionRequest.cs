using Microsoft.VisualStudio.LanguageServer.Protocol;

namespace MarkMpn.Sql4Cds.LanguageServer.Connection.Contracts
{
    /// <summary>
    /// Connect request mapping entry 
    /// </summary>
    public class ConnectionRequest
    {
        public const string MessageName = "connection/connect";

        public static readonly LspRequest<ConnectParams, bool> Type = new LspRequest<ConnectParams, bool>(MessageName);
    }
}
