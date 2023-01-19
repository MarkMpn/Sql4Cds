using Microsoft.VisualStudio.LanguageServer.Protocol;

namespace MarkMpn.Sql4Cds.LanguageServer.Connection.Contracts
{
    /// <summary>
    /// Cancel connect request mapping entry 
    /// </summary>
    public class CancelConnectRequest
    {
        public const string MessageName = "connection/cancelconnect";

        public static readonly LspRequest<CancelConnectParams, bool> Type = new LspRequest<CancelConnectParams, bool>(MessageName);
    }
}
