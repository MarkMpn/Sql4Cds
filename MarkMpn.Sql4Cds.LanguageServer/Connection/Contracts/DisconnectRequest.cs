using Microsoft.VisualStudio.LanguageServer.Protocol;

namespace MarkMpn.Sql4Cds.LanguageServer.Connection.Contracts
{
    /// <summary>
    /// Disconnect request mapping entry 
    /// </summary>
    public class DisconnectRequest
    {
        public const string MessageName = "connection/disconnect";

        public static readonly LspRequest<DisconnectParams, bool> Type = new LspRequest<DisconnectParams, bool>(MessageName);
    }
}
