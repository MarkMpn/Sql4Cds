using MarkMpn.Sql4Cds.LanguageServer.Configuration;
using Microsoft.VisualStudio.LanguageServer.Protocol;

namespace MarkMpn.Sql4Cds.LanguageServer.Capabilities.Contracts
{
    /// <summary>
    /// Defines a message that is sent from the client to request
    /// the version of the server.
    /// </summary>
    public class CapabilitiesRequest
    {
        public const string MessageName = "capabilities/list";

        public static readonly LspRequest<CapabilitiesRequest, CapabilitiesResult> Type = new LspRequest<CapabilitiesRequest, CapabilitiesResult>(MessageName);

        public string HostName { get; set; }

        public string HostVersion { get; set; }
    }
}
