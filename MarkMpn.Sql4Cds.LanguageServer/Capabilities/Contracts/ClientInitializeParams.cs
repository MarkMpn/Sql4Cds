using Microsoft.VisualStudio.LanguageServer.Protocol;
using Newtonsoft.Json;

namespace MarkMpn.Sql4Cds.LanguageServer.Capabilities.Contracts
{
    // The pinned LSP library predates the standard initialize.clientInfo property.
    public class ClientInitializeParams : InitializeParams
    {
        [JsonProperty("clientInfo")]
        public ClientInfo ClientInfo { get; set; }
    }

    public class ClientInfo
    {
        [JsonProperty("name")]
        public string Name { get; set; }

        [JsonProperty("version")]
        public string Version { get; set; }
    }
}
