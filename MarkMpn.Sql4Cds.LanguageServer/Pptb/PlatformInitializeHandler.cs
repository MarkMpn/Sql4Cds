using System;
using Newtonsoft.Json.Linq;
using StreamJsonRpc;

namespace MarkMpn.Sql4Cds.LanguageServer.Pptb
{
    /// <summary>
    /// Handles the Power Platform ToolBox worker handshake.
    /// </summary>
    /// <remarks>
    /// PPTB sends <c>platform/initialize</c> before any tool traffic to confirm that the worker speaks the
    /// framed JSON-RPC over stdio protocol. Other hosts (VS Code, ADS) never send it, so registering it
    /// unconditionally is harmless.
    /// </remarks>
    class PlatformInitializeHandler : IJsonRpcMethodHandler
    {
        public const string Protocol = "jsonrpc-stdio-v1";
        public const int ProtocolVersion = 1;

        public void Initialize(JsonRpc lsp)
        {
            lsp.AddLocalRpcMethod("platform/initialize", (JToken token) => HandleInitialize(token));
        }

        public static PlatformInitializeResult HandleInitialize(JToken token)
        {
            string protocol = null;
            int? protocolVersion = null;

            if (token is JObject obj)
            {
                protocol = obj.Value<string>("protocol");
                protocolVersion = obj.Value<int?>("protocolVersion");
            }
            else if (token is JArray arr && arr.Count >= 2)
            {
                protocol = arr[0].Value<string>();
                protocolVersion = arr[1].Value<int?>();
            }

            if (protocol != Protocol || protocolVersion != ProtocolVersion)
                throw new NotSupportedException($"Unsupported worker protocol {protocol} version {protocolVersion}. Expected {Protocol} version {ProtocolVersion}.");

            return new PlatformInitializeResult
            {
                Protocol = Protocol,
                ProtocolVersion = ProtocolVersion
            };
        }
    }

    public class PlatformInitializeResult
    {
        public string Protocol { get; set; }

        public int ProtocolVersion { get; set; }
    }
}
