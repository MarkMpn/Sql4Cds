using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.LanguageServer.Connection;
using Microsoft.VisualStudio.LanguageServer.Protocol;
using StreamJsonRpc;

namespace MarkMpn.Sql4Cds.LanguageServer.Configuration
{
    class ConfigurationHandler : IJsonRpcMethodHandler
    {
        private readonly ConnectionManager _connections;

        public ConfigurationHandler(ConnectionManager connections)
        {
            _connections = connections;
        }

        public void Initialize(JsonRpc lsp)
        {
            lsp.AddHandler(Methods.WorkspaceDidChangeConfiguration, HandleDidChangeConfiguration);
        }

        public void HandleDidChangeConfiguration(DidChangeConfigurationParams request)
        {
            Sql4CdsSettings.Instance = ((Newtonsoft.Json.Linq.JObject)request.Settings).GetValue("SQL4CDS").ToObject<Sql4CdsSettings>();
            _connections.SetIncludeDetailedErrorTelemetry(Sql4CdsSettings.Instance.IncludeDetailedErrorTelemetry);
        }
    }
}
