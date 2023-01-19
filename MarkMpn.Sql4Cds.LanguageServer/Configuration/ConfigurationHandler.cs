using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.LanguageServer.Protocol;
using StreamJsonRpc;

namespace MarkMpn.Sql4Cds.LanguageServer.Configuration
{
    class ConfigurationHandler : IJsonRpcMethodHandler
    {
        public void Initialize(JsonRpc lsp)
        {
            lsp.AddHandler(Methods.WorkspaceDidChangeConfiguration, HandleDidChangeConfiguration);
        }

        public void HandleDidChangeConfiguration(DidChangeConfigurationParams request)
        {
            Sql4CdsSettings.Instance = ((Newtonsoft.Json.Linq.JObject)request.Settings).GetValue("SQL4CDS").ToObject<Sql4CdsSettings>();
        }
    }
}
