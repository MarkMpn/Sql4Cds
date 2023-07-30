using System;
using System.Linq;
using System.ServiceModel.Channels;
using MarkMpn.Sql4Cds.LanguageServer.Connection;
using Microsoft.SqlTools.ServiceLayer.Scripting.Contracts;
using StreamJsonRpc;
using static Microsoft.ApplicationInsights.MetricDimensionNames.TelemetryContext;

namespace MarkMpn.Sql4Cds.LanguageServer.Scripting
{
    class ScriptingHandler : IJsonRpcMethodHandler
    {
        private readonly JsonRpc _lsp;
        private readonly ConnectionManager _connectionManager;

        public ScriptingHandler(JsonRpc lsp, ConnectionManager connectionManager)
        {
            _lsp = lsp;
            _connectionManager = connectionManager;
        }

        public void Initialize(JsonRpc lsp)
        {
            lsp.AddHandler(ScriptingRequest.Type, HandleScriptingRequest);
        }

        private ScriptingResult HandleScriptingRequest(ScriptingParams operation)
        {
            if (operation.Operation != ScriptingOperationType.Select)
                throw new NotImplementedException();

            return new ScriptingResult { OperationId = Guid.NewGuid().ToString(), Script = String.Join("\r\nGO\r\n\r\n", operation.ScriptingObjects.Select(obj => $"SELECT TOP 1000 * FROM {obj.Schema}.{obj.Name}")) };
        }
    }
}
