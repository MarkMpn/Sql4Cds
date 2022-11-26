using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using MediatR;
using OmniSharp.Extensions.JsonRpc;

namespace MarkMpn.Sql4Cds.LanguageServer
{
    class ConfigurationHandler : IRequestHandler<DidChangeConfigurationParams>, IJsonRpcHandler
    {
        public Task<Unit> Handle(DidChangeConfigurationParams request, CancellationToken cancellationToken)
        {
            Sql4CdsSettings.Instance = request.Settings.SQL4CDS;
            return Unit.Task;
        }
    }

    [Method("workspace/didChangeConfiguration")]
    [Serial]
    public class DidChangeConfigurationParams : IRequest
    {
        public Settings Settings { get; set; }
    }

    public class Settings
    {
        public Sql4CdsSettings SQL4CDS { get; set; }
    }

    public class Sql4CdsSettings
    {
        public bool UseTdsEndpoint { get; set; }

        public bool BlockDeleteWithoutWhere { get; set; }

        public bool BlockUpdateWithoutWhere { get; set; }

        public bool UseBulkDelete { get; set; }

        public int BatchSize { get; set; }

        public int MaxDegreeOfParallelism { get; set; }

        public bool UseLocalTimeZone { get; set; }

        public bool BypassCustomPlugins { get; set; }

        public bool QuotedIdentifiers { get; set; }

        public static Sql4CdsSettings Instance { get; set; }
    }
}
