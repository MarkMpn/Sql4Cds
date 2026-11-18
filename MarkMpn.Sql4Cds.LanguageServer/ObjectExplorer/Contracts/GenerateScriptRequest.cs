using Microsoft.VisualStudio.LanguageServer.Protocol;

namespace MarkMpn.Sql4Cds.LanguageServer.ObjectExplorer.Contracts
{
    public class GenerateScriptRequest
    {
        public const string MessageName = "objectexplorer/generateScript";

        public static readonly LspRequest<GenerateScriptParams, string> Type =
            new LspRequest<GenerateScriptParams, string>(MessageName);
    }

    public class GenerateScriptParams
    {
        public string SessionId { get; set; }

        public string NodePath { get; set; }

        public string Action { get; set; }
    }
}
