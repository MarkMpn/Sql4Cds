using Microsoft.VisualStudio.LanguageServer.Protocol;

namespace MarkMpn.Sql4Cds.LanguageServer.Admin.Contracts
{
    /// <summary>
    /// List databases request mapping entry 
    /// </summary>
    public class ChangeDatabaseRequest
    {
        public const string MessageName = "connection/changedatabase";

        public static readonly LspRequest<ChangeDatabaseParams, bool> Type = new LspRequest<ChangeDatabaseParams, bool>(MessageName);
    }
}
