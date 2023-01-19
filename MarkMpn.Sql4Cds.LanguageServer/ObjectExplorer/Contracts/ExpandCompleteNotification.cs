using Microsoft.VisualStudio.LanguageServer.Protocol;

namespace MarkMpn.Sql4Cds.LanguageServer.ObjectExplorer.Contracts
{
    /// <summary>
    /// Expand notification mapping entry 
    /// </summary>
    public class ExpandCompleteNotification
    {
        public const string MessageName = "objectexplorer/expandCompleted";

        public static readonly LspNotification<ExpandResponse> Type = new LspNotification<ExpandResponse>(MessageName);
    }
}
