using Microsoft.VisualStudio.LanguageServer.Protocol;

namespace MarkMpn.Sql4Cds.LanguageServer.ObjectExplorer.Contracts
{
    /// <summary>
    /// A request to expand a 
    /// </summary>
    public class ExpandRequest
    {
        public const string MessageName = "objectexplorer/expand";

        /// <summary>
        /// Returns children of a given node as a <see cref="NodeInfo"/> array.
        /// </summary>
        public static readonly LspRequest<ExpandParams, bool> Type = new LspRequest<ExpandParams, bool>(MessageName);
    }
}
