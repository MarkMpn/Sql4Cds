using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.LanguageServer.Protocol;

namespace MarkMpn.Sql4Cds.LanguageServer.ObjectExplorer.Contracts
{
    /// <summary>
    /// A request to expand a 
    /// </summary>
    public class RefreshRequest
    {
        /// <summary>
        /// Returns children of a given node as a <see cref="NodeInfo"/> array.
        /// </summary>
        public static readonly LspRequest<RefreshParams, bool> Type = new LspRequest<RefreshParams, bool>("objectexplorer/refresh");
    }
}
