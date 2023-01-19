using System.Collections.Generic;

namespace MarkMpn.Sql4Cds.LanguageServer.Admin.Contracts
{
    public class DatabaseInfo
    {
        /// <summary>
        /// Gets or sets the options
        /// </summary>
        public Dictionary<string, object> Options { get; set; }

        public DatabaseInfo()
        {
            Options = new Dictionary<string, object>();
        }

    }
}
