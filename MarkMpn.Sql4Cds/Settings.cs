using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MarkMpn.Sql4Cds
{
    /// <summary>
    /// This class can help you to store settings for your plugin
    /// </summary>
    /// <remarks>
    /// This class must be XML serializable
    /// </remarks>
    public class Settings
    {
        public static Settings Instance { get; internal set; }

        public int SelectLimit { get; set; }

        public int UpdateWarnThreshold { get; set; }

        public bool BlockUpdateWithoutWhere { get; set; } = true;
        
        public int DeleteWarnThreshold { get; set; }

        public bool BlockDeleteWithoutWhere { get; set; } = true;

        public bool UseBulkDelete { get; set; }

        public int BatchSize { get; set; } = 100;

        public bool ShowEntityReferenceNames { get; set; }

        public bool ShowLocalTimes { get; set; }

        public bool QuotedIdentifiers { get; set; } = true;
    }
}