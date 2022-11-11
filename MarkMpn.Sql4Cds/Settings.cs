using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.Engine;

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

        public int InsertWarnThreshold { get; set; } = 1;

        public int UpdateWarnThreshold { get; set; }

        public bool BlockUpdateWithoutWhere { get; set; } = true;
        
        public int DeleteWarnThreshold { get; set; }

        public bool BlockDeleteWithoutWhere { get; set; } = true;

        public bool UseBulkDelete { get; set; }

        public int BatchSize { get; set; } = 100;

        public bool ShowLocalTimes { get; set; }

        public bool QuotedIdentifiers { get; set; } = true;

        public bool UseTSQLEndpoint { get; set; }

        public bool ShowIntellisenseTooltips { get; set; } = true;

        public int MaxDegreeOfPaallelism { get; set; } = 10;

        public bool IncludeFetchXml { get; set; }

        public bool AutoSizeColumns { get; set; } = true;

        public int MaxRetrievesPerQuery { get; set; } = 100;

        public bool BypassCustomPlugins { get; set; }

        public bool RememberSession { get; set; } = true;

        public TabContent[] Session { get; set; }

        public bool LocalFormatDates { get; set; }

        public bool UseNativeSqlConversion { get; set; }

        public bool ShowFetchXMLInEstimatedExecutionPlans { get; set; } = true;

        public FetchXml2SqlOptions FetchXml2SqlOptions { get; set; } = new FetchXml2SqlOptions();
    }

    public class TabContent
    {
        public string Type { get; set; }

        public string Filename { get; set; }

        public string Query { get; set; }
    }
}