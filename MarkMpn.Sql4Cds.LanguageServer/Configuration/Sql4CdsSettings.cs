namespace MarkMpn.Sql4Cds.LanguageServer.Configuration
{
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

        public int DeleteWarnThreshold { get; set; }

        public int UpdateWarnThreshold { get; set; }

        public int InsertWarnThreshold { get; set; }

        public int SelectLimit { get; set; }

        public int MaxRetrievesPerQuery { get; set; }

        public static Sql4CdsSettings Instance { get; set; }
    }
}
