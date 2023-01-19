namespace MarkMpn.Sql4Cds.LanguageServer.Capabilities.Contracts
{
    /// <summary>
    /// Defines the DMP server capabilities
    /// </summary>
    public class DmpServerCapabilities
    {
        public string ProtocolVersion { get; set; }

        public string ProviderName { get; set; }

        public string ProviderDisplayName { get; set; }

        public ConnectionProviderOptions ConnectionProvider { get; set; }

        public AdminServicesProviderOptions AdminServicesProvider { get; set; }

        /// <summary>
        /// List of features
        /// </summary>
        public FeatureMetadataProvider[] Features { get; set; }
    }
}
