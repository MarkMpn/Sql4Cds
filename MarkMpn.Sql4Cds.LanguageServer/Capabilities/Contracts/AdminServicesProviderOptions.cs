namespace MarkMpn.Sql4Cds.LanguageServer.Capabilities.Contracts
{
    /// <summary>
    /// Defines the admin services provider options that the DMP server implements. 
    /// </summary>
    public class AdminServicesProviderOptions
    {
        public ServiceOption[] DatabaseInfoOptions { get; set; }

        public ServiceOption[] DatabaseFileInfoOptions { get; set; }

        public ServiceOption[] FileGroupInfoOptions { get; set; }
    }
}
