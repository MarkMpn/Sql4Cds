namespace MarkMpn.Sql4Cds.LanguageServer.Admin.Contracts
{
    /// <summary>
    /// Params for a get database info request
    /// </summar>
    public class GetDatabaseInfoParams
    {
        /// <summary>
        /// Uri identifier for the connection to get the database info for
        /// </summary>
        public string OwnerUri { get; set; }
    }
}
