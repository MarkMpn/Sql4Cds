namespace MarkMpn.Sql4Cds.LanguageServer.Admin.Contracts
{
    public class ChangeDatabaseParams
    {
        /// <summary>
        /// URI of the owner of the connection requesting the list of databases.
        /// </summary>
        public string OwnerUri { get; set; }

        /// <summary>
        /// The database to change to
        /// </summary>
        public string NewDatabase { get; set; }
    }
}
