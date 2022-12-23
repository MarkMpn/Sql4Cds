namespace MarkMpn.Sql4Cds.LanguageServer.Admin.Contracts
{
    public class ListDatabasesParams
    {
        /// <summary>
        /// URI of the owner of the connection requesting the list of databases.
        /// </summary>
        public string OwnerUri { get; set; }

        /// <summary>
        /// whether to include the details of the databases.
        /// </summary>
        public bool? IncludeDetails { get; set; }
    }
}
