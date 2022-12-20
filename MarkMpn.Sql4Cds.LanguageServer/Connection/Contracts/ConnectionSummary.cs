namespace MarkMpn.Sql4Cds.LanguageServer.Connection.Contracts
{
    /// <summary>
    /// Provides high level information about a connection.
    /// </summary>
    public class ConnectionSummary
    {
        /// <summary>
        /// Gets or sets the connection server name
        /// </summary>
        public virtual string ServerName { get; set; }

        /// <summary>
        /// Gets or sets the connection database name
        /// </summary>
        public virtual string DatabaseName { get; set; }

        /// <summary>
        /// Gets or sets the connection user name
        /// </summary>
        public virtual string UserName { get; set; }
    }
}
