namespace MarkMpn.Sql4Cds.LanguageServer.Capabilities.Contracts
{
    public class ConnectionOption : ServiceOption
    {
        public static readonly string SpecialValueServerName = "serverName";
        public static readonly string SpecialValueDatabaseName = "databaseName";
        public static readonly string SpecialValueAuthType = "authType";
        public static readonly string SpecialValueUserName = "userName";
        public static readonly string SpecialValuePasswordName = "password";
        public static readonly string SpecialValueAppName = "appName";

        /// <summary>
        /// Determines if the parameter is one of the 'special' known values.
        /// Can be either Server Name, Database Name, Authentication Type,
        /// User Name, or Password
        /// </summary>
        public string SpecialValueType { get; set; }

        /// <summary>
        /// Flag to indicate that this option is part of the connection identity
        /// </summary>
        public bool IsIdentity { get; set; }
    }
}
