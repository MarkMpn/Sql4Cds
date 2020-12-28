using System;
using System.Data.SqlClient;

namespace MarkMpn.Sql4Cds.SSMS
{
    class AuthParams : SqlAuthenticationParameters
    {
        public AuthParams(SqlAuthenticationMethod authenticationMethod, string serverName, string databaseName, string resource, string authority, string userId, string password, Guid connectionId) : base(authenticationMethod, serverName, databaseName, resource, authority, userId, password, connectionId)
        {
        }
    }
}
