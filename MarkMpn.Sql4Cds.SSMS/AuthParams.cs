using System;
#if System_Data_SqlClient
using System.Data.SqlClient;
#else
using Microsoft.Data.SqlClient;
#endif

namespace MarkMpn.Sql4Cds.SSMS
{
    class AuthParams : SqlAuthenticationParameters
    {
#if System_Data_SqlClient
        public AuthParams(SqlAuthenticationMethod authenticationMethod, string serverName, string databaseName, string resource, string authority, string userId, string password, Guid connectionId) : base(authenticationMethod, serverName, databaseName, resource, authority, userId, password, connectionId)
#else
        public AuthParams(SqlAuthenticationMethod authenticationMethod, string serverName, string databaseName, string resource, string authority, string userId, string password, Guid connectionId, int connectionTimeout) : base(authenticationMethod, serverName, databaseName, resource, authority, userId, password, connectionId, connectionTimeout)
#endif
        {
        }
    }
}
