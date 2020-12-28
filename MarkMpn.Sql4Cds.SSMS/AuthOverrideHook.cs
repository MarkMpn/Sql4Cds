using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using Microsoft.Xrm.Tooling.Connector;
using Task = System.Threading.Tasks.Task;

namespace MarkMpn.Sql4Cds.SSMS
{
    class AuthOverrideHook : IOverrideAuthHookWrapper
    {
        private IDictionary<string, AuthParams> _authParams = new Dictionary<string, AuthParams>();

        public static AuthOverrideHook Instance { get; } = new AuthOverrideHook();

        public void AddAuth(AuthParams authParams)
        {
            _authParams[authParams.Resource] = authParams;
        }

        public string GetAuthToken(Uri connectedUri)
        {
            var uri = new Uri(connectedUri, "/");
            var authParams = _authParams[uri.ToString()];
            var authProv = SqlAuthenticationProvider.GetProvider(authParams.AuthenticationMethod);
            var token = Task.Run(() => authProv.AcquireTokenAsync(authParams)).ConfigureAwait(false).GetAwaiter().GetResult();

            return token.AccessToken;
        }
    }
}
