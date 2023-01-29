using System;
using System.Collections.Generic;
using System.IdentityModel.Tokens.Jwt;
#if System_Data_SqlClient
using System.Data.SqlClient;
#else
using Microsoft.Data.SqlClient;
#endif
using Microsoft.Xrm.Tooling.Connector;
using Task = System.Threading.Tasks.Task;

namespace MarkMpn.Sql4Cds.SSMS
{
    class AuthOverrideHook : IOverrideAuthHookWrapper
    {
        private IDictionary<string, AuthParams> _authParams = new Dictionary<string, AuthParams>();
        private IDictionary<string, string> _tokenCache = new Dictionary<string, string>();

        public static AuthOverrideHook Instance { get; } = new AuthOverrideHook();

        public void AddAuth(AuthParams authParams)
        {
            _authParams[authParams.Resource] = authParams;
        }

        public string GetAuthToken(Uri connectedUri)
        {
            if (_tokenCache.TryGetValue(connectedUri.ToString(), out var existingToken))
            {
                // Check if the token is still valid
                var parsedToken = new JwtSecurityToken(existingToken);
                if (parsedToken.ValidTo > DateTime.Now)
                    return existingToken;
            }

            var uri = new Uri(connectedUri, "/");
            var authParams = _authParams[uri.ToString()];
            var authProv = SqlAuthenticationProvider.GetProvider(authParams.AuthenticationMethod);
            var token = Task.Run(() => authProv.AcquireTokenAsync(authParams)).ConfigureAwait(false).GetAwaiter().GetResult();

            _tokenCache[connectedUri.ToString()] = token.AccessToken;
            return token.AccessToken;
        }
    }
}
