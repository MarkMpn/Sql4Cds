using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data.Common;
using System.IdentityModel.Tokens.Jwt;
using System.IO;
using System.Linq;
using System.Security.Policy;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Data8.PowerPlatform.Dataverse.Client;
using MarkMpn.Sql4Cds.Engine;
using MarkMpn.Sql4Cds.LanguageServer.Connection.Contracts;
using Microsoft.Crm.Sdk.Messages;
using Microsoft.PowerPlatform.Dataverse.Client;
using Microsoft.PowerPlatform.Dataverse.Client.Model;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Metadata;

namespace MarkMpn.Sql4Cds.LanguageServer.Connection
{
    class ConnectionManager
    {
        private readonly PersistentMetadataCache _persistentMetadataCache;
        private readonly ConcurrentDictionary<string, DataSource> _dataSources;
        private readonly ConcurrentDictionary<string, string> _connectedDataSource;
        private readonly ConcurrentDictionary<string, Sql4CdsConnection> _connections;

        public ConnectionManager(PersistentMetadataCache persistentMetadataCache)
        {
            _persistentMetadataCache = persistentMetadataCache;
            _dataSources = new ConcurrentDictionary<string, DataSource>();
            _connectedDataSource = new ConcurrentDictionary<string, string>();
            _connections = new ConcurrentDictionary<string, Sql4CdsConnection>();
        }

        public Session Connect(ConnectionDetails connection, string ownerUri)
        {
            var dataSourceName = GetDataSourceName(connection);

            if (ownerUri == null)
                ownerUri = Guid.NewGuid().ToString("N");

            var ds = _dataSources.GetOrAdd(dataSourceName, (_) => CreateDataSource(connection));
            // Names are SQL database aliases, not credential identities. Never silently
            // reuse a name for another profile or for changed authentication settings.
            if (((DataSourceWithInfo)ds).ConnectionIdentity != GetConnectionIdentity(connection))
                throw new InvalidOperationException("A connection with this name is still using different settings. Disconnect its query editors and refresh Object Explorer, then reconnect.");
            _connectedDataSource[ownerUri] = dataSourceName;

            return GetConnection(ownerUri);
        }

        public void Disconnect(string ownerUri)
        {
            if (_connectedDataSource.TryRemove(ownerUri, out var dataSourceName) &&
                !_connectedDataSource.Any(kvp => kvp.Value == dataSourceName))
                _dataSources.TryRemove(dataSourceName, out _);

            _connections.TryRemove(ownerUri, out _);
        }

        public Session AssociateConnection(string pendingOwnerUri, string ownerUri)
        {
            var dataSourceName = _connectedDataSource[pendingOwnerUri];
            Disconnect(ownerUri);
            _connectedDataSource[ownerUri] = dataSourceName;
            if (_connections.TryRemove(pendingOwnerUri, out var connection))
                _connections[ownerUri] = connection;
            _connectedDataSource.TryRemove(pendingOwnerUri, out _);
            return GetConnection(ownerUri);
        }

        public Session GetConnection(string ownerUri)
        {
            if (!_connectedDataSource.TryGetValue(ownerUri, out var dsName))
                return null;

            if (!_dataSources.TryGetValue(dsName, out var ds))
                return null;

            var con = _connections.GetOrAdd(ownerUri, _ => new Sql4CdsConnection(_dataSources) { ApplicationName = "SQL 4 CDS" });
            con.ChangeDatabase(ds.Name);

            return new Session
            {
                SessionId = ownerUri,
                DataSource = (DataSourceWithInfo)ds,
                Connection = con
            };
        }

        public IDictionary<string, DataSource> GetAllConnections()
        {
            return _dataSources
                .Values
                .Cast<DataSource>()
                .ToDictionary(ds => ds.Name);
        }

        private Uri GetUri(ConnectionDetails connection)
        {
            string url = null;

            if (connection.Options.TryGetValue("connectionString", out var x) && x is string conStr)
            {
                var builder = new DbConnectionStringBuilder();
                builder.ConnectionString = conStr;

                foreach (var keyword in new[] { "ServiceUri", "Service Uri", "Url", "Server"})
                {
                    if (builder.TryGetValue(keyword, out x) && x is string s)
                    {
                        url = s;
                        break;
                    }
                }

                if (url == null)
                    throw new ArgumentOutOfRangeException("Missing url");
            }
            else if (!connection.Options.TryGetValue("authenticationType", out x) || !(x is string authType))
            {
                throw new ArgumentOutOfRangeException("Missing authenticationType");
            }
            else if (!connection.Options.TryGetValue("url", out x) || (url = x as string) == null)
            {
                throw new ArgumentOutOfRangeException("Missing url");
            }
            else
            {
                if (!url.Contains("://"))
                    url = "https://" + url;
                else if (!url.StartsWith("https://"))
                    throw new ArgumentOutOfRangeException("Only HTTPS URLs are allowed");
            }

            return new Uri(url);
        }

        private string GetDataSourceName(ConnectionDetails connection)
        {
            if (connection.Options.TryGetValue("connectionName", out var x) && x is string name && !String.IsNullOrEmpty(name))
            {
                return name;
            }
            else
            {
                var uri = GetUri(connection);

                if (uri.Host.EndsWith(".dynamics.com") ||
                    String.IsNullOrEmpty(uri.AbsolutePath) ||
                    uri.AbsolutePath == "/" ||
                    uri.AbsolutePath.Equals("/XRMServices/2011/Organization.svc", StringComparison.OrdinalIgnoreCase))
                {
                    // Online and IFD instances are identified by the host name
                    return uri.Host;
                }
                else
                {
                    // On-prem instances are identified by the org name in the URL
                    return uri.AbsolutePath.Split('/')[1];
                }
            }
        }

        private DataSourceWithInfo CreateDataSource(ConnectionDetails connection)
        {
            IOrganizationService org;
            string url;

            if (connection.Options.TryGetValue("connectionString", out var x) && x is string conStr)
            {
                org = new ServiceClient(conStr);
                url = ((ServiceClient)org).ConnectedOrgUriActual.ToString();
            }
            else if (!connection.Options.TryGetValue("authenticationType", out x) || !(x is string authType))
            {
                throw new ArgumentOutOfRangeException("Missing authenticationType");
            }
            else if (!connection.Options.TryGetValue("url", out x) || (url = x as string) == null)
            {
                throw new ArgumentOutOfRangeException("Missing url");
            }
            else
            {
                if (!url.Contains("://"))
                    url = "https://" + url;
                else if (!url.StartsWith("https://"))
                    throw new ArgumentOutOfRangeException("Only HTTPS URLs are allowed");

                switch (authType)
                {
                    case "AzureMFA":
                        string oauthUsername = null;

                        // Azure Data Studio supplies an account token. Other hosts can supply a user
                        // name directly or omit it and allow ServiceClient to show an account picker.
                        if (connection.Options.TryGetValue("azureAccountToken", out x) && x is string accountToken && !String.IsNullOrWhiteSpace(accountToken))
                        {
                            var token = new JwtSecurityToken(accountToken);
                            oauthUsername = token.Claims.FirstOrDefault(c => c.Type == "upn" || c.Type == "preferred_username")?.Value;
                        }
                        else if (connection.Options.TryGetValue("user", out x) && x is string user && !String.IsNullOrWhiteSpace(user))
                        {
                            oauthUsername = user;
                        }

                        var oauth = new DbConnectionStringBuilder
                        {
                            ["AuthType"] = "OAuth", ["Url"] = url,
                            ["AppId"] = "51f81489-12ee-4a9e-aaae-a2591f45987d",
                            ["RedirectUri"] = "http://localhost", ["LoginPrompt"] = "Auto",
                            ["TokenCacheStorePath"] = GetTokenCachePath()
                        };
                        if (oauthUsername != null) oauth["Username"] = oauthUsername;
                        org = new ServiceClient(oauth.ConnectionString);
                        break;

                    case "None":
                        if (!connection.Options.TryGetValue("clientid", out x) || !(x is string clientId))
                            throw new ArgumentOutOfRangeException("Missing Client ID");
                        if (!connection.Options.TryGetValue("clientsecret", out x) || !(x is string clientSecret))
                            throw new ArgumentOutOfRangeException("Missing Client Secret");
                        if (!connection.Options.TryGetValue("redirectUrl", out x) || !(x is string redirectUrl))
                            throw new ArgumentOutOfRangeException("Missing Redirect URL");

                        org = new ServiceClient(new DbConnectionStringBuilder
                        {
                            ["AuthType"] = "ClientSecret", ["Url"] = url,
                            ["ClientId"] = clientId, ["ClientSecret"] = clientSecret,
                            ["RedirectUri"] = redirectUrl, ["LoginPrompt"] = "Never",
                            ["TokenCacheStorePath"] = GetTokenCachePath()
                        }.ConnectionString);
                        break;

                    case "SqlLogin":
                        if (!connection.Options.TryGetValue("user", out x) || !(x is string ifdUsername))
                            throw new ArgumentOutOfRangeException("Missing user");
                        if (!connection.Options.TryGetValue("password", out x) || !(x is string ifdPassword))
                            throw new ArgumentOutOfRangeException("Missing password");

                        org = new OnPremiseClient(url, ifdUsername, ifdPassword);
                        break;

                    case "Integrated":
                        org = new OnPremiseClient(url);
                        break;

                    default:
                        throw new ArgumentOutOfRangeException("Unknown authenticationType " + authType);
                }
            }

            ValidateConnection(org);

            var dataSource = new DataSourceWithInfo(org, url, _persistentMetadataCache);
            dataSource.Name = GetDataSourceName(connection);
            dataSource.ConnectionIdentity = GetConnectionIdentity(connection);

            return dataSource;
        }

        private string GetTokenCachePath()
        {
            var dataDir = Environment.GetEnvironmentVariable("SQL4CDS_DATA_DIR");

            if (String.IsNullOrWhiteSpace(dataDir))
                dataDir = Path.GetDirectoryName(GetType().Assembly.Location);

            Directory.CreateDirectory(dataDir);
            return Path.Combine(dataDir, "TokenCache");
        }

        private static string GetConnectionIdentity(ConnectionDetails connection)
        {
            // ADS refreshes its account token independently of the underlying connection.
            // Profile identity is supplied by VS Code; keep legacy ADS reuse unchanged.
            if (!connection.Options.ContainsKey("connectionId")) return null;
            var options = new SortedDictionary<string, object>(connection.Options, StringComparer.Ordinal);
            var json = Newtonsoft.Json.JsonConvert.SerializeObject(options);
            return Convert.ToBase64String(SHA256.HashData(Encoding.UTF8.GetBytes(json)));
        }

        private void ValidateConnection(IOrganizationService org)
        {
            if (org is ServiceClient svc && !svc.IsReady)
                throw new InvalidOperationException(svc.LastError);
        }

        public bool ChangeConnection(string ownerUri, string newDatabase)
        {
            if (!_dataSources.ContainsKey(newDatabase))
                return false;

            _connectedDataSource[ownerUri] = newDatabase;
            return true;
        }
    }

    class DataSourceWithInfo : DataSource
    {
        public string ConnectionIdentity { get; set; }
        private readonly string _url;

        public DataSourceWithInfo(IOrganizationService org, string url, PersistentMetadataCache persistentMetadataCache) : base(org)
        {
            UniqueName = Name;
            ServerName = new Uri(url).Host;
            _url = url;

            using (var con = new Sql4CdsConnection(new Dictionary<string, DataSource> { [Name] = this }))
            using (var cmd = con.CreateCommand())
            {
                con.ApplicationName = "SQL 4 CDS";
                cmd.CommandText = "SELECT fullname FROM systemuser WHERE systemuserid = CURRENT_USER";
                Username = (string)cmd.ExecuteScalar();
            }

            if (org is ServiceClient svc)
            {
                Version = svc.ConnectedOrgVersion.ToString();
                OrgId = svc.ConnectedOrgId;
            }
            else
            {
                var resp = (RetrieveVersionResponse)org.Execute(new RetrieveVersionRequest());
                Version = resp.Version;

                var whoAmI = (WhoAmIResponse)org.Execute(new WhoAmIRequest());
                OrgId = whoAmI.OrganizationId;
            }

            Metadata = new CachedMetadata(org, persistentMetadataCache);
        }

        public string UniqueName { get; set; }

        public Guid OrgId { get; set; }

        public string ServerName { get; set; }

        public string Version { get; set; }

        public string Username { get; set; }

        internal string GetEntityReferenceUrl(SqlEntityReference reference)
        {
            if (reference.IsNull)
            {
                return string.Empty;
            }
            var url = _url;
            url = string.Concat(url,
                url.EndsWith("/") ? "" : "/",
                "main.aspx?etn=",
                reference.LogicalName,
                "&pagetype=entityrecord&id=",
                reference.Id.ToString());
            return url;
        }
    }

    class Session
    {
        public string SessionId { get; set; }

        public DataSourceWithInfo DataSource { get; set; }

        public Sql4CdsConnection Connection { get; set; }
    }
}
