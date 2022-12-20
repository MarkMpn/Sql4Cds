using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Data8.PowerPlatform.Dataverse.Client;
using MarkMpn.Sql4Cds.Engine;
using MarkMpn.Sql4Cds.LanguageServer.Connection.Contracts;
using Microsoft.Crm.Sdk.Messages;
using Microsoft.PowerPlatform.Dataverse.Client;
using Microsoft.PowerPlatform.Dataverse.Client.Model;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.LanguageServer.Connection
{
    class ConnectionManager
    {
        private ConcurrentDictionary<string, DataSourceWithInfo> _dataSources;
        private ConcurrentDictionary<string, string> _connectedDataSource;

        public ConnectionManager()
        {
            _dataSources = new ConcurrentDictionary<string, DataSourceWithInfo>();
            _connectedDataSource = new ConcurrentDictionary<string, string>();
        }

        public Session Connect(ConnectionDetails connection, string ownerUri)
        {
            connection.Options.TryGetValue("user", out var user);
            connection.Options.TryGetValue("url", out var url);
            var key = System.Text.Encodings.Web.UrlEncoder.Default.Encode(user as string ?? "").Replace("@", "%40") + "@" + url;

            if (ownerUri == null)
                ownerUri = key;

            var ds = _dataSources.GetOrAdd(key, (_) => CreateDataSource(connection));
            _connectedDataSource[ownerUri] = key;

            return GetConnection(ownerUri);
        }

        public void Disconnect(string ownerUri)
        {
            _connectedDataSource.TryRemove(ownerUri, out _);
        }

        public Session GetConnection(string ownerUri)
        {
            if (!_connectedDataSource.TryGetValue(ownerUri, out var dsName))
                return null;

            if (!_dataSources.TryGetValue(dsName, out var ds))
                return null;

            var con = new Sql4CdsConnection(GetAllConnections())
            {
                ApplicationName = "Azure Data Studio"
            };
            con.ChangeDatabase(ds.Name);

            return new Session
            {
                SessionId = ownerUri,
                DataSource = ds,
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
                    case "AzureMFAAndUser":
                        if (!connection.Options.TryGetValue("user", out x) || !(x is string oauthUsername))
                            throw new ArgumentOutOfRangeException("Missing user");

                        org = new ServiceClient($"AuthType=OAuth;Username={oauthUsername};Url={url};AppId=51f81489-12ee-4a9e-aaae-a2591f45987d;RedirectUri=http://localhost;LoginPrompt=Auto;TokenCacheStorePath=" + Path.Combine(Path.GetDirectoryName(GetType().Assembly.Location), "TokenCache"));
                        break;

                    case "SqlLogin":
                        if (!connection.Options.TryGetValue("user", out x) || !(x is string ifdUsername))
                            throw new ArgumentOutOfRangeException("Missing user");
                        if (!connection.Options.TryGetValue("password", out x) || !(x is string ifdPassword))
                            throw new ArgumentOutOfRangeException("Missing password");

                        org = new OnPremiseClient(url, ifdUsername, ifdPassword);
                        break;

                    default:
                        throw new ArgumentOutOfRangeException("Unknown authenticationType " + authType);
                }
            }

            ValidateConnection(org);

            return new DataSourceWithInfo(org, url);
        }

        private void ValidateConnection(IOrganizationService org)
        {
            if (org is ServiceClient svc && !svc.IsReady)
                throw new InvalidOperationException(svc.LastError);
        }
    }

    class DataSourceWithInfo : DataSource
    {
        public DataSourceWithInfo(IOrganizationService org, string url) : base(org)
        {
            ServerName = new Uri(url).Host;

            using (var con = new Sql4CdsConnection(new Dictionary<string, DataSource> { [Name] = this }))
            using (var cmd = con.CreateCommand())
            {
                con.ApplicationName = "Azure Data Studio";
                cmd.CommandText = "SELECT fullname FROM systemuser WHERE systemuserid = CURRENT_USER";
                Username = (string)cmd.ExecuteScalar();
            }

            if (org is ServiceClient svc)
            {
                Version = svc.ConnectedOrgVersion.ToString();
            }
            else
            {
                var resp = (RetrieveVersionResponse)org.Execute(new RetrieveVersionRequest());
                Version = resp.Version;
            }
        }

        public string ServerName { get; set; }

        public string Version { get; set; }

        public string Username { get; set; }
    }

    class Session
    {
        public string SessionId { get; set; }

        public DataSourceWithInfo DataSource { get; set; }

        public Sql4CdsConnection Connection { get; set; }
    }
}
