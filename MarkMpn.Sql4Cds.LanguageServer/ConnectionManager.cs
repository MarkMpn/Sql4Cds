using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.Engine;
using Microsoft.PowerPlatform.Dataverse.Client;
using Microsoft.PowerPlatform.Dataverse.Client.Model;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.LanguageServer
{
    class ConnectionManager
    {
        private ConcurrentDictionary<string, DataSource> _dataSources;

        public ConnectionManager()
        {
            _dataSources = new ConcurrentDictionary<string, DataSource>();
        }

        public Session Connect(ConnectionDetails connection)
        {
            connection.Options.TryGetValue("user", out var user);
            connection.Options.TryGetValue("url", out var url);
            var key = System.Text.Encodings.Web.UrlEncoder.Default.Encode(user as string ?? "").Replace("@", "%40") + "@" + url;

            var ds = _dataSources.GetOrAdd(key, (_) => CreateDataSource(connection));

            return new Session
            {
                SessionId = key,
                DataSource = ds,
                Connection = new Sql4CdsConnection(new Dictionary<string, DataSource> { [ds.Name] = ds })
            };
        }

        public Session GetConnection(string sessionId)
        {
            _dataSources.TryGetValue(sessionId, out var ds);

            if (ds == null)
                return null;

            return new Session
            {
                SessionId = sessionId,
                DataSource = ds,
                Connection = new Sql4CdsConnection(new Dictionary<string, DataSource> { [ds.Name] = ds })
            };
        }

        private DataSource CreateDataSource(ConnectionDetails connection)
        {
            IOrganizationService org;

            if (connection.Options.TryGetValue("connectionString", out var x) && x is string conStr)
            {
                org = new ServiceClient(conStr);
            }
            else if (!connection.Options.TryGetValue("authenticationType", out x) || !(x is string authType))
            {
                throw new ArgumentOutOfRangeException("Missing authenticationType");
            }
            else if (!connection.Options.TryGetValue("url", out x) || !(x is string url))
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
                        if (!connection.Options.TryGetValue("user", out x) || !(x is string username))
                            throw new ArgumentOutOfRangeException("Missing user");

                        org = new ServiceClient($"AuthType=OAuth;Username={username};Url={url};AppId=51f81489-12ee-4a9e-aaae-a2591f45987d;RedirectUri=http://localhost;LoginPrompt=Auto");
                        break;

                    default:
                        throw new ArgumentOutOfRangeException("Unknown authenticationType");
                }
            }

            ValidateConnection(org);

            return new DataSource(org);
        }

        private void ValidateConnection(IOrganizationService org)
        {
            if (org is ServiceClient svc && !svc.IsReady)
                throw new InvalidOperationException(svc.LastError);
        }
    }

    class Session
    {
        public DataSource DataSource { get; set; }

        public Sql4CdsConnection Connection { get; set; }

        public string SessionId { get; set; }
    }
}
