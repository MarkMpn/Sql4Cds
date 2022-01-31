using System;
using System.Collections.Generic;
using System.Linq;
using System.Xml;
using Microsoft.Xrm.Sdk.Query;
using System.Net.Sockets;
using Microsoft.Xrm.Sdk;
#if NETCOREAPP
using Microsoft.PowerPlatform.Dataverse.Client;
#else
using Microsoft.Xrm.Tooling.Connector;
#endif

namespace MarkMpn.Sql4Cds.Engine
{
    public static class TDSEndpoint
    {
        private static readonly IDictionary<string, bool> _cache = new Dictionary<string, bool>();

#if NETCOREAPP
        public static bool IsEnabled(ServiceClient svc)
        {
            var host = svc.ConnectedOrgUriActual.Host;
#else
        public static bool IsEnabled(CrmServiceClient svc)
        {
            var host = svc.CrmConnectOrgUriActual.Host;
#endif
            if (!host.EndsWith(".dynamics.com"))
                return false;

            if (_cache.TryGetValue(host, out var enabled))
                return enabled;

            var qry = new QueryExpression("organization");
            qry.ColumnSet = new ColumnSet("orgdborgsettings");
            var orgSettings = svc.RetrieveMultiple(qry).Entities.Single().GetAttributeValue<string>("orgdborgsettings");

            if (String.IsNullOrEmpty(orgSettings))
            {
                enabled = TestConnection(svc);
            }
            else
            {
                var xml = new XmlDocument();
                xml.LoadXml(orgSettings);

                var enabledNode = xml.SelectSingleNode("//EnableTDSEndpoint/text()");

                if (enabledNode != null)
                    enabled = enabledNode.Value == "true";
                else
                    enabled = TestConnection(svc);
            }

            _cache[host] = enabled;

            return enabled;
        }

#if NETCOREAPP
        private static bool TestConnection(ServiceClient svc)
#else
        private static bool TestConnection(CrmServiceClient svc)
#endif
        {
            var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);

#if NETCOREAPP
            var result = socket.BeginConnect(svc.ConnectedOrgUriActual.Host, 1433, null, null);
#else
            var result = socket.BeginConnect(svc.CrmConnectOrgUriActual.Host, 1433, null, null);
#endif
            var success = result.AsyncWaitHandle.WaitOne(1000, true);

            if (socket.Connected)
            {
                socket.EndConnect(result);
                return true;
            }
            else
            {
                socket.Close();
                return false;
            }
        }

#if NETCOREAPP
        public static void Enable(ServiceClient svc)
#else
        public static void Enable(CrmServiceClient svc)
#endif
        {
            var qry = new QueryExpression("organization");
            qry.ColumnSet = new ColumnSet("orgdborgsettings");
            var org = svc.RetrieveMultiple(qry).Entities.Single();

            var xml = new XmlDocument();
            xml.LoadXml(org.GetAttributeValue<string>("orgdborgsettings"));

            var enabled = xml.SelectSingleNode("//EnableTDSEndpoint/text()");
            var updateRequired = false;

            if (enabled == null)
            {
                enabled = xml.CreateElement("EnableTDSEndpoint");
                enabled.InnerText = "true";
                xml.SelectSingleNode("/OrgSettings").AppendChild(enabled);
                updateRequired = true;
            }
            else if (enabled.InnerText != "true")
            {
                enabled.InnerText = "true";
                updateRequired = true;
            }

            if (updateRequired)
            {
                org["orgdborgsettings"] = xml.OuterXml;
                svc.Update(org);
            }

#if NETCOREAPP
            _cache[svc.ConnectedOrgUriActual.Host] = true;
#else
            _cache[svc.CrmConnectOrgUriActual.Host] = true;
#endif
        }

#if NETCOREAPP
        public static void Disable(ServiceClient svc)
#else
        public static void Disable(CrmServiceClient svc)
#endif
        {
            var qry = new QueryExpression("organization");
            qry.ColumnSet = new ColumnSet("orgdborgsettings");
            var org = svc.RetrieveMultiple(qry).Entities.Single();

            var xml = new XmlDocument();
            xml.LoadXml(org.GetAttributeValue<string>("orgdborgsettings"));

            var enabled = xml.SelectSingleNode("//EnableTDSEndpoint/text()");
            var updateRequired = false;

            if (enabled?.InnerText == "true")
            {
                enabled.InnerText = "false";
                updateRequired = true;
            }

            if (updateRequired)
            {
                org["orgdborgsettings"] = xml.OuterXml;
                svc.Update(org);
            }

#if NETCOREAPP
            _cache[svc.ConnectedOrgUriActual.Host] = true;
#else
            _cache[svc.CrmConnectOrgUriActual.Host] = true;
#endif
        }

        /// <summary>
        /// Checks if the TDS endpoint is valid to be used with a specific connection and set of options
        /// </summary>
        /// <param name="options">The <see cref="IQueryExecutionOptions"/> that describe how a query should be executed</param>
        /// <param name="org">The <see cref="IOrganizationService"/> that is connected to the instance to use</param>
        /// <returns><c>true</c> if the TDS endpoint can be used for this connection and options, or <c>false</c> otherwise</returns>
        public static bool CanUseTDSEndpoint(IQueryExecutionOptions options, IOrganizationService org)
        {
            if (!options.UseTDSEndpoint)
                return false;

            if (options.UseLocalTimeZone)
                return false;

            // Allow generating TDS-based plans in tests
            if (org == null)
                return true;

#if NETCOREAPP
            var svc = org as ServiceClient;
#else
            var svc = org as CrmServiceClient;
#endif

            if (svc == null)
                return false;

            if (svc.CallerId != Guid.Empty)
                return false;

            if (String.IsNullOrEmpty(svc.CurrentAccessToken))
                return false;

            if (!IsEnabled(svc))
                return false;

            return true;
        }
    }
}
