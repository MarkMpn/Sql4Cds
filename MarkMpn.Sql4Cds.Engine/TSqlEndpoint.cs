using System;
using System.Collections.Generic;
using System.Linq;
using System.Xml;
using Microsoft.Xrm.Sdk.Query;
#if NETCOREAPP
using Microsoft.PowerPlatform.Dataverse.Client;
#else
using Microsoft.Xrm.Tooling.Connector;
#endif

namespace MarkMpn.Sql4Cds.Engine
{
    public static class TSqlEndpoint
    {
        private static IDictionary<string, bool> _cache = new Dictionary<string, bool>();

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
                enabled = false;
            }
            else
            {
                var xml = new XmlDocument();
                xml.LoadXml(orgSettings);

                var enabledNode = xml.SelectSingleNode("//EnableTDSEndpoint/text()");

                enabled = enabledNode?.Value == "true";
            }

            _cache[host] = enabled;

            return enabled;
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
    }
}
