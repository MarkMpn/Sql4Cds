using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using McTools.Xrm.Connection;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.XTB
{
    /// <summary>
    /// Thanks @rappen
    /// https://github.com/rappen/FetchXMLBuilder/blob/master/FetchXmlBuilder/AppCode/ConnectionExtensions.cs
    /// </summary>
    static class ConnectionExtensions
    {
        public static string GetFullWebApplicationUrl(this ConnectionDetail connectiondetail)
        {
            var url = connectiondetail.WebApplicationUrl;
            if (string.IsNullOrEmpty(url))
            {
                url = connectiondetail.ServerName;
            }
            if (!url.ToLower().StartsWith("http"))
            {
                url = string.Concat("http://", url);
            }
            var uri = new Uri(url);
            if (!uri.Host.EndsWith(".dynamics.com"))
            {
                if (string.IsNullOrEmpty(uri.AbsolutePath.Trim('/')))
                {
                    uri = new Uri(uri, connectiondetail.Organization);
                }
            }
            return uri.ToString();
        }

        public static string GetEntityReferenceUrl(this ConnectionDetail connectiondetail, EntityReference entref)
        {
            if (string.IsNullOrWhiteSpace(entref?.LogicalName) || Guid.Empty.Equals(entref.Id))
            {
                return string.Empty;
            }
            var url = connectiondetail.GetFullWebApplicationUrl();
            url = string.Concat(url,
                url.EndsWith("/") ? "" : "/",
                "main.aspx?etn=",
                entref.LogicalName,
                "&pagetype=entityrecord&id=",
                entref.Id.ToString());
            return url;
        }
    }
}
