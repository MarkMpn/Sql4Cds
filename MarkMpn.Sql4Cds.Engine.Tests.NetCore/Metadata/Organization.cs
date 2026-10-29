using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Client;

namespace MarkMpn.Sql4Cds.Engine.Tests.NetCore.Metadata
{
    [EntityLogicalName("organization")]
    class Organization
    {
        [AttributeLogicalName("organizationid")]
        public Guid Id { get; set; }

        [AttributeLogicalName("organizationid")]
        public Guid OrganizationId { get; set; }

        [AttributeLogicalName("name")]
        public string Name { get; set; }

        [AttributeLogicalName("localeid")]
        public int? LocaleId { get; set; }
    }
}
