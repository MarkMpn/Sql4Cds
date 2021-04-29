using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Client;

namespace MarkMpn.Sql4Cds.Engine.Tests.Metadata
{
    [EntityLogicalName("systemuser")]
    class SystemUser
    {
        [AttributeLogicalName("systemuserid")]
        public Guid Id { get; set; }

        [AttributeLogicalName("systemuserid")]
        public Guid SystemUserId { get; set; }

        [AttributeLogicalName("domainname")]
        public string DomainName { get; set; }
    }
}
