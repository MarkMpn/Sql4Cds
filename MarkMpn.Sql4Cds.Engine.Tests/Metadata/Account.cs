using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Client;
using System;

namespace MarkMpn.Sql4Cds.Engine.Tests.Metadata
{
    [EntityLogicalName("account")]
    class Account
    {
        [AttributeLogicalName("accountid")]
        public Guid Id { get; set; }

        [AttributeLogicalName("accountid")]
        public Guid AccountId { get; set; }

        [AttributeLogicalName("name")]
        public string Name { get; set; }
    }
}
