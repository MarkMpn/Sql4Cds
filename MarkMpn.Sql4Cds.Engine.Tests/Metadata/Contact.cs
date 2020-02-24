using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Client;
using System;

namespace MarkMpn.Sql4Cds.Engine.Tests.Metadata
{
    [EntityLogicalName("contact")]
    class Contact
    {
        [AttributeLogicalName("contactid")]
        public Guid Id { get; set; }

        [AttributeLogicalName("contactid")]
        public Guid ContactId { get; set; }

        [AttributeLogicalName("firstname")]
        public string FirstName { get; set; }

        [AttributeLogicalName("lastname")]
        public string LastName { get; set; }

        [AttributeLogicalName("parentcustomerid")]
        public EntityReference ParentCustomerId { get; set; }

        [AttributeLogicalName("createdon")]
        public DateTime? CreatedOn { get; set; }
    }
}
