using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Client;
using System;
using System.Collections.Generic;

namespace MarkMpn.Sql4Cds.Engine.FetchXml.Tests.Metadata
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

        [AttributeLogicalName("createdon")]
        public DateTime? CreatedOn { get; set; }

        [RelationshipSchemaName("contact_account")]
        public IEnumerable<Contact> Contacts { get; }

        [AttributeLogicalName("primarycontactid")]
        public EntityReference PrimaryContactId { get; set; }

        [AttributeLogicalName("primarycontactid")]
        [RelationshipSchemaName("account_primarycontact")]
        public Contact PrimaryContact { get; set; }

        [AttributeLogicalName("parentaccountid")]
        public EntityReference ParentAccountId { get; set; }

        [AttributeLogicalName("parentaccountid")]
        [RelationshipSchemaName("account_parentaccount")]
        public Account ParentAccount { get; set; }
    }
}
