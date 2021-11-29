using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Client;
using System;
using System.Collections.Generic;

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

        [AttributeLogicalName("createdon")]
        public DateTime? CreatedOn { get; set; }

        [AttributeLogicalName("turnover")]
        public Money Turnover { get; set; }

        [AttributeLogicalName("employees")]
        public int? Employees { get; set; }

        [RelationshipSchemaName("contact_account")]
        public IEnumerable<Contact> Contacts { get; }

        [AttributeLogicalName("primarycontactid")]
        public EntityReference PrimaryContactId { get; set; }

        [AttributeLogicalName("primarycontactid")]
        [RelationshipSchemaName("account_primarycontact")]
        public Contact PrimaryContact { get; set; }

        [AttributeLogicalName("primarycontactidname")]
        public string PrimaryContactIdName { get; }

        [AttributeLogicalName("ownerid")]
        public EntityReference OwnerId { get; set; }

        [AttributeLogicalName("ownerid")]
        [RelationshipSchemaName("account_owner")]
        public SystemUser Owner { get; set; }

        [AttributeLogicalName("owneridname")]
        public string OwnerIdName { get; }
    }
}
