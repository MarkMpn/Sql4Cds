using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Client;
using System;
using System.Collections.Generic;

namespace MarkMpn.Sql4Cds.Engine.Tests.Metadata
{
    [EntityLogicalName("audit")]
    class Audit
    {
        [AttributeLogicalName("auditid")]
        public Guid Id { get; set; }

        [AttributeLogicalName("auditid")]
        public Guid AuditId { get; set; }

        [AttributeLogicalName("callinguserid")]
        public EntityReference CallingUserId { get; set; }

        [AttributeLogicalName("callinguserid")]
        [RelationshipSchemaName("audit_callinguser")]
        public SystemUser CallingUser { get; set; }

        [AttributeLogicalName("userid")]
        public EntityReference UserId { get; set; }

        [AttributeLogicalName("userid")]
        [RelationshipSchemaName("audit_user")]
        public SystemUser User { get; set; }

        [AttributeLogicalName("objectid")]
        public EntityReference ObjectId { get; set; }

        [AttributeLogicalName("objecttypecode")]
        public string ObjectTypeCode { get; set; }
    }
}
