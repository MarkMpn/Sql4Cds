using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Client;
using System;
using System.Collections.Generic;

namespace MarkMpn.Sql4Cds.Engine.FetchXml.Tests.Metadata
{
    [EntityLogicalName("new_customentity")]
    class New_CustomEntity
    {
        [AttributeLogicalName("new_customentityid")]
        public Guid Id { get; set; }

        [AttributeLogicalName("new_customentityid")]
        public Guid New_CustomEntityId { get; set; }

        [AttributeLogicalName("new_name")]
        public string Name { get; set; }

        [AttributeLogicalName("new_parentid")]
        public EntityReference New_ParentId { get; set; }

        [AttributeLogicalName("new_parentid")]
        [RelationshipSchemaName("new_customentity_children")]
        public New_CustomEntity Parent { get; set; }

        [RelationshipSchemaName("new_customentity_children")]
        public IEnumerable<New_CustomEntity> Children { get; }
    }
}
