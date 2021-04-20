using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Client;
using System;
using System.Collections.Generic;

namespace MarkMpn.Sql4Cds.Engine.Tests.Metadata
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

        [AttributeLogicalName("new_optionsetvalue")]
        public New_OptionSet? New_OptionSetValue { get; set; }

        [AttributeLogicalName("new_optionsetvaluename")]
        public string New_OptionSetValueName { get; set; }

        [AttributeLogicalName("new_boolprop")]
        public bool? New_BoolProp { get; set; }

        [AttributeLogicalName("new_optionsetvaluecollection")]
        public OptionSetValueCollection New_OptionSetValueCollection { get; set; }
    }

    enum New_OptionSet
    {
        Value1 = 100001,
        Value2,
        Value3
    }
}
