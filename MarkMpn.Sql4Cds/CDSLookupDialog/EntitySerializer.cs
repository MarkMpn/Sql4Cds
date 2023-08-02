using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Metadata;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Xml;

namespace xrmtb.XrmToolBox.Controls
{
    public class EntitySerializer
    {
        private static string guidtemplate = "FFFFEEEEDDDDCCCCBBBBAAAA99998888";

        public static object AttributeToBaseType(object attribute, bool showFriendlyNames = false)
        {
            if (attribute is AliasedValue) {
                return AttributeToBaseType(((AliasedValue)attribute).Value, showFriendlyNames);
            }
            else if (attribute is EntityReference) {
                if (showFriendlyNames) {
                    return ((EntityReference)attribute).Name;
                }
                else { 
                    return ((EntityReference)attribute).Id;
                }
            }
            else if (attribute is EntityReferenceCollection)
            {
                var referencedEntity = "";
                foreach (var er in (EntityReferenceCollection)attribute)
                {
                    if (referencedEntity == "")
                    {
                        referencedEntity = er.LogicalName;
                    }
                    else if (referencedEntity != er.LogicalName)
                    {
                        referencedEntity = "";
                        break;
                    }
                }
                var result = "";
                foreach (var er in (EntityReferenceCollection)attribute)
                {
                    if (result != "")
                    {
                        result += ",";
                    }
                    if (referencedEntity != "")
                    {
                        result += er.Id.ToString();
                    }
                    else
                    {
                        result += er.LogicalName + ":" + er.Id.ToString();
                    }
                }
                return result;
            }
            else if (attribute is EntityCollection)
            {
                var result = "";
                if (((EntityCollection)attribute).Entities.Count > 0)
                {
                    foreach (var entity in ((EntityCollection)attribute).Entities)
                    {
                        if (result != "")
                        {
                            result += ",";
                        }
                        result += entity.Id.ToString();
                    }
                    result = ((EntityCollection)attribute).EntityName + ":" + result;
                }
                return result;
            }
            else if (attribute is OptionSetValue)
                return ((OptionSetValue)attribute).Value;
            else if (attribute is OptionSetValueCollection)
                return "[" + string.Join(",", ((OptionSetValueCollection)attribute).Select(v => v.Value.ToString())) + "]";
            else if (attribute is Money)
                return ((Money)attribute).Value;
            else if (attribute is BooleanManagedProperty)
                return ((BooleanManagedProperty)attribute).Value;
            else
                return attribute;
        }

        public static string AttributeToString(object attribute, AttributeMetadata meta, string format)
        {
            if (attribute == null)
            {
                return "";
            }
            if (attribute is AliasedValue aliasedValue)
            {
                return AttributeToString(aliasedValue.Value, meta, format);
            }
            else if (attribute is EntityReference entityReference)
            {
                if (!string.IsNullOrEmpty(entityReference.Name))
                {
                    return entityReference.Name;
                }
                return entityReference.Id.ToString();
            }
            else if (attribute is EntityCollection entityCollection && entityCollection.EntityName == "activityparty")
            {
                var result = "";
                if (entityCollection.Entities.Count > 0)
                {
                    foreach (var entity in entityCollection.Entities)
                    {
                        var party = "";
                        if (entity.Contains("partyid") && entity["partyid"] is EntityReference)
                        {
                            party = ((EntityReference)entity["partyid"]).Name;
                        }
                        if (string.IsNullOrEmpty(party) && entity.Contains("addressused"))
                        {
                            party = entity["addressused"].ToString();
                        }
                        if (string.IsNullOrEmpty(party))
                        {
                            party = entity.Id.ToString();
                        }
                        if (!string.IsNullOrEmpty(result))
                        {
                            result += ", ";
                        }
                        result += party;
                    }
                }
                return result;
            }
            else if (attribute is OptionSetValue optionSetValue)
            {
                return GetOptionSetLabel(meta, optionSetValue.Value);
            }
            else if (attribute is OptionSetValueCollection optionSetValues)
            {
                return string.Join("; ", optionSetValues.Select(v => GetOptionSetLabel(meta, v.Value)));
            }
            else if (attribute is Money money)
            {
                return money.Value.ToString();
            }
            else if (attribute is BooleanManagedProperty booleanManagedProperty)
            {
                return booleanManagedProperty.Value.ToString();
            }
            else if (attribute is bool boolValue)
            {
                return (GetBooleanLabel(meta, boolValue));
            }
            return string.Format("{0:" + format + "}", attribute);
        }

        public static string AttributeToString(object attribute, AttributeMetadata meta)
        {
            if (attribute == null)
            {
                return "";
            }
            if (attribute is AliasedValue aliasedValue)
            {
                return AttributeToString(aliasedValue.Value, meta);
            }
            else if (attribute is EntityReference entityReference)
            {
                if (!string.IsNullOrEmpty(entityReference.Name))
                {
                    return entityReference.Name;
                }
                return entityReference.Id.ToString();
            }
            else if (attribute is EntityCollection entityCollection && entityCollection.EntityName == "activityparty")
            {
                var result = "";
                if (entityCollection.Entities.Count > 0)
                {
                    foreach (var entity in entityCollection.Entities)
                    {
                        var party = "";
                        if (entity.Contains("partyid") && entity["partyid"] is EntityReference)
                        {
                            party = ((EntityReference)entity["partyid"]).Name;
                        }
                        if (string.IsNullOrEmpty(party) && entity.Contains("addressused"))
                        {
                            party = entity["addressused"].ToString();
                        }
                        if (string.IsNullOrEmpty(party))
                        {
                            party = entity.Id.ToString();
                        }
                        if (!string.IsNullOrEmpty(result))
                        {
                            result += ", ";
                        }
                        result += party;
                    }
                }
                return result;
            }
            else if (attribute is OptionSetValue optionSetValue)
            {
                return GetOptionSetLabel(meta, optionSetValue.Value);
            }
            else if (attribute is OptionSetValueCollection optionSetValues)
            {
                return string.Join("; ", optionSetValues.Select(v => GetOptionSetLabel(meta, v.Value)));
            }
            else if (attribute is Money money)
            {
                return money.Value.ToString();
            }
            else if (attribute is BooleanManagedProperty booleanManagedProperty)
            {
                return booleanManagedProperty.Value.ToString();
            }
            else if (attribute is bool boolValue)
            {
                return (GetBooleanLabel(meta, boolValue));
            }
            return attribute.ToString();
        }

        private static string GetOptionSetLabel(AttributeMetadata meta, int value)
        {
            if (meta != null && meta is EnumAttributeMetadata)
            {
                foreach (var osv in ((EnumAttributeMetadata)meta).OptionSet.Options)
                {
                    if (osv.Value == value)
                    {
                        return osv.Label.UserLocalizedLabel.Label;
                    }
                }
            }
            return value.ToString();
        }

        private static string GetBooleanLabel(AttributeMetadata meta, bool value)
        {
            if (meta is BooleanAttributeMetadata bmeta)
            {
                if (value)
                {
                    return bmeta.OptionSet.TrueOption.Label.UserLocalizedLabel.Label;
                }
                else
                {
                    return bmeta.OptionSet.FalseOption.Label.UserLocalizedLabel.Label;
                }
            }
            return value.ToString();
        }
    }
}
