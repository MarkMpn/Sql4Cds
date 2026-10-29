using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Xrm.Sdk.Metadata;

namespace MarkMpn.Sql4Cds.LanguageServer.Pptb
{
    internal static class WebApiMetadataExtensions
    {
        public static AttributeMetadata GetAttribute(this EntityMetadata entity, string logicalName)
        {
            return entity.Attributes?.FirstOrDefault(a => String.Equals(a.LogicalName, logicalName, StringComparison.OrdinalIgnoreCase));
        }

        public static bool IsLookup(this AttributeMetadata attribute)
        {
            return attribute?.AttributeType == AttributeTypeCode.Lookup ||
                attribute?.AttributeType == AttributeTypeCode.Customer ||
                attribute?.AttributeType == AttributeTypeCode.Owner;
        }

        public static IEnumerable<string> GetNavigationProperties(this EntityMetadata entity, string attribute)
        {
            return (entity.ManyToOneRelationships ?? Array.Empty<OneToManyRelationshipMetadata>())
                .Where(r => String.Equals(r.ReferencingAttribute, attribute, StringComparison.OrdinalIgnoreCase))
                .Select(r => r.ReferencingEntityNavigationPropertyName)
                .Where(n => n != null)
                .Distinct(StringComparer.OrdinalIgnoreCase);
        }

        public static string GetNavigationProperty(this EntityMetadata entity, string attribute, string targetEntity)
        {
            var relationship = entity.ManyToOneRelationships?.FirstOrDefault(r =>
                String.Equals(r.ReferencingAttribute, attribute, StringComparison.OrdinalIgnoreCase) &&
                String.Equals(r.ReferencedEntity, targetEntity, StringComparison.OrdinalIgnoreCase) &&
                r.ReferencingEntityNavigationPropertyName != null);
            if (relationship != null)
                return relationship.ReferencingEntityNavigationPropertyName;

            var properties = entity.GetNavigationProperties(attribute).ToArray();
            return properties.Length == 1 ? properties[0] : null;
        }
    }
}
