using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using MarkMpn.Sql4Cds.Engine;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Metadata;
using Newtonsoft.Json.Linq;

namespace MarkMpn.Sql4Cds.LanguageServer.Pptb
{
    /// <summary>
    /// Converts SDK <see cref="Entity"/> objects to the JSON format used by the Web API
    /// </summary>
    public class WebApiEntitySerializer
    {
        internal static readonly Dictionary<string, int> PartyListParticipationTypes = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase)
        {
            ["from"] = 1,
            ["to"] = 2,
            ["cc"] = 3,
            ["bcc"] = 4,
            ["requiredattendees"] = 5,
            ["optionalattendees"] = 6,
            ["organizer"] = 7,
            ["regardingobjectid"] = 8,
            ["ownerid"] = 9,
            ["resources"] = 10,
            ["customers"] = 11,
            ["partners"] = 12,
            ["related"] = 13,
        };

        private readonly IAttributeMetadataCache _metadata;

        public WebApiEntitySerializer(IAttributeMetadataCache metadata)
        {
            _metadata = metadata;
        }

        /// <summary>
        /// Gets the relative URL that identifies a specific record
        /// </summary>
        public string GetRecordUrl(string logicalName, Guid id)
        {
            var info = _metadata[logicalName];
            return $"{info.EntitySetName}({id:D})";
        }

        /// <summary>
        /// Gets the relative URL that identifies a specific record
        /// </summary>
        public string GetRecordUrl(EntityReference entityRef)
        {
            var info = _metadata[entityRef.LogicalName];

            if (entityRef.Id != Guid.Empty)
                return $"{info.EntitySetName}({entityRef.Id:D})";

            if (entityRef.KeyAttributes != null && entityRef.KeyAttributes.Count > 0)
                return $"{info.EntitySetName}({FormatKey(info, entityRef.KeyAttributes)})";

            throw new NotSupportedException($"No ID or alternate key specified for {entityRef.LogicalName} record");
        }

        /// <summary>
        /// Gets the relative URL that identifies a specific record
        /// </summary>
        public string GetRecordUrl(Entity entity)
        {
            var info = _metadata[entity.LogicalName];
            var id = entity.Id;

            if (id == Guid.Empty && info.PrimaryIdAttribute != null && entity.Attributes.TryGetValue(info.PrimaryIdAttribute, out var pk) && pk is Guid pkGuid)
                id = pkGuid;

            if (id != Guid.Empty)
                return $"{info.EntitySetName}({id:D})";

            if (entity.KeyAttributes != null && entity.KeyAttributes.Count > 0)
                return $"{info.EntitySetName}({FormatKey(info, entity.KeyAttributes)})";

            throw new NotSupportedException($"No ID or alternate key specified for {entity.LogicalName} record");
        }

        private string FormatKey(EntityMetadata info, KeyAttributeCollection keys)
        {
            return String.Join(",", keys.Select(kvp =>
            {
                var attr = info.GetAttribute(kvp.Key);
                var name = kvp.Key;

                if (attr.IsLookup())
                    name = "_" + name + "_value";

                return name + "=" + WebApiJson.FormatLiteral(kvp.Value);
            }));
        }

        /// <summary>
        /// Converts an entity to the JSON body to use in a create or update request
        /// </summary>
        /// <param name="entity">The entity to convert</param>
        /// <param name="isCreate">Indicates if the entity will be used to create a new record</param>
        /// <param name="includeType">Indicates if the <c>@odata.type</c> annotation should be included</param>
        public JObject Serialize(Entity entity, bool isCreate, bool includeType = false)
        {
            var info = _metadata[entity.LogicalName];
            var json = new JObject();

            if (includeType)
                json["@odata.type"] = "Microsoft.Dynamics.CRM." + info.LogicalName;

            if (isCreate && entity.Id != Guid.Empty && info.PrimaryIdAttribute != null)
                json[info.PrimaryIdAttribute] = entity.Id.ToString("D");

            foreach (var attr in entity.Attributes)
            {
                var attrInfo = info.GetAttribute(attr.Key);

                if (attrInfo?.AttributeOf != null)
                    continue;

                if (!isCreate && attr.Key.Equals(info.PrimaryIdAttribute, StringComparison.OrdinalIgnoreCase))
                    continue;

                if (attr.Value is EntityCollection || attrInfo?.AttributeType == AttributeTypeCode.PartyList)
                {
                    SerializePartyList(json, info, attr.Key, (EntityCollection)attr.Value, isCreate);
                    continue;
                }

                if (attr.Value is EntityReference entityRef || (attr.Value == null && attrInfo.IsLookup()))
                {
                    SerializeLookup(json, info, attr.Key, (EntityReference)attr.Value);
                    continue;
                }

                json[attr.Key] = SerializeValue(attr.Value, attrInfo);
            }

            return json;
        }

        private void SerializeLookup(JObject json, EntityMetadata info, string attribute, EntityReference value)
        {
            if (value == null)
            {
                // We don't know which type the lookup currently points at, so clear all of them
                var navProps = info.GetNavigationProperties(attribute).ToList();

                if (navProps.Count == 0)
                    throw new NotSupportedException($"Cannot find navigation property for {info.LogicalName}.{attribute}");

                foreach (var navProp in navProps)
                    json[navProp + "@odata.bind"] = null;

                return;
            }

            var nav = info.GetNavigationProperty(attribute, value.LogicalName);

            if (nav == null)
                throw new NotSupportedException($"Cannot find navigation property for {info.LogicalName}.{attribute} targeting {value.LogicalName}");

            json[nav + "@odata.bind"] = "/" + GetRecordUrl(value);
        }

        private void SerializePartyList(JObject json, EntityMetadata info, string attribute, EntityCollection parties, bool isCreate)
        {
            if (!PartyListParticipationTypes.TryGetValue(attribute, out var participationType))
                throw new NotSupportedException($"Unknown party list attribute {attribute}");

            var collectionName = info.LogicalName + "_activity_parties";

            if (!(json[collectionName] is JArray array))
            {
                array = new JArray();
                json[collectionName] = array;
            }

            if (parties == null || parties.Entities.Count == 0)
                return;

            var activityParty = _metadata["activityparty"];

            foreach (var party in parties.Entities)
            {
                var partyJson = new JObject
                {
                    ["participationtypemask"] = participationType
                };

                if (!isCreate && party.Id != Guid.Empty)
                    partyJson["activitypartyid"] = party.Id.ToString("D");

                foreach (var partyAttr in party.Attributes)
                {
                    if (partyAttr.Key == "participationtypemask")
                        continue;

                    var partyAttrInfo = activityParty.GetAttribute(partyAttr.Key);

                    if (partyAttr.Value is EntityReference partyRef)
                    {
                        if (partyAttr.Key != "partyid")
                            continue;

                        var nav = activityParty.GetNavigationProperty("partyid", partyRef.LogicalName) ?? ("partyid_" + partyRef.LogicalName);
                        partyJson[nav + "@odata.bind"] = "/" + GetRecordUrl(partyRef);
                    }
                    else if (partyAttrInfo?.AttributeOf == null && partyAttr.Value != null)
                    {
                        partyJson[partyAttr.Key] = SerializeValue(partyAttr.Value, partyAttrInfo);
                    }
                }

                array.Add(partyJson);
            }
        }

        /// <summary>
        /// Converts a simple attribute value to the equivalent JSON value
        /// </summary>
        public static JToken SerializeValue(object value, AttributeMetadata attrInfo)
        {
            switch (value)
            {
                case null:
                    return JValue.CreateNull();

                case OptionSetValue osv:
                    return osv.Value;

                case OptionSetValueCollection osvc:
                    return String.Join(",", osvc.Select(o => o.Value.ToString(CultureInfo.InvariantCulture)));

                case Money money:
                    return money.Value;

                case DateTime dt:
                    return WebApiJson.FormatDateTime(dt, attrInfo is DateTimeAttributeMetadata date && date.Format == DateTimeFormat.DateOnly);

                case Guid guid:
                    return guid.ToString("D");

                case byte[] bytes:
                    return Convert.ToBase64String(bytes);

                case BooleanManagedProperty bmp:
                    return new JObject { ["Value"] = bmp.Value };

                case Enum e:
                    return Convert.ToInt32(e, CultureInfo.InvariantCulture);

                default:
                    return JToken.FromObject(value);
            }
        }
    }
}
