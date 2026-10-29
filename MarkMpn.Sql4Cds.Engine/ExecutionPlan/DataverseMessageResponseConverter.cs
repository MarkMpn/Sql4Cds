using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Xrm.Sdk;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// Custom JSON converters for Dataverse message response types
    /// </summary>
    public static class DataverseMessageResponseConverters
    {
        /// <summary>
        /// Gets the set of converters for serializing Dataverse types
        /// </summary>
        public static IList<JsonConverter> GetConverters()
        {
            return new JsonConverter[]
            {
                new OptionSetValueConverter(),
                new MoneyConverter(),
                new EntityReferenceConverter(),
                new EntityConverter(),
                new EntityCollectionConverter()
            };
        }

        private class OptionSetValueConverter : JsonConverter<OptionSetValue>
        {
            public override OptionSetValue ReadJson(JsonReader reader, Type objectType, OptionSetValue existingValue, bool hasExistingValue, JsonSerializer serializer)
            {
                if (reader.TokenType == JsonToken.Null)
                    return null;

                return new OptionSetValue((int)serializer.Deserialize(reader, typeof(int)));
            }

            public override void WriteJson(JsonWriter writer, OptionSetValue value, JsonSerializer serializer)
            {
                if (value == null)
                {
                    writer.WriteNull();
                    return;
                }

                writer.WriteValue(value.Value);
            }
        }

        private class MoneyConverter : JsonConverter<Money>
        {
            public override Money ReadJson(JsonReader reader, Type objectType, Money existingValue, bool hasExistingValue, JsonSerializer serializer)
            {
                if (reader.TokenType == JsonToken.Null)
                    return null;

                return new Money((decimal)serializer.Deserialize(reader, typeof(decimal)));
            }

            public override void WriteJson(JsonWriter writer, Money value, JsonSerializer serializer)
            {
                if (value == null)
                {
                    writer.WriteNull();
                    return;
                }

                writer.WriteValue(value.Value);
            }
        }

        private class EntityReferenceConverter : JsonConverter<EntityReference>
        {
            public override EntityReference ReadJson(JsonReader reader, Type objectType, EntityReference existingValue, bool hasExistingValue, JsonSerializer serializer)
            {
                if (reader.TokenType == JsonToken.Null)
                    return null;

                var obj = JObject.Load(reader);
                var er = new EntityReference();

                if (obj.TryGetValue("@odata.id", StringComparison.OrdinalIgnoreCase, out var idToken))
                    er.Id = Guid.Parse(idToken.Value<string>());

                if (obj.TryGetValue("@odata.type", StringComparison.OrdinalIgnoreCase, out var typeToken))
                    er.LogicalName = typeToken.Value<string>();

                if (obj.TryGetValue("name", StringComparison.OrdinalIgnoreCase, out var nameToken))
                    er.Name = nameToken.Value<string>();

                return er;
            }

            public override void WriteJson(JsonWriter writer, EntityReference value, JsonSerializer serializer)
            {
                if (value == null)
                {
                    writer.WriteNull();
                    return;
                }

                writer.WriteStartObject();

                if (value.Id != Guid.Empty)
                {
                    writer.WritePropertyName("@odata.id");
                    writer.WriteValue(value.Id.ToString());
                }

                if (!string.IsNullOrEmpty(value.LogicalName))
                {
                    writer.WritePropertyName("@odata.type");
                    writer.WriteValue(value.LogicalName);
                }

                if (!string.IsNullOrEmpty(value.Name))
                {
                    writer.WritePropertyName("name");
                    writer.WriteValue(value.Name);
                }

                writer.WriteEndObject();
            }
        }

        private class EntityConverter : JsonConverter<Entity>
        {
            public override Entity ReadJson(JsonReader reader, Type objectType, Entity existingValue, bool hasExistingValue, JsonSerializer serializer)
            {
                if (reader.TokenType == JsonToken.Null)
                    return null;

                var obj = JObject.Load(reader);
                var entity = new Entity();

                // Extract the type and ID from known fields
                if (obj.TryGetValue("@odata.type", StringComparison.OrdinalIgnoreCase, out var typeToken))
                    entity.LogicalName = typeToken.Value<string>();

                if (obj.TryGetValue("@odata.id", StringComparison.OrdinalIgnoreCase, out var idToken))
                    entity.Id = Guid.Parse(idToken.Value<string>());

                // Process all other attributes
                foreach (var prop in obj.Properties())
                {
                    if (prop.Name.StartsWith("@odata.", StringComparison.OrdinalIgnoreCase) || prop.Name.EndsWith("@odata.type", StringComparison.OrdinalIgnoreCase))
                        continue;

                    // Skip virtual attributes that are derived from other attributes
                    if ((prop.Name.EndsWith("name", StringComparison.OrdinalIgnoreCase) || prop.Name.EndsWith("type", StringComparison.OrdinalIgnoreCase)) &&
                        obj.ContainsKey(prop.Name.Substring(0, prop.Name.Length - (prop.Name.EndsWith("type", StringComparison.OrdinalIgnoreCase) ? 4 : 4)) + "@odata.type"))
                        continue;

                    var typeAnnotationKey = prop.Name + "@odata.type";
                    if (obj.TryGetValue(typeAnnotationKey, StringComparison.OrdinalIgnoreCase, out var typeAnnotation))
                    {
                        var typeName = typeAnnotation.Value<string>();
                        entity[prop.Name] = DeserializeTypedValue(prop.Value, typeName, serializer);
                    }
                    else
                    {
                        entity[prop.Name] = prop.Value.ToObject(typeof(object), serializer);
                    }
                }

                return entity;
            }

            public override void WriteJson(JsonWriter writer, Entity value, JsonSerializer serializer)
            {
                if (value == null)
                {
                    writer.WriteNull();
                    return;
                }

                writer.WriteStartObject();

                if (!string.IsNullOrEmpty(value.LogicalName))
                {
                    writer.WritePropertyName("@odata.type");
                    writer.WriteValue(value.LogicalName);
                }

                if (value.Id != Guid.Empty)
                {
                    writer.WritePropertyName("@odata.id");
                    writer.WriteValue(value.Id.ToString());
                }

                if (value.Attributes != null)
                {
                    foreach (var attribute in value.Attributes)
                    {
                        writer.WritePropertyName(attribute.Key);
                        SerializeAttribute(writer, attribute.Value, serializer);

                        // Add type annotation for types that need special deserialization
                        if (attribute.Value != null && !(attribute.Value is string) && !(attribute.Value is bool))
                        {
                            writer.WritePropertyName(attribute.Key + "@odata.type");
                            writer.WriteValue(attribute.Value.GetType().Name);
                        }
                    }
                }

                writer.WriteEndObject();
            }

            private void SerializeAttribute(JsonWriter writer, object value, JsonSerializer serializer)
            {
                if (value == null)
                {
                    writer.WriteNull();
                }
                else if (value is OptionSetValue osv)
                {
                    writer.WriteValue(osv.Value);
                }
                else if (value is Money money)
                {
                    writer.WriteValue(money.Value);
                }
                else if (value is EntityReference er)
                {
                    serializer.Serialize(writer, er);
                }
                else if (value is Entity nestedEntity)
                {
                    serializer.Serialize(writer, nestedEntity);
                }
                else if (value is EntityCollection nestedCollection)
                {
                    serializer.Serialize(writer, nestedCollection);
                }
                else
                {
                    serializer.Serialize(writer, value);
                }
            }

            private object DeserializeTypedValue(JToken token, string typeName, JsonSerializer serializer)
            {
                switch (typeName)
                {
                    case "Int32": return token.Value<int>();
                    case "Int64": return token.Value<long>();
                    case "Guid": return Guid.Parse(token.Value<string>());
                    case "DateTime": return token.Type == JTokenType.Date ? token.Value<DateTime>() :
                        DateTime.Parse(token.Value<string>(), System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.RoundtripKind);
                    case "Decimal": return token.Value<decimal>();
                    case "Double": return token.Value<double>();
                    case "Single": return token.Value<float>();
                    case "Byte[]": return Convert.FromBase64String(token.Value<string>());
                    case "OptionSetValue": return new OptionSetValue((int)token);
                    case "Money": return new Money((decimal)token);
                    case "EntityReference": return token.ToObject<EntityReference>(serializer);
                    case "Entity": return token.ToObject<Entity>(serializer);
                    case "EntityCollection": return token.ToObject<EntityCollection>(serializer);
                    default: return token.ToObject(typeof(object), serializer);
                }
            }
        }

        private class EntityCollectionConverter : JsonConverter<EntityCollection>
        {
            public override EntityCollection ReadJson(JsonReader reader, Type objectType, EntityCollection existingValue, bool hasExistingValue, JsonSerializer serializer)
            {
                if (reader.TokenType == JsonToken.Null)
                    return null;

                var obj = JObject.Load(reader);
                var collection = new EntityCollection();

                if (obj.TryGetValue("entityName", StringComparison.OrdinalIgnoreCase, out var entityNameToken))
                    collection.EntityName = entityNameToken.Value<string>();

                if (obj.TryGetValue("entities", StringComparison.OrdinalIgnoreCase, out var entitiesToken) && entitiesToken.Type == JTokenType.Array)
                {
                    foreach (var entityToken in entitiesToken)
                        collection.Entities.Add(entityToken.ToObject<Entity>(serializer));
                }

                if (obj.TryGetValue("moreRecords", StringComparison.OrdinalIgnoreCase, out var moreRecordsToken))
                    collection.MoreRecords = moreRecordsToken.Value<bool>();

                if (obj.TryGetValue("pagingCookie", StringComparison.OrdinalIgnoreCase, out var pagingCookieToken))
                    collection.PagingCookie = pagingCookieToken.Value<string>();

                if (obj.TryGetValue("totalRecordCount", StringComparison.OrdinalIgnoreCase, out var totalRecordCountToken))
                    collection.TotalRecordCount = totalRecordCountToken.Value<int>();

                return collection;
            }

            public override void WriteJson(JsonWriter writer, EntityCollection value, JsonSerializer serializer)
            {
                if (value == null)
                {
                    writer.WriteNull();
                    return;
                }

                writer.WriteStartObject();

                if (!string.IsNullOrEmpty(value.EntityName))
                {
                    writer.WritePropertyName("entityName");
                    writer.WriteValue(value.EntityName);
                }

                writer.WritePropertyName("entities");
                writer.WriteStartArray();

                foreach (var entity in value.Entities)
                    serializer.Serialize(writer, entity);

                writer.WriteEndArray();

                if (value.MoreRecords)
                {
                    writer.WritePropertyName("moreRecords");
                    writer.WriteValue(true);
                }

                if (!string.IsNullOrEmpty(value.PagingCookie))
                {
                    writer.WritePropertyName("pagingCookie");
                    writer.WriteValue(value.PagingCookie);
                }

                if (value.TotalRecordCount > 0)
                {
                    writer.WritePropertyName("totalRecordCount");
                    writer.WriteValue(value.TotalRecordCount);
                }

                writer.WriteEndObject();
            }
        }
    }
}