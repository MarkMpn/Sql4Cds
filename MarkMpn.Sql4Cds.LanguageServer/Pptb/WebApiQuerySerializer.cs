using System;
using System.Collections;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Runtime.Serialization;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace MarkMpn.Sql4Cds.LanguageServer.Pptb
{
    internal static class WebApiQuerySerializer
    {
        public static JToken Serialize(object value)
        {
            if (value == null)
                return JValue.CreateNull();
            if (value is Enum)
                return value.ToString();
            if (value is IEnumerable collection && !(value is string))
                return new JArray(collection.Cast<object>().Select(Serialize));
            if (value.GetType().Namespace != typeof(QueryExpression).Namespace)
                return JToken.FromObject(value);

            var result = new JObject { ["@odata.type"] = "Microsoft.Dynamics.CRM." + value.GetType().Name };
            foreach (var property in value.GetType().GetProperties(BindingFlags.Instance | BindingFlags.Public))
            {
                if (!property.CanRead || property.GetIndexParameters().Length != 0 ||
                    property.GetCustomAttribute<DataMemberAttribute>() == null)
                    continue;

                var propertyValue = property.GetValue(value);
                if (propertyValue == null)
                    continue;
                result[property.Name] = value is ConditionExpression && property.Name == nameof(ConditionExpression.Values)
                    ? new JArray(((ConditionExpression)value).Values.Select(SerializeConditionValue))
                    : Serialize(propertyValue);
            }
            return result;
        }

        private static JObject SerializeConditionValue(object value)
        {
            if (value is OptionSetValue option)
                value = option.Value;
            else if (value is Money money)
                value = money.Value;
            else if (value is EntityReference reference)
                value = reference.Id;

            if (value == null)
                throw new NotSupportedException("Null query condition values are not supported; use the Null condition operator");

            var type = value.GetType();
            if (type != typeof(string) && type != typeof(Guid) && type != typeof(DateTime) &&
                type != typeof(bool) && type != typeof(int) && type != typeof(long) &&
                type != typeof(decimal) && type != typeof(double) && type != typeof(float))
                throw new NotSupportedException($"Unsupported query condition value type {type.FullName}");

            return new JObject
            {
                ["Type"] = type.FullName,
                ["Value"] = value is DateTime date ? date.ToString("O", CultureInfo.InvariantCulture) :
                    Convert.ToString(value, CultureInfo.InvariantCulture)
            };
        }

        public static QueryExpression Deserialize(JToken token)
        {
            if (!(token is JObject json) || String.IsNullOrWhiteSpace(json.Value<string>("EntityName")))
                throw new InvalidOperationException("Dataverse did not return a QueryExpression");
            return token.ToObject<QueryExpression>(JsonSerializer.Create(new JsonSerializerSettings
            {
                Converters = { new ConditionConverter() }
            }));
        }

        private sealed class ConditionConverter : JsonConverter
        {
            public override bool CanConvert(Type objectType) => objectType == typeof(ConditionExpression);
            public override bool CanWrite => false;
            public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer) => throw new NotSupportedException();

            public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
            {
                var json = JObject.Load(reader);
                var valuesToken = json["Values"];
                if (valuesToken != null && valuesToken.Type != JTokenType.Null && !(valuesToken is JArray))
                    throw new InvalidOperationException("Dataverse returned invalid query condition values");
                var values = valuesToken as JArray;
                json.Remove("Values");
                var condition = new ConditionExpression();
                using (var objectReader = json.CreateReader())
                    serializer.Populate(objectReader, condition);
                if (values != null)
                {
                    foreach (var value in values)
                    {
                        if (!(value is JObject) || value["Value"]?.Type != JTokenType.String)
                            throw new InvalidOperationException("Dataverse returned an invalid query condition value");
                        var text = value.Value<string>("Value");
                        switch (value.Value<string>("Type"))
                        {
                            case "System.String": condition.Values.Add(text); break;
                            case "System.Guid": condition.Values.Add(Guid.Parse(text)); break;
                            case "System.DateTime": condition.Values.Add(DateTime.Parse(text, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind)); break;
                            case "System.Boolean": condition.Values.Add(Boolean.Parse(text)); break;
                            case "System.Int32": condition.Values.Add(Int32.Parse(text, CultureInfo.InvariantCulture)); break;
                            case "System.Int64": condition.Values.Add(Int64.Parse(text, CultureInfo.InvariantCulture)); break;
                            case "System.Decimal": condition.Values.Add(Decimal.Parse(text, CultureInfo.InvariantCulture)); break;
                            case "System.Double": condition.Values.Add(Double.Parse(text, CultureInfo.InvariantCulture)); break;
                            case "System.Single": condition.Values.Add(Single.Parse(text, CultureInfo.InvariantCulture)); break;
                            default: throw new NotSupportedException($"Unsupported query condition value type {value.Value<string>("Type")}");
                        }
                    }
                }
                return condition;
            }
        }
    }
}
