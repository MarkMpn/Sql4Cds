using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using Microsoft.Xrm.Sdk;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace MarkMpn.Sql4Cds.LanguageServer.Pptb
{
    /// <summary>
    /// Helper methods for working with Dataverse Web API JSON
    /// </summary>
    public static class WebApiJson
    {
        public const string FormattedValueAnnotation = "@OData.Community.Display.V1.FormattedValue";
        public const string LookupLogicalNameAnnotation = "@Microsoft.Dynamics.CRM.lookuplogicalname";
        public const string IncludeAnnotationsPreference = "odata.include-annotations=\"*\"";

        /// <summary>
        /// Parses JSON text without any automatic date conversion and preserving decimal precision
        /// </summary>
        public static JToken Parse(string json)
        {
            if (String.IsNullOrEmpty(json))
                return null;

            using (var reader = new JsonTextReader(new StringReader(json)))
            {
                reader.DateParseHandling = DateParseHandling.None;
                reader.FloatParseHandling = FloatParseHandling.Decimal;
                return JToken.ReadFrom(reader);
            }
        }

        /// <summary>
        /// Serializes a JSON token to compact text
        /// </summary>
        public static string Serialize(JToken token)
        {
            return token.ToString(Formatting.None);
        }

        /// <summary>
        /// Formats a value as an OData literal suitable for use in a URL key or function parameter
        /// </summary>
        public static string FormatLiteral(object value)
        {
            switch (value)
            {
                case null:
                    return "null";

                case string s:
                    return "'" + s.Replace("'", "''") + "'";

                case Guid g:
                    return g.ToString("D");

                case bool b:
                    return b ? "true" : "false";

                case DateTime dt:
                    return FormatDateTime(dt, false);

                case OptionSetValue osv:
                    return osv.Value.ToString(CultureInfo.InvariantCulture);

                case Money m:
                    return m.Value.ToString(CultureInfo.InvariantCulture);

                case EntityReference er:
                    return er.Id.ToString("D");

                case Enum e:
                    return Convert.ToInt32(e, CultureInfo.InvariantCulture).ToString(CultureInfo.InvariantCulture);

                case IFormattable f:
                    return f.ToString(null, CultureInfo.InvariantCulture);

                default:
                    return Serialize(JToken.FromObject(value));
            }
        }

        /// <summary>
        /// Formats a DateTime value in the ISO format expected by the Web API
        /// </summary>
        public static string FormatDateTime(DateTime dt, bool dateOnly)
        {
            if (dateOnly)
                return dt.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);

            if (dt.Kind == DateTimeKind.Local)
                dt = dt.ToUniversalTime();

            return dt.ToString("yyyy-MM-dd'T'HH:mm:ss.fff'Z'", CultureInfo.InvariantCulture);
        }

        /// <summary>
        /// Parses a date/time value returned by the Web API
        /// </summary>
        public static DateTime ParseDateTime(JToken token)
        {
            if (token.Type == JTokenType.Date)
            {
                var dt = token.Value<DateTime>();

                if (dt.Kind == DateTimeKind.Local)
                    dt = dt.ToUniversalTime();
                else if (dt.Kind == DateTimeKind.Unspecified)
                    dt = DateTime.SpecifyKind(dt, DateTimeKind.Utc);

                return dt;
            }

            return DateTime.Parse((string)token, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal);
        }

        /// <summary>
        /// Extracts the ID from an OData entity ID URL, e.g. <c>https://org.crm.dynamics.com/api/data/v9.2/accounts(00000000-0000-0000-0000-000000000000)</c>
        /// </summary>
        public static Guid? ParseEntityId(string entityIdUrl)
        {
            if (String.IsNullOrEmpty(entityIdUrl))
                return null;

            var end = entityIdUrl.LastIndexOf(')');
            var start = end < 0 ? -1 : entityIdUrl.LastIndexOf('(', end);

            if (start < 0)
                return null;

            if (Guid.TryParse(entityIdUrl.Substring(start + 1, end - start - 1), out var id))
                return id;

            return null;
        }

        /// <summary>
        /// Converts a JSON value to the most appropriate .NET type when we have no metadata available
        /// to give a more definite type.
        /// </summary>
        public static object ToClrValue(JToken token)
        {
            if (token == null)
                return null;

            switch (token.Type)
            {
                case JTokenType.Null:
                case JTokenType.Undefined:
                    return null;

                case JTokenType.Boolean:
                    return token.Value<bool>();

                case JTokenType.Integer:
                    var l = token.Value<long>();
                    if (l >= Int32.MinValue && l <= Int32.MaxValue)
                        return (int)l;
                    return l;

                case JTokenType.Float:
                    return token.Value<decimal>();

                case JTokenType.Date:
                    return ParseDateTime(token);

                case JTokenType.Guid:
                    return token.Value<Guid>();

                case JTokenType.String:
                    var s = (string)token;

                    if (s.Length == 36 && Guid.TryParse(s, out var guid))
                        return guid;

                    if (s.Length >= 20 && s[4] == '-' && s[7] == '-' && s[10] == 'T' && DateTime.TryParse(s, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal, out var dt))
                        return dt;

                    return s;

                case JTokenType.Array:
                    var arr = (JArray)token;

                    if (arr.Count > 0 && arr.All(t => t is JObject))
                        return new EntityCollection(arr.Cast<JObject>().Select(ToEntity).ToList());

                    var values = arr.Select(ToClrValue).ToList();

                    if (values.Count == 0)
                        return Array.Empty<string>();

                    var elementType = values[0]?.GetType() ?? typeof(object);

                    if (values.Any(v => v == null || v.GetType() != elementType))
                        elementType = typeof(object);

                    var typed = Array.CreateInstance(elementType, values.Count);

                    for (var i = 0; i < values.Count; i++)
                        typed.SetValue(values[i], i);

                    return typed;

                case JTokenType.Object:
                    return ToEntity((JObject)token);

                default:
                    return token.ToString();
            }
        }

        /// <summary>
        /// Converts a JSON object to an <see cref="Entity"/> using only the information available in the JSON
        /// </summary>
        public static Entity ToEntity(JObject obj)
        {
            var entity = new Entity();
            var type = obj.Value<string>("@odata.type");

            if (type != null)
                entity.LogicalName = type.Split('.').Last();

            foreach (var prop in obj.Properties())
            {
                if (prop.Name.Contains('@'))
                    continue;

                var key = prop.Name;
                object value;

                if (obj[key + LookupLogicalNameAnnotation] is JValue lookupType && prop.Value.Type != JTokenType.Null)
                {
                    if (key.StartsWith("_") && key.EndsWith("_value"))
                        key = key.Substring(1, key.Length - 7);

                    value = new EntityReference((string)lookupType, Guid.Parse((string)prop.Value))
                    {
                        Name = obj.Value<string>(prop.Name + FormattedValueAnnotation)
                    };
                }
                else
                {
                    value = ToClrValue(prop.Value);
                }

                entity[key] = value;

                if (obj[prop.Name + FormattedValueAnnotation] is JValue formatted && !(value is EntityReference))
                    entity.FormattedValues[key] = (string)formatted;
            }

            return entity;
        }
    }
}
