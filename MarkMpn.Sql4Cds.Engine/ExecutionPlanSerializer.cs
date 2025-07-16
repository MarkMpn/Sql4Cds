using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Text;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using Newtonsoft.Json;

namespace MarkMpn.Sql4Cds.Engine
{
    public static class ExecutionPlanSerializer
    {
        class NullableConverter : JsonConverter
        {
            public override bool CanConvert(Type objectType)
            {
                return typeof(INullable).IsAssignableFrom(objectType);
            }

            public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
            {
                if (reader.TokenType == JsonToken.Null)
                    return objectType.GetField("Null").GetValue(null);

                var rawType = objectType.GetProperty("Value").PropertyType;
                var rawValue = serializer.Deserialize(reader, rawType);
                var conversionMethod = objectType.GetMethod("op_Implicit", new[] { rawType });
                return conversionMethod.Invoke(null, new[] { rawValue });
            }

            public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
            {
                var nullable = (INullable)value;

                if (nullable.IsNull)
                    writer.WriteNull();
                else
                    serializer.Serialize(writer, nullable.GetType().GetProperty("Value").GetValue(value));
            }
        }

        public static string Serialize(IRootExecutionPlanNode plan)
        {
            var settings = new JsonSerializerSettings
            {
                TypeNameHandling = TypeNameHandling.Auto,
                Converters = new List<JsonConverter> { new NullableConverter() }
            };
            return JsonConvert.SerializeObject(plan, typeof(IRootExecutionPlanNode), settings);
        }

        public static IRootExecutionPlanNode Deserialize(string plan)
        {
            var settings = new JsonSerializerSettings
            {
                TypeNameHandling = TypeNameHandling.Auto,
                Converters = new List<JsonConverter> { new NullableConverter() }
            };
            return JsonConvert.DeserializeObject<IRootExecutionPlanNode>(plan, settings);
        }
    }
}
