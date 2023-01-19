using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk;
using Newtonsoft.Json.Serialization;
using Newtonsoft.Json;

namespace MarkMpn.Sql4Cds.LanguageServer.Connection
{

    /// <summary>
    /// Provides a contract resolver to be used for (de)serializing <see cref="MetadataCache"/> instances using
    /// Json.NET.
    /// </summary>
    /// <remarks>
    /// We need to add in a custom converter to allow (de)serializing some of the metadata types used by D365.
    /// As we can't change the SDK types to add the attributes to control Json.NET directly, we use this custom
    /// <see cref="IContractResolver"/> implementation to add the converters dynamically.
    /// </remarks>
    internal class MetadataCacheContractResolver : DefaultContractResolver
    {
        /// <summary>
        /// A reusable instance of the <see cref="MetadataCacheContractResolver"/>
        /// </summary>
        public static readonly MetadataCacheContractResolver Instance = new MetadataCacheContractResolver();

        protected override JsonContract CreateContract(Type objectType)
        {
            var contract = base.CreateContract(objectType);

            // The default Json.NET converters can't handle the KeyAttributeCollection class, so add in this converter
            // manually. Do this in a reused IContractResolver instance for better performance than adding the converter
            // directly to the serializer as per https://www.newtonsoft.com/json/help/html/Performance.htm
            if (objectType == typeof(KeyAttributeCollection))
                contract.Converter = new KeyAttributeCollectionConverter();

            return contract;
        }

        /// <summary>
        /// Generates all the contracts that can be required to (de)serialize a full object tree
        /// </summary>
        /// <param name="type">The type of the root object</param>
        /// <remarks>
        /// There are a lot of different types involved in the metadata cache and generating the contracts used for
        /// (de)serialization takes around 0.5 seconds on the first run. This method generates and caches the contracts
        /// so they are available when they are first needed.
        /// </remarks>
        public void PreloadContracts(Type type)
        {
            var contracts = new HashSet<Type>();
            PreloadContracts(type, contracts);
        }

        private void PreloadContracts(Type type, HashSet<Type> contracts)
        {
            // Avoid infinite recursion when a type references itself
            if (contracts.Contains(type))
                return;

            // Get the contract for this type. This is then cached in the base implementation.
            ResolveContract(type);
            contracts.Add(type);

            // No need to recurse for simple types
            if (type.IsPrimitive || type == typeof(string) || type == typeof(DateTime))
                return;

            // For arrays, recurse through each possible derived type (e.g. for AttributeMetadata[], generate the contracts
            // for StringAttributeMetadata, LookupAttributeMetadata etc.)
            if (type.IsArray)
            {
                var elementType = type.GetElementType();

                foreach (var derivedType in elementType.Assembly.GetTypes().Where(t => elementType.IsAssignableFrom(t)))
                    PreloadContracts(derivedType, contracts);

                return;
            }

            // Generate the contracts for each property within the type.
            foreach (var prop in type.GetProperties())
                PreloadContracts(prop.PropertyType, contracts);
        }

        /// <summary>
        /// A converter for the KeyAttributeCollection class
        /// </summary>
        /// <remarks>
        /// The <see cref="KeyAttributeCollection"/> class is used in the <see cref="EntityKeyMetadata.AsyncJob"/>
        /// entity reference, and isn't handled by the standard Json.NET converts. Although the collection appears to
        /// always be empty (at least for the current versions of D365), this converter attempts to handle it by converting
        /// the dictionary structure to a JSON object.
        /// </remarks>
        private class KeyAttributeCollectionConverter : JsonConverter<KeyAttributeCollection>
        {
            public override KeyAttributeCollection ReadJson(JsonReader reader, Type objectType, KeyAttributeCollection existingValue, bool hasExistingValue, JsonSerializer serializer)
            {
                if (reader.TokenType == JsonToken.Null)
                {
                    reader.Read();
                    return null;
                }

                if (reader.TokenType != JsonToken.StartObject)
                    throw new FormatException();

                var col = new KeyAttributeCollection();

                while (reader.Read())
                {
                    if (reader.TokenType == JsonToken.EndObject)
                        return col;

                    if (reader.TokenType != JsonToken.PropertyName)
                        throw new FormatException();

                    var key = (string)reader.Value;
                    var value = serializer.Deserialize(reader);

                    col[key] = value;
                }

                throw new FormatException();
            }

            public override void WriteJson(JsonWriter writer, KeyAttributeCollection value, JsonSerializer serializer)
            {
                if (value == null)
                {
                    writer.WriteNull();
                    return;
                }

                writer.WriteStartObject();

                foreach (var kvp in value)
                {
                    writer.WritePropertyName(kvp.Key);
                    serializer.Serialize(writer, kvp.Value);
                }

                writer.WriteEndObject();
            }
        }
    }
}
