using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Metadata;

namespace MarkMpn.Sql4Cds.Engine
{
    /// <summary>
    /// Custom <see cref="IAttributeMetadataCache"/> wrapper to inject details of metadata classes that can be queried
    /// </summary>
    class MetadataCache : IAttributeMetadataCache
    {
        private readonly IAttributeMetadataCache _inner;
        private static IDictionary<string, EntityMetadata> _customMetadata;

        static MetadataCache()
        {
            _customMetadata = new Dictionary<string, EntityMetadata>();

            _customMetadata["globaloptionset"] = GetEntityMetadata(typeof(OptionSetMetadataBase));
            _customMetadata["localizedlabel"] = GetEntityMetadata(typeof(LocalizedLabel));
        }

        private static EntityMetadata GetEntityMetadata(Type type)
        {
            var metadata = new EntityMetadata();
            metadata.LogicalName = type.Name.ToLower();
            
            var attributes = type.GetProperties()
                .SelectMany(prop => GetAttributeMetadata(prop))
                .Where(attr => attr != null)
                .ToList();

            if (type == typeof(LocalizedLabel))
                attributes.Add(new UniqueIdentifierAttributeMetadata("LabelId") { LogicalName = "labelid" });

            SetSealedProperty(metadata, nameof(metadata.Attributes), attributes.ToArray());

            return metadata;
        }

        private static IEnumerable<AttributeMetadata> GetAttributeMetadata(PropertyInfo prop)
        {
            if (!prop.CanRead)
                yield break;

            var type = prop.PropertyType;

            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>))
                type = type.GetGenericArguments()[0];

            if (type == typeof(string) || type == typeof(Microsoft.Xrm.Sdk.Label))
                yield return new StringAttributeMetadata(prop.Name) { LogicalName = prop.Name.ToLower() };

            if (type == typeof(Label))
                yield return new UniqueIdentifierAttributeMetadata(prop.Name + "Id") { LogicalName = prop.Name.ToLower() + "id" };

            // TODO: Add support for more property types
        }

        private static void SetSealedProperty(object target, string prop, object value)
        {
            target.GetType().GetProperty(prop).SetValue(target, value, null);
        }

        /// <summary>
        /// Creates a new <see cref="MetadataCache"/>
        /// </summary>
        /// <param name="inner">The <see cref="IAttributeMetadataCache"/> that provides the metadata for the standard data entities</param>
        public MetadataCache(IAttributeMetadataCache inner)
        {
            _inner = inner;
        }

        /// <inheritdoc/>
        public EntityMetadata this[string name]
        {
            get
            {
                if (_customMetadata.TryGetValue(name, out var metadata))
                    return metadata;

                return _inner[name];
            }
        }

        /// <inheritdoc/>
        public bool TryGetValue(string logicalName, out EntityMetadata metadata)
        {
            if (_customMetadata.TryGetValue(logicalName, out metadata))
                return true;

            return _inner.TryGetValue(logicalName, out metadata);
        }
    }
}
