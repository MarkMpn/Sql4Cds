using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
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
        }

        private static EntityMetadata GetEntityMetadata(Type type)
        {
            var metadata = new EntityMetadata();
            metadata.LogicalName = type.Name.ToLower();
            
            var attributes = type.GetProperties()
                .Select(prop => GetAttributeMetadata(prop))
                .Where(attr => attr != null)
                .ToArray();

            SetSealedProperty(metadata, nameof(metadata.Attributes), attributes);

            return metadata;
        }

        private static AttributeMetadata GetAttributeMetadata(PropertyInfo prop)
        {
            if (!prop.CanRead)
                return null;

            var type = prop.PropertyType;

            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>))
                type = type.GetGenericArguments()[0];

            if (type == typeof(string) || type == typeof(Microsoft.Xrm.Sdk.Label))
                return new StringAttributeMetadata(prop.Name) { LogicalName = prop.Name.ToLower() };

            // TODO: Add support for more property types

            return null;
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
