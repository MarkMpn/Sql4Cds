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

            foreach (var metadata in MetaMetadata.GetMetadata())
                _customMetadata[metadata.LogicalName] = metadata.GetEntityMetadata();
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
