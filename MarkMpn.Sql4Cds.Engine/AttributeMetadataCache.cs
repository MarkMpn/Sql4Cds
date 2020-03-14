using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Metadata;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace MarkMpn.Sql4Cds.Engine
{
    /// <summary>
    /// A default implementation of <see cref="IAttributeMetadataCache"/>
    /// </summary>
    public class AttributeMetadataCache : IAttributeMetadataCache
    {
        private readonly IOrganizationService _org;
        private readonly IDictionary<string, EntityMetadata> _metadata;
        private readonly ISet<string> _loading;

        /// <summary>
        /// Creates a new <see cref="AttributeMetadataCache"/>
        /// </summary>
        /// <param name="org">The <see cref="IOrganizationService"/> to retrieve the metadata from</param>
        public AttributeMetadataCache(IOrganizationService org)
        {
            _org = org;
            _metadata = new Dictionary<string, EntityMetadata>();
            _loading = new HashSet<string>();
        }

        /// <inheritdoc cref="IAttributeMetadataCache.this{string}"/>
        public EntityMetadata this[string name]
        {
            get
            {
                if (_metadata.TryGetValue(name, out var value))
                    return value;

                var metadata = (RetrieveEntityResponse)_org.Execute(new RetrieveEntityRequest
                {
                    LogicalName = name,
                    EntityFilters = EntityFilters.Attributes | EntityFilters.Relationships
                });

                _metadata[name] = metadata.EntityMetadata;
                return metadata.EntityMetadata;
            }
        }

        public bool TryGetValue(string logicalName, out EntityMetadata metadata)
        {
            if (_metadata.TryGetValue(logicalName, out metadata))
                return true;

            if (_loading.Add(logicalName))
                Task.Run(() => this[logicalName]);

            return false;
        }
    }
}
