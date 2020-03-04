using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Metadata;
using System.Collections.Generic;

namespace MarkMpn.Sql4Cds.Engine
{
    /// <summary>
    /// A default implementation of <see cref="IAttributeMetadataCache"/>
    /// </summary>
    public class AttributeMetadataCache : IAttributeMetadataCache
    {
        private readonly IOrganizationService _org;
        private readonly IDictionary<string, EntityMetadata> _metadata;

        /// <summary>
        /// Creates a new <see cref="AttributeMetadataCache"/>
        /// </summary>
        /// <param name="org">The <see cref="IOrganizationService"/> to retrieve the metadata from</param>
        public AttributeMetadataCache(IOrganizationService org)
        {
            _org = org;
            _metadata = new Dictionary<string, EntityMetadata>();
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
                    EntityFilters = EntityFilters.Attributes
                });

                _metadata[name] = metadata.EntityMetadata;
                return metadata.EntityMetadata;
            }
        }
    }
}
