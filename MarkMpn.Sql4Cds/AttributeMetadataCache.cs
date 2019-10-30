using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Metadata;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MarkMpn.Sql4Cds
{
    public class AttributeMetadataCache
    {
        private readonly IOrganizationService _org;
        private readonly IDictionary<string, EntityMetadata> _metadata;

        public AttributeMetadataCache(IOrganizationService org)
        {
            _org = org;
            _metadata = new Dictionary<string, EntityMetadata>();
        }

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
