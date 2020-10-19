using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.Engine;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Metadata;

namespace MarkMpn.Sql4Cds
{
    static class EntityCache
    {
        private static IDictionary<IOrganizationService, EntityMetadata[]> _cache = new Dictionary<IOrganizationService, EntityMetadata[]>();
        private static ISet<IOrganizationService> _loading = new HashSet<IOrganizationService>();

        public static EntityMetadata[] GetEntities(IOrganizationService org)
        {
            if (!_cache.TryGetValue(org, out var entities))
            {
                entities = ((RetrieveAllEntitiesResponse) org.Execute(new RetrieveAllEntitiesRequest
                {
                    EntityFilters = EntityFilters.Entity
                })).EntityMetadata
                .Where(e => !MetaMetadata.GetMetadata().Any(md => e.LogicalName == md.LogicalName))
                .ToArray();

                _cache[org] = entities;
            }

            return entities;
        }

        public static bool TryGetEntities(IOrganizationService org, out EntityMetadata[] entities)
        {
            if (_cache.TryGetValue(org, out entities))
                return true;

            if (_loading.Add(org))
                Task.Run(() => GetEntities(org));

            return false;
        }
    }
}
