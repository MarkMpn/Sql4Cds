using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.Engine;
using McTools.Xrm.Connection;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Metadata;
using Microsoft.Xrm.Sdk.Metadata.Query;

namespace MarkMpn.Sql4Cds.XTB
{
    static class EntityCache
    {
        private static IDictionary<IOrganizationService, EntityMetadata[]> _cache = new Dictionary<IOrganizationService, EntityMetadata[]>();
        private static ISet<IOrganizationService> _loading = new HashSet<IOrganizationService>();

        public static EntityMetadata[] GetEntities(Task<MetadataCache> cacheLoader, IOrganizationService org)
        {
            if (cacheLoader.IsCompleted && cacheLoader.Status == TaskStatus.RanToCompletion && cacheLoader.Result != null)
                return cacheLoader.Result.EntityMetadata;

            if (!_cache.TryGetValue(org, out var entities))
            {
                entities = ((RetrieveMetadataChangesResponse) org.Execute(new RetrieveMetadataChangesRequest
                {
                    Query = new EntityQueryExpression
                    {
                        Properties = new MetadataPropertiesExpression
                        {
                            PropertyNames =
                            {
                                nameof(EntityMetadata.LogicalName),
                                nameof(EntityMetadata.DisplayName),
                                nameof(EntityMetadata.Description)
                            }
                        }
                    }
                })).EntityMetadata
                .ToArray();

                _cache[org] = entities;
            }

            return entities;
        }

        public static bool TryGetEntities(Task<MetadataCache> cacheLoader, IOrganizationService org, out EntityMetadata[] entities)
        {
            entities = cacheLoader.Status == TaskStatus.RanToCompletion ? cacheLoader.Result?.EntityMetadata : null;
            if (entities != null)
                return true;

            if (_cache.TryGetValue(org, out entities))
                return true;

            if (_loading.Add(org))
                Task.Run(() => GetEntities(cacheLoader, org));

            return false;
        }
    }
}
