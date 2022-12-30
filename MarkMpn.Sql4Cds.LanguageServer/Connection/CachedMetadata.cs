using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Xml.Linq;
using MarkMpn.Sql4Cds.Engine;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Metadata;

namespace MarkMpn.Sql4Cds.LanguageServer.Connection
{
    internal class CachedMetadata : IAttributeMetadataCache
    {
        private readonly AttributeMetadataCache _defaultCache;
        private Dictionary<string, EntityMetadata> _persistentCacheByName;
        private Dictionary<int, EntityMetadata> _persistentCacheByOtc;

        public CachedMetadata(IOrganizationService org, PersistentMetadataCache persistentMetadataCache)
        {
            _defaultCache = new AttributeMetadataCache(org);
            _ = persistentMetadataCache.GetMetadataAsync(org, false)
                .ContinueWith(loader =>
                {
                    if (loader.IsCanceled || loader.IsFaulted)
                        return;

                    _persistentCacheByName = loader.Result
                        .ToDictionary(e => e.LogicalName);
                    _persistentCacheByOtc = loader.Result
                        .Where(e => e.ObjectTypeCode != null)
                        .ToDictionary(e => e.ObjectTypeCode.Value);
                }, TaskScheduler.Default);
        }

        public EntityMetadata this[string name]
        {
            get
            {
                if (_persistentCacheByName != null)
                    return _persistentCacheByName[name];

                return _defaultCache[name];
            }
        }

        public EntityMetadata this[int otc]
        {
            get
            {
                if (_persistentCacheByOtc != null)
                    return _persistentCacheByOtc[otc];

                return _defaultCache[otc];
            }
        }

        public bool TryGetMinimalData(string logicalName, out EntityMetadata metadata)
        {
            if (_persistentCacheByName != null)
                return _persistentCacheByName.TryGetValue(logicalName, out metadata);

            return _defaultCache.TryGetMinimalData(logicalName, out metadata);
        }

        public bool TryGetValue(string logicalName, out EntityMetadata metadata)
        {
            if (_persistentCacheByName != null)
                return _persistentCacheByName.TryGetValue(logicalName, out metadata);

            return _defaultCache.TryGetValue(logicalName, out metadata);
        }
    }
}