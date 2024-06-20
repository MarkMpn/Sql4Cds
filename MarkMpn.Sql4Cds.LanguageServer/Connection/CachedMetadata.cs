using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceModel;
using System.Threading.Tasks;
using System.Xml.Linq;
using MarkMpn.Sql4Cds.Engine;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Metadata;
using Microsoft.Xrm.Sdk.Metadata.Query;

namespace MarkMpn.Sql4Cds.LanguageServer.Connection
{
    internal class CachedMetadata : IAttributeMetadataCache
    {
        private readonly IOrganizationService _org;
        private readonly AttributeMetadataCache _defaultCache;
        private Dictionary<string, EntityMetadata> _persistentCacheByName;
        private Dictionary<int, EntityMetadata> _persistentCacheByOtc;
        private bool _cacheUnavailable;
        private EntityMetadata[] _autocompleteCache;

        public CachedMetadata(IOrganizationService org, PersistentMetadataCache persistentMetadataCache)
        {
            _org = org;
            _defaultCache = new AttributeMetadataCache(org);
            _ = Task.Run(() => persistentMetadataCache.GetMetadataAsync(org, false))
                .ContinueWith(loader =>
                {
                    if (loader.IsCanceled || loader.IsFaulted || loader.Result == null)
                    {
                        _cacheUnavailable = true;
                        return;
                    }

                    _persistentCacheByName = loader.Result
                        .ToDictionary(e => e.LogicalName);
                    _persistentCacheByOtc = loader.Result
                        .Where(e => e.ObjectTypeCode != null)
                        .ToDictionary(e => e.ObjectTypeCode.Value);
                    _autocompleteCache = loader.Result;
                }, TaskScheduler.Default);
        }

        public EntityMetadata this[string name]
        {
            get
            {
                if (_persistentCacheByName != null)
                {
                    if (_persistentCacheByName.TryGetValue(name, out var entity))
                        return entity;

                    throw new FaultException("Unknown entity " + name);
                }

                return _defaultCache[name];
            }
        }

        public EntityMetadata this[int otc]
        {
            get
            {
                if (_persistentCacheByOtc != null)
                {
                    if (_persistentCacheByOtc.TryGetValue(otc, out var entity))
                        return entity;

                    throw new FaultException("Unknown entity " + otc);
                }

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

        public string[] RecycleBinEntities => _defaultCache.RecycleBinEntities;

        public IEnumerable<EntityMetadata> GetAllEntities()
        {
            if (_cacheUnavailable && _autocompleteCache == null)
            {
                // Start loading a basic cache in the background to use for autocomplete
                _autocompleteCache = Array.Empty<EntityMetadata>();
                _cacheUnavailable = false;

                _ = Task.Run(() =>
                {
                    _autocompleteCache = ((RetrieveMetadataChangesResponse)_org.Execute(new RetrieveMetadataChangesRequest
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
                });
            }

            return _autocompleteCache ?? Array.Empty<EntityMetadata>();
        }
    }
}