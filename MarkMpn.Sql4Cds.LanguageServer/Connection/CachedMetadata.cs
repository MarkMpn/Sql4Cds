using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.ServiceModel;
using System.Threading.Tasks;
using System.Threading;
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
        private volatile bool _cacheUnavailable;
        private volatile EntityMetadata[] _autocompleteCache;
        private readonly PersistentMetadataCache _persistentMetadataCache;
        private readonly Guid _orgId;
        private readonly Lazy<string> _version;
        private readonly ConcurrentDictionary<string, Lazy<EntityMetadata>> _lazyEntities;
        private readonly Dictionary<string, Task> _lazyLoading = new Dictionary<string, Task>(StringComparer.OrdinalIgnoreCase);
        private readonly object _backgroundLoadLock = new object();
        private readonly SemaphoreSlim _backgroundLoader = new SemaphoreSlim(1);
        private readonly object _autocompleteLock = new object();

        public CachedMetadata(IOrganizationService org, PersistentMetadataCache persistentMetadataCache, Guid orgId, string version, bool lazy)
        {
            _org = org;
            _defaultCache = new AttributeMetadataCache(org);
            if (lazy)
            {
                _persistentMetadataCache = persistentMetadataCache;
                _orgId = orgId;
                _version = new Lazy<string>(() =>
                {
                    // An impossible entity ID returns the metadata stamp without loading any entities.
                    var response = (RetrieveMetadataChangesResponse)org.Execute(new RetrieveMetadataChangesRequest
                    {
                        Query = new EntityQueryExpression
                        {
                            Criteria = new MetadataFilterExpression
                            {
                                Conditions =
                                {
                                    new MetadataConditionExpression(nameof(EntityMetadata.MetadataId), MetadataConditionOperator.Equals, Guid.Empty)
                                }
                            },
                            Properties = new MetadataPropertiesExpression(nameof(EntityMetadata.MetadataId))
                        }
                    });
                    if (String.IsNullOrEmpty(response.ServerVersionStamp))
                        throw new InvalidOperationException("Dataverse did not return a metadata version stamp");
                    return version + ":" + response.ServerVersionStamp;
                }, LazyThreadSafetyMode.ExecutionAndPublication);
                _lazyEntities = new ConcurrentDictionary<string, Lazy<EntityMetadata>>(StringComparer.OrdinalIgnoreCase);
                _cacheUnavailable = true;
                return;
            }

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
                if (_lazyEntities != null)
                    return GetLazyEntity("name:" + name.ToLowerInvariant(), () => _defaultCache[name]);

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
                if (_lazyEntities != null)
                {
                    var loaded = _lazyEntities.Values
                        .Where(e => e.IsValueCreated)
                        .Select(e => e.Value)
                        .FirstOrDefault(e => e.ObjectTypeCode == otc);
                    return loaded ?? GetLazyEntity("otc:" + otc, () => _defaultCache[otc]);
                }

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
            if (_lazyEntities != null)
                return TryGetValue(logicalName, out metadata);

            if (_persistentCacheByName != null)
                return _persistentCacheByName.TryGetValue(logicalName, out metadata);

            return _defaultCache.TryGetMinimalData(logicalName, out metadata);
        }

        public bool TryGetValue(string logicalName, out EntityMetadata metadata)
        {
            if (_lazyEntities != null)
            {
                var entry = GetLazyEntry("name:" + logicalName.ToLowerInvariant(), () => _defaultCache[logicalName]);
                if (entry.IsValueCreated)
                {
                    lock (_backgroundLoadLock)
                        _lazyLoading.Remove(logicalName);
                    metadata = entry.Value;
                    return true;
                }

                lock (_backgroundLoadLock)
                {
                    if (!_lazyLoading.TryGetValue(logicalName, out var load) || load.IsCompleted)
                        _lazyLoading[logicalName] = LoadLazyEntityAsync(logicalName);
                }
                metadata = null;
                return false;
            }

            if (_persistentCacheByName != null)
                return _persistentCacheByName.TryGetValue(logicalName, out metadata);

            return _defaultCache.TryGetValue(logicalName, out metadata);
        }

        public string[] RecycleBinEntities => _defaultCache.RecycleBinEntities;

        private async Task LoadLazyEntityAsync(string logicalName)
        {
            await _backgroundLoader.WaitAsync().ConfigureAwait(false);
            try
            {
                await Task.Run(() => this[logicalName]).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _persistentMetadataCache.ReportLazyLoadFailure(logicalName, ex);
            }
            finally
            {
                _backgroundLoader.Release();
            }
        }

        private Lazy<EntityMetadata> GetLazyEntry(string key, Func<EntityMetadata> load)
        {
            return _lazyEntities.GetOrAdd(key, _ => new Lazy<EntityMetadata>(
                () => _persistentMetadataCache.GetEntityMetadata(_orgId, _version.Value, key, load),
                LazyThreadSafetyMode.ExecutionAndPublication));
        }

        private EntityMetadata GetLazyEntity(string key, Func<EntityMetadata> load)
        {
            var entry = GetLazyEntry(key, load);
            try
            {
                return entry.Value;
            }
            catch
            {
                _lazyEntities.TryRemove(key, out _);
                throw;
            }
        }

        public IEnumerable<EntityMetadata> GetAllEntities()
        {
            lock (_autocompleteLock)
            {
                if (_cacheUnavailable && _autocompleteCache == null)
                {
                    // Start loading a basic cache in the background to use for autocomplete
                    _autocompleteCache = Array.Empty<EntityMetadata>();
                    _cacheUnavailable = false;

                    var load = Task.Run(() =>
                    {
                        var properties = new MetadataPropertiesExpression(
                            nameof(EntityMetadata.LogicalName),
                            nameof(EntityMetadata.DisplayName),
                            nameof(EntityMetadata.Description));
                        if (_lazyEntities != null)
                        {
                            properties.PropertyNames.Add(nameof(EntityMetadata.ObjectTypeCode));
                            properties.PropertyNames.Add(nameof(EntityMetadata.DataProviderId));
                        }

                        _autocompleteCache = ((RetrieveMetadataChangesResponse)_org.Execute(new RetrieveMetadataChangesRequest
                        {
                            Query = new EntityQueryExpression
                            {
                                Properties = properties
                            }
                        })).EntityMetadata
                        .ToArray();
                    });
                    if (_lazyEntities != null)
                    {
                        _ = load.ContinueWith(task =>
                        {
                            if (task.IsFaulted)
                            {
                                _persistentMetadataCache.ReportLazyLoadFailure("autocomplete", task.Exception.GetBaseException());
                                lock (_autocompleteLock)
                                {
                                    _autocompleteCache = null;
                                    _cacheUnavailable = true;
                                }
                            }
                        }, TaskScheduler.Default);
                    }
                }

                return _autocompleteCache ?? Array.Empty<EntityMetadata>();
            }
        }

        public string[] TryGetRecycleBinEntities() => _defaultCache.TryGetRecycleBinEntities();
    }
}