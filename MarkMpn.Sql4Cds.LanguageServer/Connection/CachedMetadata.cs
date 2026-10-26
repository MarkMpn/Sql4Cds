using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;
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
        private volatile bool _cacheUnavailable = true;
        private volatile EntityMetadata[] _autocompleteCache;
        private readonly PersistentMetadataCache _persistentMetadataCache;
        private readonly Guid _orgId;
        private readonly Lazy<string> _version;
        private readonly ConcurrentDictionary<string, Lazy<EntityMetadata>> _lazyEntities = new ConcurrentDictionary<string, Lazy<EntityMetadata>>(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, Task> _lazyLoading = new Dictionary<string, Task>(StringComparer.OrdinalIgnoreCase);
        private readonly object _backgroundLoadLock = new object();
        private readonly SemaphoreSlim _backgroundLoader = new SemaphoreSlim(1);
        private readonly object _autocompleteLock = new object();
        private Task _autocompleteLoad;
        private Lazy<string[]> _recycleBinEntities;
        private Task<string[]> _recycleBinLoad;

        public CachedMetadata(IOrganizationService org, PersistentMetadataCache persistentMetadataCache, Guid orgId, string version)
        {
            _org = org;
            _defaultCache = new AttributeMetadataCache(org);
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
            _recycleBinEntities = new Lazy<string[]>(() => _persistentMetadataCache.GetLazyMetadata(
                _orgId, _version.Value, "autocomplete:recyclebin", 1, () => _defaultCache.RecycleBinEntities, allowNull: true),
                LazyThreadSafetyMode.ExecutionAndPublication);
        }

        public EntityMetadata this[string name]
        {
            get
            {
                return GetLazyEntity("name:" + name.ToLowerInvariant(), () => _defaultCache[name]);
            }
        }

        public EntityMetadata this[int otc]
        {
            get
            {
                var loaded = _lazyEntities.Values
                    .Where(e => e.IsValueCreated)
                    .Select(e => e.Value)
                    .FirstOrDefault(e => e.ObjectTypeCode == otc);
                return loaded ?? GetLazyEntity("otc:" + otc, () => _defaultCache[otc]);
            }
        }

        public bool TryGetMinimalData(string logicalName, out EntityMetadata metadata)
        {
            return TryGetValue(logicalName, out metadata);
        }

        public bool TryGetValue(string logicalName, out EntityMetadata metadata)
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
                {
                    var key = "name:" + logicalName.ToLowerInvariant();
                    if (_persistentMetadataCache.TryGetLazyMetadata(_orgId, () => _version.Value, key, 2, out metadata))
                    {
                        var cached = metadata;
                        var diskEntry = new Lazy<EntityMetadata>(() => cached, LazyThreadSafetyMode.ExecutionAndPublication);
                        _ = diskEntry.Value;
                        _lazyEntities[key] = diskEntry;
                        if (cached.ObjectTypeCode != null)
                            _lazyEntities.TryAdd("otc:" + cached.ObjectTypeCode.Value, diskEntry);
                        return true;
                    }
                    _lazyLoading[logicalName] = LoadLazyEntityAsync(logicalName);
                }
            }
            metadata = null;
            return false;
        }

        public string[] RecycleBinEntities
        {
            get
            {
                Lazy<string[]> entities;
                lock (_backgroundLoadLock)
                    entities = _recycleBinEntities;
                return entities.Value;
            }
        }

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
                var entity = entry.Value;
                _lazyEntities.TryAdd("name:" + entity.LogicalName.ToLowerInvariant(), entry);
                if (entity.ObjectTypeCode != null)
                    _lazyEntities.TryAdd("otc:" + entity.ObjectTypeCode.Value, entry);
                return entity;
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
                    if (_persistentMetadataCache.TryGetLazyMetadata(
                        _orgId, () => _version.Value, "autocomplete:entities", 2, out EntityMetadata[] cached))
                    {
                        _autocompleteCache = cached;
                        _cacheUnavailable = false;
                        return cached;
                    }

                    // Start loading a basic cache in the background to use for autocomplete
                    _autocompleteCache = Array.Empty<EntityMetadata>();
                    _cacheUnavailable = false;

                    var load = _autocompleteLoad = Task.Run(() =>
                    {
                        var properties = new MetadataPropertiesExpression(
                            nameof(EntityMetadata.LogicalName),
                            nameof(EntityMetadata.DisplayName),
                            nameof(EntityMetadata.Description),
                            nameof(EntityMetadata.IsArchivalEnabled),
                            nameof(EntityMetadata.IsRetentionEnabled),
                            nameof(EntityMetadata.ObjectTypeCode),
                            nameof(EntityMetadata.DataProviderId));

                        EntityMetadata[] LoadEntities() => ((RetrieveMetadataChangesResponse)_org.Execute(new RetrieveMetadataChangesRequest
                        {
                            Query = new EntityQueryExpression
                            {
                                Properties = properties
                            }
                        })).EntityMetadata
                        .ToArray();

                        _autocompleteCache = _persistentMetadataCache.GetLazyMetadata(_orgId, _version.Value, "autocomplete:entities", 2, LoadEntities);
                    });
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

                return _autocompleteCache ?? Array.Empty<EntityMetadata>();
            }
        }

        internal async Task EnsureAllEntitiesLoadedAsync()
        {
            Task load;
            lock (_autocompleteLock)
            {
                GetAllEntities();
                load = _autocompleteLoad;
            }
            if (load != null)
                await load.ConfigureAwait(false);
        }

        public string[] TryGetRecycleBinEntities()
        {
            Task<string[]> load;
            lock (_backgroundLoadLock)
            {
                if (_recycleBinEntities.IsValueCreated)
                    return _recycleBinEntities.Value;

                if (_recycleBinLoad == null && _persistentMetadataCache.TryGetLazyMetadata(
                    _orgId, () => _version.Value, "autocomplete:recyclebin", 1, out string[] cached, allowNull: true))
                {
                    _recycleBinEntities = new Lazy<string[]>(() => cached, LazyThreadSafetyMode.ExecutionAndPublication);
                    return _recycleBinEntities.Value;
                }
                load = _recycleBinLoad ??= Task.Run(() => _recycleBinEntities.Value);
            }
            if (load.IsCompleted)
                return load.GetAwaiter().GetResult();
            return Array.Empty<string>();
        }
    }
}