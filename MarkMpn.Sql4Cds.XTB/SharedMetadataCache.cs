using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceModel;
using System.Text;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.Engine;
using McTools.Xrm.Connection;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Metadata;

namespace MarkMpn.Sql4Cds.XTB
{
    /// <summary>
    /// An <see cref="IAttributeMetadataCache"/> implementation that uses the metadata cache provided by XrmToolBox where possible
    /// </summary>
    class SharedMetadataCache : IAttributeMetadataCache
    {
        private readonly ConnectionDetail _connection;
        private readonly AttributeMetadataCache _innerCache;
        private MetadataCache _lastCache;
        private IDictionary<string, EntityMetadata> _entitiesByName;
        private IDictionary<int, EntityMetadata> _entitiesByOtc;

        // Metadata cache updating was broken in first release, make sure we're only using it on later versions
        private static readonly bool _metadataCacheSupported = typeof(ConnectionDetail).Assembly.GetName().Version >= new Version("1.2021.6.44");

        public SharedMetadataCache(ConnectionDetail connection, IOrganizationService org)
        {
            _connection = connection;
            _innerCache = new AttributeMetadataCache(org);
        }

        /// <inheritdoc />
        public EntityMetadata this[string name]
        {
            get
            {
                if (!CacheReady)
                    return _innerCache[name];

                LoadCache();

                if (!_entitiesByName.TryGetValue(name, out var meta))
                    throw new FaultException($"Unknown table {name}");

                return meta;
            }
        }

        /// <inheritdoc />
        public EntityMetadata this[int otc]
        {
            get
            {
                if (!CacheReady)
                    return _innerCache[otc];

                LoadCache();

                if (!_entitiesByOtc.TryGetValue(otc, out var meta))
                    throw new FaultException($"Unknown table {otc}");

                return meta;
            }
        }

        /// <inheritdoc />
        public bool TryGetMinimalData(string logicalName, out EntityMetadata metadata)
        {
            if (!CacheReady)
                return _innerCache.TryGetMinimalData(logicalName, out metadata);

            LoadCache();

            return _entitiesByName.TryGetValue(logicalName, out metadata);
        }

        /// <inheritdoc />
        public bool TryGetValue(string logicalName, out EntityMetadata metadata)
        {
            if (!CacheReady)
                return _innerCache.TryGetValue(logicalName, out metadata);

            LoadCache();

            return _entitiesByName.TryGetValue(logicalName, out metadata);
        }

        /// <inheritdoc />
        public string[] RecycleBinEntities => _innerCache.RecycleBinEntities;

        private  bool CacheReady
        {
            get
            {
                if (!_metadataCacheSupported)
                    return false;

                if (_connection.MetadataCacheLoader == null)
                    return false;

                if (_connection.MetadataCacheLoader.Status != TaskStatus.RanToCompletion)
                    return false;

                if (_connection.MetadataCacheLoader.Result == null)
                    return false;

                return true;
            }
        }

        private void LoadCache()
        {
            var cache = _connection.MetadataCacheLoader.Result;

            if (_lastCache == cache)
                return;

            _entitiesByName = cache.EntityMetadata.ToDictionary(e => e.LogicalName, StringComparer.OrdinalIgnoreCase);
            _entitiesByOtc = cache.EntityMetadata.ToDictionary(e => e.ObjectTypeCode.Value);
            _lastCache = cache;
        }

        public event EventHandler<MetadataLoadingEventArgs> MetadataLoading
        {
            add { _innerCache.MetadataLoading += value; }
            remove { _innerCache.MetadataLoading -= value; }
        }
    }
}
