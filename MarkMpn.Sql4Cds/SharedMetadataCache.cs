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

namespace MarkMpn.Sql4Cds
{
    class SharedMetadataCache : IAttributeMetadataCache
    {
        private readonly ConnectionDetail _connection;
        private readonly AttributeMetadataCache _innerCache;

        // Metadata cache updating was broken in first release, make sure we're only using it on later versions
        private static readonly bool _metadataCacheSupported = typeof(ConnectionDetail).Assembly.GetName().Version >= new Version("1.2021.6.44");

        public SharedMetadataCache(ConnectionDetail connection, IOrganizationService org)
        {
            _connection = connection;
            _innerCache = new AttributeMetadataCache(org);
        }

        public EntityMetadata this[string name]
        {
            get
            {
                if (!_metadataCacheSupported || _connection.MetadataCache == null)
                    return _innerCache[name];

                var meta = _connection.MetadataCache.SingleOrDefault(e => e.LogicalName == name.ToLowerInvariant());

                if (meta == null)
                    throw new FaultException($"Unknown table {name}");

                return meta;
            }
        }

        public EntityMetadata this[int otc]
        {
            get
            {
                if (!_metadataCacheSupported || _connection.MetadataCache == null)
                    return _innerCache[otc];

                var meta = _connection.MetadataCache.SingleOrDefault(e => e.ObjectTypeCode == otc);

                if (meta == null)
                    throw new FaultException($"Unknown table {otc}");

                return meta;
            }
        }

        public bool TryGetMinimalData(string logicalName, out EntityMetadata metadata)
        {
            if (!_metadataCacheSupported || _connection.MetadataCache == null)
                return _innerCache.TryGetMinimalData(logicalName, out metadata);

            metadata = _connection.MetadataCache.SingleOrDefault(e => e.LogicalName == logicalName.ToLowerInvariant());
            return metadata != null;
        }

        public bool TryGetValue(string logicalName, out EntityMetadata metadata)
        {
            if (!_metadataCacheSupported || _connection.MetadataCache == null)
                return _innerCache.TryGetValue(logicalName, out metadata);

            metadata = _connection.MetadataCache.SingleOrDefault(e => e.LogicalName == logicalName.ToLowerInvariant());
            return metadata != null;
        }

        public event EventHandler<MetadataLoadingEventArgs> MetadataLoading
        {
            add { _innerCache.MetadataLoading += value; }
            remove { _innerCache.MetadataLoading -= value; }
        }
    }
}
