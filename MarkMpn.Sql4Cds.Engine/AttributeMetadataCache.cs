using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Metadata;
using Microsoft.Xrm.Sdk.Metadata.Query;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace MarkMpn.Sql4Cds.Engine
{
    /// <summary>
    /// A default implementation of <see cref="IAttributeMetadataCache"/>
    /// </summary>
    public class AttributeMetadataCache : IAttributeMetadataCache
    {
        private readonly IOrganizationService _org;
        private readonly IDictionary<string, EntityMetadata> _metadata;
        private readonly ISet<string> _loading;
        private readonly IDictionary<string, EntityMetadata> _minimalMetadata;
        private readonly ISet<string> _minimalLoading;

        /// <summary>
        /// Creates a new <see cref="AttributeMetadataCache"/>
        /// </summary>
        /// <param name="org">The <see cref="IOrganizationService"/> to retrieve the metadata from</param>
        public AttributeMetadataCache(IOrganizationService org)
        {
            _org = org;
            _metadata = new Dictionary<string, EntityMetadata>();
            _loading = new HashSet<string>();
            _minimalMetadata = new Dictionary<string, EntityMetadata>();
            _minimalLoading = new HashSet<string>();
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
                    EntityFilters = EntityFilters.Attributes | EntityFilters.Relationships
                });

                _metadata[name] = metadata.EntityMetadata;
                return metadata.EntityMetadata;
            }
        }

        /// <inheritdoc/>
        public bool TryGetValue(string logicalName, out EntityMetadata metadata)
        {
            if (_metadata.TryGetValue(logicalName, out metadata))
                return true;

            if (_loading.Add(logicalName))
            {
                var task = Task.Run(() => this[logicalName]);
                OnMetadataLoading(new MetadataLoadingEventArgs(logicalName, task));
            }

            return false;
        }

        public bool TryGetMinimalData(string logicalName, out EntityMetadata metadata)
        {
            if (_metadata.TryGetValue(logicalName, out metadata))
                return true;

            if (_minimalMetadata.TryGetValue(logicalName, out metadata))
                return true;

            if (!_loading.Contains(logicalName) && _minimalLoading.Add(logicalName))
            {
                var task = Task.Run(() =>
                {
                    var metadataChanges = (RetrieveMetadataChangesResponse)_org.Execute(new RetrieveMetadataChangesRequest
                    {
                        Query = new EntityQueryExpression
                        {
                            Criteria = new MetadataFilterExpression
                            {
                                Conditions =
                                {
                                    new MetadataConditionExpression(nameof(EntityMetadata.LogicalName), MetadataConditionOperator.Equals, logicalName)
                                }
                            },
                            Properties = new MetadataPropertiesExpression
                            {
                                PropertyNames =
                                {
                                    nameof(EntityMetadata.LogicalName),
                                    nameof(EntityMetadata.DisplayName),
                                    nameof(EntityMetadata.Description),
                                    nameof(EntityMetadata.Attributes),
                                    nameof(EntityMetadata.ManyToOneRelationships),
                                    nameof(EntityMetadata.OneToManyRelationships),
                                }
                            },
                            AttributeQuery = new AttributeQueryExpression
                            {
                                Properties = new MetadataPropertiesExpression
                                {
                                    PropertyNames =
                                    {
                                        nameof(AttributeMetadata.LogicalName),
                                        nameof(AttributeMetadata.AttributeOf),
                                        nameof(AttributeMetadata.DisplayName),
                                        nameof(AttributeMetadata.Description),
                                        nameof(AttributeMetadata.AttributeType),
                                        nameof(AttributeMetadata.IsValidForUpdate),
                                        nameof(AttributeMetadata.IsValidForCreate)
                                    }
                                }
                            },
                            RelationshipQuery = new RelationshipQueryExpression
                            {
                                Properties = new MetadataPropertiesExpression
                                {
                                    PropertyNames =
                                    {
                                        nameof(OneToManyRelationshipMetadata.ReferencedEntity),
                                        nameof(OneToManyRelationshipMetadata.ReferencedAttribute),
                                        nameof(OneToManyRelationshipMetadata.ReferencingEntity),
                                        nameof(OneToManyRelationshipMetadata.ReferencingAttribute),
                                    }
                                }
                            }
                        }
                    });

                    _minimalMetadata[logicalName] = metadataChanges.EntityMetadata[0];

                    var entityLogicalNameProp = typeof(AttributeMetadata).GetProperty(nameof(AttributeMetadata.EntityLogicalName));
                    foreach (var attr in metadataChanges.EntityMetadata[0].Attributes)
                        entityLogicalNameProp.SetValue(attr, logicalName, null);
                });
                OnMetadataLoading(new MetadataLoadingEventArgs(logicalName, task));
            }

            return false;
            
        }

        public event EventHandler<MetadataLoadingEventArgs> MetadataLoading;

        protected void OnMetadataLoading(MetadataLoadingEventArgs args)
        {
            MetadataLoading?.Invoke(this, args);
        }
    }

    /// <summary>
    /// Holds the details of a metadata background loading event
    /// </summary>
    public class MetadataLoadingEventArgs : EventArgs
    {
        /// <summary>
        /// Creates a new <see cref="MetadataLoadingEventArgs"/>
        /// </summary>
        /// <param name="logicalName">The name of the entity the metadata is being loaded for</param>
        /// <param name="task">A task that indicates when the metadata has been loaded</param>
        public MetadataLoadingEventArgs(string logicalName, Task task)
        {
            LogicalName = logicalName;
            Task = task;
        }

        /// <summary>
        /// The name of the entity the metadata is being loaded for
        /// </summary>
        public string LogicalName { get; }

        /// <summary>
        /// A task that indicates when the metadata has been loaded
        /// </summary>
        public Task Task { get; }
    }
}
