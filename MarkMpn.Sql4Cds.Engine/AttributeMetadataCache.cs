using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Metadata;
using Microsoft.Xrm.Sdk.Metadata.Query;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceModel;
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
        private readonly IDictionary<string, Exception> _invalidEntities;
        private readonly Lazy<string[]> _recycleBinEntities;

        /// <summary>
        /// Creates a new <see cref="AttributeMetadataCache"/>
        /// </summary>
        /// <param name="org">The <see cref="IOrganizationService"/> to retrieve the metadata from</param>
        public AttributeMetadataCache(IOrganizationService org)
        {
            _org = org;
            _metadata = new Dictionary<string, EntityMetadata>(StringComparer.OrdinalIgnoreCase);
            _loading = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            _minimalMetadata = new Dictionary<string, EntityMetadata>(StringComparer.OrdinalIgnoreCase);
            _minimalLoading = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            _invalidEntities = new Dictionary<string, Exception>(StringComparer.OrdinalIgnoreCase);
            _recycleBinEntities = new Lazy<string[]>(() =>
            {
                // Check the recyclebinconfig entity exists
                try
                {
                    _ = this["recyclebinconfig"];
                }
                catch
                {
                    return null;
                }

                // https://learn.microsoft.com/en-us/power-apps/developer/data-platform/restore-deleted-records?tabs=sdk#detect-which-tables-are-enabled-for-recycle-bin
                var qry = new FetchExpression(@"
                    <fetch>
                      <entity name='recyclebinconfig'>
                        <filter type='and'>
                          <condition attribute='statecode'
                            operator='eq'
                            value='0' />
                          <condition attribute='isreadyforrecyclebin'
                            operator='eq'
                            value='1' />
                        </filter>
                        <link-entity name='entity'
                          from='entityid'
                          to='extensionofrecordid'
                          link-type='inner'
                          alias='entity'>
                          <attribute name='logicalname' />
                          <order attribute='logicalname' />
                        </link-entity>
                      </entity>
                    </fetch>");

                var resp = _org.RetrieveMultiple(qry);
                return resp.Entities
                    .Select(e => e.GetAttributeValue<AliasedValue>("entity.logicalname").Value as string)
                    .ToArray();
            });
        }

        /// <inheritdoc cref="IAttributeMetadataCache.this{string}"/>
        public EntityMetadata this[string name]
        {
            get
            {
                if (_metadata.TryGetValue(name, out var value))
                    return value;

                if (_invalidEntities.TryGetValue(name, out var cachedEx))
                    throw cachedEx;

                try
                {
                    var metadata = (RetrieveEntityResponse)_org.Execute(new RetrieveEntityRequest
                    {
                        LogicalName = name.ToLowerInvariant(),
                        EntityFilters = EntityFilters.Attributes | EntityFilters.Relationships
                    });

                    _metadata[name] = metadata.EntityMetadata;
                    return metadata.EntityMetadata;
                }
                catch (FaultException ex)
                {
                    _invalidEntities[name] = ex;
                    throw;
                }
                catch (Exception ex)
                {
                    _invalidEntities[name] = ex;
                    throw new FaultException<OrganizationServiceFault>(new OrganizationServiceFault { Message = ex.Message });
                }
            }
        }

        /// <inheritdoc cref="IAttributeMetadataCache.this{int}"/>
        public EntityMetadata this[int otc]
        {
            get
            {
                var metadata = _metadata.Values.SingleOrDefault(e => e.ObjectTypeCode == otc);

                if (metadata != null)
                    return metadata;

                var qry = new RetrieveMetadataChangesRequest
                {
                    Query = new EntityQueryExpression
                    {
                        Criteria = new MetadataFilterExpression
                        {
                            Conditions =
                            {
                                new MetadataConditionExpression(nameof(EntityMetadata.ObjectTypeCode), MetadataConditionOperator.Equals, otc)
                            }
                        },
                        Properties = new MetadataPropertiesExpression { AllProperties = true }
                    }
                };

                var resp = (RetrieveMetadataChangesResponse) _org.Execute(qry);

                _metadata[resp.EntityMetadata[0].LogicalName] = resp.EntityMetadata[0];
                return resp.EntityMetadata[0];
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

        /// <inheritdoc/>
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
                                    new MetadataConditionExpression(nameof(EntityMetadata.LogicalName), MetadataConditionOperator.Equals, logicalName.ToLowerInvariant())
                                }
                            },
                            Properties = new MetadataPropertiesExpression
                            {
                                PropertyNames =
                                {
                                    nameof(EntityMetadata.LogicalName),
                                    nameof(EntityMetadata.DisplayName),
                                    nameof(EntityMetadata.Description),
                                    nameof(EntityMetadata.IsIntersect),
                                    nameof(EntityMetadata.Attributes),
                                    nameof(EntityMetadata.ManyToOneRelationships),
                                    nameof(EntityMetadata.OneToManyRelationships),
                                    nameof(EntityMetadata.ManyToManyRelationships),
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
                                        nameof(AttributeMetadata.IsValidForCreate),
                                        nameof(AttributeMetadata.IsValidForRead),
                                        nameof(LookupAttributeMetadata.Targets),
                                        nameof(EnumAttributeMetadata.OptionSet)
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
                                        nameof(ManyToManyRelationshipMetadata.Entity1IntersectAttribute),
                                        nameof(ManyToManyRelationshipMetadata.Entity2IntersectAttribute)
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

        /// <inheritdoc/>
        public string[] RecycleBinEntities => _recycleBinEntities.Value;

        public event EventHandler<MetadataLoadingEventArgs> MetadataLoading;

        protected void OnMetadataLoading(MetadataLoadingEventArgs args)
        {
            MetadataLoading?.Invoke(this, args);
        }

        /// <inheritdoc/>
        public IEnumerable<EntityMetadata> GetAllEntities()
        {
            return _metadata.Values;
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
