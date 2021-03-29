using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Metadata;

namespace MarkMpn.Sql4Cds.Engine
{
    /// <summary>
    /// Custom <see cref="IAttributeMetadataCache"/> wrapper to inject details of metadata classes that can be queried
    /// </summary>
    public class MetaMetadataCache : IAttributeMetadataCache
    {
        private readonly IAttributeMetadataCache _inner;
        private static readonly IDictionary<string, EntityMetadata> _customMetadata;

        static MetaMetadataCache()
        {
            _customMetadata = new Dictionary<string, EntityMetadata>(StringComparer.OrdinalIgnoreCase);

            // Reuse the schema exposed by the nodes that actually execute the metadata queries
            var metadataNode = new MetadataQueryNode();
            metadataNode.MetadataSource = MetadataSource.Entity | MetadataSource.Attribute | MetadataSource.OneToManyRelationship | MetadataSource.ManyToOneRelationship | MetadataSource.ManyToManyRelationship;
            metadataNode.EntityAlias = "entity";
            metadataNode.AttributeAlias = "attribute";
            metadataNode.OneToManyRelationshipAlias = "relationship_1_n";
            metadataNode.ManyToOneRelationshipAlias = "relationship_n_1";
            metadataNode.ManyToManyRelationshipAlias = "relationship_n_n";

            var metadataSchema = metadataNode.GetSchema(null, null);

            _customMetadata["metadata." + metadataNode.EntityAlias] = SchemaToMetadata(metadataSchema, metadataNode.EntityAlias);
            _customMetadata["metadata." + metadataNode.AttributeAlias] = SchemaToMetadata(metadataSchema, metadataNode.AttributeAlias);
            _customMetadata["metadata." + metadataNode.OneToManyRelationshipAlias] = SchemaToMetadata(metadataSchema, metadataNode.OneToManyRelationshipAlias);
            _customMetadata["metadata." + metadataNode.ManyToOneRelationshipAlias] = SchemaToMetadata(metadataSchema, metadataNode.ManyToOneRelationshipAlias);
            _customMetadata["metadata." + metadataNode.ManyToManyRelationshipAlias] = SchemaToMetadata(metadataSchema, metadataNode.ManyToManyRelationshipAlias);

            var optionsetNode = new GlobalOptionSetQueryNode();
            optionsetNode.Alias = "globaloptionset";

            var optionsetSchema = optionsetNode.GetSchema(null, null);

            _customMetadata["metadata." + optionsetNode.Alias] = SchemaToMetadata(optionsetSchema, optionsetNode.Alias);
        }

        private static EntityMetadata SchemaToMetadata(NodeSchema schema, string alias)
        {
            var metadata = new EntityMetadata
            {
                LogicalName = alias,
                DataProviderId = ProviderId
            };

            var attributes = schema.Schema
                .Where(kvp => kvp.Key.StartsWith(alias + "."))
                .Select(kvp =>
                {
                    var attr = CreateAttribute(kvp.Value);
                    attr.LogicalName = kvp.Key.Substring(alias.Length + 1);
                    return attr;
                })
                .ToArray();

            SetSealedProperty(metadata, nameof(metadata.Attributes), attributes);

            SetSealedProperty(metadata, nameof(metadata.OneToManyRelationships), CreateOneToManyRelationships("metadata." + alias));
            SetSealedProperty(metadata, nameof(metadata.ManyToOneRelationships), CreateManyToOneRelationships("metadata." + alias));
            SetSealedProperty(metadata, nameof(metadata.ManyToManyRelationships), Array.Empty<ManyToManyRelationshipMetadata>());

            return metadata;
        }

        private static OneToManyRelationshipMetadata[] CreateOneToManyRelationships(string alias)
        {
            if (alias == "metadata.entity")
            {
                var attrRelationship = new OneToManyRelationshipMetadata
                {
                    SchemaName = "entity_attributes",
                    ReferencedEntity = "metadata.entity",
                    ReferencedAttribute = "logicalname",
                    ReferencingEntity = "metadata.attribute",
                    ReferencingAttribute = "entitylogicalname"
                };

                var rel1nRelationship = new OneToManyRelationshipMetadata
                {
                    SchemaName = "entity_one_to_many_relationships",
                    ReferencedEntity = "metadata.entity",
                    ReferencedAttribute = "logicalname",
                    ReferencingEntity = "metadata.relationship_1_n",
                    ReferencingAttribute = "referencedentity"
                };

                var reln1Relationship = new OneToManyRelationshipMetadata
                {
                    SchemaName = "entity_many_to_one_relationships",
                    ReferencedEntity = "metadata.entity",
                    ReferencedAttribute = "logicalname",
                    ReferencingEntity = "metadata.relationship_n_1",
                    ReferencingAttribute = "referencingentity"
                };

                var relnnRelationship1 = new OneToManyRelationshipMetadata
                {
                    SchemaName = "entity_many_to_many_relationships_entity1",
                    ReferencedEntity = "metadata.entity",
                    ReferencedAttribute = "logicalname",
                    ReferencingEntity = "metadata.relationship_n_n",
                    ReferencingAttribute = "entity1logicalname"
                };

                var relnnRelationship2 = new OneToManyRelationshipMetadata
                {
                    SchemaName = "entity_many_to_many_relationships_entity2",
                    ReferencedEntity = "metadata.entity",
                    ReferencedAttribute = "logicalname",
                    ReferencingEntity = "metadata.relationship_n_n",
                    ReferencingAttribute = "entity2logicalname"
                };

                var relnnRelationshipIntersect = new OneToManyRelationshipMetadata
                {
                    SchemaName = "entity_many_to_many_relationships_intersect",
                    ReferencedEntity = "metadata.entity",
                    ReferencedAttribute = "logicalname",
                    ReferencingEntity = "metadata.relationship_n_n",
                    ReferencingAttribute = "intersectentityname"
                };

                return new[]
                {
                    attrRelationship,
                    rel1nRelationship,
                    reln1Relationship,
                    relnnRelationship1,
                    relnnRelationship2,
                    relnnRelationshipIntersect
                };
            }

            return Array.Empty<OneToManyRelationshipMetadata>();
        }

        private static OneToManyRelationshipMetadata[] CreateManyToOneRelationships(string alias)
        {
            var relationships = CreateOneToManyRelationships("metadata.entity");

            return relationships.Where(r => r.ReferencingEntity == alias).ToArray();
        }

        private static void SetSealedProperty(object target, string prop, object value)
        {
            target.GetType().GetProperty(prop).SetValue(target, value, null);
        }

        private static AttributeMetadata CreateAttribute(Type type)
        {
            if (type == typeof(SqlBoolean))
                return new BooleanAttributeMetadata();
            if (type == typeof(SqlDateTime))
                return new DateTimeAttributeMetadata();
            if (type == typeof(SqlDecimal))
                return new DecimalAttributeMetadata();
            if (type == typeof(SqlDouble))
                return new DoubleAttributeMetadata();
            if (type == typeof(SqlBinary))
                return new ImageAttributeMetadata();
            if (type == typeof(SqlInt16))
                return new IntegerAttributeMetadata();
            if (type == typeof(SqlInt32))
                return new IntegerAttributeMetadata();
            if (type == typeof(SqlInt64))
                return new BigIntAttributeMetadata();
            if (type == typeof(SqlGuid))
                return new UniqueIdentifierAttributeMetadata();
            if (type == typeof(SqlString))
                return new StringAttributeMetadata();

            throw new ArgumentOutOfRangeException(nameof(type), $"Unexpected attribute type {type}");
        }

        public static Guid ProviderId { get; } = new Guid("{F126FEBD-2DBC-46BD-9DE6-8BB91E06B2D1}");

        public static IEnumerable<EntityMetadata> GetMetadata()
        {
            return _customMetadata.Values;
        }

        /// <summary>
        /// Creates a new <see cref="MetaMetadataCache"/>
        /// </summary>
        /// <param name="inner">The <see cref="IAttributeMetadataCache"/> that provides the metadata for the standard data entities</param>
        public MetaMetadataCache(IAttributeMetadataCache inner)
        {
            _inner = inner;
        }

        /// <inheritdoc/>
        public EntityMetadata this[string name]
        {
            get
            {
                if (_customMetadata.TryGetValue(name, out var metadata))
                    return metadata;

                return _inner[name];
            }
        }

        /// <inheritdoc/>
        public EntityMetadata this[int otc]
        {
            get
            {
                return _inner[otc];
            }
        }

        /// <inheritdoc/>
        public bool TryGetValue(string logicalName, out EntityMetadata metadata)
        {
            if (_customMetadata.TryGetValue(logicalName, out metadata))
                return true;

            return _inner.TryGetValue(logicalName, out metadata);
        }

        /// <inheritdoc/>
        public bool TryGetMinimalData(string logicalName, out EntityMetadata metadata)
        {
            if (_customMetadata.TryGetValue(logicalName, out metadata))
                return true;

            return _inner.TryGetMinimalData(logicalName, out metadata);
        }
    }
}
