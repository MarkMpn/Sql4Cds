using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Crm.Sdk.Messages;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Metadata;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// Implements an INSERT operation
    /// </summary>
    class InsertNode : BaseDmlNode
    {
        private int _executionCount;
        private readonly Timer _timer = new Timer();

        public override int ExecutionCount => _executionCount;

        public override TimeSpan Duration => _timer.Duration;

        /// <summary>
        /// The logical name of the entity to insert
        /// </summary>
        [Category("Insert")]
        [Description("The logical name of the entity to insert")]
        public string LogicalName { get; set; }

        /// <summary>
        /// The columns to insert and the associated column to take the new value from
        /// </summary>
        [Category("Insert")]
        [Description("The columns to insert and the associated column to take the new value from")]
        [DisplayName("Column Mappings")]
        public IDictionary<string, string> ColumnMappings { get; } = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        public override void AddRequiredColumns(IDictionary<string, DataSource> dataSources, IDictionary<string, Type> parameterTypes, IList<string> requiredColumns)
        {
            foreach (var col in ColumnMappings.Values)
            {
                if (!requiredColumns.Contains(col))
                    requiredColumns.Add(col);
            }

            Source.AddRequiredColumns(dataSources, parameterTypes, requiredColumns);
        }

        public override string Execute(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues)
        {
            _executionCount++;

            try
            {
                if (!dataSources.TryGetValue(DataSource, out var dataSource))
                    throw new QueryExecutionException("Missing datasource " + DataSource);

                List<Entity> entities;
                EntityMetadata meta;
                Dictionary<string, AttributeMetadata> attributes;
                Dictionary<string, Func<Entity, object>> attributeAccessors;
                Func<Entity, object> primaryIdAccessor;

                using (_timer.Run())
                {
                    entities = GetDmlSourceEntities(dataSources, options, parameterTypes, parameterValues, out var schema);

                    // Precompile mappings with type conversions
                    meta = dataSource.Metadata[LogicalName];
                    attributes = meta.Attributes.ToDictionary(a => a.LogicalName);
                    var dateTimeKind = options.UseLocalTimeZone ? DateTimeKind.Local : DateTimeKind.Utc;
                    attributeAccessors = CompileColumnMappings(meta, ColumnMappings, schema, attributes, dateTimeKind);
                    attributeAccessors.TryGetValue(meta.PrimaryIdAttribute, out primaryIdAccessor);
                }

                // Check again that the update is allowed. Don't count any UI interaction in the execution time
                if (options.Cancelled || !options.ConfirmInsert(entities.Count, meta))
                    throw new OperationCanceledException("INSERT cancelled by user");

                using (_timer.Run())
                {
                    return ExecuteDmlOperation(
                        dataSource.Connection,
                        options,
                        entities,
                        meta,
                        entity => CreateInsertRequest(meta, entity, attributeAccessors, primaryIdAccessor, attributes),
                        new OperationNames
                        {
                            InProgressUppercase = "Inserting",
                            InProgressLowercase = "inserting",
                            CompletedLowercase = "inserted"
                        });
                }
            }
            catch (QueryExecutionException ex)
            {
                if (ex.Node == null)
                    ex.Node = this;

                throw;
            }
            catch (Exception ex)
            {
                throw new QueryExecutionException(ex.Message, ex) { Node = this };
            }
        }

        private OrganizationRequest CreateInsertRequest(EntityMetadata meta, Entity entity, Dictionary<string,Func<Entity,object>> attributeAccessors, Func<Entity,object> primaryIdAccessor, Dictionary<string,AttributeMetadata> attributes)
        {
            // Special cases for intersect entities
            if (LogicalName == "listmember")
            {
                var listId = (Guid?)attributeAccessors["listid"](entity);
                var entityId = (Guid?)attributeAccessors["entityid"](entity);

                if (listId == null)
                    throw new QueryExecutionException("Cannot insert value NULL into listmember.listid");

                if (entityId == null)
                    throw new QueryExecutionException("Cannot insert value NULL into listmember.entityid");

                return new OrganizationRequest
                {
                    RequestName = "AddMemberList",
                    Parameters = new ParameterCollection
                    {
                        ["ListId"] = listId.Value,
                        ["EntityId"] = entityId.Value
                    }
                };
            }
            
            if (meta.IsIntersect == true)
            {
                // For generic intersect entities we expect a single many-to-many relationship in the metadata which describes
                // the relationship that this is the intersect entity for
                var relationship = meta.ManyToManyRelationships.Single();

                var e1 = (Guid?)attributeAccessors[relationship.Entity1IntersectAttribute](entity);
                var e2 = (Guid?)attributeAccessors[relationship.Entity2IntersectAttribute](entity);

                if (e1 == null)
                    throw new QueryExecutionException($"Cannot insert value NULL into {relationship.Entity1IntersectAttribute}");

                if (e2 == null)
                    throw new QueryExecutionException($"Cannot insert value NULL into {relationship.Entity2IntersectAttribute}");

                return new AssociateRequest
                {
                    Target = new EntityReference(relationship.Entity1LogicalName, e1.Value),
                    Relationship = new Relationship(relationship.SchemaName) { PrimaryEntityRole = EntityRole.Referencing },
                    RelatedEntities = new EntityReferenceCollection(new[] { new EntityReference(relationship.Entity2LogicalName, e2.Value) })
                };
            }

            var insert = new Entity(LogicalName);

            if (primaryIdAccessor != null)
                insert.Id = (Guid) primaryIdAccessor(entity);

            foreach (var attributeAccessor in attributeAccessors)
            {
                if (attributeAccessor.Key == meta.PrimaryIdAttribute)
                    continue;

                var attr = attributes[attributeAccessor.Key];

                if (!String.IsNullOrEmpty(attr.AttributeOf))
                    continue;

                var value = attributeAccessor.Value(entity);

                insert[attr.LogicalName] = value;
            }

            return new CreateRequest { Target = insert };
        }

        public override string ToString()
        {
            return "INSERT";
        }
    }
}
