using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Crm.Sdk.Messages;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Metadata;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// Implements an DELETE operation
    /// </summary>
    class DeleteNode : BaseDmlNode
    {
        private int _executionCount;
        private readonly Timer _timer = new Timer();

        public override int ExecutionCount => _executionCount;

        public override TimeSpan Duration => _timer.Duration;

        /// <summary>
        /// The logical name of the entity to delete
        /// </summary>
        [Category("Delete")]
        [Description("The logical name of the entity to delete")]
        public string LogicalName { get; set; }

        /// <summary>
        /// The column that contains the primary ID of the records to delete
        /// </summary>
        [Category("Delete")]
        [Description("The column that contains the primary ID of the records to delete")]
        [DisplayName("PrimaryId Source")]
        public string PrimaryIdSource { get; set; }

        /// <summary>
        /// The column that contains the secondary ID of the records to delete (used for many-to-many intersect tables)
        /// </summary>
        [Category("Delete")]
        [Description("The column that contains the secondary ID of the records to delete (used for many-to-many intersect tables)")]
        [DisplayName("SecondaryId Source")]
        public string SecondaryIdSource { get; set; }

        [Category("Delete")]
        public override int MaxDOP { get; set; }

        [Category("Delete")]
        public override int BatchSize { get; set; }

        [Category("Delete")]
        public override bool BypassCustomPluginExecution { get; set; }

        public override void AddRequiredColumns(NodeCompilationContext context, IList<string> requiredColumns)
        {
            if (!requiredColumns.Contains(PrimaryIdSource))
                requiredColumns.Add(PrimaryIdSource);

            if (SecondaryIdSource != null && !requiredColumns.Contains(SecondaryIdSource))
                requiredColumns.Add(SecondaryIdSource);

            Source.AddRequiredColumns(context, requiredColumns);
        }

        public override IRootExecutionPlanNodeInternal[] FoldQuery(NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            var result = base.FoldQuery(context, hints);

            if (result.Length != 1 || result[0] != this)
                return result;

            if (!context.DataSources.TryGetValue(DataSource, out var dataSource))
                throw new NotSupportedQueryFragmentException("Missing datasource " + DataSource);

            // Use bulk delete if requested & possible
            if ((context.Options.UseBulkDelete || LogicalName == "audit") &&
                Source is FetchXmlScan fetch &&
                LogicalName == fetch.Entity.name &&
                PrimaryIdSource.Equals($"{fetch.Alias}.{dataSource.Metadata[LogicalName].PrimaryIdAttribute}") &&
                String.IsNullOrEmpty(SecondaryIdSource))
            {
                return new[] { new BulkDeleteJobNode { DataSource = DataSource, Source = fetch } };
            }

            return new[] { this };
        }

        protected override void RenameSourceColumns(IDictionary<string, string> columnRenamings)
        {
            if (columnRenamings.TryGetValue(PrimaryIdSource, out var primaryIdSourceRenamed))
                PrimaryIdSource = primaryIdSourceRenamed;

            if (SecondaryIdSource != null && columnRenamings.TryGetValue(SecondaryIdSource, out var secondaryIdSourceRenamed))
                SecondaryIdSource = secondaryIdSourceRenamed;
        }

        public override string Execute(NodeExecutionContext context, out int recordsAffected)
        {
            _executionCount++;

            try
            {
                if (!context.DataSources.TryGetValue(DataSource, out var dataSource))
                    throw new QueryExecutionException("Missing datasource " + DataSource);

                List<Entity> entities;
                EntityMetadata meta;
                Func<Entity, object> primaryIdAccessor;
                Func<Entity, object> secondaryIdAccessor = null;

                using (_timer.Run())
                {
                    entities = GetDmlSourceEntities(context, out var schema);

                    // Precompile mappings with type conversions
                    meta = dataSource.Metadata[LogicalName];
                    var dateTimeKind = context.Options.UseLocalTimeZone ? DateTimeKind.Local : DateTimeKind.Utc;
                    var primaryKey = meta.PrimaryIdAttribute;
                    string secondaryKey = null;

                    // Special cases for the keys used for intersect entities
                    if (meta.LogicalName == "listmember")
                    {
                        primaryKey = "listid";
                        secondaryKey = "entityid";
                    }
                    else if (meta.IsIntersect == true)
                    {
                        var relationship = meta.ManyToManyRelationships.Single();
                        primaryKey = relationship.Entity1IntersectAttribute;
                        secondaryKey = relationship.Entity2IntersectAttribute;
                    }

                    var fullMappings = new Dictionary<string, string>
                    {
                        [primaryKey] = PrimaryIdSource
                    };

                    if (secondaryKey != null)
                        fullMappings[secondaryKey] = SecondaryIdSource;

                    var attributeAccessors = CompileColumnMappings(dataSource, LogicalName, fullMappings, schema, dateTimeKind, entities);
                    primaryIdAccessor = attributeAccessors[primaryKey];

                    if (SecondaryIdSource != null)
                        secondaryIdAccessor = attributeAccessors[secondaryKey];
                }

                // Check again that the update is allowed. Don't count any UI interaction in the execution time
                var confirmArgs = new ConfirmDmlStatementEventArgs(entities.Count, meta, BypassCustomPluginExecution);
                if (context.Options.CancellationToken.IsCancellationRequested)
                    confirmArgs.Cancel = true;
                context.Options.ConfirmDelete(confirmArgs);
                if (confirmArgs.Cancel)
                    throw new OperationCanceledException("DELETE cancelled by user");

                using (_timer.Run())
                {
                    return ExecuteDmlOperation(
                        dataSource.Connection,
                        context.Options,
                        entities,
                        meta,
                        entity => CreateDeleteRequest(meta, entity, primaryIdAccessor, secondaryIdAccessor),
                        new OperationNames
                        {
                            InProgressUppercase = "Deleting",
                            InProgressLowercase = "deleting",
                            CompletedLowercase = "deleted"
                        },
                        out recordsAffected,
                        context.ParameterValues);
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

        private OrganizationRequest CreateDeleteRequest(EntityMetadata meta, Entity entity, Func<Entity,object> primaryIdAccessor, Func<Entity,object> secondaryIdAccessor)
        {
            var id = (Guid)primaryIdAccessor(entity);

            // Special case messages for intersect entities
            if (meta.IsIntersect == true)
            {
                var secondaryId = (Guid)secondaryIdAccessor(entity);

                if (meta.LogicalName == "listmember")
                {
                    return new OrganizationRequest
                    {
                        RequestName = "RemoveMemberList",
                        Parameters = new ParameterCollection
                        {
                            ["ListId"] = id,
                            ["EntityId"] = secondaryId
                        }
                    };
                }

                var relationship = meta.ManyToManyRelationships.Single();

                return new DisassociateRequest
                {
                    Target = new EntityReference(relationship.Entity1LogicalName, id),
                    RelatedEntities = new EntityReferenceCollection(new[] { new EntityReference(relationship.Entity2LogicalName, secondaryId) }),
                    Relationship = new Relationship(relationship.SchemaName) { PrimaryEntityRole = EntityRole.Referencing }
                };
            }

            return new DeleteRequest
            {
                Target = new EntityReference(LogicalName, id)
            };
        }

        protected override ExecuteMultipleResponse ExecuteMultiple(IOrganizationService org, EntityMetadata meta, ExecuteMultipleRequest req)
        {
            if (meta.DataProviderId == DataProviders.ElasticDataProvider)
            {
                // Elastic tables can use DeleteMultiple for better performance than ExecuteMultiple
                var entities = new EntityReferenceCollection();

                foreach (DeleteRequest delete in req.Requests)
                    entities.Add(delete.Target);

                var deleteMultiple = new OrganizationRequest("DeleteMultiple")
                {
                    ["Targets"] = entities
                };

                org.Execute(deleteMultiple);

                var multipleResp = new ExecuteMultipleResponse
                {
                    ["Responses"] = new ExecuteMultipleResponseItemCollection()
                };

                for (var i = 0; i < req.Requests.Count; i++)
                {
                    multipleResp.Responses.Add(new ExecuteMultipleResponseItem
                    {
                        RequestIndex = i,
                        Response = new DeleteResponse()
                    });
                }

                return multipleResp;
            }

            return base.ExecuteMultiple(org, meta, req);
        }

        public override string ToString()
        {
            return "DELETE";
        }

        public override object Clone()
        {
            var clone = new DeleteNode
            {
                BypassCustomPluginExecution = BypassCustomPluginExecution,
                DataSource = DataSource,
                Index = Index,
                Length = Length,
                LogicalName = LogicalName,
                MaxDOP = MaxDOP,
                PrimaryIdSource = PrimaryIdSource,
                SecondaryIdSource = SecondaryIdSource,
                Source = (IExecutionPlanNodeInternal)Source.Clone(),
                Sql = Sql
            };

            clone.Source.Parent = clone;
            return clone;
        }
    }
}
