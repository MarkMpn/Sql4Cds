using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlTypes;
using System.Linq;
using System.ServiceModel;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Crm.Sdk.Messages;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Metadata;
using Newtonsoft.Json;

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
        /// The columns that are used for the delete requests and their corresponding source columns
        /// </summary>
        [Category("Delete")]
        [Description("The columns that are used for the delete requests and their corresponding source columns")]
        [DisplayName("Column Mappings")]
        public Dictionary<string, string> ColumnMappings { get; set; }

        [Category("Delete")]
        public override int MaxDOP { get; set; }

        [Category("Delete")]
        public override int BatchSize { get; set; }

        [Category("Delete")]
        public override bool BypassCustomPluginExecution { get; set; }

        [Category("Delete")]
        public override bool ContinueOnError { get; set; }

        protected override bool IgnoresSomeErrors => true;

        public override void AddRequiredColumns(NodeCompilationContext context, IList<string> requiredColumns)
        {
            foreach (var col in ColumnMappings.Values)
            {
                if (!requiredColumns.Contains(col))
                    requiredColumns.Add(col);
            }

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
                ColumnMappings.Count == 1)
            {
                var metadata = dataSource.Metadata[LogicalName];

                if (ColumnMappings.TryGetValue(metadata.PrimaryIdAttribute, out var primaryIdSource) &&
                    primaryIdSource == $"{fetch.Alias}.{dataSource.Metadata[LogicalName].PrimaryIdAttribute}")
                {
                    return new[] { new BulkDeleteJobNode { DataSource = DataSource, Source = fetch } };
                }
            }

            return new[] { this };
        }

        protected override void RenameSourceColumns(IDictionary<string, string> columnRenamings)
        {
            var renamedMappings = new Dictionary<string, string>();

            foreach (var mapping in ColumnMappings)
            {
                if (columnRenamings.TryGetValue(mapping.Value, out var renamed))
                    renamedMappings[mapping.Key] = renamed;
                else
                    renamedMappings[mapping.Key] = mapping.Value;
            }
        }

        public override void Execute(NodeExecutionContext context, out int recordsAffected, out string message)
        {
            _executionCount++;

            try
            {
                if (!context.DataSources.TryGetValue(DataSource, out var dataSource))
                    throw new QueryExecutionException("Missing datasource " + DataSource);

                List<Entity> entities;
                EntityMetadata meta;
                Dictionary<string, Func<Entity, object>> attributeAccessors;
                
                using (_timer.Run())
                {
                    entities = GetDmlSourceEntities(context, out var schema);

                    // Precompile mappings with type conversions
                    meta = dataSource.Metadata[LogicalName];
                    var dateTimeKind = context.Options.UseLocalTimeZone ? DateTimeKind.Local : DateTimeKind.Utc;

                    var mappingsToCompile = ColumnMappings;

                    // Entity type codes will be presented as a string but the mapping compilation will try to convert
                    // them to an int, so remove it and we can access the string directly
                    if (LogicalName == "principalobjectaccess")
                    {
                        mappingsToCompile = new Dictionary<string, string>(ColumnMappings);
                        mappingsToCompile.Remove("objecttypecode");
                        mappingsToCompile.Remove("principaltypecode");
                    }
                    else if (LogicalName == "activitypointer")
                    {
                        mappingsToCompile = new Dictionary<string, string>(ColumnMappings);
                        mappingsToCompile.Remove("activitytypecode");
                    }

                    attributeAccessors = CompileColumnMappings(dataSource, LogicalName, mappingsToCompile, schema, dateTimeKind, entities);
                }

                // Check again that the update is allowed. Don't count any UI interaction in the execution time
                var confirmArgs = new ConfirmDmlStatementEventArgs(entities.Count, meta, BypassCustomPluginExecution);
                if (context.Options.CancellationToken.IsCancellationRequested)
                    confirmArgs.Cancel = true;
                context.Options.ConfirmDelete(confirmArgs);
                if (confirmArgs.Cancel)
                    throw new QueryExecutionException(new Sql4CdsError(11, 0, 0, null, null, 0, "DELETE cancelled by user", null));

                using (_timer.Run())
                {
                    ExecuteDmlOperation(
                        dataSource,
                        context.Options,
                        entities,
                        meta,
                        entity => CreateDeleteRequest(meta, entity, attributeAccessors),
                        new OperationNames
                        {
                            InProgressUppercase = "Deleting",
                            InProgressLowercase = "deleting",
                            CompletedLowercase = "deleted"
                        },
                        context,
                        out recordsAffected,
                        out message);
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

        private OrganizationRequest CreateDeleteRequest(EntityMetadata meta, Entity entity, Dictionary<string,Func<Entity,object>> attributeAccessors)
        {
            if (meta.LogicalName == "principalobjectaccess")
            {
                var objectId = (Guid)attributeAccessors["objectid"](entity);
                var objectTypeCode = entity.GetAttributeValue<SqlString>(ColumnMappings["objecttypecode"]).Value;
                var principalId = (Guid)attributeAccessors["principalid"](entity);
                var principalTypeCode = entity.GetAttributeValue<SqlString>(ColumnMappings["principaltypecode"]).Value;

                return new RevokeAccessRequest
                {
                    Target = new EntityReference(objectTypeCode, objectId),
                    Revokee = new EntityReference(principalTypeCode, principalId)
                };
            }

            // Special case messages for intersect entities
            if (meta.LogicalName == "listmember")
            {
                return new RemoveMemberListRequest
                {
                    ListId = (Guid)attributeAccessors["listid"](entity),
                    EntityId = (Guid)attributeAccessors["entityid"](entity)
                };
            }
            else if (meta.IsIntersect == true)
            {
                var relationship = meta.ManyToManyRelationships.Single();

                var targetId = (Guid)attributeAccessors[relationship.Entity1IntersectAttribute](entity);
                var relatedId = (Guid)attributeAccessors[relationship.Entity2IntersectAttribute](entity);

                return new DisassociateRequest
                {
                    Target = new EntityReference(relationship.Entity1LogicalName, targetId),
                    RelatedEntities = new EntityReferenceCollection { new EntityReference(relationship.Entity2LogicalName, relatedId) },
                    Relationship = new Relationship(relationship.SchemaName) { PrimaryEntityRole = EntityRole.Referencing }
                };
            }

            var id = (Guid)attributeAccessors[meta.PrimaryIdAttribute](entity);
            var req = new DeleteRequest
            {
                Target = new EntityReference(LogicalName, id)
            };

            // Special case for elastic entities - partitionid is required as part of the key
            if (meta.DataProviderId == DataProviders.ElasticDataProvider)
            {
                req.Target = new EntityReference(LogicalName)
                {
                    KeyAttributes =
                    {
                        [meta.PrimaryIdAttribute] = id,
                        ["partitionid"] = attributeAccessors["partitionid"](entity)
                    }
                };
            }

            // Special case for activitypointer - need to set the specific activity type code
            if (LogicalName == "activitypointer")
                req.Target.LogicalName = entity.GetAttributeValue<SqlString>(ColumnMappings["activitytypecode"]).Value;

            return req;
        }

        protected override bool FilterErrors(NodeExecutionContext context, OrganizationRequest request, OrganizationServiceFault fault)
        {
            // Ignore errors trying to delete records that don't exist - record may have been deleted by another
            // process in parallel.
            return fault.ErrorCode != -2147185406 && // IsvAbortedNotFound
                fault.ErrorCode != -2147220969 && // ObjectDoesNotExist
                fault.ErrorCode != 404; // Elastic tables
        }

        protected override ExecuteMultipleResponse ExecuteMultiple(DataSource dataSource, IOrganizationService org, EntityMetadata meta, ExecuteMultipleRequest req)
        {
            if (!req.Requests.All(r => r is DeleteRequest))
                return base.ExecuteMultiple(dataSource, org, meta, req);

            if (meta.DataProviderId == DataProviders.ElasticDataProvider
                && req.Requests.Cast<DeleteRequest>().GroupBy(r => r.Target.LogicalName).Count() == 1
                // DeleteMultiple is only supported on elastic tables, even if other tables do define the message
                /* || dataSource.MessageCache.IsMessageAvailable(meta.LogicalName, "DeleteMultiple")*/)
            {
                // Elastic tables can use DeleteMultiple for better performance than ExecuteMultiple
                var entities = new EntityReferenceCollection();

                foreach (DeleteRequest delete in req.Requests)
                    entities.Add(delete.Target);

                var deleteMultiple = new OrganizationRequest("DeleteMultiple")
                {
                    ["Targets"] = entities
                };

                if (BypassCustomPluginExecution)
                    deleteMultiple["BypassCustomPluginExecution"] = true;

                try
                {
                    dataSource.Execute(org, deleteMultiple);

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
                catch (FaultException<OrganizationServiceFault> ex)
                {
                    // https://learn.microsoft.com/en-us/power-apps/developer/data-platform/org-service/use-createmultiple-updatemultiple?tabs=sdk#no-continue-on-error

                    // If this is an elastic table we can extract the individual errors from the response
                    if (ex.Detail.ErrorDetails.TryGetValue("Plugin.BulkApiErrorDetails", out var errorDetails))
                    {
                        var details = JsonConvert.DeserializeObject<BulkApiErrorDetail[]>((string)errorDetails);

                        var multipleResp = new ExecuteMultipleResponse
                        {
                            ["Responses"] = new ExecuteMultipleResponseItemCollection()
                        };

                        foreach (var detail in details)
                        {
                            multipleResp.Responses.Add(new ExecuteMultipleResponseItem
                            {
                                RequestIndex = detail.RequestIndex,
                                Response = detail.StatusCode >= 200 && detail.StatusCode < 300 ? new DeleteResponse() : null,
                                Fault = detail.StatusCode >= 200 && detail.StatusCode < 300 ? null : new OrganizationServiceFault
                                {
                                    ErrorCode = detail.StatusCode,
                                    Message = ex.Message
                                }
                            });
                        }

                        return multipleResp;
                    }
                    else if (req.Requests.Count == 1)
                    {
                        // We only have one request so the error must have come from that
                        var multipleResp = new ExecuteMultipleResponse
                        {
                            ["Responses"] = new ExecuteMultipleResponseItemCollection()
                        };

                        multipleResp.Responses.Add(new ExecuteMultipleResponseItem
                        {
                            RequestIndex = 0,
                            Response = null,
                            Fault = ex.Detail
                        });

                        return multipleResp;
                    }
                    else
                    {
                        // We can't get the individual errors, so fall back to ExecuteMultiple
                    }
                }
            }

            return base.ExecuteMultiple(dataSource, org, meta, req);
        }

        public override string ToString()
        {
            return "DELETE";
        }

        public override object Clone()
        {
            var clone = new DeleteNode
            {
                BatchSize = BatchSize,
                BypassCustomPluginExecution = BypassCustomPluginExecution,
                ContinueOnError = ContinueOnError,
                DataSource = DataSource,
                Index = Index,
                Length = Length,
                LogicalName = LogicalName,
                MaxDOP = MaxDOP,
                ColumnMappings = ColumnMappings,
                Source = (IExecutionPlanNodeInternal)Source.Clone(),
                Sql = Sql
            };

            clone.Source.Parent = clone;
            return clone;
        }
    }
}
