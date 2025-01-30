using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Linq;
using System.ServiceModel;
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
        /// The columns to use to identify the records to update and the associated column to take the new value from
        /// </summary>
        [Category("Delete")]
        [Description("The columns to use to identify the records to delete and the associated column to take the value from")]
        [DisplayName("PrimaryId Mappings")]
        public List<AttributeAccessor> PrimaryIdAccessors { get; set; }

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
            AddRequiredColumns(requiredColumns, PrimaryIdAccessors);

            Source.AddRequiredColumns(context, requiredColumns);
        }

        public override IRootExecutionPlanNodeInternal[] FoldQuery(NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            var result = base.FoldQuery(context, hints);

            if (result.Length != 1 || result[0] != this)
                return result;

            if (!context.Session.DataSources.TryGetValue(DataSource, out var dataSource))
                throw new NotSupportedQueryFragmentException("Missing datasource " + DataSource);

            // Use bulk delete if requested & possible
            if ((context.Options.UseBulkDelete || LogicalName == "audit") &&
                Source is FetchXmlScan fetch &&
                LogicalName == fetch.Entity.name &&
                PrimaryIdAccessors.Count == 1)
            {
                var metadata = dataSource.Metadata[LogicalName];

                if (PrimaryIdAccessors[0].TargetAttribute == metadata.PrimaryIdAttribute &&
                    PrimaryIdAccessors[0].SourceAttributes.Single() == $"{fetch.Alias}.{dataSource.Metadata[LogicalName].PrimaryIdAttribute}")
                {
                    return new[] { new BulkDeleteJobNode { DataSource = DataSource, Source = fetch } };
                }
            }

            // Replace a source query with a list of known IDs if possible
            FoldIdsToConstantScan(context, hints, LogicalName, PrimaryIdAccessors);

            return new[] { this };
        }

        public override void Execute(NodeExecutionContext context, out int recordsAffected, out string message)
        {
            _executionCount++;

            try
            {
                if (!context.Session.DataSources.TryGetValue(DataSource, out var dataSource))
                    throw new QueryExecutionException("Missing datasource " + DataSource);

                List<Entity> entities;
                DataTable dataTable;
                EntityMetadata meta;
                var eec = new ExpressionExecutionContext(context);

                using (_timer.Run())
                {
                    entities = GetDmlSourceEntities(context, out var schema);
                    dataTable = context.Session.TempDb.Tables[LogicalName];
                    meta = dataTable == null ? dataSource.Metadata[LogicalName] : null;
                }

                // Check again that the update is allowed. Don't count any UI interaction in the execution time
                var confirmArgs = dataTable != null
                    ? new ConfirmDmlStatementEventArgs(entities.Count, dataTable, BypassCustomPluginExecution)
                    : new ConfirmDmlStatementEventArgs(entities.Count, meta, BypassCustomPluginExecution);
                if (context.Options.CancellationToken.IsCancellationRequested)
                    confirmArgs.Cancel = true;
                context.Options.ConfirmDelete(confirmArgs);
                if (confirmArgs.Cancel)
                    throw new QueryExecutionException(new Sql4CdsError(11, 0, 0, null, null, 0, "DELETE cancelled by user", null));

                using (_timer.Run())
                {
                    var operationNames = new OperationNames
                    {
                        InProgressUppercase = "Deleting",
                        InProgressLowercase = "deleting",
                        CompletedLowercase = "deleted",
                        CompletedUppercase = "Deleted"
                    };

                    if (dataTable != null)
                    {
                        ExecuteTableOperation(
                            context,
                            entities,
                            dataTable,
                            entity =>
                            {
                                eec.Entity = entity;
                                var primaryId = PrimaryIdAccessors.Single().Accessor(eec);
                                var row = dataTable.Rows.Find(primaryId);

                                if (row == null)
                                    return false;

                                dataTable.Rows.Remove(row);
                                return true;
                            },
                            operationNames,
                            out recordsAffected,
                            out message);
                    }
                    else
                    {
                        ExecuteDmlOperation(
                            dataSource,
                            context.Options,
                            entities,
                            meta,
                            entity =>
                            {
                                eec.Entity = entity;
                                return CreateDeleteRequest(meta, eec, PrimaryIdAccessors.ToDictionary(a => a.TargetAttribute, a => a.Accessor), dataSource);
                            },
                            operationNames,
                            context,
                            out recordsAffected,
                            out message);
                    }
                }
            }
            catch (QueryExecutionException ex)
            {
                if (ex.Node == null)
                    ex.Node = this;

                throw;
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new QueryExecutionException(ex.Message, ex) { Node = this };
            }
        }

        private OrganizationRequest CreateDeleteRequest(EntityMetadata meta, ExpressionExecutionContext context, Dictionary<string, Func<ExpressionExecutionContext, object>> attributeAccessors, DataSource dataSource)
        {
            // Special case messages for intersect entities
            if (meta.LogicalName == "principalobjectaccess")
            {
                var objectId = (EntityReference)attributeAccessors["objectid"](context);
                var principalId = (EntityReference)attributeAccessors["principalid"](context);

                return new RevokeAccessRequest
                {
                    Target = objectId,
                    Revokee = principalId
                };
            }
            else if (meta.LogicalName == "listmember")
            {
                return new RemoveMemberListRequest
                {
                    ListId = (Guid)attributeAccessors["listid"](context),
                    EntityId = (Guid)attributeAccessors["entityid"](context)
                };
            }
            else if (meta.LogicalName == "solutioncomponent")
            {
                return new RemoveSolutionComponentRequest
                {
                    ComponentId = (Guid)attributeAccessors["objectid"](context),
                    ComponentType = ((OptionSetValue)attributeAccessors["componenttype"](context)).Value,
                    SolutionUniqueName = GetSolutionName(((EntityReference)attributeAccessors["solutionid"](context)).Id, dataSource)
                };
            }
            else if (meta.IsIntersect == true)
            {
                var relationship = meta.ManyToManyRelationships.Single();

                var targetId = (Guid)attributeAccessors[relationship.Entity1IntersectAttribute](context);
                var relatedId = (Guid)attributeAccessors[relationship.Entity2IntersectAttribute](context);

                return new DisassociateRequest
                {
                    Target = new EntityReference(relationship.Entity1LogicalName, targetId),
                    RelatedEntities = new EntityReferenceCollection { new EntityReference(relationship.Entity2LogicalName, relatedId) },
                    Relationship = new Relationship(relationship.SchemaName) { PrimaryEntityRole = EntityRole.Referencing }
                };
            }

            var id = (Guid)attributeAccessors[meta.PrimaryIdAttribute](context);
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
                        ["partitionid"] = attributeAccessors["partitionid"](context)
                    }
                };
            }

            // Special case for activitypointer - need to set the specific activity type code
            if (LogicalName == "activitypointer")
                req.Target.LogicalName = (string)attributeAccessors["activitytypecode"](context);

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
                PrimaryIdAccessors = PrimaryIdAccessors,
                Source = (IDataExecutionPlanNodeInternal)Source.Clone(),
                Sql = Sql
            };

            clone.Source.Parent = clone;
            return clone;
        }
    }
}
