using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.ServiceModel;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;
using Microsoft.Crm.Sdk.Messages;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Metadata;
using Newtonsoft.Json;

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

        [Category("Insert")]
        public override int MaxDOP { get; set; }

        [Category("Insert")]
        public override int BatchSize { get; set; }

        [Category("Insert")]
        public override bool BypassCustomPluginExecution { get; set; }

        [Category("Insert")]
        public override bool ContinueOnError { get; set; }

        [Category("Insert")]
        [DisplayName("Ignore Duplicate Key")]
        [Description("Ignores any duplicate key errors encountered. Errors will be logged but the query will complete.")]
        public bool IgnoreDuplicateKey { get; set; }

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
            IgnoreDuplicateKey = GetIgnoreDuplicateKey(context, hints);

            return base.FoldQuery(context, hints);
        }

        private bool GetIgnoreDuplicateKey(NodeCompilationContext context, IList<OptimizerHint> queryHints)
        {
            var ignoreDupKey = GetIgnoreDuplicateKeyHint(queryHints);

            if (!ignoreDupKey && LogicalName == "listmember")
            {
                ignoreDupKey = true;
                context.Log(new Sql4CdsError(10, LineNumber, 0, null, context.DataSources[DataSource].Name, 0, "Duplicate entries will be silently ignored for listmember inserts"));
            }

            return ignoreDupKey;
        }

        private bool GetIgnoreDuplicateKeyHint(IList<OptimizerHint> queryHints)
        {
            if (queryHints == null)
                return false;

            var ignoreDupKey = queryHints
                .OfType<UseHintList>()
                .Where(hint => hint.Hints.Any(s => s.Value.Equals("IGNORE_DUP_KEY", StringComparison.OrdinalIgnoreCase)))
                .Any();

            return ignoreDupKey;
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
                Dictionary<string, AttributeMetadata> attributes;
                Dictionary<string, Func<Entity, object>> attributeAccessors;
                Func<Entity, object> primaryIdAccessor;

                using (_timer.Run())
                {
                    entities = GetDmlSourceEntities(context, out var schema);

                    // Precompile mappings with type conversions
                    meta = dataSource.Metadata[LogicalName];
                    attributes = meta.Attributes.ToDictionary(a => a.LogicalName, StringComparer.OrdinalIgnoreCase);
                    var dateTimeKind = context.Options.UseLocalTimeZone ? DateTimeKind.Local : DateTimeKind.Utc;
                    attributeAccessors = CompileColumnMappings(dataSource, LogicalName, ColumnMappings, schema, dateTimeKind, entities);
                    attributeAccessors.TryGetValue(meta.PrimaryIdAttribute, out primaryIdAccessor);
                }

                // Check again that the update is allowed. Don't count any UI interaction in the execution time
                var confirmArgs = new ConfirmDmlStatementEventArgs(entities.Count, meta, BypassCustomPluginExecution);
                if (context.Options.CancellationToken.IsCancellationRequested)
                    confirmArgs.Cancel = true;
                context.Options.ConfirmInsert(confirmArgs);
                if (confirmArgs.Cancel)
                    throw new OperationCanceledException("INSERT cancelled by user");

                using (_timer.Run())
                {
                    ExecuteDmlOperation(
                        dataSource,
                        context.Options,
                        entities,
                        meta,
                        entity => CreateInsertRequest(meta, entity, attributeAccessors, primaryIdAccessor, attributes),
                        new OperationNames
                        {
                            InProgressUppercase = "Inserting",
                            InProgressLowercase = "inserting",
                            CompletedLowercase = "inserted"
                        },
                        context,
                        out recordsAffected,
                        out message,
                        LogicalName == "listmember" || meta.IsIntersect == true ? null : (Action<OrganizationResponse>) ((r) => SetIdentity(r, context.ParameterValues))
                        );
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
                    throw new QueryExecutionException(new Sql4CdsError(16, 515, "Cannot insert value NULL into listmember.listid"));

                if (entityId == null)
                    throw new QueryExecutionException(new Sql4CdsError(16, 515, "Cannot insert value NULL into listmember.entityid"));

                return new AddMemberListRequest
                {
                    ListId = listId.Value,
                    EntityId = entityId.Value
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
                    throw new QueryExecutionException(new Sql4CdsError(16, 515, $"Cannot insert value NULL into {relationship.Entity1IntersectAttribute}"));

                if (e2 == null)
                    throw new QueryExecutionException(new Sql4CdsError(16, 515, $"Cannot insert value NULL into {relationship.Entity2IntersectAttribute}"));

                return new AssociateRequest
                {
                    Target = new EntityReference(relationship.Entity1LogicalName, e1.Value),
                    Relationship = new Relationship(relationship.SchemaName) { PrimaryEntityRole = EntityRole.Referencing },
                    RelatedEntities = new EntityReferenceCollection { new EntityReference(relationship.Entity2LogicalName, e2.Value) }
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

        protected override bool FilterErrors(NodeExecutionContext context, OrganizationRequest request, OrganizationServiceFault fault)
        {
            if (IgnoreDuplicateKey)
            {
                if (fault.ErrorCode == -2147220937 || fault.ErrorCode == -2147088238 || fault.ErrorCode == 409)
                {
                    var logMessage = "Ignoring duplicate key error";

                    if (fault.ErrorCode == -2147088238)
                    {
                        // Duplicate alternate key. The duplicated values are available in the fault details
                        if (fault.ErrorDetails.TryGetValue("DuplicateAttributes", out var value) &&
                            value is string duplicateAttributes)
                        {
                            var xml = new XmlDocument();
                            xml.LoadXml(duplicateAttributes);

                            logMessage += $". The duplicate values were ({String.Join(", ", xml.SelectNodes("/DuplicateAttributes/*").OfType<XmlElement>().Select(attr => attr.InnerText))})";
                        }
                    }
                    else
                    {
                        // Duplicate primary key.
                        if (request is AssociateRequest associate)
                            logMessage += $". The duplicate values were ({associate.Target.Id}, {associate.RelatedEntities[0].Id})";
                        else if (request is CreateRequest create)
                            logMessage += $". The duplicate values were ({create.Target.Id})";
                    }

                    context.Log(new Sql4CdsError(10, LineNumber, 0, null, context.DataSources[DataSource].Name, 0, logMessage));
                    return false;
                }
            }

            return true;
        }

        private void SetIdentity(OrganizationResponse response, IDictionary<string, object> parameterValues)
        {
            var create = (CreateResponse)response;
            parameterValues["@@IDENTITY"] = new SqlEntityReference(DataSource, LogicalName, create.id);
        }

        protected override void RenameSourceColumns(IDictionary<string, string> columnRenamings)
        {
            foreach (var kvp in ColumnMappings.ToList())
            {
                if (columnRenamings.TryGetValue(kvp.Value, out var renamed))
                    ColumnMappings[kvp.Key] = renamed;
            }
        }

        protected override ExecuteMultipleResponse ExecuteMultiple(DataSource dataSource, IOrganizationService org, EntityMetadata meta, ExecuteMultipleRequest req)
        {
            if (!req.Requests.All(r => r is CreateRequest))
                return base.ExecuteMultiple(dataSource, org, meta, req);

            if (meta.DataProviderId == DataProviders.ElasticDataProvider || dataSource.MessageCache.IsMessageAvailable(meta.LogicalName, "CreateMultiple"))
            {
                // Elastic tables can use CreateMultiple for better performance than ExecuteMultiple
                var entities = new EntityCollection { EntityName = meta.LogicalName };

                foreach (CreateRequest create in req.Requests)
                    entities.Entities.Add(create.Target);

                var createMultiple = new OrganizationRequest("CreateMultiple")
                {
                    ["Targets"] = entities
                };

                if (BypassCustomPluginExecution)
                    createMultiple["BypassCustomPluginExecution"] = true;

                try
                {
                    var resp = dataSource.Execute(org, createMultiple);
                    var ids = (Guid[])resp["Ids"];

                    var multipleResp = new ExecuteMultipleResponse
                    {
                        ["Responses"] = new ExecuteMultipleResponseItemCollection()
                    };

                    for (var i = 0; i < ids.Length; i++)
                    {
                        multipleResp.Responses.Add(new ExecuteMultipleResponseItem
                        {
                            RequestIndex = i,
                            Response = new CreateResponse { ["id"] = ids[i] }
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
                                Response = detail.StatusCode >= 200 && detail.StatusCode < 300 ? new CreateResponse { ["id"] = detail.Id } : null,
                                Fault = detail.StatusCode >= 200 && detail.StatusCode < 300 ? null : new OrganizationServiceFault
                                {
                                    ErrorCode = detail.StatusCode,
                                    Message = ex.Message
                                }
                            });
                        }

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
            return "INSERT";
        }

        public override object Clone()
        {
            var clone = new InsertNode
            {
                BatchSize = BatchSize,
                BypassCustomPluginExecution = BypassCustomPluginExecution,
                ContinueOnError = ContinueOnError,
                DataSource = DataSource,
                Index = Index,
                Length = Length,
                LogicalName = LogicalName,
                MaxDOP = MaxDOP,
                IgnoreDuplicateKey = IgnoreDuplicateKey,
                Source = (IExecutionPlanNodeInternal)Source.Clone(),
                Sql = Sql,
                LineNumber = LineNumber,
            };

            foreach (var kvp in ColumnMappings)
                clone.ColumnMappings.Add(kvp);

            clone.Source.Parent = clone;

            return clone;
        }
    }
}
