using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.SqlTypes;
using System.Linq;
using System.ServiceModel;
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
        public List<AttributeAccessor> Accessors { get; set; }

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
            AddRequiredColumns(requiredColumns, Accessors);

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
                context.Log(new Sql4CdsError(10, LineNumber, 0, null, context.Session.DataSources[DataSource].Name, 0, "Duplicate entries will be silently ignored for listmember inserts"));
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
                if (!context.Session.DataSources.TryGetValue(DataSource, out var dataSource))
                    throw new QueryExecutionException("Missing datasource " + DataSource);

                List<Entity> entities;
                DataTable dataTable;
                EntityMetadata meta;
                Dictionary<string, AttributeMetadata> attributes;
                Dictionary<string, Func<ExpressionExecutionContext, object>> attributeAccessors;
                Func<ExpressionExecutionContext, object> primaryIdAccessor;
                var eec = new ExpressionExecutionContext(context);

                using (_timer.Run())
                {
                    entities = GetDmlSourceEntities(context, out var schema);
                    dataTable = context.Session.TempDb.Tables[LogicalName];
                    meta = dataTable == null ? dataSource.Metadata[LogicalName] : null;
                    attributes = meta?.Attributes.ToDictionary(a => a.LogicalName, StringComparer.OrdinalIgnoreCase);
                    var dateTimeKind = context.Options.UseLocalTimeZone ? DateTimeKind.Local : DateTimeKind.Utc;
                    attributeAccessors = Accessors.ToDictionary(a => a.TargetAttribute, a => a.Accessor);

                    if (dataTable != null && dataTable.PrimaryKey.Length == 1)
                        attributeAccessors.TryGetValue(dataTable.PrimaryKey[0].ColumnName, out primaryIdAccessor);
                    else if (meta != null)
                        attributeAccessors.TryGetValue(meta.PrimaryIdAttribute, out primaryIdAccessor);
                    else
                        primaryIdAccessor = null;
                }

                // Check again that the insert is allowed. Don't count any UI interaction in the execution time
                var confirmArgs = dataTable != null
                    ? new ConfirmDmlStatementEventArgs(entities.Count, dataTable, BypassCustomPluginExecution)
                    : new ConfirmDmlStatementEventArgs(entities.Count, meta, BypassCustomPluginExecution);
                if (context.Options.CancellationToken.IsCancellationRequested)
                    confirmArgs.Cancel = true;
                context.Options.ConfirmInsert(confirmArgs);
                if (confirmArgs.Cancel)
                    throw new QueryExecutionException(new Sql4CdsError(11, 0, 0, null, null, 0, "INSERT cancelled by user", null));

                var operationNames = new OperationNames
                {
                    InProgressUppercase = "Inserting",
                    InProgressLowercase = "inserting",
                    CompletedLowercase = "inserted",
                    CompletedUppercase = "Inserted",
                };

                using (_timer.Run())
                {
                    if (dataTable != null)
                    {
                        ExecuteTableOperation(
                            context,
                            entities,
                            dataTable,
                            entity =>
                            {
                                eec.Entity = entity;
                                dataTable.Rows.Add(CreateDataRow(dataTable, eec, attributeAccessors));
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
                                return CreateInsertRequest(meta, eec, attributeAccessors, primaryIdAccessor, attributes, dataSource);
                            },
                            operationNames,
                            context,
                            out recordsAffected,
                            out message,
                            r => SetIdentity(r, context.ParameterValues)
                            );
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

        private DataRow CreateDataRow(DataTable dataTable, ExpressionExecutionContext eec, Dictionary<string, Func<ExpressionExecutionContext, object>> attributeAccessors)
        {
            var row = dataTable.NewRow();

            foreach (var col in attributeAccessors)
                row[col.Key] = col.Value(eec);

            return row;
        }

        private OrganizationRequest CreateInsertRequest(EntityMetadata meta, ExpressionExecutionContext context, Dictionary<string,Func<ExpressionExecutionContext,object>> attributeAccessors, Func<ExpressionExecutionContext,object> primaryIdAccessor, Dictionary<string,AttributeMetadata> attributes, DataSource dataSource)
        {
            // Special cases for intersect entities
            if (LogicalName == "listmember")
            {
                var listId = GetNotNull<Guid>("listid", context, attributeAccessors);
                var entityId = GetNotNull<Guid>("entityid", context, attributeAccessors);

                return new AddMemberListRequest
                {
                    ListId = listId,
                    EntityId = entityId
                };
            }
            
            if (meta.IsIntersect == true)
            {
                // For generic intersect entities we expect a single many-to-many relationship in the metadata which describes
                // the relationship that this is the intersect entity for
                var relationship = meta.ManyToManyRelationships.Single();

                var e1 = GetNotNull<Guid>(relationship.Entity1IntersectAttribute, context, attributeAccessors);
                var e2 = GetNotNull<Guid>(relationship.Entity2IntersectAttribute, context, attributeAccessors);

                return new AssociateRequest
                {
                    Target = new EntityReference(relationship.Entity1LogicalName, e1),
                    Relationship = new Relationship(relationship.SchemaName) { PrimaryEntityRole = EntityRole.Referencing },
                    RelatedEntities = new EntityReferenceCollection { new EntityReference(relationship.Entity2LogicalName, e2) }
                };
            }

            if (LogicalName == "principalobjectaccess")
            {
                // Insert into principalobjectaccess is equivalent to a share
                var objectId = GetNotNull<EntityReference>("objectid", context, attributeAccessors);
                var principalId = GetNotNull<EntityReference>("principalid", context, attributeAccessors);
                var accessRightsMask = GetNotNull<int>("accessrightsmask", context, attributeAccessors);

                return new GrantAccessRequest
                {
                    Target = objectId,
                    PrincipalAccess = new PrincipalAccess
                    {
                        Principal = principalId,
                        AccessMask = (AccessRights)accessRightsMask
                    }
                };
            }

            if (LogicalName == "solutioncomponent")
            {
                var componentId = GetNotNull<Guid>("objectid", context, attributeAccessors);
                var componentType = GetNotNull<OptionSetValue>("componenttype", context, attributeAccessors);
                var solutionId = GetNotNull<EntityReference>("solutionid", context, attributeAccessors);
                OptionSetValue rootComponentBehavior = null;
                if (attributeAccessors.TryGetValue("rootcomponentbehavior", out var accessor))
                    rootComponentBehavior = (OptionSetValue)accessor(context);

                return new AddSolutionComponentRequest
                {
                    ComponentId = componentId,
                    ComponentType = componentType.Value,
                    SolutionUniqueName = GetSolutionName(solutionId.Id, dataSource),
                    DoNotIncludeSubcomponents = rootComponentBehavior != null && rootComponentBehavior.Value != 0,
                    IncludedComponentSettingsValues = rootComponentBehavior != null && rootComponentBehavior.Value == 2 ? Array.Empty<string>() : null,
                    AddRequiredComponents = false
                };
            }

            var insert = new Entity(LogicalName);

            if (primaryIdAccessor != null)
                insert.Id = (Guid) primaryIdAccessor(context);

            foreach (var attributeAccessor in attributeAccessors)
            {
                if (attributeAccessor.Key == meta.PrimaryIdAttribute)
                    continue;

                var attr = attributes[attributeAccessor.Key];

                if (!String.IsNullOrEmpty(attr.AttributeOf))
                    continue;

                var value = attributeAccessor.Value(context);

                insert[attr.LogicalName] = value;
            }

            return new CreateRequest { Target = insert };
        }

        private T GetNotNull<T>(string attribute, ExpressionExecutionContext context, Dictionary<string, Func<ExpressionExecutionContext, object>> attributeAccessors)
        {
            var value = attributeAccessors[attribute](context);

            if (value == null)
                throw new QueryExecutionException(Sql4CdsError.NotNullInsert(new Identifier { Value = attribute }, new Identifier { Value = LogicalName }, "Insert"));

            return (T)value;
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

                    context.Log(new Sql4CdsError(10, LineNumber, 0, null, context.Session.DataSources[DataSource].Name, 0, logMessage));
                    return false;
                }
            }

            return true;
        }

        private void SetIdentity(OrganizationResponse response, IDictionary<string, INullable> parameterValues)
        {
            if (response is CreateResponse create)
                parameterValues["@@IDENTITY"] = new SqlEntityReference(DataSource, LogicalName, create.id);
        }

        protected override ExecuteMultipleResponse ExecuteMultiple(DataSource dataSource, IOrganizationService org, EntityMetadata meta, ExecuteMultipleRequest req)
        {
            if (!req.Requests.All(r => r is CreateRequest))
                return base.ExecuteMultiple(dataSource, org, meta, req);

            if (meta.DataProviderId == DataProviders.ElasticDataProvider || meta.DataProviderId == null && dataSource.MessageCache.IsMessageAvailable(meta.LogicalName, "CreateMultiple"))
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
                Accessors = Accessors,
                Sql = Sql,
                LineNumber = LineNumber,
            };

            clone.Source.Parent = clone;

            return clone;
        }
    }
}
