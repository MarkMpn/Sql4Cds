using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.ServiceModel;
using System.Threading;
using Microsoft.Crm.Sdk.Messages;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Metadata;
using Newtonsoft.Json;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// Implements an UPDATE operation
    /// </summary>
    class UpdateNode : BaseDmlNode
    {
        private int _executionCount;
        private readonly Timer _timer = new Timer();

        public override int ExecutionCount => _executionCount;

        public override TimeSpan Duration => _timer.Duration;

        /// <summary>
        /// The logical name of the entity to update
        /// </summary>
        [Category("Update")]
        [Description("The logical name of the entity to update")]
        public string LogicalName { get; set; }

        /// <summary>
        /// The columns to use to identify the records to update and the associated column to take the new value from
        /// </summary>
        [Category("Update")]
        [Description("The columns to use to identify the records to update and the associated column to take the new value from")]
        [DisplayName("PrimaryId Mappings")]
        public List<AttributeAccessor> PrimaryIdAccessors { get; set; }

        /// <summary>
        /// The columns to update and the associated column to take the new value from
        /// </summary>
        [Category("Update")]
        [Description("The columns to update and the associated column to take the new value from")]
        [DisplayName("New Column Mappings")]
        public List<AttributeAccessor> NewValueAccessors { get; set; }

        /// <summary>
        /// The columns to update and the associated column to take the existing value from
        /// </summary>
        [Category("Update")]
        [Description("The columns to update and the associated column to take the existing value from")]
        [DisplayName("Existing Column Mappings")]
        public List<AttributeAccessor> ExistingValueAccessors { get; set; }

        [Category("Update")]
        public override int MaxDOP { get; set; }

        [Category("Update")]
        public override int BatchSize { get; set; }

        [Category("Update")]
        public override bool BypassCustomPluginExecution { get; set; }

        [Category("Update")]
        public override bool ContinueOnError { get; set; }

        [Browsable(false)]
        public IDictionary<int, StatusWithState> StateTransitions { get; set; }

        [Category("Update")]
        [Description("The state transition graph that will be navigated automatically when applying updates")]
        [DisplayName("State Transitions")]
        public IDictionary<string, Transitions> StateTransitionsDisplay => StateTransitions == null ? null : StateTransitions.Values.ToDictionary(s => $"{s.Name} ({s.StatusCode})", s => new Transitions(s.Transitions.Keys.Select(t => $"{t.Name} ({t.StatusCode})").OrderBy(n => n)));

        [Category("Update")]
        [DisplayName("Use Legacy Update Messages")]
        [Description("Use the legacy update messages for specialized attributes")]
        public bool UseLegacyUpdateMessages { get; set; }

        [Category("Update")]
        [DisplayName("Minimal Updates")]
        [Description("Only update attributes that have changed from their current values")]
        public bool MinimalUpdates { get; set; }

        protected override bool IgnoresSomeErrors => true;

        public override void AddRequiredColumns(NodeCompilationContext context, IList<string> requiredColumns)
        {
            AddRequiredColumns(requiredColumns, PrimaryIdAccessors);
            AddRequiredColumns(requiredColumns, NewValueAccessors);
            AddRequiredColumns(requiredColumns, ExistingValueAccessors);

            Source.AddRequiredColumns(context, requiredColumns);
        }

        public override IRootExecutionPlanNodeInternal[] FoldQuery(NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            UseLegacyUpdateMessages = GetUseLegacyUpdateMessages(context, hints);

            var result = base.FoldQuery(context, hints);

            if (result.Length != 1 || result[0] != this)
                return result;

            // If we don't need to use any of the original values and we're filtering by ID we can bypass reading
            // the record to update
            if (ExistingValueAccessors.Count == 0)
                FoldIdsToConstantScan(context, hints, LogicalName, PrimaryIdAccessors.Concat(NewValueAccessors).ToList());

            return new[] { this };
        }

        private bool GetUseLegacyUpdateMessages(NodeCompilationContext context, IList<OptimizerHint> queryHints)
        {
            if (queryHints == null)
                return false;

            var continueOnError = queryHints
                .OfType<UseHintList>()
                .Where(hint => hint.Hints.Any(s => s.Value.Equals("USE_LEGACY_UPDATE_MESSAGES", StringComparison.OrdinalIgnoreCase)))
                .Any();

            return continueOnError;
        }

        public override void Execute(NodeExecutionContext context, out int recordsAffected, out string message)
        {
            _executionCount++;

            try
            {
                if (!context.Session.DataSources.TryGetValue(DataSource, out var dataSource))
                    throw new QueryExecutionException("Missing datasource " + DataSource);

                List<Entity> entities;
                EntityMetadata meta;
                Dictionary<string, AttributeMetadata> attributes;
                Dictionary<string, Func<ExpressionExecutionContext, object>> newAttributeAccessors;
                Dictionary<string, Func<ExpressionExecutionContext, object>> oldAttributeAccessors;
                Dictionary<string, Func<ExpressionExecutionContext, object>> primaryIdAccessors;
                var eec = new ExpressionExecutionContext(context);

                using (_timer.Run())
                {
                    entities = GetDmlSourceEntities(context, out var schema);

                    // Precompile mappings with type conversions
                    meta = dataSource.Metadata[LogicalName];
                    attributes = meta.Attributes.ToDictionary(a => a.LogicalName, StringComparer.OrdinalIgnoreCase);
                    newAttributeAccessors = NewValueAccessors.ToDictionary(a => a.TargetAttribute, a => a.Accessor);
                    oldAttributeAccessors = ExistingValueAccessors.ToDictionary(a => a.TargetAttribute, a => a.Accessor);
                    primaryIdAccessors = PrimaryIdAccessors.ToDictionary(a => a.TargetAttribute, a => a.Accessor);
                }

                // Check again that the update is allowed. Don't count any UI interaction in the execution time
                var confirmArgs = new ConfirmDmlStatementEventArgs(entities.Count, meta, BypassCustomPluginExecution);
                if (context.Options.CancellationToken.IsCancellationRequested)
                    confirmArgs.Cancel = true;
                context.Options.ConfirmUpdate(confirmArgs);
                if (confirmArgs.Cancel)
                    throw new QueryExecutionException(new Sql4CdsError(11, 0, 0, null, null, 0, "UPDATE cancelled by user", null));

                var isSysAdminOrBackOfficeIntegrationUser = new Lazy<bool>(() =>
                {
                    // Check if the current user is a system administrator by looking for the known role guid
                    // https://learn.microsoft.com/en-us/power-apps/developer/data-platform/security-roles
                    var roleQry = new Microsoft.Xrm.Sdk.Query.QueryExpression("role");
                    roleQry.Criteria.AddCondition("roletemplateid", Microsoft.Xrm.Sdk.Query.ConditionOperator.Equal, new Guid("627090FF-40A3-4053-8790-584EDC5BE201"));
                    var userRoles = roleQry.AddLink("systemuserroles", "roleid", "roleid");
                    userRoles.LinkCriteria.AddCondition("systemuserid", Microsoft.Xrm.Sdk.Query.ConditionOperator.EqualUserId);
                    roleQry.ColumnSet = new Microsoft.Xrm.Sdk.Query.ColumnSet("roleid");
                    roleQry.TopCount = 1;
                    var sysAdminRoles = dataSource.Connection.RetrieveMultiple(roleQry).Entities;

                    if (sysAdminRoles.Any())
                        return true;

                    // If the current user is not a sysadmin, check if it is an integration user and SOP integration is enabled
                    // This only applies to salesorder and invoice, not quote
                    if (LogicalName == "quote")
                        return false;

                    var orgQry = new Microsoft.Xrm.Sdk.Query.QueryExpression("organization");
                    orgQry.ColumnSet = new Microsoft.Xrm.Sdk.Query.ColumnSet("issopintegrationenabled", "integrationuserid");
                    var org = dataSource.Connection.RetrieveMultiple(orgQry).Entities[0];
                    var isSOPIntegrationEnabled = org.GetAttributeValue<bool>("issopintegrationenabled");
                    var integrationUserId = org.GetAttributeValue<Guid>("integrationuserid");

                    if (!isSOPIntegrationEnabled)
                        return false;

                    var userQry = new Microsoft.Xrm.Sdk.Query.QueryExpression("systemuser");
                    userQry.Criteria.AddCondition("systemuserid", Microsoft.Xrm.Sdk.Query.ConditionOperator.EqualUserId);
                    userQry.ColumnSet = new Microsoft.Xrm.Sdk.Query.ColumnSet("isintegrationuser");
                    var user = dataSource.Connection.RetrieveMultiple(userQry).Entities[0];
                    var isIntegrationUser = user.GetAttributeValue<bool>("isintegrationuser");

                    return isIntegrationUser || integrationUserId == user.Id;
                }, LazyThreadSafetyMode.ExecutionAndPublication);

                using (_timer.Run())
                {
                    ExecuteDmlOperation(
                        dataSource,
                        context.Options,
                        entities,
                        meta,
                        entity =>
                        {
                            eec.Entity = entity;
                            var preImage = ExtractEntity(eec, meta, attributes, oldAttributeAccessors, primaryIdAccessors);
                            var update = ExtractEntity(eec, meta, attributes, newAttributeAccessors, primaryIdAccessors);

                            var requests = new OrganizationRequestCollection();

                            if (meta.IsIntersect == true)
                            {
                                ManyToManyRelationshipMetadata relationship;
                                string entity1IntersectAttribute;
                                string entity2IntersectAttribute;

                                if (meta.LogicalName == "listmember")
                                {
                                    relationship = null;
                                    entity1IntersectAttribute = "listid";
                                    entity2IntersectAttribute = "entityid";
                                }
                                else
                                {
                                    relationship = meta.ManyToManyRelationships.Single();
                                    entity1IntersectAttribute = relationship.Entity1IntersectAttribute;
                                    entity2IntersectAttribute = relationship.Entity2IntersectAttribute;
                                }

                                var e1Prev = preImage.GetAttributeValue<Guid?>(entity1IntersectAttribute);
                                var e2Prev = preImage.GetAttributeValue<Guid?>(entity2IntersectAttribute);
                                var e1New = update.GetAttributeValue<Guid?>(entity1IntersectAttribute);
                                var e2New = update.GetAttributeValue<Guid?>(entity2IntersectAttribute);

                                if (!update.Contains(entity1IntersectAttribute))
                                    e1New = e1Prev;
                                if (!update.Contains(entity2IntersectAttribute))
                                    e2New = e2Prev;

                                if (e1New == null)
                                    throw new QueryExecutionException(Sql4CdsError.NotNullInsert(new Identifier { Value = entity1IntersectAttribute }, new Identifier { Value = meta.LogicalName }, "Update"));

                                if (e2New == null)
                                    throw new QueryExecutionException(Sql4CdsError.NotNullInsert(new Identifier { Value = entity2IntersectAttribute }, new Identifier { Value = meta.LogicalName }, "Update"));

                                if (meta.LogicalName == "listmember")
                                {
                                    requests.Add(new RemoveMemberListRequest
                                    {
                                        ListId = e1Prev.Value,
                                        EntityId = e2Prev.Value
                                    });
                                    requests.Add(new AddMemberListRequest
                                    {
                                        ListId = e1New.Value,
                                        EntityId = e2New.Value
                                    });
                                }
                                else
                                {
                                    requests.Add(new DisassociateRequest
                                    {
                                        Target = new EntityReference(relationship.Entity1LogicalName, e1Prev.Value),
                                        RelatedEntities = new EntityReferenceCollection { new EntityReference(relationship.Entity2LogicalName, e2Prev.Value) },
                                        Relationship = new Relationship(relationship.SchemaName) { PrimaryEntityRole = EntityRole.Referencing }
                                    });
                                    requests.Add(new AssociateRequest
                                    {
                                        Target = new EntityReference(relationship.Entity1LogicalName, e1New.Value),
                                        RelatedEntities = new EntityReferenceCollection { new EntityReference(relationship.Entity2LogicalName, e2New.Value) },
                                        Relationship = new Relationship(relationship.SchemaName) { PrimaryEntityRole = EntityRole.Referencing }
                                    });
                                }
                            }
                            else if (meta.LogicalName == "principalobjectaccess")
                            {
                                var objectIdPrev = preImage.GetAttributeValue<EntityReference>("objectid");
                                var principalIdPrev = preImage.GetAttributeValue<EntityReference>("principalid");
                                var accessMaskPrev = (AccessRights)preImage.GetAttributeValue<int>("accessrightsmask");
                                var objectIdNew = update.GetAttributeValue<EntityReference>("objectid") ?? objectIdPrev;
                                var principalIdNew = update.GetAttributeValue<EntityReference>("principalid") ?? principalIdPrev;
                                var accessMaskNew = (AccessRights?)update.GetAttributeValue<int>("accessrightsmask") ?? accessMaskPrev;

                                // Check if we need to remove any previous share permissions
                                if (!objectIdPrev.Equals(objectIdNew) || !principalIdPrev.Equals(principalIdNew) || (accessMaskNew & accessMaskPrev) != accessMaskPrev)
                                {
                                    requests.Add(new RevokeAccessRequest
                                    {
                                        Target = objectIdPrev,
                                        Revokee = principalIdPrev
                                    });
                                }

                                // Check if we need to add any new share permissions
                                if (accessMaskNew != AccessRights.None)
                                {
                                    requests.Add(new GrantAccessRequest
                                    {
                                        Target = objectIdNew,
                                        PrincipalAccess = new PrincipalAccess
                                        {
                                            Principal = principalIdNew,
                                            AccessMask = accessMaskNew
                                        }
                                    });
                                }
                            }
                            else
                            {
                                var updateRequest = new UpdateRequest { Target = update };

                                var requestedState = update.GetAttributeValue<OptionSetValue>("statecode");
                                var requestedStatus = update.GetAttributeValue<OptionSetValue>("statuscode");
                                var currentState = preImage.GetAttributeValue<OptionSetValue>("statecode");
                                var currentStatus = preImage.GetAttributeValue<OptionSetValue>("statuscode");

                                if (requestedState == null && requestedStatus != null)
                                    requestedState = GetStateCode(meta, requestedStatus.Value);
                                else if (requestedState != null && requestedStatus == null)
                                    requestedStatus = GetDefaultStatusCode(meta, requestedState.Value);

                                if ((LogicalName == "quote" || LogicalName == "salesorder" || LogicalName == "invoice") &&
                                    currentState?.Value != 0 &&
                                    !isSysAdminOrBackOfficeIntegrationUser.Value)
                                {
                                    // QOI records can only be updated if they are in an editable state or the user is a sysadmin or integration user
                                    // Add a request to change the status back to editable before making the update, then change the status back again
                                    // afterwards
                                    var defaultDraftStatusCode = GetDefaultStatusCode(meta, 0);

                                    requests.Insert(0, new UpdateRequest
                                    {
                                        Target = new Entity(LogicalName, update.Id)
                                        {
                                            ["statecode"] = new OptionSetValue(0),
                                            ["statuscode"] = defaultDraftStatusCode
                                        }
                                    });

                                    if (requestedState == null)
                                    {
                                        requestedState = currentState;
                                        update["statecode"] = requestedState;
                                    }

                                    if (requestedStatus == null)
                                    {
                                        requestedStatus = currentStatus;
                                        update["statuscode"] = requestedStatus;
                                    }

                                    currentState = new OptionSetValue(0);
                                    currentStatus = defaultDraftStatusCode;
                                }

                                if ((requestedState != null || requestedStatus != null) && StateTransitions != null)
                                {
                                    update.Attributes.Remove("statecode");
                                    update.Attributes.Remove("statuscode");
                                }

                                if (MinimalUpdates)
                                {
                                    // Remove any attributes from the update target that have the same value in the pre image
                                    foreach (var attr in update.Attributes.Keys.ToList())
                                    {
                                        if (preImage.Contains(attr) && Equals(preImage[attr], update[attr]))
                                            update.Attributes.Remove(attr);
                                    }
                                }

                                if (update.Attributes.Any())
                                    requests.Add(updateRequest);

                                if (requestedStatus != null && StateTransitions != null)
                                    AddStateTransitions(update, currentStatus, requestedStatus, requests);

                                if (UseLegacyUpdateMessages)
                                {
                                    // Replace updates for special attributes with dedicated messages
                                    for (var i = 0; i < requests.Count; i++)
                                    {
                                        if (!(requests[i] is UpdateRequest updateReq))
                                            continue;

                                        if (updateReq.Target.Contains("ownerid"))
                                        {
                                            requests.Insert(i, new AssignRequest
                                            {
                                                Target = updateReq.Target.ToEntityReference(),
                                                Assignee = updateReq.Target.GetAttributeValue<EntityReference>("ownerid")
                                            });
                                            updateReq.Target.Attributes.Remove("ownerid");
                                            i++;
                                        }

                                        if (updateReq.Target.Contains("statecode") || updateReq.Target.Contains("statuscode"))
                                        {
                                            requests.Insert(i, new SetStateRequest
                                            {
                                                EntityMoniker = updateReq.Target.ToEntityReference(),
                                                State = requestedState ?? currentState,
                                                Status = requestedStatus ?? new OptionSetValue(-1)
                                            });
                                            updateReq.Target.Attributes.Remove("statecode");
                                            updateReq.Target.Attributes.Remove("statuscode");
                                            i++;
                                        }

                                        // SetParentSystemUserRequest SetParentTeamRequest, and SetBusinessSystemUserRequest have important extra parameters,
                                        // so don't automatically convert to them. They should be called using the stored procedure syntax instead.

                                        if (updateReq.Target.Contains("parentbusinessunitid") && updateReq.Target.LogicalName == "businessunitid")
                                        {
                                            requests.Insert(i, new SetParentBusinessUnitRequest
                                            {
                                                BusinessUnitId = updateReq.Target.Id,
                                                ParentId = updateReq.Target.GetAttributeValue<EntityReference>("parentbusinessunitid").Id
                                            });
                                            updateReq.Target.Attributes.Remove("parentbusinessunitid");
                                            i++;
                                        }

                                        if (updateReq.Target.Contains("businessunitid") && updateReq.Target.LogicalName == "equipment")
                                        {
                                            requests.Insert(i, new SetBusinessEquipmentRequest
                                            {
                                                BusinessUnitId = updateReq.Target.GetAttributeValue<EntityReference>("businessunitid").Id,
                                                EquipmentId = updateReq.Target.Id
                                            });
                                            updateReq.Target.Attributes.Remove("businessunitid");
                                            i++;
                                        }

                                        if (updateReq.Target.Attributes.Count == 0)
                                        {
                                            requests.RemoveAt(i);
                                            i--;
                                        }
                                    }
                                }
                            }

                            if (requests.Count == 1)
                                return requests[0];

                            return new ExecuteTransactionRequest { Requests = requests };
                        },
                        new OperationNames
                        {
                            InProgressUppercase = "Updating",
                            InProgressLowercase = "updating",
                            CompletedLowercase = "updated",
                            CompletedUppercase = "Updated"
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
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new QueryExecutionException(ex.Message, ex) { Node = this };
            }
        }

        private void AddStateTransitions(Entity entity, OptionSetValue currentStatus, OptionSetValue requestedStatus, OrganizationRequestCollection requests)
        {
            if (!StateTransitions.TryGetValue(currentStatus.Value, out var startNode))
                throw new QueryExecutionException("Unknown current status code " + currentStatus.Value);

            if (!StateTransitions.TryGetValue(requestedStatus.Value, out var endNode))
                throw new QueryExecutionException("Unknown requested status code " + requestedStatus.Value);

            var states = BfsSearchStateTransitions(startNode, endNode);

            for (var i = 1; i < states.Count; i++)
            {
                var transition = states[i - 1].Transitions[states[i]];
                var request = transition(entity, states[i]);
                requests.Add(request);
            }
        }

        private List<StatusWithState> BfsSearchStateTransitions(StatusWithState startNode, StatusWithState endNode)
        {
            var prevNode = new Dictionary<StatusWithState, StatusWithState>
            {
                [startNode] = null
            };
            var distance = new Dictionary<StatusWithState, int>
            {
                [startNode] = 0
            };
            var queue = new Queue<StatusWithState>();
            queue.Enqueue(startNode);

            while (queue.Count > 0)
            {
                var node = queue.Dequeue();

                if (node == endNode)
                    break;

                var nextDistance = distance[node] + 1;

                foreach (var nextNode in node.Transitions.Keys)
                {
                    if (distance.ContainsKey(nextNode))
                        continue;

                    distance[nextNode] = nextDistance;
                    prevNode[nextNode] = node;
                    queue.Enqueue(nextNode);
                }
            }

            var path = new List<StatusWithState>();
            var pathNode = endNode;

            while (pathNode != startNode)
            {
                if (!prevNode.TryGetValue(pathNode, out var prev))
                    throw new QueryExecutionException("No transition available from status code " + startNode.StatusCode + " to " + endNode.StatusCode);

                path.Insert(0, pathNode);
                pathNode = prev;
            }

            path.Insert(0, startNode);

            return path;
        }

        private OptionSetValue GetStateCode(EntityMetadata meta, int statuscode)
        {
            var statusCode = meta
                .Attributes
                .OfType<StatusAttributeMetadata>()
                .Single(a => a.LogicalName == "statuscode")
                .OptionSet
                .Options
                .Single(o => o.Value == statuscode);

            return new OptionSetValue((int)((StatusOptionMetadata)statusCode).State);
        }

        private OptionSetValue GetDefaultStatusCode(EntityMetadata meta, int statecode)
        {
            var stateCode = meta
                .Attributes
                .OfType<StateAttributeMetadata>()
                .Single(a => a.LogicalName == "statecode")
                .OptionSet
                .Options
                .Single(o => o.Value == statecode);

            return new OptionSetValue((int)((StateOptionMetadata)stateCode).DefaultStatus);
        }

        private Entity ExtractEntity(ExpressionExecutionContext context, EntityMetadata meta, Dictionary<string, AttributeMetadata> attributes, Dictionary<string, Func<ExpressionExecutionContext, object>> attributeAccessors, Dictionary<string, Func<ExpressionExecutionContext, object>> primaryIdAccessors)
        {
            var update = new Entity(LogicalName);

            if (primaryIdAccessors.TryGetValue(meta.PrimaryIdAttribute, out var primaryIdAccessor))
                update.Id = (Guid)primaryIdAccessor(context);

            foreach (var attributeAccessor in attributeAccessors)
            {
                if (attributeAccessor.Key == meta.PrimaryIdAttribute)
                    continue;

                var attr = attributes[attributeAccessor.Key];

                if (!String.IsNullOrEmpty(attr.AttributeOf))
                    continue;

                var value = attributeAccessor.Value(context);

                update[attr.LogicalName] = value;
            }

            // Special case for activitypointer - need to set the specific activity type code
            if (LogicalName == "activitypointer")
            {
                update.LogicalName = (string)primaryIdAccessors["activitytypecode"](context);

                // When updating using TDS Endpoint as the data source, the type code is returned as an otc instead of logical name
                if (Int32.TryParse(update.LogicalName, out var otc))
                    update.LogicalName = context.Session.DataSources[DataSource].Metadata[otc].LogicalName;
            }

            return update;
        }

        protected override bool FilterErrors(NodeExecutionContext context, OrganizationRequest request, OrganizationServiceFault fault)
        {
            // Ignore errors trying to update records that don't exist - record may have been deleted by another
            // process in parallel.
            return fault.ErrorCode != -2147185406 && // IsvAbortedNotFound
                fault.ErrorCode != -2147220969 && // ObjectDoesNotExist
                fault.ErrorCode != 404; // Elastic tables
        }

        protected override ExecuteMultipleResponse ExecuteMultiple(DataSource dataSource, IOrganizationService org, EntityMetadata meta, ExecuteMultipleRequest req)
        {
            if (!req.Requests.All(r => r is UpdateRequest))
                return base.ExecuteMultiple(dataSource, org, meta, req);

            if (meta.DataProviderId == DataProviders.ElasticDataProvider || meta.DataProviderId == null &&
                dataSource.MessageCache.IsMessageAvailable(meta.LogicalName, "UpdateMultiple") &&
                req.Requests.Cast<UpdateRequest>().GroupBy(r => r.Target.LogicalName).Count() == 1)
            {
                // Elastic tables can use UpdateMultiple for better performance than ExecuteMultiple
                var entities = new EntityCollection { EntityName = meta.LogicalName };

                foreach (UpdateRequest update in req.Requests)
                    entities.Entities.Add(update.Target);

                var updateMultiple = new OrganizationRequest("UpdateMultiple")
                {
                    ["Targets"] = entities
                };

                if (BypassCustomPluginExecution)
                    updateMultiple["BypassCustomPluginExecution"] = true;

                try
                {
                    dataSource.Execute(org, updateMultiple);

                    var multipleResp = new ExecuteMultipleResponse
                    {
                        ["Responses"] = new ExecuteMultipleResponseItemCollection()
                    };

                    for (var i = 0; i < req.Requests.Count; i++)
                    {
                        multipleResp.Responses.Add(new ExecuteMultipleResponseItem
                        {
                            RequestIndex = i,
                            Response = new UpdateResponse()
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
                                Response = detail.StatusCode >= 200 && detail.StatusCode < 300 ? new UpdateResponse() : null,
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
            return "UPDATE";
        }

        public override object Clone()
        {
            var clone = new UpdateNode
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
                NewValueAccessors = NewValueAccessors,
                ExistingValueAccessors = ExistingValueAccessors,
                StateTransitions = StateTransitions,
                UseLegacyUpdateMessages = UseLegacyUpdateMessages,
                MinimalUpdates = MinimalUpdates,
                Source = (IExecutionPlanNodeInternal)Source.Clone(),
                Sql = Sql
            };

            clone.Source.Parent = clone;
            return clone;
        }
    }

    class UpdateMapping
    {
        public string OldValueColumn { get; set; }
        public string NewValueColumn { get; set; }
    }

    class Transitions : List<string>
    {
        public Transitions(IEnumerable<string> values) : base(values)
        {
        }
    }
}
