using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlTypes;
using System.Linq;
using System.Text;
using Microsoft.Crm.Sdk.Messages;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Metadata;
using Microsoft.Xrm.Sdk.Query;
using Newtonsoft.Json;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    class ExecuteMessageNode : BaseDataNode, IDmlQueryExecutionPlanNode
    {
        private Dictionary<string, Func<ExpressionExecutionContext, object>> _inputParameters;
        private string _primaryKeyColumn;

        /// <summary>
        /// The SQL string that the query was converted from
        /// </summary>
        [Browsable(false)]
        public string Sql { get; set; }

        /// <summary>
        /// The position of the SQL query within the entire query text
        /// </summary>
        [Browsable(false)]
        public int Index { get; set; }

        /// <summary>
        /// The length of the SQL query within the entire query text
        /// </summary>
        [Browsable(false)]
        public int Length { get; set; }

        /// <summary>
        /// The number of the first line of the statement
        /// </summary>
        [Browsable(false)]
        public int LineNumber { get; set; }

        /// <summary>
        /// The instance that this node will be executed against
        /// </summary>
        [Category("Data Source")]
        [Description("The data source this query is executed against")]
        public string DataSource { get; set; }

        /// <summary>
        /// The alias for the data source
        /// </summary>
        [Category("Execute Message")]
        [Description("The alias for the data source")]
        public string Alias { get; set; }

        /// <summary>
        /// The name of the message to execute
        /// </summary>
        [Category("Execute Message")]
        [Description("The name of the message to execute")]
        public string MessageName { get; set; }

        /// <summary>
        /// The values to supply as input parameters to the message
        /// </summary>
        [Category("Execute Message")]
        [Description("The values to supply as input parameters to the message")]
        public IDictionary<string, ScalarExpression> Values { get; } = new Dictionary<string, ScalarExpression>(StringComparer.OrdinalIgnoreCase);

        /// <summary>
        /// The raw CLR types expected for each input parameter
        /// </summary>
        [Browsable(false)]
        public IDictionary<string, Type> ValueTypes { get; } = new Dictionary<string, Type>(StringComparer.OrdinalIgnoreCase);

        /// <summary>
        /// The name of the response parameter that contains the entity to extract values from
        /// </summary>
        [Browsable(false)]
        public string EntityResponseParameter { get; set; }

        /// <summary>
        /// The name of the response parameter that contains the entity collection to extract values from
        /// </summary>
        [Browsable(false)]
        public string EntityCollectionResponseParameter { get; set; }

        /// <summary>
        /// The name of the request parameter that is used to retrieve multiple pages of data
        /// </summary>
        [Browsable(false)]
        public string PagingParameter { get; set; }

        /// <summary>
        /// The types of values to be returned
        /// </summary>
        [Browsable(false)]
        public IDictionary<string, IColumnDefinition> Schema { get; private set; } = new ColumnList();

        /// <summary>
        /// Indicates if custom plugins should be skipped
        /// </summary>
        [Category("Execute Message")]
        [DisplayName("Bypass Plugin Execution")]
        [Description("Indicates if custom plugins should be skipped")]
        public bool BypassCustomPluginExecution { get; set; }

        /// <summary>
        /// Shows the number of pages that were retrieved in the last execution of this node
        /// </summary>
        [Category("Execute Message")]
        [Description("Shows the number of pages that were retrieved in the last execution of this node")]
        [DisplayName("Pages Retrieved")]
        public int PagesRetrieved { get; set; }

        public override void AddRequiredColumns(NodeCompilationContext context, IList<string> requiredColumns)
        {
        }

        public override IDataExecutionPlanNodeInternal FoldQuery(NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            var expressionContext = new ExpressionCompilationContext(context, null, null);

            _inputParameters = Values
                .ToDictionary(value => value.Key, value =>
                {
                    var sourceSqlType = value.Value.GetType(expressionContext, out _);
                    var destNetType = ValueTypes[value.Key];
                    var destSqlType = SqlTypeConverter.NetToSqlType(destNetType);
                    var expr = value.Value.Compile(expressionContext);
                    var sqlConversion = SqlTypeConverter.GetConversion(sourceSqlType, destSqlType);
                    var netConversion = SqlTypeConverter.GetConversion(destSqlType, destNetType);
                    return (Func<ExpressionExecutionContext, object>) ((ExpressionExecutionContext ctx) => netConversion(sqlConversion(expr(ctx))));
                });

            BypassCustomPluginExecution = GetBypassPluginExecution(hints, context.Options);

            return this;
        }

        private bool GetBypassPluginExecution(IList<OptimizerHint> queryHints, IQueryExecutionOptions options)
        {
            if (queryHints == null)
                return options.BypassCustomPlugins;

            var bypassPluginExecution = queryHints
                .OfType<UseHintList>()
                .Where(hint => hint.Hints.Any(s => s.Value.Equals("BYPASS_CUSTOM_PLUGIN_EXECUTION", StringComparison.OrdinalIgnoreCase)))
                .Any();

            return bypassPluginExecution || options.BypassCustomPlugins;
        }

        public override INodeSchema GetSchema(NodeCompilationContext context)
        {
            var schema = new ColumnList();

            foreach (var col in Schema)
                schema[PrefixWithAlias(col.Key)] = col.Value;

            return new NodeSchema(
                primaryKey: null,
                schema: schema,
                aliases: Schema.ToDictionary(kvp => kvp.Key, kvp => (IReadOnlyList<string>)new List<string> { PrefixWithAlias(kvp.Key) }, StringComparer.OrdinalIgnoreCase),
                sortOrder: null);
        }

        private void SetOutputSchema(DataSource dataSource, Message message, TSqlFragment source)
        {
            // Add the response fields to the node schema
            if (message.OutputParameters.All(f => f.IsScalarType()))
            {
                foreach (var value in message.OutputParameters)
                    AddSchemaColumn(value.Name, SqlTypeConverter.NetToSqlType(value.Type).ToSqlType(dataSource)); // TODO: How are OSV and ER fields represented?
            }
            else
            {
                var firstValue = message.OutputParameters.Single();
                var audit = false;
                var type = firstValue.Type;
                var otc = firstValue.OTC;

                if (type == typeof(AuditDetail))
                {
                    type = typeof(Entity);
                    otc = dataSource.Metadata["audit"].ObjectTypeCode;
                    audit = true;
                }
                else if (firstValue.Type == typeof(AuditDetailCollection))
                {
                    type = typeof(EntityCollection);
                    otc = dataSource.Metadata["audit"].ObjectTypeCode;
                    audit = true;
                }

                if (type == typeof(Entity))
                    EntityResponseParameter = firstValue.Name;
                else
                    EntityCollectionResponseParameter = firstValue.Name;

                foreach (var attrMetadata in dataSource.Metadata[otc.Value].Attributes.Where(a => a.AttributeOf == null))
                {
                    AddSchemaColumn(attrMetadata.LogicalName, attrMetadata.GetAttributeSqlType(dataSource, false));

                    // Add standard virtual attributes
                    if (attrMetadata is EnumAttributeMetadata || attrMetadata is BooleanAttributeMetadata)
                        AddSchemaColumn(attrMetadata.LogicalName + "name", DataTypeHelpers.NVarChar(FetchXmlScan.LabelMaxLength, dataSource.DefaultCollation, CollationLabel.CoercibleDefault));

                    if (attrMetadata is LookupAttributeMetadata lookup)
                    {
                        AddSchemaColumn(attrMetadata.LogicalName + "name", DataTypeHelpers.NVarChar(lookup.Targets == null || lookup.Targets.Length == 0 ? 100 : lookup.Targets.Select(e => ((StringAttributeMetadata)dataSource.Metadata[e].Attributes.SingleOrDefault(a => a.LogicalName == dataSource.Metadata[e].PrimaryNameAttribute))?.MaxLength ?? 100).Max(), dataSource.DefaultCollation, CollationLabel.CoercibleDefault));

                        if (lookup.Targets?.Length != 1 && lookup.AttributeType != AttributeTypeCode.PartyList)
                            AddSchemaColumn(attrMetadata.LogicalName + "type", DataTypeHelpers.NVarChar(MetadataExtensions.EntityLogicalNameMaxLength, dataSource.DefaultCollation, CollationLabel.CoercibleDefault));
                    }
                }

                if (audit)
                {
                    AddSchemaColumn("newvalues", DataTypeHelpers.NVarChar(Int32.MaxValue, dataSource.DefaultCollation, CollationLabel.CoercibleDefault));
                    AddSchemaColumn("oldvalues", DataTypeHelpers.NVarChar(Int32.MaxValue, dataSource.DefaultCollation, CollationLabel.CoercibleDefault));
                }

                _primaryKeyColumn = PrefixWithAlias(dataSource.Metadata[otc.Value].PrimaryIdAttribute);
            }

            if (!String.IsNullOrEmpty(PagingParameter) && EntityCollectionResponseParameter == null)
                throw new NotSupportedQueryFragmentException($"Paging request parameter found but no collection response parameter", source);
        }

        private void AddSchemaColumn(string name, DataTypeReference type)
        {
            Schema[name] = new ColumnDefinition(type, true, false);
        }

        private string PrefixWithAlias(string columnName)
        {
            if (String.IsNullOrEmpty(Alias))
                return columnName;

            return Alias + "." + columnName;
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            return Array.Empty<IExecutionPlanNode>();
        }

        protected override RowCountEstimate EstimateRowsOutInternal(NodeCompilationContext context)
        {
            if (EntityCollectionResponseParameter == null)
                return RowCountEstimateDefiniteRange.ExactlyOne;

            return new RowCountEstimate(10);
        }

        protected override IEnumerable<string> GetVariablesInternal()
        {
            return Values.Values.SelectMany(v => v.GetVariables());
        }

        protected override IEnumerable<Entity> ExecuteInternal(NodeExecutionContext context)
        {
            PagesRetrieved = 0;

            if (!context.DataSources.TryGetValue(DataSource, out var dataSource))
                throw new NotSupportedQueryFragmentException("Missing datasource " + DataSource);

            context.Options.Progress(0, $"Executing {MessageName}...");

            // Get the first page of results
            if (!context.Options.ContinueRetrieve(0))
                yield break;

            var request = new OrganizationRequest(MessageName);
            var pageNumber = 1;
            var expressionContext = new ExpressionExecutionContext(context);

            foreach (var value in _inputParameters)
                request[value.Key] = value.Value(expressionContext);

            if (BypassCustomPluginExecution)
                request.Parameters["BypassCustomPluginExecution"] = true;

            var response = dataSource.Connection.Execute(request);
            var entities = GetEntityCollection(response);
            PagesRetrieved++;

            var count = entities.Entities.Count;

            foreach (var entity in entities.Entities)
            {
                OnRetrievedEntity(entity, context.Options, dataSource);
                yield return entity;
            }

            while (PagingParameter != null && entities.MoreRecords && context.Options.ContinueRetrieve(count))
            {
                pageNumber++;
                request[PagingParameter] = new PagingInfo
                {
                    Count = entities.Entities.Count,
                    PageNumber = pageNumber,
                    PagingCookie = entities.PagingCookie
                };


                response = dataSource.Connection.Execute(request);
                entities = GetEntityCollection(response);
                PagesRetrieved++;

                foreach (var entity in entities.Entities)
                {
                    OnRetrievedEntity(entity, context.Options, dataSource);
                    yield return entity;
                }

                count += entities.Entities.Count;
            }
        }

        private EntityCollection GetEntityCollection(OrganizationResponse response)
        {
            EntityCollection entities;

            if (EntityCollectionResponseParameter != null)
            {
                if (response[EntityCollectionResponseParameter] is AuditDetailCollection audit)
                {
                    entities = new EntityCollection(audit.AuditDetails.Select(a => GetAuditEntity(a)).ToList());
                    entities.MoreRecords = audit.MoreRecords;
                    entities.PagingCookie = audit.PagingCookie;
                    entities.TotalRecordCount = audit.TotalRecordCount;
                }
                else
                {
                    entities = (EntityCollection)response[EntityCollectionResponseParameter];
                }
            }
            else if (EntityResponseParameter != null)
            {
                entities = new EntityCollection();
                Entity entity;

                if (response[EntityResponseParameter] is AuditDetail audit)
                    entity = GetAuditEntity(audit);
                else
                    entity = (Entity)response[EntityResponseParameter];

                if (entity != null)
                    entities.Entities.Add(entity);
            }
            else
            {
                entities = new EntityCollection();
                var entity = new Entity();

                foreach (var value in response.Results)
                    entity[value.Key] = value.Value;

                entities.Entities.Add(entity);
            }

            return entities;
        }

        private Entity GetAuditEntity(AuditDetail audit)
        {
            var entity = audit.AuditRecord;

            if (audit is AttributeAuditDetail attributeAudit)
            {
                // Expose the old and new values for attribute audits easily.
                // Attribute list could vary from record to record depending on the entity type being audited,
                // so can't expose this as a definite list of columns. Instead, serialize them as a string and
                // allow the values to be accessed later using some custom functions.
                entity["newvalues"] = SerializeAttributeAuditValues(attributeAudit.NewValue);
                entity["oldvalues"] = SerializeAttributeAuditValues(attributeAudit.OldValue);
            }

            return entity;
        }

        private string SerializeAttributeAuditValues(Entity entity)
        {
            var values = new Dictionary<string, object>();

            if (entity != null && entity.Attributes != null)
            {
                foreach (var attribute in entity.Attributes)
                {
                    if (attribute.Value is OptionSetValue osv)
                    {
                        values[attribute.Key] = osv.Value;
                    }
                    else if (attribute.Value is Money money)
                    {
                        values[attribute.Key] = money.Value;
                    }
                    else if (attribute.Value is EntityReference er)
                    {
                        values[attribute.Key] = er.Id;
                        values[attribute.Key + "name"] = er.Name;
                        values[attribute.Key + "type"] = er.LogicalName;
                    }
                    else
                    {
                        values[attribute.Key] = attribute.Value;
                    }
                }
            }

            return JsonConvert.SerializeObject(values);
        }

        private void OnRetrievedEntity(Entity entity, IQueryExecutionOptions options, DataSource dataSource)
        {
            // Expose any formatted values for OptionSetValue and EntityReference values
            foreach (var formatted in entity.FormattedValues)
            {
                if (!entity.Contains(formatted.Key + "name"))
                    entity[formatted.Key + "name"] = formatted.Value;
            }

            if (options.UseLocalTimeZone)
            {
                // For any datetime values, check the metadata to see if they are affected by timezones and convert them
                foreach (var attribute in entity.Attributes.ToList())
                {
                    var entityName = entity.LogicalName;
                    var attributeName = attribute.Key;
                    var value = attribute.Value;

                    if (value is AliasedValue alias)
                    {
                        entityName = alias.EntityLogicalName;
                        attributeName = alias.AttributeLogicalName;
                        value = alias.Value;
                    }

                    if (value is DateTime dt)
                    {
                        dt = dt.ToLocalTime();
                        entity[attribute.Key] = dt;
                    }
                }
            }

            // Expose the type of lookup values
            foreach (var attribute in entity.Attributes.Where(attr => attr.Value is EntityReference).ToList())
            {
                if (!entity.Contains(attribute.Key + "type"))
                    entity[attribute.Key + "type"] = ((EntityReference)attribute.Value).LogicalName;

                if (!entity.Contains(attribute.Key + "name"))
                    entity[attribute.Key + "name"] = ((EntityReference)attribute.Value).Name;
            }

            // Convert values to SQL types
            foreach (var col in Schema)
            {
                object sqlValue;

                if (entity.Attributes.TryGetValue(col.Key, out var value) && value != null)
                    sqlValue = SqlTypeConverter.NetToSqlType(dataSource, value, col.Value.Type);
                else
                    sqlValue = SqlTypeConverter.GetNullValue(col.Value.Type.ToNetType(out _));

                if (_primaryKeyColumn == col.Key && sqlValue is SqlGuid guid)
                    sqlValue = new SqlEntityReference(DataSource, entity.LogicalName, guid);

                entity[col.Key] = sqlValue;
            }

            // Prefix all attributes of the main entity with the expected alias
            foreach (var attribute in entity.Attributes.ToList())
                entity[PrefixWithAlias(attribute.Key)] = attribute.Value;
        }

        public override object Clone()
        {
            var clone = new ExecuteMessageNode
            {
                Sql = Sql,
                Index = Index,
                Length = Length,
                LineNumber = LineNumber,
                DataSource = DataSource,
                MessageName = MessageName,
                EntityResponseParameter = EntityResponseParameter,
                EntityCollectionResponseParameter = EntityCollectionResponseParameter,
                Schema = Schema,
                Alias = Alias,
                _inputParameters = _inputParameters,
                BypassCustomPluginExecution = BypassCustomPluginExecution,
                _primaryKeyColumn = _primaryKeyColumn,
                PagingParameter = PagingParameter,
            };

            foreach (var value in Values)
                clone.Values[value.Key] = value.Value.Clone();

            return clone;
        }

        public static ExecuteMessageNode FromMessage(SchemaObjectFunctionTableReference tvf, DataSource dataSource, ExpressionCompilationContext context)
        {
            // All messages are in the "dbo" schema
            if (tvf.SchemaObject.SchemaIdentifier != null && !String.IsNullOrEmpty(tvf.SchemaObject.SchemaIdentifier.Value) &&
                !tvf.SchemaObject.SchemaIdentifier.Value.Equals("dbo", StringComparison.OrdinalIgnoreCase))
                throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidObjectName(tvf.SchemaObject));

            if (!dataSource.MessageCache.TryGetValue(tvf.SchemaObject.BaseIdentifier.Value, out var message))
                throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidObjectName(tvf.SchemaObject));

            if (!message.IsValidAsTableValuedFunction())
                throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidObjectName(tvf.SchemaObject))
                {
                    Suggestion = "Messages must only have scalar type inputs and must produce either one or more scalar type outputs or a single Entity or EntityCollection output"
                };

            var node = new ExecuteMessageNode
            {
                Alias = tvf.Alias?.Value,
                DataSource = dataSource.Name,
                MessageName = message.Name,
            };

            // Check the number and type of input parameters matches
            var expectedInputParameters = new List<MessageParameter>();
            var pagingInfoPosition = -1;

            for (var i = 0; i < message.InputParameters.Count; i++)
            {
                if (message.InputParameters[i].Type == typeof(PagingInfo))
                {
                    pagingInfoPosition = i;
                    node.PagingParameter = message.InputParameters[i].Name;
                }
                else
                {
                    expectedInputParameters.Add(message.InputParameters[i]);
                }
            }

            // Check we have the right number of parameters
            if (expectedInputParameters.Count > tvf.Parameters.Count)
                throw new NotSupportedQueryFragmentException(Sql4CdsError.InsufficientArguments(tvf.SchemaObject))
                {
                    Suggestion = "The following parameters are required:" + String.Join("", expectedInputParameters.Select(p => $"\r\n* {p.Name}"))
                };

            if (expectedInputParameters.Count < tvf.Parameters.Count)
                throw new NotSupportedQueryFragmentException(Sql4CdsError.TooManyArguments(tvf.SchemaObject, false));

            // Add the parameter values to the node, including any required type conversions
            for (var i = 0; i < expectedInputParameters.Count; i++)
            {
                var f = expectedInputParameters[i];
                var sourceExpression = tvf.Parameters[i];
                sourceExpression.GetType(context, out var sourceType);
                var expectedType = SqlTypeConverter.NetToSqlType(f.Type).ToSqlType(context.PrimaryDataSource);

                if (!SqlTypeConverter.CanChangeTypeImplicit(sourceType, expectedType))
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.TypeClash(tvf.Parameters[f.Position], sourceType, expectedType));

                if (sourceType.IsSameAs(expectedType))
                {
                    node.Values[f.Name] = sourceExpression;
                }
                else
                {
                    node.Values[f.Name] = new ConvertCall
                    {
                        Parameter = sourceExpression,
                        DataType = expectedType
                    };
                }

                node.ValueTypes[f.Name] = f.Type;
            }

            node.SetOutputSchema(dataSource, message, tvf.SchemaObject);

            return node;
        }

        public static ExecuteMessageNode FromMessage(ExecutableProcedureReference sproc, DataSource dataSource, ExpressionCompilationContext context)
        {
            // All messages are in the "dbo" schema
            if (sproc.ProcedureReference.ProcedureReference.Name.SchemaIdentifier != null && !String.IsNullOrEmpty(sproc.ProcedureReference.ProcedureReference.Name.SchemaIdentifier.Value) &&
                !sproc.ProcedureReference.ProcedureReference.Name.SchemaIdentifier.Value.Equals("dbo", StringComparison.OrdinalIgnoreCase))
                throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidSprocName(sproc.ProcedureReference.ProcedureReference.Name));

            if (!dataSource.MessageCache.TryGetValue(sproc.ProcedureReference.ProcedureReference.Name.BaseIdentifier.Value, out var message))
                throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidSprocName(sproc.ProcedureReference.ProcedureReference.Name));

            if (!message.IsValidAsStoredProcedure())
                throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidSprocName(sproc.ProcedureReference.ProcedureReference.Name))
                {
                    Suggestion = "Message is not valid to be called as a stored procedure\r\nMessages must only have scalar type inputs and must produce no more than one Entity or EntityCollection output or any number of scalar type outputs"
                };

            var node = new ExecuteMessageNode
            {
                DataSource = dataSource.Name,
                MessageName = message.Name
            };

            // Check the number and type of input parameters matches
            var expectedInputParameters = new List<MessageParameter>();
            var pagingInfoPosition = -1;

            for (var i = 0; i < message.InputParameters.Count; i++)
            {
                if (message.InputParameters[i].Type == typeof(PagingInfo))
                {
                    pagingInfoPosition = i;
                    node.PagingParameter = message.InputParameters[i].Name;
                }
                else
                {
                    expectedInputParameters.Add(message.InputParameters[i]);
                }
            }

            // Add the parameter values to the node, including any required type conversions
            var usedParamName = false;

            for (var i = 0; i < sproc.Parameters.Count; i++)
            {
                if (sproc.Parameters[i].Variable != null)
                    usedParamName = true;

                if (usedParamName && sproc.Parameters[i].Variable == null)
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.NamedParametersRequiredAfter(sproc.Parameters[i], i + 1));

                if (sproc.Parameters[i].IsOutput)
                    continue;

                string targetParamName;

                if (sproc.Parameters[i].Variable == null)
                {
                    var paramIndex = i;

                    if (pagingInfoPosition != -1 && i >= pagingInfoPosition)
                        paramIndex++;

                    if (paramIndex >= message.InputParameters.Count)
                        throw new NotSupportedQueryFragmentException(Sql4CdsError.TooManyArguments(sproc.ProcedureReference.ProcedureReference.Name, true));

                    targetParamName = message.InputParameters[paramIndex].Name;
                }
                else
                {
                    targetParamName = sproc.Parameters[i].Variable.Name.Substring(1);
                }

                var targetParam = message.InputParameters.SingleOrDefault(p => p.Name.Equals(targetParamName, StringComparison.OrdinalIgnoreCase));

                if (targetParam == null)
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidParameterName(sproc.Parameters[i], sproc.ProcedureReference.ProcedureReference.Name));

                var sourceExpression = sproc.Parameters[i].ParameterValue;
                sourceExpression.GetType(context, out var sourceType);
                var expectedType = SqlTypeConverter.NetToSqlType(targetParam.Type).ToSqlType(context.PrimaryDataSource);

                if (!SqlTypeConverter.CanChangeTypeImplicit(sourceType, expectedType))
                {
                    var err = Sql4CdsError.TypeClash(sproc.Parameters[i].ParameterValue, sourceType, expectedType);
                    err.Procedure = message.Name;
                    throw new NotSupportedQueryFragmentException(err);
                }

                if (sourceType.IsSameAs(expectedType))
                {
                    node.Values[targetParam.Name] = sproc.Parameters[i].ParameterValue;
                }
                else
                {
                    node.Values[targetParam.Name] = new ConvertCall
                    {
                        Parameter = sproc.Parameters[i].ParameterValue,
                        DataType = expectedType
                    };
                }

                node.ValueTypes[targetParam.Name] = targetParam.Type;
            }

            // Check if we are missing any parameters
            foreach (var inputParameter in message.InputParameters)
            {
                if (node.Values.ContainsKey(inputParameter.Name))
                    continue;

                if (!inputParameter.Optional)
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.MissingParameter(sproc.ProcedureReference.ProcedureReference.Name, inputParameter.Name, true));
            }

            node.SetOutputSchema(dataSource, message, sproc.ProcedureReference);

            return node;
        }

        public void Execute(NodeExecutionContext context, out int recordsAffected, out string message)
        {
            recordsAffected = Execute(context).Count();
            message = "Executed " + MessageName;
        }

        IRootExecutionPlanNodeInternal[] IRootExecutionPlanNodeInternal.FoldQuery(NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            FoldQuery(context, hints);
            return new[] { this };
        }

        public override string ToString()
        {
            return $"Table Valued Function\r\n[{MessageName}]";
        }
    }
}
