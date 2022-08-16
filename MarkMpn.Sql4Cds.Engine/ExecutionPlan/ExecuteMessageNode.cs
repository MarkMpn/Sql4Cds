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
    class ExecuteMessageNode : BaseDataNode
    {
        private Dictionary<string, Func<IDictionary<string, object>, IQueryExecutionOptions, object>> _inputParameters;
        private string _primaryKeyColumn;

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
        public Dictionary<string, DataTypeReference> Schema { get; private set; } = new Dictionary<string, DataTypeReference>();

        /// <summary>
        /// Indicates if custom plugins should be skipped
        /// </summary>
        [DisplayName("Bypass Plugin Execution")]
        [Description("Indicates if custom plugins should be skipped")]
        public bool BypassCustomPluginExecution { get; set; }

        public override void AddRequiredColumns(IDictionary<string, DataSource> dataSources, IDictionary<string, DataTypeReference> parameterTypes, IList<string> requiredColumns)
        {
        }

        public override IDataExecutionPlanNodeInternal FoldQuery(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, DataTypeReference> parameterTypes, IList<OptimizerHint> hints)
        {
            _inputParameters = Values
                .ToDictionary(value => value.Key, value =>
                {
                    var exprType = value.Value.GetType(null, null, parameterTypes, out _);
                    var expectedType = ValueTypes[value.Key];
                    var expr = value.Value.Compile(null, parameterTypes);
                    var conversion = SqlTypeConverter.GetConversion(exprType, expectedType);
                    return (Func<IDictionary<string,object>, IQueryExecutionOptions, object>) ((IDictionary<string, object> parameterValues, IQueryExecutionOptions opts) => conversion(expr(null, parameterValues, opts)));
                });

            BypassCustomPluginExecution = GetBypassPluginExecution(hints, options);

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

        public override INodeSchema GetSchema(IDictionary<string, DataSource> dataSources, IDictionary<string, DataTypeReference> parameterTypes)
        {
            return new NodeSchema(
                primaryKey: null,
                schema: Schema.ToDictionary(kvp => PrefixWithAlias(kvp.Key), kvp => kvp.Value, StringComparer.OrdinalIgnoreCase),
                aliases: Schema.ToDictionary(kvp => kvp.Key, kvp => (IReadOnlyList<string>)new List<string> { PrefixWithAlias(kvp.Key) }, StringComparer.OrdinalIgnoreCase),
                notNullColumns: null,
                sortOrder: null);
        }

        private void AddSchemaColumn(string name, DataTypeReference type)
        {
            Schema[name] = type;
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

        protected override RowCountEstimate EstimateRowsOutInternal(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, DataTypeReference> parameterTypes)
        {
            if (EntityCollectionResponseParameter == null)
                return RowCountEstimateDefiniteRange.ExactlyOne;

            return new RowCountEstimate(10);
        }

        protected override IEnumerable<Entity> ExecuteInternal(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, DataTypeReference> parameterTypes, IDictionary<string, object> parameterValues)
        {
            var request = new OrganizationRequest(MessageName);
            var pageNumber = 1;

            foreach (var value in _inputParameters)
                request[value.Key] = value.Value(parameterValues, options);

            if (BypassCustomPluginExecution)
                request.Parameters["BypassCustomPluginExecution"] = true;

            var response = dataSources[DataSource].Connection.Execute(request);
            var entities = GetEntityCollection(response);

            foreach (var entity in entities.Entities)
            {
                OnRetrievedEntity(entity, options);
                yield return entity;
            }

            while (PagingParameter != null && entities.MoreRecords)
            {
                pageNumber++;
                request[PagingParameter] = new PagingInfo
                {
                    Count = entities.Entities.Count,
                    PageNumber = pageNumber,
                    PagingCookie = entities.PagingCookie
                };


                response = dataSources[DataSource].Connection.Execute(request);
                entities = GetEntityCollection(response);

                foreach (var entity in entities.Entities)
                {
                    OnRetrievedEntity(entity, options);
                    yield return entity;
                }
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

            return JsonConvert.SerializeObject(values);
        }

        private void OnRetrievedEntity(Entity entity, IQueryExecutionOptions options)
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

            // Prefix all attributes of the main entity with the expected alias
            foreach (var attribute in entity.Attributes.ToList())
                entity[PrefixWithAlias(attribute.Key)] = attribute.Value;

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
                    sqlValue = SqlTypeConverter.NetToSqlType(DataSource, value, col.Value);
                else
                    sqlValue = SqlTypeConverter.GetNullValue(col.Value.ToNetType(out _));

                if (_primaryKeyColumn == col.Key && sqlValue is SqlGuid guid)
                    sqlValue = new SqlEntityReference(DataSource, entity.LogicalName, guid);

                entity[col.Key] = sqlValue;
            }
        }

        public override object Clone()
        {
            var clone = new ExecuteMessageNode
            {
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

        public static ExecuteMessageNode FromMessage(SchemaObjectFunctionTableReference tvf, DataSource dataSource, IDictionary<string, DataTypeReference> parameterTypes)
        {
            // All messages are in the "dbo" schema
            if (tvf.SchemaObject.SchemaIdentifier != null && !String.IsNullOrEmpty(tvf.SchemaObject.SchemaIdentifier.Value) &&
                !tvf.SchemaObject.SchemaIdentifier.Value.Equals("dbo", StringComparison.OrdinalIgnoreCase))
                throw new NotSupportedQueryFragmentException("Invalid function name", tvf.SchemaObject);

            if (!dataSource.MessageCache.TryGetValue(tvf.SchemaObject.BaseIdentifier.Value, out var message))
                throw new NotSupportedQueryFragmentException("Invalid function name", tvf.SchemaObject);

            if (!message.IsValidAsTableValuedFunction())
                throw new NotSupportedQueryFragmentException("Message is not valid to be called as a table valued function", tvf.SchemaObject)
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
                throw new NotSupportedQueryFragmentException($"Missing parameter '{expectedInputParameters[tvf.Parameters.Count].Name}'", tvf);

            if (expectedInputParameters.Count < tvf.Parameters.Count)
                throw new NotSupportedQueryFragmentException("Unexpected parameter", tvf.Parameters[expectedInputParameters.Count]);

            // Add the parameter values to the node, including any required type conversions
            foreach (var f in expectedInputParameters)
            {
                if (pagingInfoPosition != -1 && f.Position > pagingInfoPosition)
                    f.Position--;

                var sourceExpression = tvf.Parameters[f.Position];
                sourceExpression.GetType(null, null, parameterTypes, out var sourceType);
                var expectedType = SqlTypeConverter.NetToSqlType(f.Type).ToSqlType();

                if (!SqlTypeConverter.CanChangeTypeImplicit(sourceType, expectedType))
                    throw new NotSupportedQueryFragmentException($"Cannot convert value of type {sourceType.ToSql()} to {expectedType.ToSql()}", tvf.Parameters[f.Position]);

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

            // Add the response fields to the node schema
            if (message.OutputParameters.All(f => f.IsScalarType()))
            {
                foreach (var value in message.OutputParameters)
                    node.AddSchemaColumn(value.Name, SqlTypeConverter.NetToSqlType(value.Type).ToSqlType()); // TODO: How are OSV and ER fields represented?
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
                    node.EntityResponseParameter = firstValue.Name;
                else
                    node.EntityCollectionResponseParameter = firstValue.Name;

                foreach (var attrMetadata in dataSource.Metadata[otc.Value].Attributes.Where(a => a.AttributeOf == null))
                {
                    node.AddSchemaColumn(attrMetadata.LogicalName, attrMetadata.GetAttributeSqlType(dataSource.Metadata, false));

                    // Add standard virtual attributes
                    if (attrMetadata is EnumAttributeMetadata || attrMetadata is BooleanAttributeMetadata)
                        node.AddSchemaColumn(attrMetadata.LogicalName + "name", DataTypeHelpers.NVarChar(FetchXmlScan.LabelMaxLength));

                    if (attrMetadata is LookupAttributeMetadata lookup)
                    {
                        node.AddSchemaColumn(attrMetadata.LogicalName + "name", DataTypeHelpers.NVarChar(lookup.Targets == null || lookup.Targets.Length == 0 ? 100 : lookup.Targets.Select(e => ((StringAttributeMetadata)dataSource.Metadata[e].Attributes.SingleOrDefault(a => a.LogicalName == dataSource.Metadata[e].PrimaryNameAttribute))?.MaxLength ?? 100).Max()));

                        if (lookup.Targets?.Length != 1 && lookup.AttributeType != AttributeTypeCode.PartyList)
                            node.AddSchemaColumn(attrMetadata.LogicalName + "type", DataTypeHelpers.NVarChar(MetadataExtensions.EntityLogicalNameMaxLength));
                    }
                }

                if (audit)
                {
                    node.AddSchemaColumn("newvalues", DataTypeHelpers.NVarChar(Int32.MaxValue));
                    node.AddSchemaColumn("oldvalues", DataTypeHelpers.NVarChar(Int32.MaxValue));
                }

                node._primaryKeyColumn = node.PrefixWithAlias(dataSource.Metadata[otc.Value].PrimaryIdAttribute);
            }

            if (!String.IsNullOrEmpty(node.PagingParameter) && node.EntityCollectionResponseParameter == null)
                throw new NotSupportedQueryFragmentException($"Paging request parameter found but no collection response parameter", tvf.SchemaObject);

            return node;
        }
    }
}
