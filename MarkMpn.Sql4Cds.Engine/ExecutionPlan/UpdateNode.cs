using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.SqlTypes;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Metadata;
using Microsoft.Xrm.Tooling.Connector;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// Implements an UPDATE operation
    /// </summary>
    class UpdateNode : BaseDmlNode
    {
        private int _executionCount;
        private TimeSpan _duration;

        public override int ExecutionCount => _executionCount;

        public override TimeSpan Duration => _duration;

        [Browsable(false)]
        public IExecutionPlanNode Source { get; set; }

        /// <summary>
        /// The logical name of the entity to update
        /// </summary>
        [Category("Update")]
        [Description("The logical name of the entity to update")]
        public string LogicalName { get; set; }

        /// <summary>
        /// The column that contains the primary ID of the records to update
        /// </summary>
        [Category("Update")]
        [Description("The column that contains the primary ID of the records to update")]
        public string PrimaryIdSource { get; set; }

        /// <summary>
        /// The columns to update and the associated column to take the new value from
        /// </summary>
        [Category("Update")]
        [Description("The columns to update and the associated column to take the new value from")]
        public IDictionary<string, string> ColumnMappings { get; } = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        public override void AddRequiredColumns(IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes, IList<string> requiredColumns)
        {
            if (!requiredColumns.Contains(PrimaryIdSource))
                requiredColumns.Add(PrimaryIdSource);

            foreach (var col in ColumnMappings.Values)
            {
                if (!requiredColumns.Contains(col))
                    requiredColumns.Add(col);
            }

            Source.AddRequiredColumns(metadata, parameterTypes, requiredColumns);
        }

        class Timer : IDisposable
        {
            private readonly UpdateNode _node;
            private DateTime? _startTime;

            public Timer(UpdateNode node)
            {
                _node = node;
                _startTime = DateTime.Now;
            }

            public void Pause()
            {
                var endTime = DateTime.Now;

                if (_startTime != null)
                    _node._duration += (endTime - _startTime.Value);

                _startTime = null;
            }

            public void Resume()
            {
                _startTime = DateTime.Now;
            }

            public void Dispose()
            {
                Pause();
            }
        }

        public override string Execute(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues)
        {
            _executionCount++;

            using (var timer = new Timer(this))
            {
                var meta = metadata[LogicalName];
                var attributes = meta.Attributes.ToDictionary(a => a.LogicalName);

                List<Entity> entities;
                NodeSchema schema;

                if (Source is IDataExecutionPlanNode dataSource)
                {
                    schema = dataSource.GetSchema(metadata, parameterTypes);
                    entities = dataSource.Execute(org, metadata, options, parameterTypes, parameterValues).ToList();
                }
                else if (Source is IDataSetExecutionPlanNode dataSetSource)
                {
                    var dataTable = dataSetSource.Execute(org, metadata, options, parameterTypes, parameterValues);
                    var columnSqlTypes = dataTable.Columns.Cast<DataColumn>().Select(col => SqlTypeConverter.NetToSqlType(col.DataType)).ToArray();
                    var columnNullValues = columnSqlTypes.Select(type => SqlTypeConverter.GetNullValue(type)).ToArray();

                    // Values will be stored as BCL types, convert them to SqlXxx types for consistency with IDataExecutionPlanNodes
                    schema = new NodeSchema();

                    for (var i = 0; i < dataTable.Columns.Count; i++)
                    {
                        var col = dataTable.Columns[i];
                        schema.Schema[col.ColumnName] = columnSqlTypes[i];
                    }

                    entities = dataTable.Rows
                        .Cast<DataRow>()
                        .Select(row =>
                        {
                            var entity = new Entity();

                            for (var i = 0; i < dataTable.Columns.Count; i++)
                                entity[dataTable.Columns[i].ColumnName] = DBNull.Value.Equals(row[i]) ? columnNullValues[i] : SqlTypeConverter.NetToSqlType(row[i]);

                            return entity;
                        })
                        .ToList();
                }
                else
                {
                    throw new QueryExecutionException("Unexpected UPDATE data source") { Node = this };
                }

                // Precompile mappings with type conversions
                var attributeAccessors = new Dictionary<string, Func<Entity, object>>();
                Func<Entity,Guid> primaryIdAccessor;
                var entityParam = Expression.Parameter(typeof(Entity));

                foreach (var mapping in ColumnMappings)
                {
                    var sourceColumnName = mapping.Value;
                    var destAttributeName = mapping.Key;

                    if (!schema.ContainsColumn(sourceColumnName, out sourceColumnName))
                        throw new QueryExecutionException($"Missing source column {mapping.Value}") { Node = this };

                    var sourceType = schema.Schema[sourceColumnName];
                    var destType = attributes[destAttributeName].GetAttributeType();
                    var destSqlType = SqlTypeConverter.NetToSqlType(destType);

                    var expr = (Expression)Expression.Property(entityParam, typeof(Entity).GetCustomAttribute<DefaultMemberAttribute>().MemberName, Expression.Constant(sourceColumnName));
                    expr = SqlTypeConverter.Convert(expr, sourceType);
                    expr = SqlTypeConverter.Convert(expr, destSqlType);
                    expr = SqlTypeConverter.Convert(expr, destType);

                    attributeAccessors[destAttributeName] = Expression.Lambda<Func<Entity, object>>(expr, entityParam).Compile();
                }

                if (!schema.ContainsColumn(PrimaryIdSource, out var primaryIdColumn))
                    throw new QueryExecutionException($"Missing source column {PrimaryIdSource}") { Node = this };

                var primaryIdSourceType = schema.Schema[primaryIdColumn];
                var primaryIdExpr = (Expression)Expression.Property(entityParam, typeof(Entity).GetCustomAttribute<DefaultMemberAttribute>().MemberName, Expression.Constant(primaryIdColumn));
                primaryIdExpr = SqlTypeConverter.Convert(primaryIdExpr, primaryIdSourceType);
                primaryIdExpr = SqlTypeConverter.Convert(primaryIdExpr, typeof(SqlGuid));
                primaryIdExpr = SqlTypeConverter.Convert(primaryIdExpr, typeof(Guid));
                primaryIdAccessor = Expression.Lambda<Func<Entity, Guid>>(primaryIdExpr, entityParam).Compile();

                // Check again that the update is allowed. Don't count any UI interaction in the execution time
                timer.Pause();
                if (options.Cancelled || !options.ConfirmUpdate(entities.Count, meta))
                    throw new OperationCanceledException("UPDATE cancelled by user");
                timer.Resume();

                var inProgressCount = 0;
                var count = 0;

                var maxDop = options.MaxDegreeOfParallelism;
                var svc = org as CrmServiceClient;

                if (maxDop < 1 || svc == null)
                    maxDop = 1;

                try
                {
                    using (UseParallelConnections())
                    {
                        Parallel.ForEach(entities,
                            new ParallelOptions { MaxDegreeOfParallelism = maxDop },
                            () => new { Service = svc?.Clone() ?? org, EMR = default(ExecuteMultipleRequest) },
                            (entity, loopState, index, threadLocalState) =>
                            {
                                if (options.Cancelled)
                                {
                                    loopState.Stop();
                                    return threadLocalState;
                                }

                                var update = new Entity(LogicalName, primaryIdAccessor(entity));

                                foreach (var attributeAccessor in attributeAccessors)
                                {
                                    var value = attributeAccessor.Value(entity);
                                    var attr = attributes[attributeAccessor.Key];

                                    if (!String.IsNullOrEmpty(attr.AttributeOf))
                                        continue;

                                    if (value != null)
                                    {
                                        if (attr is LookupAttributeMetadata lookupAttr)
                                        {
                                            value = new EntityReference { Id = (Guid)value };

                                            if (lookupAttr.Targets.Length == 1)
                                                ((EntityReference)value).LogicalName = lookupAttr.Targets[0];
                                            else
                                                ((EntityReference)value).LogicalName = (string)attributeAccessors[attr.LogicalName + "type"](entity);
                                        }
                                        else if (attr is EnumAttributeMetadata)
                                        {
                                            value = new OptionSetValue((int)value);
                                        }
                                        else if (attr is MoneyAttributeMetadata)
                                        {
                                            value = new Money((decimal)value);
                                        }
                                    }

                                    update[attr.LogicalName] = value;
                                }

                                if (options.BatchSize == 1)
                                {
                                    var newCount = Interlocked.Increment(ref inProgressCount);
                                    var progress = (double)newCount / entities.Count;
                                    options.Progress(progress, $"Updating {newCount:N0} of {entities.Count:N0} {GetDisplayName(0, meta)} ({progress:P0})...");
                                    threadLocalState.Service.Update(update);
                                    Interlocked.Increment(ref count);
                                }
                                else
                                {
                                    if (threadLocalState.EMR == null)
                                    {
                                        threadLocalState = new
                                        {
                                            threadLocalState.Service,
                                            EMR = new ExecuteMultipleRequest
                                            {
                                                Requests = new OrganizationRequestCollection(),
                                                Settings = new ExecuteMultipleSettings
                                                {
                                                    ContinueOnError = false,
                                                    ReturnResponses = false
                                                }
                                            }
                                        };
                                    }

                                    threadLocalState.EMR.Requests.Add(new UpdateRequest { Target = update });

                                    if (threadLocalState.EMR.Requests.Count == options.BatchSize)
                                    {
                                        var newCount = Interlocked.Add(ref inProgressCount, threadLocalState.EMR.Requests.Count);
                                        var progress = (double)newCount / entities.Count;
                                        options.Progress(progress, $"Updating {GetDisplayName(0, meta)} {newCount + 1 - threadLocalState.EMR.Requests.Count:N0} - {newCount:N0} of {entities.Count:N0}...");
                                        var resp = (ExecuteMultipleResponse)threadLocalState.Service.Execute(threadLocalState.EMR);

                                        if (resp.IsFaulted)
                                        {
                                            var error = resp.Responses[0];
                                            Interlocked.Add(ref count, error.RequestIndex);
                                            throw new ApplicationException($"Error updating {GetDisplayName(0, meta)} - " + error.Fault.Message);
                                        }
                                        else
                                        {
                                            Interlocked.Add(ref count, threadLocalState.EMR.Requests.Count);
                                        }

                                        threadLocalState = new { threadLocalState.Service, EMR = default(ExecuteMultipleRequest) };
                                    }
                                }

                                return threadLocalState;
                            },
                            (threadLocalState) =>
                            {
                                if (threadLocalState.EMR != null)
                                {
                                    var newCount = Interlocked.Add(ref inProgressCount, threadLocalState.EMR.Requests.Count);
                                    var progress = (double)newCount / entities.Count;
                                    options.Progress(progress, $"Updating {GetDisplayName(0, meta)} {newCount + 1 - threadLocalState.EMR.Requests.Count:N0} - {newCount:N0} of {entities.Count:N0}...");
                                    var resp = (ExecuteMultipleResponse)threadLocalState.Service.Execute(threadLocalState.EMR);

                                    if (resp.IsFaulted)
                                    {
                                        var error = resp.Responses[0];
                                        Interlocked.Add(ref count, error.RequestIndex);
                                        throw new ApplicationException($"Error updating {GetDisplayName(0, meta)} - " + error.Fault.Message);
                                    }
                                    else
                                    {
                                        Interlocked.Add(ref count, threadLocalState.EMR.Requests.Count);
                                    }
                                }

                                if (threadLocalState.Service != org)
                                    ((CrmServiceClient)threadLocalState.Service)?.Dispose();
                            });
                    }
                }
                catch (Exception ex)
                {
                    if (count == 0)
                        throw;

                    throw new PartialSuccessException($"{count:N0} {GetDisplayName(count, meta)} updated", ex);
                }

                return $"{count:N0} {GetDisplayName(count, meta)} updated";
            }
        }

        public override IRootExecutionPlanNode FoldQuery(IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes)
        {
            if (Source is IDataExecutionPlanNode dataNode)
                Source = dataNode.FoldQuery(metadata, options, parameterTypes);
            else if (Source is IDataSetExecutionPlanNode dataSetNode)
                Source = dataSetNode.FoldQuery(metadata, options, parameterTypes);

            return this;
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            yield return Source;
        }

        public override string ToString()
        {
            return "UPDATE";
        }
    }
}
