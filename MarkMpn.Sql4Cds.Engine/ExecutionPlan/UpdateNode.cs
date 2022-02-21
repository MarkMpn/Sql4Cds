using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Metadata;

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
        /// The column that contains the primary ID of the records to update
        /// </summary>
        [Category("Update")]
        [Description("The column that contains the primary ID of the records to update")]
        [DisplayName("PrimaryId Source")]
        public string PrimaryIdSource { get; set; }

        /// <summary>
        /// The columns to update and the associated column to take the new value from
        /// </summary>
        [Category("Update")]
        [Description("The columns to update and the associated column to take the new value from")]
        [DisplayName("Column Mappings")]
        public IDictionary<string, string> ColumnMappings { get; } = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        public override void AddRequiredColumns(IDictionary<string, DataSource> dataSources, IDictionary<string, DataTypeReference> parameterTypes, IList<string> requiredColumns)
        {
            if (!requiredColumns.Contains(PrimaryIdSource))
                requiredColumns.Add(PrimaryIdSource);

            foreach (var col in ColumnMappings.Values)
            {
                if (!requiredColumns.Contains(col))
                    requiredColumns.Add(col);
            }

            Source.AddRequiredColumns(dataSources, parameterTypes, requiredColumns);
        }

        public override string Execute(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, DataTypeReference> parameterTypes, IDictionary<string, object> parameterValues, out int recordsAffected, CancellationToken cancellationToken)
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
                    entities = GetDmlSourceEntities(dataSources, options, parameterTypes, parameterValues, out var schema, cancellationToken);

                    // Precompile mappings with type conversions
                    meta = dataSource.Metadata[LogicalName];
                    attributes = meta.Attributes.ToDictionary(a => a.LogicalName);
                    var dateTimeKind = options.UseLocalTimeZone ? DateTimeKind.Local : DateTimeKind.Utc;
                    var fullMappings = new Dictionary<string, string>(ColumnMappings);
                    fullMappings[meta.PrimaryIdAttribute] = PrimaryIdSource;
                    attributeAccessors = CompileColumnMappings(meta, fullMappings, schema, attributes, dateTimeKind);
                    primaryIdAccessor = attributeAccessors[meta.PrimaryIdAttribute];
                }

                // Check again that the update is allowed. Don't count any UI interaction in the execution time
                if (cancellationToken.IsCancellationRequested || !options.ConfirmUpdate(entities.Count, meta))
                    throw new OperationCanceledException("UPDATE cancelled by user");

                using (_timer.Run())
                {
                    return ExecuteDmlOperation(
                        dataSource.Connection,
                        options,
                        entities,
                        meta,
                        entity =>
                        {
                            var update = new Entity(LogicalName, (Guid)primaryIdAccessor(entity));

                            foreach (var attributeAccessor in attributeAccessors)
                            {
                                if (attributeAccessor.Key == meta.PrimaryIdAttribute)
                                    continue;

                                var attr = attributes[attributeAccessor.Key];

                                if (!String.IsNullOrEmpty(attr.AttributeOf))
                                    continue;

                                var value = attributeAccessor.Value(entity);

                                update[attr.LogicalName] = value;
                            }

                            return new UpdateRequest { Target = update };
                        },
                        new OperationNames
                        {
                            InProgressUppercase = "Updating",
                            InProgressLowercase = "updating",
                            CompletedLowercase = "updated"
                        },
                        out recordsAffected,
                        parameterValues,
                        cancellationToken);
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

        public override string ToString()
        {
            return "UPDATE";
        }

        public override object Clone()
        {
            var clone = new UpdateNode
            {
                DataSource = DataSource,
                Index = Index,
                Length = Length,
                LogicalName = LogicalName,
                PrimaryIdSource = PrimaryIdSource,
                Source = (IExecutionPlanNodeInternal)Source.Clone(),
                Sql = Sql
            };

            foreach (var kvp in ColumnMappings)
                clone.ColumnMappings.Add(kvp);

            clone.Source.Parent = clone;
            return clone;
        }
    }
}
