using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Tooling.Connector;

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
        public IDictionary<string, string> ColumnMappings { get; } = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        public override void AddRequiredColumns(IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes, IList<string> requiredColumns)
        {
            foreach (var col in ColumnMappings.Values)
            {
                if (!requiredColumns.Contains(col))
                    requiredColumns.Add(col);
            }

            Source.AddRequiredColumns(metadata, parameterTypes, requiredColumns);
        }

        public override string Execute(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues)
        {
            _executionCount++;

            try
            {
                _timer.Resume();

                var entities = GetDmlSourceEntities(org, metadata, options, parameterTypes, parameterValues, out var schema);

                // Precompile mappings with type conversions
                var meta = metadata[LogicalName];
                var attributes = meta.Attributes.ToDictionary(a => a.LogicalName);
                var dateTimeKind = options.UseLocalTimeZone ? DateTimeKind.Local : DateTimeKind.Utc;
                var attributeAccessors = CompileColumnMappings(ColumnMappings, schema, attributes, dateTimeKind);
                attributeAccessors.TryGetValue(meta.PrimaryIdAttribute, out var primaryIdAccessor);

                return ExecuteDmlOperation(
                    org,
                    options,
                    entities,
                    meta,
                    entity =>
                    {
                        var insert = new Entity(LogicalName);

                        if (primaryIdAccessor != null)
                            insert.Id = (Guid)primaryIdAccessor(entity);

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
                    },
                    new OperationNames
                    {
                        InProgressUppercase = "Inserting",
                        InProgressLowercase = "inserting",
                        CompletedLowercase = "inserted"
                    });
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
            finally
            {
                _timer.Pause();
            }
        }

        public override string ToString()
        {
            return "INSERT";
        }
    }
}
