﻿using System;
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
        /// The column that contains the primary ID of the records to delete
        /// </summary>
        [Category("Delete")]
        [Description("The column that contains the primary ID of the records to delete")]
        public string PrimaryIdSource { get; set; }

        public override void AddRequiredColumns(IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes, IList<string> requiredColumns)
        {
            if (!requiredColumns.Contains(PrimaryIdSource))
                requiredColumns.Add(PrimaryIdSource);

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
                var fullMappings = new Dictionary<string, string>
                {
                    [meta.PrimaryIdAttribute] = PrimaryIdSource
                };
                var attributeAccessors = CompileColumnMappings(fullMappings, schema, attributes, dateTimeKind);
                var primaryIdAccessor = attributeAccessors[meta.PrimaryIdAttribute];

                // Check again that the update is allowed. Don't count any UI interaction in the execution time
                _timer.Pause();
                if (options.Cancelled || !options.ConfirmDelete(entities.Count, meta))
                    throw new OperationCanceledException("DELETE cancelled by user");

                _timer.Resume();
                return ExecuteDmlOperation(
                    org,
                    options,
                    entities,
                    meta,
                    entity =>
                    {
                        return new DeleteRequest
                        {
                            Target = new EntityReference(LogicalName, (Guid)primaryIdAccessor(entity))
                        };
                    },
                    new OperationNames
                    {
                        InProgressUppercase = "Deleting",
                        InProgressLowercase = "deleting",
                        CompletedLowercase = "deleted"
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
            return "DELETE";
        }
    }
}