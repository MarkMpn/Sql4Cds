using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Metadata;
using Microsoft.Xrm.Tooling.Connector;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    public class UpdateNode : BaseDmlNode
    {
        private int _executionCount;
        private TimeSpan _duration;

        [Browsable(false)]
        public override int ExecutionCount => _executionCount;

        [Browsable(false)]
        public override TimeSpan Duration => _duration;

        public IDataExecutionPlanNode Source { get; set; }

        public string LogicalName { get; set; }

        public string PrimaryIdSource { get; set; }

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
                var entities = Source.Execute(org, metadata, options, parameterTypes, parameterValues).ToList();

                var meta = metadata[LogicalName];
                var attributes = meta.Attributes.ToDictionary(a => a.LogicalName);

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

                                var update = new Entity(LogicalName, (Guid)entity[PrimaryIdSource]);

                                foreach (var mapping in ColumnMappings)
                                {
                                    var value = entity[mapping.Value];
                                    var attr = attributes[mapping.Key];

                                    if (!String.IsNullOrEmpty(attr.AttributeOf))
                                        continue;

                                    if (value != null)
                                    {
                                        value = SqlTypeConverter.ChangeType(value, attr.GetAttributeType());

                                        if (attr is LookupAttributeMetadata lookupAttr)
                                        {
                                            value = new EntityReference { Id = (Guid)value };

                                            if (lookupAttr.Targets.Length == 1)
                                                ((EntityReference)value).LogicalName = lookupAttr.Targets[0];
                                            else
                                                ((EntityReference)value).LogicalName = (string)entity[ColumnMappings[attr.LogicalName + "type"]];
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
            Source = Source.FoldQuery(metadata, options, parameterTypes);
            return this;
        }

        public override IEnumerable<IDataExecutionPlanNode> GetSources()
        {
            yield return Source;
        }

        public override string ToString()
        {
            return "UPDATE";
        }
    }
}
