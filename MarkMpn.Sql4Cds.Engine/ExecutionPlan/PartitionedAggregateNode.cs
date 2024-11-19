using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlTypes;
using System.IO;
using System.Linq;
using System.Numerics;
using System.ServiceModel;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;
using MarkMpn.Sql4Cds.Engine.FetchXml;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Metadata;
using System.Collections.Concurrent;
using System.Threading;
#if NETCOREAPP
using Microsoft.PowerPlatform.Dataverse.Client;
#else
using Microsoft.Xrm.Tooling.Connector;
#endif

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// Produces aggregate values by repeatedly executing a related FetchXML query over multiple partitions
    /// and combining the results
    /// </summary>
    class PartitionedAggregateNode : BaseAggregateNode
    {
        public class PartitionOverflowException : Exception
        {
        }

        class Partition
        {
            public SqlDateTime MinValue { get; set; }
            public SqlDateTime MaxValue { get; set; }
            public double Percentage { get; set; }
            public int Depth { get; set; }
        }

        private double _progress;
        private BlockingCollection<Partition> _queue;
        private int _pendingPartitions;
        private object _lock;

        /// <summary>
        /// The maximum degree of parallelism to apply to this operation
        /// </summary>
        [Category("Partitioned Aggregate")]
        [Description("The maximum number of partitions that will be executed in parallel")]
        public int MaxDOP { get; set; }

        [Browsable(false)]
        internal string ProgressMessage { get; set; }

        public override IDataExecutionPlanNodeInternal FoldQuery(NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            Source = Source.FoldQuery(context, hints);
            Source.Parent = this;
            MaxDOP = GetMaxDOP(context, hints);
            return this;
        }

        private int GetMaxDOP(NodeCompilationContext context, IList<OptimizerHint> queryHints)
        {
            var fetchXmlNode = (FetchXmlScan)Source;

            if (fetchXmlNode.DataSource == null)
                return 1;

            if (!context.Session.DataSources.TryGetValue(fetchXmlNode.DataSource, out var dataSource))
                throw new NotSupportedQueryFragmentException("Unknown datasource");

            return ParallelismHelper.GetMaxDOP(dataSource, context, queryHints);
        }

        public override void AddRequiredColumns(NodeCompilationContext context, IList<string> requiredColumns)
        {
            // All required columns must already have been added during the original folding of the HashMatchAggregateNode
        }

        protected override IEnumerable<Entity> ExecuteInternal(NodeExecutionContext context)
        {
            var schema = Source.GetSchema(context);
            var groupByCols = GetGroupingColumns(schema);
            var groups = new ConcurrentDictionary<Entity, Dictionary<string, AggregateFunctionState>>(new DistinctEqualityComparer(groupByCols));
            var expressionCompilationContext = new ExpressionCompilationContext(context, schema, null);
            var expressionExecutionContext = new ExpressionExecutionContext(context);

            InitializePartitionedAggregates(expressionCompilationContext);
            var aggregates = CreateAggregateFunctions(expressionExecutionContext, true);

            // Clone the source FetchXmlScan node so we can safely modify it later by adding the partitioning filter
            // We might be called in a loop and need to execute again, so don't make any changes to the original
            // source query.
            var fetchXmlNode = (FetchXmlScan)Source.Clone();

            var name = fetchXmlNode.Entity.name;
            var meta = context.Session.DataSources[fetchXmlNode.DataSource].Metadata[name];
            context.Options.Progress(0, $"Partitioning {GetDisplayName(0, meta)}...");

            // Get the minimum and maximum primary keys from the source
            var minKey = GetMinMaxKey(fetchXmlNode, context, false);
            var maxKey = GetMinMaxKey(fetchXmlNode, context, true);

            if (minKey.IsNull || maxKey.IsNull || minKey >= maxKey)
                throw new PartitionOverflowException();

            // Add the filter to the FetchXML to partition the results
            fetchXmlNode.Entity.AddItem(new filter
            {
                Items = new object[]
                {
                    new condition { attribute = "createdon", @operator = @operator.gt, value = "@PartitionStart", IsVariable = true },
                    new condition { attribute = "createdon", @operator = @operator.le, value = "@PartitionEnd", IsVariable = true }
                }
            });

            var partitionParameterTypes = new Dictionary<string, DataTypeReference>
            {
                ["@PartitionStart"] = DataTypeHelpers.DateTime,
                ["@PartitionEnd"] = DataTypeHelpers.DateTime
            };

            if (context.ParameterTypes != null)
            {
                foreach (var kvp in context.ParameterTypes)
                    partitionParameterTypes[kvp.Key] = kvp.Value;
            }

            // Split recursively, add up values below & above split value if query returns successfully, or re-split on error
            // Range is > MinValue AND <= MaxValue, so start from just before first record to ensure the first record is counted
            var fullRange = new Partition
            {
                MinValue = minKey.Value.AddSeconds(-1),
                MaxValue = maxKey,
                Percentage = 1
            };

            _queue = new BlockingCollection<Partition>();
            _pendingPartitions = 1;

            SplitPartition(fullRange);

            // Multi-thread where possible
            var org = context.Session.DataSources[fetchXmlNode.DataSource].Connection;
            _lock = new object();

#if NETCOREAPP
            var svc = org as ServiceClient;
#else
            var svc = org as CrmServiceClient;
#endif

            var maxDop = MaxDOP;

            if (!ParallelismHelper.CanParallelise(org))
                maxDop = 1;

            if (maxDop == 1)
                svc = null;

            try
            {
                var partitioner = Partitioner.Create(_queue.GetConsumingEnumerable(), EnumerablePartitionerOptions.NoBuffering);
                Parallel.ForEach(partitioner,
                    new ParallelOptions { MaxDegreeOfParallelism = maxDop, CancellationToken = context.Options.CancellationToken },
                    () =>
                    {
                        var ds = new Dictionary<string, DataSource>
                        {
                            [fetchXmlNode.DataSource] = new DataSource
                            {
                                Connection = svc?.Clone() ?? org,
                                Metadata = context.Session.DataSources[fetchXmlNode.DataSource].Metadata,
                                Name = fetchXmlNode.DataSource,
                                TableSizeCache = context.Session.DataSources[fetchXmlNode.DataSource].TableSizeCache,
                                MessageCache = context.Session.DataSources[fetchXmlNode.DataSource].MessageCache
                            }
                        };

                        var fetch = new FetchXmlScan
                        {
                            Alias = fetchXmlNode.Alias,
                            DataSource = fetchXmlNode.DataSource,
                            FetchXml = CloneFetchXml(fetchXmlNode.FetchXml),
                            Parent = this
                        };

                        var partitionParameterValues = new Dictionary<string, INullable>
                        {
                            ["@PartitionStart"] = minKey,
                            ["@PartitionEnd"] = maxKey
                        };

                        if (context.ParameterValues != null)
                        {
                            foreach (var kvp in context.ParameterValues)
                                partitionParameterValues[kvp.Key] = kvp.Value;
                        }

                        var partitionContext = new NodeExecutionContext(context, context.ParameterTypes, partitionParameterValues);

                        return new { Context = partitionContext, Fetch = fetch };
                    },
                    (partition, loopState, index, threadLocalState) =>
                    {
                        try
                        {
                            // Execute the query for this partition
                            ExecuteAggregate(threadLocalState.Context, expressionExecutionContext, aggregates, groups, threadLocalState.Fetch, partition.MinValue, partition.MaxValue);

                            lock (_lock)
                            {
                                _progress += partition.Percentage;
                                ProgressMessage = $"Partitioning {GetDisplayName(0, meta)} ({_progress:P0})...";

                                context.Options.Progress(0, ProgressMessage);
                            }

                            if (Interlocked.Decrement(ref _pendingPartitions) == 0)
                                _queue.CompleteAdding();
                        }
                        catch (Exception ex)
                        {
                            if (ex is QueryExecutionException qee)
                                qee.Node = this;

                            lock (_queue)
                            {
                                if (!GetOrganizationServiceFault(ex, out var fault))
                                {
                                    _queue.CompleteAdding();
                                    throw;
                                }

                                if (!IsAggregateQueryLimitExceeded(fault))
                                {
                                    _queue.CompleteAdding();
                                    throw;
                                }

                                SplitPartition(partition);
                            }
                        }

                        return threadLocalState;
                    },
                    (threadLocalState) =>
                    {
                        // Merge the stats from this clone of the FetchXML node so we can still see total number of executions etc.
                        // in the main query plan.
                        lock (fetchXmlNode)
                        {
                            fetchXmlNode.MergeStatsFrom(threadLocalState.Fetch);
                        }
                    });
            }
            catch (AggregateException aggEx)
            {
                throw aggEx.InnerExceptions[0];
            }

            foreach (var group in groups)
            {
                var result = new Entity();

                for (var i = 0; i < groupByCols.Count; i++)
                    result[groupByCols[i]] = group.Key[groupByCols[i]];

                foreach (var aggregate in GetValues(group.Value))
                    result[aggregate.Key] = aggregate.Value;

                yield return result;
            }
        }

        private void SplitPartition(Partition partition)
        {
            if (_queue.IsAddingCompleted)
                return;

            // Fail if we get stuck on a particularly dense partition. If there's > 50K records in a 10 second window we probably
            // won't be able to split it successfully
            if (partition.MaxValue.Value < partition.MinValue.Value.AddSeconds(10))
            {
                _queue.CompleteAdding();
                throw new PartitionOverflowException();
            }

            // Start splitting partitions in half. Once we've done that a few times and are still hitting the 50K limit, start
            // pre-emptively splitting into smaller chunks
            var splitCount = 2;

            if (partition.Depth > 5)
                splitCount = 4;

            Interlocked.Add(ref _pendingPartitions, splitCount - 1);

            var partitionSize = TimeSpan.FromSeconds((partition.MaxValue.Value - partition.MinValue.Value).TotalSeconds / splitCount);

            var partitionStart = partition.MinValue;

            for (var i = 0; i < splitCount; i++)
            {
                var partitionEnd = partitionStart + partitionSize;

                if (i == splitCount - 1)
                    partitionEnd = partition.MaxValue;

                _queue.Add(new Partition
                {
                    MinValue = partitionStart,
                    MaxValue = partitionEnd,
                    Percentage = partition.Percentage / splitCount,
                    Depth = partition.Depth + 1
                });

                partitionStart = partitionEnd;
            }
        }

        private void ExecuteAggregate(NodeExecutionContext context, ExpressionExecutionContext expressionContext, Dictionary<string, AggregateFunction> aggregates, ConcurrentDictionary<Entity, Dictionary<string, AggregateFunctionState>> groups, FetchXmlScan fetchXmlNode, SqlDateTime minValue, SqlDateTime maxValue)
        {
            context.ParameterValues["@PartitionStart"] = minValue;
            context.ParameterValues["@PartitionEnd"] = maxValue;

            var results = fetchXmlNode.Execute(context);

            foreach (var entity in results)
            {
                // Update aggregates
                var values = groups.GetOrAdd(entity, _ => ResetAggregates(aggregates));

                lock (expressionContext)
                {
                    expressionContext.Entity = entity;

                    foreach (var func in values.Values)
                        func.AggregateFunction.NextPartition(func.State, expressionContext);
                }
            }
        }

        private SqlDateTime GetMinMaxKey(FetchXmlScan fetchXmlNode, NodeExecutionContext context, bool max)
        {
            // Create a new FetchXmlScan node with a copy of the original query
            var minMaxNode = new FetchXmlScan
            {
                Alias = "minmax",
                DataSource = fetchXmlNode.DataSource,
                FetchXml = CloneFetchXml(fetchXmlNode.FetchXml),
                Parent = this
            };

            // Remove the aggregate settings and all attributes from the query
            minMaxNode.FetchXml.aggregate = false;
            RemoveAttributesAndOrders(minMaxNode.Entity);

            // Add the primary key attribute of the root entity
            minMaxNode.Entity.AddItem(new FetchAttributeType { name = "createdon" });

            // Sort by the primary key
            minMaxNode.Entity.AddItem(new FetchOrderType { attribute = "createdon", descending = max });

            // Only need to retrieve the first item
            minMaxNode.FetchXml.top = "1";

            try
            {
                var result = minMaxNode.Execute(context).FirstOrDefault();

                if (result == null)
                    return SqlDateTime.Null;

                return (SqlDateTime)result["minmax.createdon"];
            }
            catch (QueryExecutionException ex)
            {
                ex.Node = this;
                throw;
            }
        }

        private void RemoveAttributesAndOrders(FetchEntityType entity)
        {
            if (entity.Items != null)
            {
                entity.Items = entity.Items.Where(o => !(o is FetchAttributeType) && !(o is allattributes) && !(o is FetchOrderType)).ToArray();

                foreach (var linkEntity in entity.Items.OfType<FetchLinkEntityType>())
                    RemoveAttributesAndOrders(linkEntity);
            }
        }

        private void RemoveAttributesAndOrders(FetchLinkEntityType entity)
        {
            if (entity.Items != null)
            {
                entity.Items = entity.Items.Where(o => !(o is FetchAttributeType) && !(o is allattributes) && !(o is FetchOrderType)).ToArray();

                foreach (var linkEntity in entity.Items.OfType<FetchLinkEntityType>())
                    RemoveAttributesAndOrders(linkEntity);
            }
        }

        private FetchXml.FetchType CloneFetchXml(FetchXml.FetchType fetchXml)
        {
            var serializer = new XmlSerializer(typeof(FetchXml.FetchType));
            using (var writer = new StringWriter())
            {
                serializer.Serialize(writer, fetchXml);

                using (var reader = new StringReader(writer.ToString()))
                {
                    return (FetchXml.FetchType)serializer.Deserialize(reader);
                }
            }
        }

        public override object Clone()
        {
            var clone = new PartitionedAggregateNode
            {
                Source = (IDataExecutionPlanNodeInternal)Source.Clone(),
                MaxDOP = MaxDOP
            };

            foreach (var kvp in Aggregates)
                clone.Aggregates.Add(kvp.Key, kvp.Value);

            clone.GroupBy.AddRange(GroupBy);
            clone.Source.Parent = clone;

            foreach (var sort in WithinGroupSorts)
                clone.WithinGroupSorts.Add(sort.Clone());

            return clone;
        }
    }
}
