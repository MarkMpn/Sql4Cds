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

        public override IDataExecutionPlanNodeInternal FoldQuery(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, DataTypeReference> parameterTypes, IList<OptimizerHint> hints)
        {
            Source = Source.FoldQuery(dataSources, options, parameterTypes, hints);
            Source.Parent = this;
            return this;
        }

        public override void AddRequiredColumns(IDictionary<string, DataSource> dataSources, IDictionary<string, DataTypeReference> parameterTypes, IList<string> requiredColumns)
        {
            // All required columns must already have been added during the original folding of the HashMatchAggregateNode
        }

        protected override IEnumerable<Entity> ExecuteInternal(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, DataTypeReference> parameterTypes, IDictionary<string, object> parameterValues)
        {
            var schema = Source.GetSchema(dataSources, parameterTypes);
            var groupByCols = GetGroupingColumns(schema);
            var groups = new ConcurrentDictionary<Entity, Dictionary<string, AggregateFunctionState>>(new DistinctEqualityComparer(groupByCols));

            InitializePartitionedAggregates(schema, parameterTypes);
            var aggregates = CreateAggregateFunctions(parameterValues, options, true);

            var fetchXmlNode = (FetchXmlScan)Source;

            var name = fetchXmlNode.Entity.name;
            var meta = dataSources[fetchXmlNode.DataSource].Metadata[name];
            options.Progress(0, $"Partitioning {GetDisplayName(0, meta)}...");

            // Get the minimum and maximum primary keys from the source
            var minKey = GetMinMaxKey(fetchXmlNode, dataSources, options, parameterTypes, parameterValues, false);
            var maxKey = GetMinMaxKey(fetchXmlNode, dataSources, options, parameterTypes, parameterValues, true);

            if (minKey.IsNull || maxKey.IsNull || minKey == maxKey)
                throw new QueryExecutionException("Cannot partition query");

            // Add the filter to the FetchXML to partition the results
            fetchXmlNode.Entity.AddItem(new filter
            {
                Items = new object[]
                {
                    new condition { attribute = "createdon", @operator = @operator.gt, value = "@PartitionStart" },
                    new condition { attribute = "createdon", @operator = @operator.le, value = "@PartitionEnd" }
                }
            });

            var partitionParameterTypes = new Dictionary<string, DataTypeReference>
            {
                ["@PartitionStart"] = new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.DateTime },
                ["@PartitionEnd"] = new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.DateTime }
            };

            if (parameterTypes != null)
            {
                foreach (var kvp in parameterTypes)
                    partitionParameterTypes[kvp.Key] = kvp.Value;
            }

            if (minKey > maxKey)
                throw new QueryExecutionException("Cannot partition query");

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
            var org = dataSources[fetchXmlNode.DataSource].Connection;
            var maxDop = options.MaxDegreeOfParallelism;
            _lock = new object();

#if NETCOREAPP
            var svc = org as ServiceClient;

            if (maxDop <= 1 || svc == null || svc.ActiveAuthenticationType != Microsoft.PowerPlatform.Dataverse.Client.AuthenticationType.OAuth)
            {
                maxDop = 1;
                svc = null;
            }
#else
            var svc = org as CrmServiceClient;

            if (maxDop <= 1 || svc == null || svc.ActiveAuthenticationType != Microsoft.Xrm.Tooling.Connector.AuthenticationType.OAuth)
            {
                maxDop = 1;
                svc = null;
            }
#endif

            try
            {
                Parallel.For(0, maxDop, index =>
                {
                    var ds = new Dictionary<string, DataSource>
                    {
                        [fetchXmlNode.DataSource] = new DataSource
                        {
                            Connection = svc?.Clone() ?? org,
                            Metadata = dataSources[fetchXmlNode.DataSource].Metadata,
                            Name = fetchXmlNode.DataSource,
                            TableSizeCache = dataSources[fetchXmlNode.DataSource].TableSizeCache
                        }
                    };

                    var fetch = new FetchXmlScan
                    {
                        Alias = fetchXmlNode.Alias,
                        DataSource = fetchXmlNode.DataSource,
                        FetchXml = CloneFetchXml(fetchXmlNode.FetchXml),
                        Parent = this
                    };

                    var partitionParameterValues = new Dictionary<string, object>
                    {
                        ["@PartitionStart"] = minKey,
                        ["@PartitionEnd"] = maxKey
                    };

                    if (parameterValues != null)
                    {
                        foreach (var kvp in parameterValues)
                            partitionParameterValues[kvp.Key] = kvp.Value;
                    }

                    foreach (var partition in _queue.GetConsumingEnumerable())
                    {
                        try
                        {
                            // Execute the query for this partition
                            ExecuteAggregate(ds, options, partitionParameterTypes, partitionParameterValues, aggregates, groups, fetch, partition.MinValue, partition.MaxValue);

                            lock (_lock)
                            {
                                _progress += partition.Percentage;
                                options.Progress(0, $"Partitioning {GetDisplayName(0, meta)} ({_progress:P0})...");
                            }

                            if (Interlocked.Decrement(ref _pendingPartitions) == 0)
                                _queue.CompleteAdding();
                        }
                        catch (Exception ex)
                        {
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
                    }

                    // Merge the stats from this clone of the FetchXML node so we can still see total number of executions etc.
                    // in the main query plan.
                    lock (fetchXmlNode)
                    {
                        fetchXmlNode.MergeStatsFrom(fetch);
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
                throw new PartitionOverflowException();

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

        private void ExecuteAggregate(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, DataTypeReference> parameterTypes, IDictionary<string, object> parameterValues, Dictionary<string, AggregateFunction> aggregates, ConcurrentDictionary<Entity, Dictionary<string, AggregateFunctionState>> groups, FetchXmlScan fetchXmlNode, SqlDateTime minValue, SqlDateTime maxValue)
        {
            parameterValues["@PartitionStart"] = minValue;
            parameterValues["@PartitionEnd"] = maxValue;

            var results = fetchXmlNode.Execute(dataSources, options, parameterTypes, parameterValues);

            foreach (var entity in results)
            {
                // Update aggregates
                var values = groups.GetOrAdd(entity, _ => ResetAggregates(aggregates));

                lock (values)
                {
                    foreach (var func in values.Values)
                        func.AggregateFunction.NextPartition(entity, func.State);
                }
            }
        }

        private SqlDateTime GetMinMaxKey(FetchXmlScan fetchXmlNode, IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, DataTypeReference> parameterTypes, IDictionary<string, object> parameterValues, bool max)
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
                var result = minMaxNode.Execute(dataSources, options, parameterTypes, parameterValues).FirstOrDefault();

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
    }
}
