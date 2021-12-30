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
        }

        private double _progress;
        private Queue<Partition> _queue;

        public override IDataExecutionPlanNode FoldQuery(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes, IList<OptimizerHint> hints)
        {
            Source = Source.FoldQuery(dataSources, options, parameterTypes, hints);
            Source.Parent = this;
            return this;
        }

        public override void AddRequiredColumns(IDictionary<string, DataSource> dataSources, IDictionary<string, Type> parameterTypes, IList<string> requiredColumns)
        {
            // All required columns must already have been added during the original folding of the HashMatchAggregateNode
        }

        protected override IEnumerable<Entity> ExecuteInternal(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues)
        {
            var groups = new Dictionary<GroupingKey, Dictionary<string, AggregateFunctionState>>();
            var schema = Source.GetSchema(dataSources, parameterTypes);
            var groupByCols = GetGroupingColumns(schema);

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

            var partitionParameterTypes = new Dictionary<string, Type>
            {
                ["@PartitionStart"] = typeof(SqlDateTime),
                ["@PartitionEnd"] = typeof(SqlDateTime)
            };

            var partitionParameterValues = new Dictionary<string, object>
            {
                ["@PartitionStart"] = minKey,
                ["@PartitionEnd"] = maxKey
            };

            if (parameterTypes != null)
            {
                foreach (var kvp in parameterTypes)
                    partitionParameterTypes[kvp.Key] = kvp.Value;
            }

            if (parameterValues != null)
            {
                foreach (var kvp in parameterValues)
                    partitionParameterValues[kvp.Key] = kvp.Value;
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

            _queue = new Queue<Partition>();
            SplitPartition(fullRange);

            while (_queue.Count > 0)
            {
                var partition = _queue.Dequeue();

                try
                {
                    // Execute the query with the partition minValue -> split
                    ExecuteAggregate(dataSources, options, partitionParameterTypes, partitionParameterValues, aggregates, groups, groupByCols, fetchXmlNode, partition.MinValue, partition.MaxValue);

                    _progress += partition.Percentage;
                    options.Progress(0, $"Partitioning {GetDisplayName(0, meta)} ({_progress:P0})...");
                }
                catch (Exception ex)
                {
                    if (!GetOrganizationServiceFault(ex, out var fault))
                        throw;

                    if (!IsAggregateQueryLimitExceeded(fault))
                        throw;

                    SplitPartition(partition);
                }
            }

            foreach (var group in groups)
            {
                var result = new Entity();

                for (var i = 0; i < GroupBy.Count; i++)
                    result[groupByCols[i]] = group.Key.Values[i];

                foreach (var aggregate in GetValues(group.Value))
                    result[aggregate.Key] = aggregate.Value;

                yield return result;
            }
        }

        private void SplitPartition(Partition partition)
        {
            // Fail if we get stuck on a particularly dense partition. If there's > 50K records in a 10 second window we probably
            // won't be able to split it successfully
            if (partition.MaxValue.Value < partition.MinValue.Value.AddSeconds(10))
                throw new PartitionOverflowException();

            var split = partition.MinValue.Value + TimeSpan.FromSeconds((partition.MaxValue.Value - partition.MinValue.Value).TotalSeconds / 2);

            _queue.Enqueue(new Partition
            {
                MinValue = partition.MinValue,
                MaxValue = split,
                Percentage = partition.Percentage / 2
            });

            _queue.Enqueue(new Partition
            {
                MinValue = split,
                MaxValue = partition.MaxValue,
                Percentage = partition.Percentage / 2
            });
        }

        private void ExecuteAggregate(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues, Dictionary<string, AggregateFunction> aggregates, Dictionary<GroupingKey, Dictionary<string, AggregateFunctionState>> groups, List<string> groupByCols, FetchXmlScan fetchXmlNode, SqlDateTime minValue, SqlDateTime maxValue)
        {
            parameterValues["@PartitionStart"] = minValue;
            parameterValues["@PartitionEnd"] = maxValue;

            var results = fetchXmlNode.Execute(dataSources, options, parameterTypes, parameterValues);

            foreach (var entity in results)
            {
                // Update aggregates
                var key = new GroupingKey(entity, groupByCols);

                if (!groups.TryGetValue(key, out var values))
                {
                    values = ResetAggregates(aggregates);
                    groups[key] = values;
                }

                foreach (var func in values.Values)
                    func.AggregateFunction.NextPartition(entity, func.State);
            }
        }

        private SqlDateTime GetMinMaxKey(FetchXmlScan fetchXmlNode, IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues, bool max)
        {
            // Create a new FetchXmlScan node with a copy of the original query
            var minMaxNode = new FetchXmlScan
            {
                Alias = "minmax",
                DataSource = fetchXmlNode.DataSource,
                FetchXml = CloneFetchXml(fetchXmlNode.FetchXml)
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
