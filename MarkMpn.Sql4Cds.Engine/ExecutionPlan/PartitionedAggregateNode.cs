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
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// Produces aggregate values by repeatedly executing a related FetchXML query over multiple partitions
    /// and combining the results
    /// </summary>
    class PartitionedAggregateNode : BaseAggregateNode
    {
        public override IDataExecutionPlanNode FoldQuery(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes)
        {
            Source = Source.FoldQuery(dataSources, options, parameterTypes);
            Source.Parent = this;
            return this;
        }

        protected override IEnumerable<Entity> ExecuteInternal(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues)
        {
            var groups = new Dictionary<GroupingKey, Dictionary<string, AggregateFunction>>();
            var schema = Source.GetSchema(dataSources, parameterTypes);
            var groupByCols = GetGroupingColumns(schema);

            InitializePartitionedAggregates(schema, parameterTypes);

            var fetchXmlNode = (FetchXmlScan)Source;

            // Get the minimum and maximum primary keys from the source
            var minKey = GetMinMaxKey(fetchXmlNode, dataSources, options, parameterTypes, parameterValues, false);
            var maxKey = GetMinMaxKey(fetchXmlNode, dataSources, options, parameterTypes, parameterValues, true);

            if (minKey.IsNull || maxKey.IsNull || minKey == maxKey)
                throw new QueryExecutionException("Cannot partition query");

            // Add the filter to the FetchXML to partition the results
            var metadata = dataSources[fetchXmlNode.DataSource].Metadata[fetchXmlNode.Entity.name];
            fetchXmlNode.Entity.AddItem(new filter
            {
                Items = new object[]
                {
                    new condition { attribute = metadata.PrimaryIdAttribute, @operator = @operator.ge, value = "@PartitionStart" },
                    new condition { attribute = metadata.PrimaryIdAttribute, @operator = @operator.le, value = "@PartitionEnd" }
                }
            });

            var partitionParameterTypes = new Dictionary<string, Type>
            {
                ["@PartitionStart"] = typeof(SqlGuid),
                ["@PartitionEnd"] = typeof(SqlGuid)
            };

            var partionParameterValues = new Dictionary<string, object>
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
                    partionParameterValues[kvp.Key] = kvp.Value;
            }

            var minValue = GuidToNumber(minKey.Value);
            var maxValue = GuidToNumber(maxKey.Value);

            if (minValue > maxValue)
                throw new QueryExecutionException("Cannot partition query");

            // Split recursively, add up values below & above split value if query returns successfully, or re-split on error
            PartitionAggregate(dataSources, options, partitionParameterTypes, partionParameterValues, groups, groupByCols, fetchXmlNode, minValue, maxValue);

            foreach (var group in groups)
            {
                var result = new Entity();

                for (var i = 0; i < GroupBy.Count; i++)
                    result[groupByCols[i]] = group.Key.Values[i];

                foreach (var aggregate in group.Value)
                    result[aggregate.Key] = aggregate.Value.Value;

                yield return result;
            }
        }

        private void PartitionAggregate(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues, Dictionary<GroupingKey, Dictionary<string, AggregateFunction>> groups, List<string> groupByCols, FetchXmlScan fetchXmlNode, BigInteger minValue, BigInteger maxValue)
        {
            // Repeatedly split the primary key space until an aggregate query returns without error
            var split = minValue + (maxValue - minValue) / 2;

            try
            {
                // Execute the query with the partition minValue -> split
                ExecuteAggregate(dataSources, options, parameterTypes, parameterValues, groups, groupByCols, fetchXmlNode, minValue, split);
            }
            catch (Exception ex)
            {
                if (!GetOrganizationServiceFault(ex, out var fault))
                    throw;

                if (!IsAggregateQueryLimitExceeded(fault))
                    throw;

                PartitionAggregate(dataSources, options, parameterTypes, parameterValues, groups, groupByCols, fetchXmlNode, minValue, split);
            }

            try
            {
                // Execute the query with the partition split + 1 -> maxValue
                ExecuteAggregate(dataSources, options, parameterTypes, parameterValues, groups, groupByCols, fetchXmlNode, split + 1, maxValue);
            }
            catch (Exception ex)
            {
                if (!GetOrganizationServiceFault(ex, out var fault))
                    throw;

                if (!IsAggregateQueryLimitExceeded(fault))
                    throw;

                PartitionAggregate(dataSources, options, parameterTypes, parameterValues, groups, groupByCols, fetchXmlNode, split + 1, maxValue);
            }
        }

        private void ExecuteAggregate(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues, Dictionary<GroupingKey, Dictionary<string, AggregateFunction>> groups, List<string> groupByCols, FetchXmlScan fetchXmlNode, BigInteger minValue, BigInteger maxValue)
        {
            parameterValues["@PartitionStart"] = new SqlGuid(NumberToGuid(minValue));
            parameterValues["@PartitionEnd"] = new SqlGuid(NumberToGuid(maxValue));

            var results = fetchXmlNode.Execute(dataSources, options, parameterTypes, parameterValues);

            foreach (var entity in results)
            {
                // Update aggregates
                var key = new GroupingKey(entity, groupByCols);

                if (!groups.TryGetValue(key, out var values))
                {
                    values = base.CreateGroupValues(parameterValues, options, true);
                    groups[key] = values;
                }

                foreach (var func in values.Values)
                    func.NextPartition(entity);
            }
        }

        private static readonly int[] _guidOrder = new []
        {
            3,
            2,
            1,
            0,
            5,
            4,
            7,
            6,
            9,
            8,
            15,
            14,
            13,
            12,
            11,
            10
        };

        public BigInteger GuidToNumber(Guid guid)
        {
            var bytes = guid.ToByteArray();

            // Shuffle the bytes into order of their significance. BigInteger uses little-endian
            var shuffled = new byte[17];
            for (var i = 0; i < 16; i++)
                shuffled[i] = bytes[_guidOrder[i]];

            var value = new BigInteger(shuffled);
            return value;
        }

        public Guid NumberToGuid(BigInteger integer)
        {
            var bytes = integer.ToByteArray();

            var shuffled = new byte[16];
            for (var i = 0; i < 16 && i < bytes.Length; i++)
                shuffled[_guidOrder[i]] = bytes[i];

            var value = new Guid(shuffled);
            return value;
        }

        private SqlGuid GetMinMaxKey(FetchXmlScan fetchXmlNode, IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues, bool max)
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
            RemoveAttributes(minMaxNode.Entity);

            // Add the primary key attribute of the root entity
            var metadata = dataSources[minMaxNode.DataSource].Metadata[minMaxNode.Entity.name];
            minMaxNode.Entity.AddItem(new FetchAttributeType { name = metadata.PrimaryIdAttribute });

            // Sort by the primary key
            minMaxNode.Entity.AddItem(new FetchOrderType { attribute = metadata.PrimaryIdAttribute, descending = max });

            // Only need to retrieve the first item
            minMaxNode.FetchXml.top = "1";

            var result = minMaxNode.Execute(dataSources, options, parameterTypes, parameterValues).FirstOrDefault();

            if (result == null)
                return SqlGuid.Null;

            return (SqlEntityReference)result[$"minmax.{metadata.PrimaryIdAttribute}"];
        }

        private void RemoveAttributes(FetchEntityType entity)
        {
            if (entity.Items != null)
            {
                entity.Items = entity.Items.Where(o => !(o is FetchAttributeType) && !(o is allattributes)).ToArray();

                foreach (var linkEntity in entity.Items.OfType<FetchLinkEntityType>())
                    RemoveAttributes(linkEntity);
            }
        }

        private void RemoveAttributes(FetchLinkEntityType entity)
        {
            if (entity.Items != null)
            {
                entity.Items = entity.Items.Where(o => !(o is FetchAttributeType) && !(o is allattributes)).ToArray();

                foreach (var linkEntity in entity.Items.OfType<FetchLinkEntityType>())
                    RemoveAttributes(linkEntity);
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
