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

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// Produces aggregate values by repeatedly executing a related FetchXML query over multiple partitions
    /// and combining the results
    /// </summary>
    class PartitionedFetchXmlAggregateNode : BaseDataNode, ISingleSourceExecutionPlanNode
    {
        class GroupingKey
        {
            private readonly int _hashCode;

            public GroupingKey(Entity entity, List<string> columns)
            {
                Values = columns.Select(col => entity[col]).ToList();
                _hashCode = 0;

                foreach (var value in Values)
                {
                    if (value == null)
                        continue;

                    _hashCode ^= StringComparer.OrdinalIgnoreCase.GetHashCode(value);
                }
            }

            public List<object> Values { get; }

            public override int GetHashCode() => _hashCode;

            public override bool Equals(object obj)
            {
                var other = (GroupingKey)obj;

                for (var i = 0; i < Values.Count; i++)
                {
                    if (Values[i] == null && other.Values[i] == null)
                        continue;

                    if (Values[i] == null || other.Values[i] == null)
                        return false;

                    if (!StringComparer.OrdinalIgnoreCase.Equals(Values[i], other.Values[i]))
                        return false;
                }

                return true;
            }
        }

        /// <summary>
        /// The list of columns to group the results by
        /// </summary>
        [Category("Partitioned FetchXML Aggregate")]
        [Description("The list of columns to group the results by")]
        [DisplayName("Group By")]
        public List<ColumnReferenceExpression> GroupBy { get; } = new List<ColumnReferenceExpression>();

        /// <summary>
        /// The list of aggregate values to produce
        /// </summary>
        [Category("Partitioned FetchXML Aggregate")]
        [Description("The list of aggregate values to produce")]
        public Dictionary<string, Aggregate> Aggregates { get; } = new Dictionary<string, Aggregate>();

        public IDataExecutionPlanNode Source { get; set; }

        public override void AddRequiredColumns(IDictionary<string, DataSource> dataSources, IDictionary<string, Type> parameterTypes, IList<string> requiredColumns)
        {
            // Columns required by previous nodes must be derived from this node, so no need to pass them through.
            // Just calculate the columns that are required to calculate the groups & aggregates
            var scalarRequiredColumns = new List<string>();
            if (GroupBy != null)
                scalarRequiredColumns.AddRange(GroupBy.Select(g => g.GetColumnName()));

            scalarRequiredColumns.AddRange(Aggregates.Where(agg => agg.Value.SqlExpression != null).SelectMany(agg => agg.Value.SqlExpression.GetColumns()).Distinct());

            Source.AddRequiredColumns(dataSources, parameterTypes, scalarRequiredColumns);
        }

        public override int EstimateRowsOut(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes)
        {
            if (GroupBy.Count == 0)
                return 1;

            return Source.EstimateRowsOut(dataSources, options, parameterTypes) * 4 / 10;
        }

        public override IDataExecutionPlanNode FoldQuery(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes)
        {
            Source = Source.FoldQuery(dataSources, options, parameterTypes);
            Source.Parent = this;
            return this;
        }

        public override NodeSchema GetSchema(IDictionary<string, DataSource> dataSources, IDictionary<string, Type> parameterTypes)
        {
            var sourceSchema = Source.GetSchema(dataSources, parameterTypes);
            var schema = new NodeSchema();

            foreach (var group in GroupBy)
            {
                var colName = group.GetColumnName();
                sourceSchema.ContainsColumn(colName, out var normalized);
                schema.Schema[normalized] = sourceSchema.Schema[normalized];

                foreach (var alias in sourceSchema.Aliases.Where(a => a.Value.Contains(normalized)))
                {
                    if (!schema.Aliases.TryGetValue(alias.Key, out var aliases))
                    {
                        aliases = new List<string>();
                        schema.Aliases[alias.Key] = aliases;
                    }

                    aliases.Add(normalized);
                }

                if (GroupBy.Count == 1)
                    schema.PrimaryKey = normalized;
            }

            foreach (var aggregate in Aggregates)
            {
                Type aggregateType;

                switch (aggregate.Value.AggregateType)
                {
                    case AggregateType.Count:
                    case AggregateType.CountStar:
                        aggregateType = typeof(SqlInt32);
                        break;

                    default:
                        aggregateType = aggregate.Value.SqlExpression.GetType(sourceSchema, null, parameterTypes);
                        break;
                }

                schema.Schema[aggregate.Key] = aggregateType;
            }

            return schema;
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            yield return Source;
        }

        protected override IEnumerable<Entity> ExecuteInternal(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues)
        {
            var groups = new Dictionary<GroupingKey, Dictionary<string, AggregateFunction>>();
            var schema = Source.GetSchema(dataSources, parameterTypes);
            var groupByCols = GroupBy
                .Select(col =>
                {
                    var colName = col.GetColumnName();
                    schema.ContainsColumn(colName, out colName);
                    return colName;
                })
                .ToList();

            foreach (var aggregate in Aggregates.Where(agg => agg.Value.SqlExpression != null))
            {
                var sourceExpression = aggregate.Value.SqlExpression;

                // Sum and Aggregate need to have Decimal values as input for their calculations to work correctly
                if (aggregate.Value.AggregateType == AggregateType.Average || aggregate.Value.AggregateType == AggregateType.Sum)
                    sourceExpression = new ConvertCall { Parameter = sourceExpression, DataType = new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.Decimal } };

                aggregate.Value.Expression = sourceExpression.Compile(schema, parameterTypes);

                aggregate.Value.ReturnType = aggregate.Value.SqlExpression.GetType(schema, null, parameterTypes);

                if (aggregate.Value.AggregateType == AggregateType.Average)
                {
                    if (aggregate.Value.ReturnType == typeof(SqlByte) || aggregate.Value.ReturnType == typeof(SqlInt16))
                        aggregate.Value.ReturnType = typeof(SqlInt32);
                }
            }

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

            while (minValue <= maxValue)
            {
                // Repeatedly split the primary key space until an aggregate query returns without error
                // Quit with an error after 10 iterations
                const int maxSplits = 10;
                var split = minValue + (maxValue - minValue) / 2;

                for (var i = 0; i < maxSplits; i++)
                {
                    try
                    {
                        // Execute the query with the partition minValue -> split
                        var results = fetchXmlNode.Execute(dataSources, options, partitionParameterTypes, partionParameterValues);

                        foreach (var entity in results)
                        {
                            // Update aggregates
                            var key = new GroupingKey(entity, groupByCols);

                            if (!groups.TryGetValue(key, out var values))
                            {
                                values = new Dictionary<string, AggregateFunction>();

                                foreach (var aggregate in Aggregates)
                                {
                                    Func<Entity, object> selector = null;

                                    if (aggregate.Value.AggregateType != AggregateType.CountStar)
                                        selector = e => aggregate.Value.Expression(e, parameterValues, options);

                                    switch (aggregate.Value.AggregateType)
                                    {
                                        case AggregateType.Average:
                                            throw new QueryExecutionException("Average aggregate not supported for partitions");

                                        case AggregateType.Count:
                                            values[aggregate.Key] = new CountColumn(selector);
                                            break;

                                        case AggregateType.CountStar:
                                            values[aggregate.Key] = new Count(null);
                                            break;

                                        case AggregateType.Max:
                                            values[aggregate.Key] = new Max(selector, aggregate.Value.ReturnType);
                                            break;

                                        case AggregateType.Min:
                                            values[aggregate.Key] = new Min(selector, aggregate.Value.ReturnType);
                                            break;

                                        case AggregateType.Sum:
                                            values[aggregate.Key] = new Sum(selector, aggregate.Value.ReturnType);
                                            break;

                                        default:
                                            throw new QueryExecutionException("Unknown aggregate type");
                                    }

                                    if (aggregate.Value.Distinct)
                                        throw new QueryExecutionException("Distinct aggregates not supported for partitions");

                                    values[aggregate.Key].Reset();
                                }

                                groups[key] = values;
                            }

                            foreach (var func in values.Values)
                                func.NextPartition(entity);
                        }

                        break;
                    }
                    catch (Exception ex)
                    {
                        if (i == maxSplits - 1)
                            throw;

                        if (!IsAggregateQueryRecordLimitExceeded(ex))
                            throw;
                    }

                    split = minValue + (split - minValue) / 2;
                }

                // Update minimum primary key and repeat to process next partition
                minValue = split + 1;
            }

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

        private static readonly int[] x_rgiGuidOrder = new int[16]
        {
          5,
          4,
          3,
          2,
          1,
          0,
          7,
          6,
          9,
          8,
          11,
          10,
          15,
          14,
          13,
          12
        };

        private BigInteger GuidToNumber(Guid guid)
        {
            var bytes = guid.ToByteArray();

            // Shuffle the bytes into order of their significance. BigInteger uses little-endian
            var shuffled = new byte[16];
            for (var i = 0; i < bytes.Length; i++)
                shuffled[i] = bytes[x_rgiGuidOrder[i]];

            var value = new BigInteger(shuffled);
            return value;
        }

        private Guid NumberToGuid(BigInteger integer)
        {
            var bytes = integer.ToByteArray();

            var shuffled = new byte[16];
            for (var i = 0; i < bytes.Length; i++)
                shuffled[x_rgiGuidOrder[i]] = bytes[i];

            var value = new Guid(bytes);
            return value;
        }

        private bool IsAggregateQueryRecordLimitExceeded(Exception ex)
        {
            if (ex is QueryExecutionException qee)
                ex = qee.InnerException;

            if (!(ex is FaultException<OrganizationServiceFault> faultEx))
                return false;

            var fault = faultEx.Detail;
            while (fault.InnerFault != null)
                fault = fault.InnerFault;

            /*
             * 0x8004E023 / -2147164125	
             * Name: AggregateQueryRecordLimitExceeded
             * Message: The maximum record limit is exceeded. Reduce the number of records.
             */
            if (fault.ErrorCode == -2147164125)
                return true;

            return false;
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

        private void RemoveAttributes(FetchXml.FetchEntityType entity)
        {
            if (entity.Items != null)
            {
                entity.Items = entity.Items.Where(o => !(o is FetchAttributeType) && !(o is allattributes)).ToArray();

                foreach (var linkEntity in entity.Items.OfType<FetchLinkEntityType>())
                    RemoveAttributes(linkEntity);
            }
        }

        private void RemoveAttributes(FetchXml.FetchLinkEntityType entity)
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
