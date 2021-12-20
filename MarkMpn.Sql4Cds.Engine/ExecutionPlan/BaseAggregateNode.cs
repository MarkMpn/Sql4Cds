using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlTypes;
using System.IO;
using System.Linq;
using System.ServiceModel;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;
using MarkMpn.Sql4Cds.Engine.FetchXml;
using MarkMpn.Sql4Cds.Engine.Visitors;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Metadata;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// Produces aggregate values
    /// </summary>
    abstract class BaseAggregateNode : BaseDataNode, ISingleSourceExecutionPlanNode
    {
        protected class GroupingKey
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
        [Category("Aggregate")]
        [Description("The list of columns to group the results by")]
        [DisplayName("Group By")]
        public List<ColumnReferenceExpression> GroupBy { get; } = new List<ColumnReferenceExpression>();

        /// <summary>
        /// The list of aggregate values to produce
        /// </summary>
        [Category("Aggregate")]
        [Description("The list of aggregate values to produce")]
        public Dictionary<string, Aggregate> Aggregates { get; } = new Dictionary<string, Aggregate>();

        [Browsable(false)]
        public IDataExecutionPlanNode Source { get; set; }

        protected void InitializeAggregates(NodeSchema schema, IDictionary<string, Type> parameterTypes)
        {
            foreach (var aggregate in Aggregates.Where(agg => agg.Value.SqlExpression != null))
            {
                var sourceExpression = aggregate.Value.SqlExpression;

                // Sum and Average need to have Decimal values as input for their calculations to work correctly
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
        }

        protected void InitializePartitionedAggregates(NodeSchema schema, IDictionary<string, Type> parameterTypes)
        {
            foreach (var aggregate in Aggregates)
            {
                var sourceExpression = aggregate.Key.ToColumnReference();
                aggregate.Value.Expression = sourceExpression.Compile(schema, parameterTypes);
                aggregate.Value.ReturnType = sourceExpression.GetType(schema, null, parameterTypes);
            }
        }

        protected List<string> GetGroupingColumns(NodeSchema schema)
        {
            var groupByCols = GroupBy
                .Select(col =>
                {
                    var colName = col.GetColumnName();
                    schema.ContainsColumn(colName, out colName);
                    return colName;
                })
                .ToList();

            return groupByCols;
        }

        protected Dictionary<string, AggregateFunction> CreateGroupValues(IDictionary<string, object> parameterValues, IQueryExecutionOptions options, bool partitioned)
        {
            var values = new Dictionary<string, AggregateFunction>();

            foreach (var aggregate in Aggregates)
            {
                Func<Entity, object> selector = null;

                if (partitioned || aggregate.Value.AggregateType != AggregateType.CountStar)
                    selector = e => aggregate.Value.Expression(e, parameterValues, options);

                switch (aggregate.Value.AggregateType)
                {
                    case AggregateType.Average:
                        values[aggregate.Key] = new Average(selector, aggregate.Value.ReturnType);
                        break;

                    case AggregateType.Count:
                        values[aggregate.Key] = new CountColumn(selector);
                        break;

                    case AggregateType.CountStar:
                        values[aggregate.Key] = new Count(selector);
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

                    case AggregateType.First:
                        values[aggregate.Key] = new First(selector, aggregate.Value.ReturnType);
                        break;

                    default:
                        throw new QueryExecutionException("Unknown aggregate type");
                }

                if (aggregate.Value.Distinct)
                    values[aggregate.Key] = new DistinctAggregate(values[aggregate.Key], selector);

                values[aggregate.Key].Reset();
            }

            return values;
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

        protected bool GetOrganizationServiceFault(Exception ex, out OrganizationServiceFault fault)
        {
            fault = null;

            if (ex is QueryExecutionException qee)
                ex = qee.InnerException;

            if (!(ex is FaultException<OrganizationServiceFault> faultEx))
                return false;

            fault = faultEx.Detail;
            while (fault.InnerFault != null)
                fault = fault.InnerFault;

            return true;
        }

        protected bool IsAggregateQueryLimitExceeded(OrganizationServiceFault fault)
        {
            /*
             * 0x8004E023 / -2147164125	
             * Name: AggregateQueryRecordLimitExceeded
             * Message: The maximum record limit is exceeded. Reduce the number of records.
             */
            if (fault.ErrorCode == -2147164125)
                return true;

            return false;
        }

        protected bool IsAggregateQueryRetryable(OrganizationServiceFault fault)
        {
            if (IsAggregateQueryLimitExceeded(fault))
                return true;

            // Triggered when trying to use aggregates on log storage tables
            if (fault.ErrorCode == -2147220970 && fault.Message == "Aggregates are not supported")
                return true;

            return false;
        }

        public override int EstimateRowsOut(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes)
        {
            if (GroupBy.Count == 0)
                return 1;

            return Source.EstimateRowsOut(dataSources, options, parameterTypes) * 4 / 10;
        }
    }
}
