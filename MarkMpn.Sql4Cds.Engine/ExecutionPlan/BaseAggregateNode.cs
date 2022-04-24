using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlTypes;
using System.Globalization;
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
        protected class AggregateFunctionState
        {
            public AggregateFunction AggregateFunction { get; set; }

            public object State { get; set; }
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

        /// <summary>
        /// Indicates if this is a scalar aggregate operation, i.e. there are no grouping columns
        /// </summary>
        [Category("Aggregate")]
        [Description("Indicates if this is a scalar aggregate operation, i.e. there are no grouping columns")]
        [DisplayName("Is Scalar Aggregate")]
        public bool IsScalarAggregate => GroupBy.Count == 0;

        [Browsable(false)]
        public IDataExecutionPlanNodeInternal Source { get; set; }

        protected void InitializeAggregates(INodeSchema schema, IDictionary<string, DataTypeReference> parameterTypes)
        {
            foreach (var aggregate in Aggregates.Where(agg => agg.Value.SqlExpression != null))
            {
                aggregate.Value.SqlExpression.GetType(schema, null, parameterTypes, out var retType);
                aggregate.Value.SourceType = retType;
                aggregate.Value.ReturnType = retType;

                aggregate.Value.Expression = aggregate.Value.SqlExpression.Compile(schema, parameterTypes);

                // Return type of SUM and AVG is based on the input type with some modifications
                // https://docs.microsoft.com/en-us/sql/t-sql/functions/avg-transact-sql?view=sql-server-ver15#return-types
                if ((aggregate.Value.AggregateType == AggregateType.Average || aggregate.Value.AggregateType == AggregateType.Sum) &&
                    aggregate.Value.ReturnType is SqlDataTypeReference sqlRetType)
                {
                    if (sqlRetType.SqlDataTypeOption == SqlDataTypeOption.TinyInt || sqlRetType.SqlDataTypeOption == SqlDataTypeOption.SmallInt)
                        aggregate.Value.ReturnType = new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.Int };
                    else if (sqlRetType.SqlDataTypeOption == SqlDataTypeOption.Decimal || sqlRetType.SqlDataTypeOption == SqlDataTypeOption.Numeric)
                        aggregate.Value.ReturnType = new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.Decimal, Parameters = { new IntegerLiteral { Value = "38" }, new IntegerLiteral { Value = Math.Max(sqlRetType.GetScale(), 6).ToString(CultureInfo.InvariantCulture) } } };
                    else if (sqlRetType.SqlDataTypeOption == SqlDataTypeOption.SmallMoney)
                        aggregate.Value.ReturnType = new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.Money };
                    else if (sqlRetType.SqlDataTypeOption == SqlDataTypeOption.Real)
                        aggregate.Value.ReturnType = new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.Float };
                }
            }
        }

        protected void InitializePartitionedAggregates(INodeSchema schema, IDictionary<string, DataTypeReference> parameterTypes)
        {
            foreach (var aggregate in Aggregates)
            {
                var sourceExpression = aggregate.Key.ToColumnReference();
                aggregate.Value.Expression = sourceExpression.Compile(schema, parameterTypes);
                sourceExpression.GetType(schema, null, parameterTypes, out var retType);
                aggregate.Value.ReturnType = retType;
            }
        }

        protected List<string> GetGroupingColumns(INodeSchema schema)
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

        protected Dictionary<string, AggregateFunction> CreateAggregateFunctions(IDictionary<string, object> parameterValues, IQueryExecutionOptions options, bool partitioned)
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
                        values[aggregate.Key] = new Average(selector, aggregate.Value.SourceType, aggregate.Value.ReturnType);
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
                        values[aggregate.Key] = new Sum(selector, aggregate.Value.SourceType, aggregate.Value.ReturnType);
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

        protected Dictionary<string, AggregateFunctionState> ResetAggregates(Dictionary<string, AggregateFunction> aggregates)
        {
            return aggregates.ToDictionary(kvp => kvp.Key, kvp => new AggregateFunctionState { AggregateFunction = kvp.Value, State = kvp.Value.Reset() });
        }

        protected IEnumerable<KeyValuePair<string, object>> GetValues(Dictionary<string, AggregateFunctionState> aggregateStates)
        {
            return aggregateStates.Select(kvp => new KeyValuePair<string, object>(kvp.Key, kvp.Value.AggregateFunction.GetValue(kvp.Value.State)));
        }

        public override INodeSchema GetSchema(IDictionary<string, DataSource> dataSources, IDictionary<string, DataTypeReference> parameterTypes)
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
                DataTypeReference aggregateType;

                switch (aggregate.Value.AggregateType)
                {
                    case AggregateType.Count:
                    case AggregateType.CountStar:
                        aggregateType = new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.Int };
                        break;

                    default:
                        aggregate.Value.SqlExpression.GetType(sourceSchema, null, parameterTypes, out aggregateType);
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

        protected override int EstimateRowsOutInternal(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, DataTypeReference> parameterTypes)
        {
            if (GroupBy.Count == 0)
                return 1;

            return Source.EstimatedRowsOut * 4 / 10;
        }

        public override void AddRequiredColumns(IDictionary<string, DataSource> dataSources, IDictionary<string, DataTypeReference> parameterTypes, IList<string> requiredColumns)
        {
            // Columns required by previous nodes must be derived from this node, so no need to pass them through.
            // Just calculate the columns that are required to calculate the groups & aggregates
            var scalarRequiredColumns = new List<string>();
            if (GroupBy != null)
                scalarRequiredColumns.AddRange(GroupBy.Select(g => g.GetColumnName()));

            scalarRequiredColumns.AddRange(Aggregates.Where(agg => agg.Value.SqlExpression != null).SelectMany(agg => agg.Value.SqlExpression.GetColumns()).Distinct());

            Source.AddRequiredColumns(dataSources, parameterTypes, scalarRequiredColumns);
        }

        protected override IEnumerable<string> GetVariablesInternal()
        {
            return Aggregates
                .Select(agg => agg.Value.SqlExpression)
                .Where(expr => expr != null)
                .SelectMany(expr => expr.GetVariables())
                .Distinct();
        }
    }
}
