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

        protected void InitializeAggregates(ExpressionCompilationContext context)
        {
            foreach (var aggregate in Aggregates.Where(agg => agg.Value.SqlExpression != null))
            {
                aggregate.Value.SqlExpression.GetType(context, out var retType);
                aggregate.Value.SourceType = retType;
                aggregate.Value.ReturnType = retType;

                aggregate.Value.Expression = aggregate.Value.SqlExpression.Compile(context);

                // Return type of SUM and AVG is based on the input type with some modifications
                // https://docs.microsoft.com/en-us/sql/t-sql/functions/avg-transact-sql?view=sql-server-ver15#return-types
                if ((aggregate.Value.AggregateType == AggregateType.Average || aggregate.Value.AggregateType == AggregateType.Sum) &&
                    aggregate.Value.ReturnType is SqlDataTypeReference sqlRetType)
                {
                    if (sqlRetType.SqlDataTypeOption == SqlDataTypeOption.TinyInt || sqlRetType.SqlDataTypeOption == SqlDataTypeOption.SmallInt)
                        aggregate.Value.ReturnType = DataTypeHelpers.Int;
                    else if (sqlRetType.SqlDataTypeOption == SqlDataTypeOption.Decimal || sqlRetType.SqlDataTypeOption == SqlDataTypeOption.Numeric)
                        aggregate.Value.ReturnType = DataTypeHelpers.Decimal(38, Math.Max(sqlRetType.GetScale(), (short) 6));
                    else if (sqlRetType.SqlDataTypeOption == SqlDataTypeOption.SmallMoney)
                        aggregate.Value.ReturnType = DataTypeHelpers.Money;
                    else if (sqlRetType.SqlDataTypeOption == SqlDataTypeOption.Real)
                        aggregate.Value.ReturnType = DataTypeHelpers.Float;
                }
            }
        }

        protected void InitializePartitionedAggregates(ExpressionCompilationContext context)
        {
            foreach (var aggregate in Aggregates)
            {
                var sourceExpression = aggregate.Key.ToColumnReference();
                aggregate.Value.Expression = sourceExpression.Compile(context);
                sourceExpression.GetType(context, out var retType);
                aggregate.Value.SourceType = retType;
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

        protected Dictionary<string, AggregateFunction> CreateAggregateFunctions(ExpressionExecutionContext context, bool partitioned)
        {
            var values = new Dictionary<string, AggregateFunction>();

            foreach (var aggregate in Aggregates)
            {
                Func<object> selector = null;

                if (partitioned || aggregate.Value.AggregateType != AggregateType.CountStar)
                    selector = () => aggregate.Value.Expression(context);
                else
                    selector = () => null;

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

        public override INodeSchema GetSchema(NodeCompilationContext context)
        {
            var sourceSchema = Source.GetSchema(context);
            var expressionContext = new ExpressionCompilationContext(context, sourceSchema, null);
            var schema = new Dictionary<string, DataTypeReference>(StringComparer.OrdinalIgnoreCase);
            var aliases = new Dictionary<string, IReadOnlyList<string>>(StringComparer.OrdinalIgnoreCase);
            var primaryKey = (string)null;
            var notNullColumns = new List<string>();

            foreach (var group in GroupBy)
            {
                var colName = group.GetColumnName();
                sourceSchema.ContainsColumn(colName, out var normalized);
                schema[normalized] = sourceSchema.Schema[normalized];

                foreach (var alias in sourceSchema.Aliases.Where(a => a.Value.Contains(normalized)))
                {
                    if (!aliases.TryGetValue(alias.Key, out var a))
                    {
                        a = new List<string>();
                        aliases[alias.Key] = a;
                    }

                    ((List<string>)a).Add(normalized);
                }

                if (GroupBy.Count == 1)
                    primaryKey = normalized;
            }

            foreach (var aggregate in Aggregates)
            {
                DataTypeReference aggregateType;

                switch (aggregate.Value.AggregateType)
                {
                    case AggregateType.Count:
                    case AggregateType.CountStar:
                        aggregateType = DataTypeHelpers.Int;
                        break;

                    default:
                        aggregate.Value.SqlExpression.GetType(expressionContext, out aggregateType);

                        // Return type of SUM and AVG is based on the input type with some modifications
                        // https://docs.microsoft.com/en-us/sql/t-sql/functions/avg-transact-sql?view=sql-server-ver15#return-types
                        if ((aggregate.Value.AggregateType == AggregateType.Average || aggregate.Value.AggregateType == AggregateType.Sum) &&
                            aggregateType is SqlDataTypeReference sqlRetType)
                        {
                            if (sqlRetType.SqlDataTypeOption == SqlDataTypeOption.TinyInt || sqlRetType.SqlDataTypeOption == SqlDataTypeOption.SmallInt)
                                aggregateType = DataTypeHelpers.Int;
                            else if (sqlRetType.SqlDataTypeOption == SqlDataTypeOption.Decimal || sqlRetType.SqlDataTypeOption == SqlDataTypeOption.Numeric)
                                aggregateType = DataTypeHelpers.Decimal(38, Math.Max(sqlRetType.GetScale(), (short)6));
                            else if (sqlRetType.SqlDataTypeOption == SqlDataTypeOption.SmallMoney)
                                aggregateType = DataTypeHelpers.Money;
                            else if (sqlRetType.SqlDataTypeOption == SqlDataTypeOption.Real)
                                aggregateType = DataTypeHelpers.Float;
                        }
                        break;
                }

                schema[aggregate.Key] = aggregateType;

                switch (aggregate.Value.AggregateType)
                {
                    case AggregateType.Count:
                    case AggregateType.CountStar:
                    case AggregateType.Sum:
                        notNullColumns.Add(aggregate.Key);
                        break;
                }
            }

            return new NodeSchema(
                primaryKey: primaryKey,
                schema: schema,
                aliases: aliases,
                notNullColumns: notNullColumns,
                sortOrder: null);
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

        protected bool IsCompositeAddressPluginBug(OrganizationServiceFault fault)
        {
            if (fault.Message.StartsWith(typeof(InvalidCastException).FullName))
                return true;

            return false;
        }

        protected override RowCountEstimate EstimateRowsOutInternal(NodeCompilationContext context)
        {
            if (GroupBy.Count == 0)
                return RowCountEstimateDefiniteRange.ExactlyOne;

            var rows = Source.EstimateRowsOut(context).Value * 4 / 10;

            return new RowCountEstimate(rows);
        }

        public override void AddRequiredColumns(NodeCompilationContext context, IList<string> requiredColumns)
        {
            // Columns required by previous nodes must be derived from this node, so no need to pass them through.
            // Just calculate the columns that are required to calculate the groups & aggregates
            var scalarRequiredColumns = new List<string>();
            if (GroupBy != null)
                scalarRequiredColumns.AddRange(GroupBy.Select(g => g.GetColumnName()));

            scalarRequiredColumns.AddRange(Aggregates.Where(agg => agg.Value.SqlExpression != null).SelectMany(agg => agg.Value.SqlExpression.GetColumns()).Distinct());

            Source.AddRequiredColumns(context, scalarRequiredColumns);
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
