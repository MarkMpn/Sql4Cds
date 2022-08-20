using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// Implements a nested loop join
    /// </summary>
    class NestedLoopNode : BaseJoinNode
    {
        /// <summary>
        /// The condition that must be true for two records to join
        /// </summary>
        [Category("Join")]
        [Description("The condition that must be true for two records to join")]
        [DisplayName("Join Condition")]
        public BooleanExpression JoinCondition { get; set; }

        /// <summary>
        /// Values from the outer query that should be passed as references to the inner query
        /// </summary>
        [Category("Join")]
        [Description("Values from the outer query that should be passed as references to the inner query")]
        [DisplayName("Outer References")]
        public Dictionary<string,string> OuterReferences { get; set; }

        protected override IEnumerable<Entity> ExecuteInternal(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, DataTypeReference> parameterTypes, IDictionary<string, object> parameterValues)
        {
            var leftSchema = LeftSource.GetSchema(dataSources, parameterTypes);
            var innerParameterTypes = GetInnerParameterTypes(leftSchema, parameterTypes);
            if (OuterReferences != null)
            {
                if (parameterTypes == null)
                    innerParameterTypes = new Dictionary<string, DataTypeReference>(StringComparer.OrdinalIgnoreCase);
                else
                    innerParameterTypes = new Dictionary<string, DataTypeReference>(parameterTypes, StringComparer.OrdinalIgnoreCase);

                foreach (var kvp in OuterReferences)
                    innerParameterTypes[kvp.Value] = leftSchema.Schema[kvp.Key];
            }

            var rightSchema = RightSource.GetSchema(dataSources, innerParameterTypes);
            var mergedSchema = GetSchema(dataSources, parameterTypes, true);
            var joinCondition = JoinCondition?.Compile(mergedSchema, parameterTypes);

            foreach (var left in LeftSource.Execute(dataSources, options, parameterTypes, parameterValues))
            {
                var innerParameters = parameterValues;

                if (OuterReferences != null)
                {
                    if (parameterValues == null)
                        innerParameters = new Dictionary<string, object>();
                    else
                        innerParameters = new Dictionary<string, object>(parameterValues);

                    foreach (var kvp in OuterReferences)
                    {
                        left.Attributes.TryGetValue(kvp.Key, out var outerValue);
                        innerParameters[kvp.Value] = outerValue;
                    }
                }

                var hasRight = false;

                foreach (var right in RightSource.Execute(dataSources, options, innerParameterTypes, innerParameters))
                {
                    var merged = Merge(left, leftSchema, right, rightSchema);

                    if (joinCondition == null || joinCondition(merged, parameterValues, options))
                        yield return merged;

                    hasRight = true;

                    if (SemiJoin && JoinType != QualifiedJoinType.RightOuter)
                        break;
                }

                if (!hasRight && JoinType == QualifiedJoinType.LeftOuter)
                    yield return Merge(left, leftSchema, null, rightSchema);
            }
        }

        private IDictionary<string, DataTypeReference> GetInnerParameterTypes(INodeSchema leftSchema, IDictionary<string, DataTypeReference> parameterTypes)
        {
            var innerParameterTypes = parameterTypes;

            if (OuterReferences != null)
            {
                if (parameterTypes == null)
                    innerParameterTypes = new Dictionary<string, DataTypeReference>(StringComparer.OrdinalIgnoreCase);
                else
                    innerParameterTypes = new Dictionary<string, DataTypeReference>(parameterTypes, StringComparer.OrdinalIgnoreCase);

                foreach (var kvp in OuterReferences)
                    innerParameterTypes[kvp.Value] = leftSchema.Schema[kvp.Key];
            }

            return innerParameterTypes;
        }

        public override IDataExecutionPlanNodeInternal FoldQuery(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, DataTypeReference> parameterTypes, IList<OptimizerHint> hints)
        {
            var leftSchema = LeftSource.GetSchema(dataSources, parameterTypes);
            LeftSource = LeftSource.FoldQuery(dataSources, options, parameterTypes, hints);
            LeftSource.Parent = this;

            var innerParameterTypes = GetInnerParameterTypes(leftSchema, parameterTypes);
            RightSource = RightSource.FoldQuery(dataSources, options, innerParameterTypes, hints);
            RightSource.Parent = this;

            if (LeftSource is ConstantScanNode constant &&
                constant.Schema.Count == 0 &&
                constant.Values.Count == 1 &&
                JoinType == QualifiedJoinType.LeftOuter &&
                SemiJoin &&
                JoinCondition == null &&
                RightSource.EstimateRowsOut(dataSources, options, parameterTypes) is RowCountEstimateDefiniteRange range &&
                range.Minimum == 1 &&
                range.Maximum == 1)
            {
                // Subquery that will always produce one value - no need for the join at all, replace with a Compute Scalar
                // to produce the same effect as the Defined Values
                if (RightSource is TableSpoolNode subquerySpool)
                    RightSource = subquerySpool.Source;

                var compute = new ComputeScalarNode
                {
                    Source = RightSource
                };

                foreach (var value in DefinedValues)
                    compute.Columns[value.Key] = value.Value.ToColumnReference();

                RightSource.Parent = compute;

                return compute;
            }

            if (RightSource is TableSpoolNode spool && LeftSource.EstimateRowsOut(dataSources, options, parameterTypes).Value <= 1)
            {
                RightSource = spool.Source;
                RightSource.Parent = this;
            }
            else if (JoinType == QualifiedJoinType.LeftOuter &&
                SemiJoin &&
                DefinedValues.Count == 1 &&
                RightSource is AssertNode assert &&
                assert.Source is StreamAggregateNode aggregate &&
                aggregate.Aggregates.Count == 2 &&
                aggregate.Aggregates[DefinedValues.Single().Value].AggregateType == AggregateType.First &&
                aggregate.Source is IndexSpoolNode indexSpool &&
                indexSpool.Source is FetchXmlScan fetch &&
                LeftSource.EstimateRowsOut(dataSources, options, parameterTypes).Value < 100 &&
                fetch.EstimateRowsOut(dataSources, options, innerParameterTypes).Value > 5000)
            {
                // Scalar subquery was folded to use an index spool due to an expected large number of outer records,
                // but the estimate has now changed (e.g. due to a TopNode being folded). Remove the index spool and replace
                // with filter
                var filter = new FilterNode
                {
                    Source = fetch,
                    Filter = new BooleanComparisonExpression
                    {
                        FirstExpression = indexSpool.KeyColumn.ToColumnReference(),
                        ComparisonType = BooleanComparisonType.Equals,
                        SecondExpression = new VariableReference { Name = indexSpool.SeekValue }
                    },
                    Parent = aggregate
                };
                aggregate.Source = filter.FoldQuery(dataSources, options, innerParameterTypes, hints);
            }

            return this;
        }

        public override void AddRequiredColumns(IDictionary<string, DataSource> dataSources, IDictionary<string, DataTypeReference> parameterTypes, IList<string> requiredColumns)
        {
            if (JoinCondition != null)
            {
                foreach (var col in JoinCondition.GetColumns())
                {
                    if (!requiredColumns.Contains(col, StringComparer.OrdinalIgnoreCase))
                        requiredColumns.Add(col);
                }
            }

            var leftSchema = LeftSource.GetSchema(dataSources, parameterTypes);
            var leftColumns = requiredColumns
                .Where(col => leftSchema.ContainsColumn(col, out _))
                .Concat((IEnumerable<string>) OuterReferences?.Keys ?? Array.Empty<string>())
                .Distinct()
                .ToList();
            var innerParameterTypes = GetInnerParameterTypes(leftSchema, parameterTypes);
            var rightSchema = RightSource.GetSchema(dataSources, innerParameterTypes);
            var rightColumns = requiredColumns
                .Where(col => rightSchema.ContainsColumn(col, out _))
                .Concat(DefinedValues.Values)
                .Distinct()
                .ToList();

            LeftSource.AddRequiredColumns(dataSources, parameterTypes, leftColumns);
            RightSource.AddRequiredColumns(dataSources, parameterTypes, rightColumns);
        }

        protected override INodeSchema GetRightSchema(IDictionary<string, DataSource> dataSources, IDictionary<string, DataTypeReference> parameterTypes)
        {
            var leftSchema = LeftSource.GetSchema(dataSources, parameterTypes);
            var innerParameterTypes = GetInnerParameterTypes(leftSchema, parameterTypes);
            return RightSource.GetSchema(dataSources, innerParameterTypes);
        }

        protected override RowCountEstimate EstimateRowsOutInternal(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, DataTypeReference> parameterTypes)
        {
            var leftEstimate = LeftSource.EstimateRowsOut(dataSources, options, parameterTypes);
            ParseEstimate(leftEstimate, out var leftMin, out var leftMax, out var leftIsRange);
            var leftSchema = LeftSource.GetSchema(dataSources, parameterTypes);
            var innerParameterTypes = GetInnerParameterTypes(leftSchema, parameterTypes);

            var rightEstimate = RightSource.EstimateRowsOut(dataSources, options, innerParameterTypes);
            ParseEstimate(rightEstimate, out var rightMin, out var rightMax, out var rightIsRange);

            if (JoinType == QualifiedJoinType.LeftOuter && SemiJoin)
                return leftEstimate;

            if (JoinType == QualifiedJoinType.RightOuter && SemiJoin)
                return rightEstimate;

            int min;
            int max;

            if (OuterReferences != null && OuterReferences.Count > 0)
            {
                min = leftMin * rightMin;
                max = leftMax * rightMax;
            }
            else if (JoinType == QualifiedJoinType.Inner)
            {
                min = Math.Min(leftMin, rightMin);
                max = Math.Max(leftMax, rightMax);
            }
            else
            {
                min = Math.Max(leftMin, rightMin);
                max = Math.Max(leftMax, rightMax);
            }

            if (JoinCondition != null)
                min = 0;

            RowCountEstimate estimate;

            if (leftIsRange && rightIsRange)
                estimate = new RowCountEstimateDefiniteRange(min, max);
            else
                estimate = new RowCountEstimate(max);

            return estimate;
        }

        protected override IEnumerable<string> GetVariablesInternal()
        {
            return JoinCondition.GetVariables();
        }

        public override object Clone()
        {
            var clone = new NestedLoopNode
            {
                JoinCondition = JoinCondition,
                JoinType = JoinType,
                LeftSource = (IDataExecutionPlanNodeInternal)LeftSource.Clone(),
                OuterReferences = OuterReferences,
                RightSource = (IDataExecutionPlanNodeInternal)RightSource.Clone(),
                SemiJoin = SemiJoin
            };

            foreach (var kvp in DefinedValues)
                clone.DefinedValues.Add(kvp);

            clone.LeftSource.Parent = clone;
            clone.RightSource.Parent = clone;

            return clone;
        }
    }
}
