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

        protected override IEnumerable<Entity> ExecuteInternal(NodeExecutionContext context)
        {
            var leftSchema = LeftSource.GetSchema(context);
            var innerParameterTypes = GetInnerParameterTypes(leftSchema, context.ParameterTypes);
            if (OuterReferences != null)
            {
                if (context.ParameterTypes == null)
                    innerParameterTypes = new Dictionary<string, DataTypeReference>(StringComparer.OrdinalIgnoreCase);
                else
                    innerParameterTypes = new Dictionary<string, DataTypeReference>(context.ParameterTypes, StringComparer.OrdinalIgnoreCase);

                foreach (var kvp in OuterReferences)
                    innerParameterTypes[kvp.Value] = leftSchema.Schema[kvp.Key].Type;
            }

            var rightCompilationContext = new NodeCompilationContext(context.DataSources, context.Options, innerParameterTypes);
            var rightSchema = RightSource.GetSchema(rightCompilationContext);
            var mergedSchema = GetSchema(context, true);
            var joinCondition = JoinCondition?.Compile(new ExpressionCompilationContext(context, mergedSchema, null));
            var joinConditionContext = joinCondition == null ? null : new ExpressionExecutionContext(context);

            foreach (var left in LeftSource.Execute(context))
            {
                var innerParameters = context.ParameterValues;

                if (OuterReferences != null)
                {
                    if (innerParameters == null)
                        innerParameters = new Dictionary<string, object>();
                    else
                        innerParameters = new Dictionary<string, object>(innerParameters);

                    foreach (var kvp in OuterReferences)
                    {
                        left.Attributes.TryGetValue(kvp.Key, out var outerValue);
                        innerParameters[kvp.Value] = outerValue;
                    }
                }

                var hasRight = false;

                foreach (var right in RightSource.Execute(new NodeExecutionContext(context.DataSources, context.Options, innerParameterTypes, innerParameters)))
                {
                    var merged = Merge(left, leftSchema, right, rightSchema);

                    if (joinCondition == null)
                    {
                        yield return merged;
                    }
                    else
                    {
                        joinConditionContext.Entity = merged;

                        if (joinCondition(joinConditionContext))
                            yield return merged;
                        else
                            continue;
                    }

                    hasRight = true;

                    if (SemiJoin)
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
                    innerParameterTypes[kvp.Value] = leftSchema.Schema[kvp.Key].Type;
            }

            return innerParameterTypes;
        }

        public override IDataExecutionPlanNodeInternal FoldQuery(NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            if (JoinType == QualifiedJoinType.RightOuter)
            {
                // Right outer join isn't supported but left outer join is, so swap the sides and change the join type
                var right = RightSource;
                RightSource = LeftSource;
                LeftSource = right;
                JoinType = QualifiedJoinType.LeftOuter;
            }

            if (JoinType == QualifiedJoinType.FullOuter)
            {
                // TODO: Use many-to-many merge join instead
            }

            var leftSchema = LeftSource.GetSchema(context);
            LeftSource = LeftSource.FoldQuery(context, hints);
            LeftSource.Parent = this;

            var innerParameterTypes = GetInnerParameterTypes(leftSchema, context.ParameterTypes);
            var innerContext = new NodeCompilationContext(context.DataSources, context.Options, innerParameterTypes);
            RightSource = RightSource.FoldQuery(innerContext, hints);
            RightSource.Parent = this;

            if (LeftSource is ConstantScanNode constant &&
                constant.Schema.Count == 0 &&
                constant.Values.Count == 1 &&
                JoinType == QualifiedJoinType.LeftOuter &&
                SemiJoin &&
                JoinCondition == null &&
                RightSource.EstimateRowsOut(context) is RowCountEstimateDefiniteRange range &&
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

            if (RightSource is TableSpoolNode spool && LeftSource.EstimateRowsOut(context).Value <= 1)
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
                LeftSource.EstimateRowsOut(context).Value < 100 &&
                fetch.EstimateRowsOut(innerContext).Value > 5000)
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
                aggregate.Source = filter.FoldQuery(innerContext, hints);
            }

            return this;
        }

        public override void AddRequiredColumns(NodeCompilationContext context, IList<string> requiredColumns)
        {
            if (JoinCondition != null)
            {
                foreach (var col in JoinCondition.GetColumns())
                {
                    if (!requiredColumns.Contains(col, StringComparer.OrdinalIgnoreCase))
                        requiredColumns.Add(col);
                }
            }

            var leftSchema = LeftSource.GetSchema(context);
            var leftColumns = requiredColumns
                .Where(col => leftSchema.ContainsColumn(col, out _))
                .Concat((IEnumerable<string>) OuterReferences?.Keys ?? Array.Empty<string>())
                .Distinct()
                .ToList();
            var innerParameterTypes = GetInnerParameterTypes(leftSchema, context.ParameterTypes);
            var innerContext = new NodeCompilationContext(context.DataSources, context.Options, innerParameterTypes);
            var rightSchema = RightSource.GetSchema(innerContext);
            var rightColumns = requiredColumns
                .Where(col => rightSchema.ContainsColumn(col, out _))
                .Concat(DefinedValues.Values)
                .Distinct()
                .ToList();

            LeftSource.AddRequiredColumns(context, leftColumns);
            RightSource.AddRequiredColumns(context, rightColumns);
        }

        protected override INodeSchema GetRightSchema(NodeCompilationContext context)
        {
            var leftSchema = LeftSource.GetSchema(context);
            var innerParameterTypes = GetInnerParameterTypes(leftSchema, context.ParameterTypes);
            var innerContext = new NodeCompilationContext(context.DataSources, context.Options, innerParameterTypes);
            return RightSource.GetSchema(innerContext);
        }

        protected override RowCountEstimate EstimateRowsOutInternal(NodeCompilationContext context)
        {
            var leftEstimate = LeftSource.EstimateRowsOut(context);
            ParseEstimate(leftEstimate, out var leftMin, out var leftMax, out var leftIsRange);
            var leftSchema = LeftSource.GetSchema(context);
            var innerParameterTypes = GetInnerParameterTypes(leftSchema, context.ParameterTypes);
            var innerContext = new NodeCompilationContext(context.DataSources, context.Options, innerParameterTypes);

            var rightEstimate = RightSource.EstimateRowsOut(innerContext);
            ParseEstimate(rightEstimate, out var rightMin, out var rightMax, out var rightIsRange);

            if (JoinType == QualifiedJoinType.LeftOuter && SemiJoin)
                return leftEstimate;

            if (JoinType == QualifiedJoinType.RightOuter && SemiJoin)
                return rightEstimate;

            int min;
            int max;

            if (OuterReferences != null && OuterReferences.Count > 0 ||
                JoinCondition == null)
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
            return JoinCondition?.GetVariables() ?? Array.Empty<string>();
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
