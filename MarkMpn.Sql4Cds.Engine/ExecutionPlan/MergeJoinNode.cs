using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.Engine.FetchXml;
using MarkMpn.Sql4Cds.Engine.Visitors;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Metadata;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// Merges two sorted data sets
    /// </summary>
    class MergeJoinNode : FoldableJoinNode
    {
        protected override IEnumerable<Entity> ExecuteInternal(NodeExecutionContext context)
        {
            // https://sqlserverfast.com/epr/merge-join/
            // Implemented inner, left outer, right outer and full outer variants
            // Not implemented semi joins
            // TODO: Handle many-to-many joins
            // TODO: Handle union & concatenate

            // Left & Right: GetNext, mark as unmatched
            var leftSchema = LeftSource.GetSchema(context);
            var rightSchema = RightSource.GetSchema(context);
            var left = LeftSource.Execute(context).GetEnumerator();
            var right = RightSource.Execute(context).GetEnumerator();
            var mergedSchema = GetSchema(context, true);
            var expressionCompilationContext = new ExpressionCompilationContext(context, mergedSchema, null);
            var expressionExecutionContext = new ExpressionExecutionContext(context);
            var additionalJoinCriteria = AdditionalJoinCriteria?.Compile(expressionCompilationContext);

            var hasLeft = left.MoveNext();
            var hasRight = right.MoveNext();
            var leftMatched = false;
            var rightMatched = false;

            var lt = new BooleanComparisonExpression
            {
                FirstExpression = LeftAttribute,
                ComparisonType = BooleanComparisonType.LessThan,
                SecondExpression = RightAttribute
            }.Compile(expressionCompilationContext);

            var eq = new BooleanComparisonExpression
            {
                FirstExpression = LeftAttribute,
                ComparisonType = BooleanComparisonType.Equals,
                SecondExpression = RightAttribute
            }.Compile(expressionCompilationContext);

            var gt = new BooleanComparisonExpression
            {
                FirstExpression = LeftAttribute,
                ComparisonType = BooleanComparisonType.GreaterThan,
                SecondExpression = RightAttribute
            }.Compile(expressionCompilationContext);

            while (!Done(hasLeft, hasRight))
            {
                // Compare key values
                var merged = Merge(hasLeft ? left.Current : null, leftSchema, hasRight ? right.Current : null, rightSchema);

                expressionExecutionContext.Entity = merged;
                var isLt = lt(expressionExecutionContext);
                var isEq = eq(expressionExecutionContext);
                var isGt = gt(expressionExecutionContext);

                if (isLt || (hasLeft && !hasRight))
                {
                    if (!leftMatched && (JoinType == QualifiedJoinType.LeftOuter || JoinType == QualifiedJoinType.FullOuter))
                        yield return Merge(left.Current, leftSchema, null, rightSchema);

                    hasLeft = left.MoveNext();
                    leftMatched = false;
                }
                else if (isEq)
                {
                    if ((!leftMatched || !SemiJoin) && (additionalJoinCriteria == null || additionalJoinCriteria(expressionExecutionContext) == true))
                        yield return merged;

                    leftMatched = true;

                    hasRight = right.MoveNext();
                    rightMatched = false;
                }
                else if (hasRight)
                {
                    if (!rightMatched && (JoinType == QualifiedJoinType.RightOuter || JoinType == QualifiedJoinType.FullOuter))
                        yield return Merge(null, leftSchema, right.Current, rightSchema);

                    hasRight = right.MoveNext();
                    rightMatched = false;
                }
            }
        }

        private bool Done(bool hasLeft, bool hasRight)
        {
            if (JoinType == QualifiedJoinType.Inner)
                return !hasLeft || !hasRight;

            if (JoinType == QualifiedJoinType.LeftOuter)
                return !hasLeft;

            if (JoinType == QualifiedJoinType.RightOuter)
                return !hasRight;

            return !hasLeft && !hasRight;
        }

        public override IDataExecutionPlanNodeInternal FoldQuery(NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            var folded = base.FoldQuery(context, hints);

            if (folded != this)
                return folded;

            // Can't fold the join down into the FetchXML, so add a sort and try to fold that in instead
            LeftSource = new SortNode
            {
                Source = LeftSource,
                Sorts =
                {
                    new ExpressionWithSortOrder
                    {
                        Expression = LeftAttribute,
                        SortOrder = SortOrder.Ascending
                    }
                }
            }.FoldQuery(context, hints);
            LeftSource.Parent = this;

            RightSource = new SortNode
            {
                Source = RightSource,
                Sorts =
                {
                    new ExpressionWithSortOrder
                    {
                        Expression = RightAttribute,
                        SortOrder = SortOrder.Ascending
                    }
                }
            }.FoldQuery(context, hints);
            RightSource.Parent = this;

            return this;
        }

        protected override IReadOnlyList<string> GetSortOrder(INodeSchema outerSchema, INodeSchema innerSchema)
        {
            outerSchema.ContainsColumn(LeftAttribute.GetColumnName(), out var left);
            innerSchema.ContainsColumn(RightAttribute.GetColumnName(), out var right);

            if (JoinType == QualifiedJoinType.Inner || JoinType == QualifiedJoinType.LeftOuter)
                return new[] { left, right };
            else if (JoinType == QualifiedJoinType.RightOuter)
                return new[] { right, left };
            else
                return null;
        }

        public override object Clone()
        {
            var clone = new MergeJoinNode
            {
                AdditionalJoinCriteria = AdditionalJoinCriteria,
                JoinType = JoinType,
                LeftAttribute = LeftAttribute,
                LeftSource = (IDataExecutionPlanNodeInternal)LeftSource.Clone(),
                RightAttribute = RightAttribute,
                RightSource =  (IDataExecutionPlanNodeInternal)RightSource.Clone(),
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
