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
        protected override IEnumerable<Entity> ExecuteInternal(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues)
        {
            // https://sqlserverfast.com/epr/merge-join/
            // Implemented inner, left outer, right outer and full outer variants
            // Not implemented semi joins
            // TODO: Handle many-to-many joins

            // Left & Right: GetNext, mark as unmatched
            var leftSchema = LeftSource.GetSchema(dataSources, parameterTypes);
            var rightSchema = RightSource.GetSchema(dataSources, parameterTypes);
            var left = LeftSource.Execute(dataSources, options, parameterTypes, parameterValues).GetEnumerator();
            var right = RightSource.Execute(dataSources, options, parameterTypes, parameterValues).GetEnumerator();
            var mergedSchema = GetSchema(dataSources, parameterTypes, true);
            var additionalJoinCriteria = AdditionalJoinCriteria?.Compile(mergedSchema, parameterTypes);

            var hasLeft = left.MoveNext();
            var hasRight = right.MoveNext();
            var leftMatched = false;
            var rightMatched = false;

            var lt = new BooleanComparisonExpression
            {
                FirstExpression = LeftAttribute,
                ComparisonType = BooleanComparisonType.LessThan,
                SecondExpression = RightAttribute
            }.Compile(mergedSchema, parameterTypes);

            var eq = new BooleanComparisonExpression
            {
                FirstExpression = LeftAttribute,
                ComparisonType = BooleanComparisonType.Equals,
                SecondExpression = RightAttribute
            }.Compile(mergedSchema, parameterTypes);

            var gt = new BooleanComparisonExpression
            {
                FirstExpression = LeftAttribute,
                ComparisonType = BooleanComparisonType.GreaterThan,
                SecondExpression = RightAttribute
            }.Compile(mergedSchema, parameterTypes);

            while (!Done(hasLeft, hasRight))
            {
                // Compare key values
                var merged = Merge(hasLeft ? left.Current : null, leftSchema, hasRight ? right.Current : null, rightSchema);

                var isLt = lt(merged, parameterValues, options);
                var isEq = eq(merged, parameterValues, options);
                var isGt = gt(merged, parameterValues, options);

                if (isLt || (hasLeft && !hasRight))
                {
                    if (!leftMatched && (JoinType == QualifiedJoinType.LeftOuter || JoinType == QualifiedJoinType.FullOuter))
                        yield return Merge(left.Current, leftSchema, null, rightSchema);

                    hasLeft = left.MoveNext();
                    leftMatched = false;
                }
                else if (isEq)
                {
                    if ((!leftMatched || !SemiJoin) && (additionalJoinCriteria == null || additionalJoinCriteria(merged, parameterValues, options) == true))
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

        public override IDataExecutionPlanNode FoldQuery(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes)
        {
            var folded = base.FoldQuery(dataSources, options, parameterTypes);

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
            }.FoldQuery(dataSources, options, parameterTypes);
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
            }.FoldQuery(dataSources, options, parameterTypes);
            RightSource.Parent = this;

            return this;
        }
    }
}
