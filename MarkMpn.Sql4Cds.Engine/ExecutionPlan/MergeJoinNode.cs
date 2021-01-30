using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// Merges two sorted data sets
    /// </summary>
    public class MergeJoinNode : BaseJoinNode
    {
        /// <summary>
        /// The attribute in the <see cref="OuterSource"/> to join on
        /// </summary>
        public ColumnReferenceExpression LeftAttribute { get; set; }

        /// <summary>
        /// The attribute in the <see cref="InnerSource"/> to join on
        /// </summary>
        public ColumnReferenceExpression RightAttribute { get; set; }

        /// <summary>
        /// Any additional criteria to apply to the join
        /// </summary>
        public BooleanExpression AdditionalJoinCriteria { get; set; }

        public override IEnumerable<Entity> Execute(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options)
        {
            // https://sqlserverfast.com/epr/merge-join/
            // Implemented inner, left outer, right outer and full outer variants
            // Not implemented semi joins
            // TODO: Handle many-to-many joins

            // Left & Right: GetNext, mark as unmatched
            var leftSchema = LeftSource.GetSchema(metadata);
            var rightSchema = RightSource.GetSchema(metadata);
            var left = LeftSource.Execute(org, metadata, options).GetEnumerator();
            var right = RightSource.Execute(org, metadata, options).GetEnumerator();

            var hasLeft = left.MoveNext();
            var hasRight = right.MoveNext();
            var leftMatched = false;
            var rightMatched = false;

            var lt = new BooleanComparisonExpression
            {
                FirstExpression = LeftAttribute,
                ComparisonType = BooleanComparisonType.LessThan,
                SecondExpression = RightAttribute
            };

            var eq = new BooleanComparisonExpression
            {
                FirstExpression = LeftAttribute,
                ComparisonType = BooleanComparisonType.Equals,
                SecondExpression = RightAttribute
            };

            var gt = new BooleanComparisonExpression
            {
                FirstExpression = LeftAttribute,
                ComparisonType = BooleanComparisonType.GreaterThan,
                SecondExpression = RightAttribute
            };

            while (!Done(hasLeft, hasRight))
            {
                // Compare key values
                var merged = Merge(hasLeft ? left.Current : null, leftSchema, hasRight ? right.Current : null, rightSchema);

                var isLt = lt.GetValue(merged);
                var isEq = eq.GetValue(merged);
                var isGt = gt.GetValue(merged);

                if (isLt || (hasLeft && !hasRight))
                {
                    if (!leftMatched && (JoinType == QualifiedJoinType.LeftOuter || JoinType == QualifiedJoinType.FullOuter))
                        yield return Merge(left.Current, leftSchema, null, rightSchema);

                    hasLeft = left.MoveNext();
                    leftMatched = false;
                }
                else if (isEq)
                {
                    if (AdditionalJoinCriteria == null || AdditionalJoinCriteria.GetValue(merged) == true)
                        yield return merged;

                    leftMatched = true;

                    hasRight = right.MoveNext();
                    rightMatched = false;
                }
                else if (isGt || (!hasLeft && hasRight))
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

        public override IEnumerable<string> GetRequiredColumns()
        {
            yield return LeftAttribute.GetColumnName();
            yield return RightAttribute.GetColumnName();
        }
    }
}
