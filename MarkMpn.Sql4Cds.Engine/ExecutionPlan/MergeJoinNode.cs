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
    class MergeJoinNode : IExecutionPlanNode
    {
        /// <summary>
        /// The first data source to merge
        /// </summary>
        public IExecutionPlanNode OuterSource { get; set; }

        /// <summary>
        /// The attribute in the <see cref="OuterSource"/> to join on
        /// </summary>
        public ColumnReferenceExpression OuterAttribute { get; set; }

        /// <summary>
        /// The second data source to merge
        /// </summary>
        public IExecutionPlanNode InnerSource { get; set; }

        /// <summary>
        /// The attribute in the <see cref="InnerSource"/> to join on
        /// </summary>
        public ColumnReferenceExpression InnerAttribute { get; set; }

        /// <summary>
        /// The type of join to apply
        /// </summary>
        public QualifiedJoinType JoinType { get; set; }

        public IEnumerable<Entity> Execute(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options)
        {
            // https://sqlserverfast.com/epr/merge-join/

            var outer = OuterSource.Execute(org, metadata, options).GetEnumerator();
            var inner = InnerSource.Execute(org, metadata, options).GetEnumerator();

            var hasOuter = outer.MoveNext();
            var hasInner = inner.MoveNext();
            var outerMatched = false;
            var innerMatched = false;

            var lt = new BooleanComparisonExpression
            {
                FirstExpression = OuterAttribute,
                ComparisonType = BooleanComparisonType.LessThan,
                SecondExpression = InnerAttribute
            };

            var eq = new BooleanComparisonExpression
            {
                FirstExpression = OuterAttribute,
                ComparisonType = BooleanComparisonType.Equals,
                SecondExpression = InnerAttribute
            };

            var gt = new BooleanComparisonExpression
            {
                FirstExpression = OuterAttribute,
                ComparisonType = BooleanComparisonType.GreaterThan,
                SecondExpression = InnerAttribute
            };

            while (!Done(hasOuter, hasInner))
            {
                var merged = Merge(outer.Current, inner.Current);

                var isLt = lt.GetValue(merged);
                var isEq = eq.GetValue(merged);
                var isGt = gt.GetValue(merged);

                if (isLt)
                {
                    if (!outerMatched && JoinType == QualifiedJoinType.LeftOuter)
                        yield return merged;

                    hasOuter = outer.MoveNext();
                    outerMatched = false;
                }
                else if (isEq)
                {
                    yield return merged;

                    outerMatched = true;
                    innerMatched = true;

                    hasInner = inner.MoveNext();
                    innerMatched = false;
                }
                else if (isGt)
                {
                    if (!innerMatched && JoinType == QualifiedJoinType.RightOuter)
                        yield return merged;

                    hasInner = inner.MoveNext();
                    innerMatched = false;
                }
            }
        }

        private Entity Merge(Entity outer, Entity inner)
        {
            var merged = new Entity();

            if (outer != null)
            {
                foreach (var attr in outer.Attributes)
                    merged[attr.Key] = attr.Value;
            }

            if (inner != null)
            {
                foreach (var attr in inner.Attributes)
                    merged[attr.Key] = attr.Value;
            }

            return merged;
        }

        private bool Done(bool hasOuter, bool hasInner)
        {
            if (JoinType == QualifiedJoinType.Inner)
                return !hasOuter || !hasInner;

            if (JoinType == QualifiedJoinType.LeftOuter)
                return !hasOuter;

            if (JoinType == QualifiedJoinType.RightOuter)
                return !hasInner;

            throw new NotSupportedException();
        }
    }
}
