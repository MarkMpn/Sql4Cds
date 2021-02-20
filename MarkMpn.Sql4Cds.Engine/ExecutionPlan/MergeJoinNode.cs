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

        public override IEnumerable<Entity> Execute(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string, object> parameterValues)
        {
            // https://sqlserverfast.com/epr/merge-join/
            // Implemented inner, left outer, right outer and full outer variants
            // Not implemented semi joins
            // TODO: Handle many-to-many joins

            // Left & Right: GetNext, mark as unmatched
            var leftSchema = LeftSource.GetSchema(metadata);
            var rightSchema = RightSource.GetSchema(metadata);
            var left = LeftSource.Execute(org, metadata, options, parameterValues).GetEnumerator();
            var right = RightSource.Execute(org, metadata, options, parameterValues).GetEnumerator();
            var mergedSchema = GetSchema(metadata);

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

                var isLt = lt.GetValue(merged, mergedSchema);
                var isEq = eq.GetValue(merged, mergedSchema);
                var isGt = gt.GetValue(merged, mergedSchema);

                if (isLt || (hasLeft && !hasRight))
                {
                    if (!leftMatched && (JoinType == QualifiedJoinType.LeftOuter || JoinType == QualifiedJoinType.FullOuter))
                        yield return Merge(left.Current, leftSchema, null, rightSchema);

                    hasLeft = left.MoveNext();
                    leftMatched = false;
                }
                else if (isEq)
                {
                    if (AdditionalJoinCriteria == null || AdditionalJoinCriteria.GetValue(merged, mergedSchema) == true)
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

        public override IExecutionPlanNode MergeNodeDown(IAttributeMetadataCache metadata, IQueryExecutionOptions options)
        {
            LeftSource = LeftSource.MergeNodeDown(metadata, options);
            RightSource = RightSource.MergeNodeDown(metadata, options);

            if (LeftSource is FetchXmlScan leftFetch && RightSource is FetchXmlScan rightFetch)
            {
                var leftEntity = leftFetch.Entity;
                var rightEntity = rightFetch.Entity;

                // Check that the join is on columns that are available in the FetchXML
                var leftSchema = LeftSource.GetSchema(metadata);
                var rightSchema = RightSource.GetSchema(metadata);
                var leftAttribute = LeftAttribute.GetColumnName();
                if (!leftSchema.ContainsColumn(leftAttribute, out leftAttribute))
                    return this;
                var rightAttribute = RightAttribute.GetColumnName();
                if (!rightSchema.ContainsColumn(rightAttribute, out rightAttribute))
                    return this;
                var leftAttributeParts = leftAttribute.Split('.');
                var rightAttributeParts = rightAttribute.Split('.');
                if (leftAttributeParts.Length != 2)
                    return this;
                if (rightAttributeParts.Length != 2)
                    return this;

                // Must be joining to the root entity of the right source, i.e. not a child link-entity
                if (!rightAttributeParts[0].Equals(rightFetch.Alias))
                    return this;

                // If there are any additional join criteria, either they must be able to be translated to FetchXml criteria
                // in the new link entity or we must be using an inner join so we can use a post-filter node
                var additionalCriteria = AdditionalJoinCriteria;

                if (TranslateCriteria(metadata, options, additionalCriteria, rightSchema, rightFetch.Alias, rightEntity.name, rightFetch.Alias, out var filter))
                {
                    if (rightEntity.Items == null)
                        rightEntity.Items = new object[] { filter };
                    else
                        rightEntity.Items = rightEntity.Items.Concat(new object[] { filter }).ToArray();

                    additionalCriteria = null;
                }

                if (additionalCriteria != null && JoinType != QualifiedJoinType.Inner)
                    return this;

                var rightLinkEntity = new FetchLinkEntityType
                {
                    alias = rightFetch.Alias,
                    name = rightEntity.name,
                    linktype = JoinType == QualifiedJoinType.Inner ? "inner" : "outer",
                    from = rightAttributeParts[1],
                    to = leftAttributeParts[1],
                    Items = rightEntity.Items
                };

                // Find where the two FetchXml documents should be merged together and return the merged version
                if (leftAttributeParts[0].Equals(leftFetch.Alias))
                {
                    if (leftEntity.Items == null)
                        leftEntity.Items = new object[] { rightLinkEntity };
                    else
                        leftEntity.Items = leftEntity.Items.Concat(new object[] { rightLinkEntity }).ToArray();
                }
                else
                {
                    var leftLinkEntity = leftFetch.Entity.FindLinkEntity(leftAttributeParts[0]);

                    if (leftLinkEntity == null)
                        return this;

                    if (leftLinkEntity.Items == null)
                        leftLinkEntity.Items = new object[] { rightLinkEntity };
                    else
                        leftLinkEntity.Items = leftLinkEntity.Items.Concat(new object[] { rightLinkEntity }).ToArray();
                }

                if (additionalCriteria != null)
                    return new FilterNode { Filter = additionalCriteria, Source = leftFetch };

                return leftFetch;
            }

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
            }.MergeNodeDown(metadata, options);

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
            }.MergeNodeDown(metadata, options);

            return this;
        }
    }
}
