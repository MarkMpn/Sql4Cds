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
    public abstract class FoldableJoinNode : BaseJoinNode
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

        public override IExecutionPlanNode FoldQuery(IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes)
        {
            LeftSource = LeftSource.FoldQuery(metadata, options, parameterTypes);
            LeftSource.Parent = this;
            RightSource = RightSource.FoldQuery(metadata, options, parameterTypes);
            RightSource.Parent = this;

            var leftSchema = LeftSource.GetSchema(metadata, parameterTypes);
            var rightSchema = RightSource.GetSchema(metadata, parameterTypes);

            if (LeftSource is FetchXmlScan leftFetch && RightSource is FetchXmlScan rightFetch)
            {
                // If one source is distinct and the other isn't, joining the two won't produce the expected results
                if (leftFetch.FetchXml.distinct ^ rightFetch.FetchXml.distinct)
                    return this;

                var leftEntity = leftFetch.Entity;
                var rightEntity = rightFetch.Entity;

                // Check that the join is on columns that are available in the FetchXML
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

                if (TranslateCriteria(metadata, options, additionalCriteria, rightSchema, rightFetch.Alias, rightEntity.name, rightFetch.Alias, rightEntity.Items, out var filter))
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
                    return new FilterNode { Filter = additionalCriteria, Source = leftFetch }.FoldQuery(metadata, options, parameterTypes);

                return leftFetch;
            }

            return this;
        }

        public override void AddRequiredColumns(IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes, IList<string> requiredColumns)
        {
            // Work out which columns need to be pushed down to which source
            var leftSchema = LeftSource.GetSchema(metadata, parameterTypes);
            var rightSchema = RightSource.GetSchema(metadata, parameterTypes);

            var leftColumns = requiredColumns
                .Where(col => leftSchema.ContainsColumn(col, out _))
                .ToList();
            var rightColumns = requiredColumns
                .Where(col => rightSchema.ContainsColumn(col, out _))
                .ToList();

            leftColumns.Add(LeftAttribute.GetColumnName());
            rightColumns.Add(RightAttribute.GetColumnName());

            LeftSource.AddRequiredColumns(metadata, parameterTypes, leftColumns);
            RightSource.AddRequiredColumns(metadata, parameterTypes, rightColumns);
        }

        public override int EstimateRowsOut(IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes, ITableSizeCache tableSize)
        {
            var leftEstimate = LeftSource.EstimateRowsOut(metadata, parameterTypes, tableSize);
            var rightEstimate = RightSource.EstimateRowsOut(metadata, parameterTypes, tableSize);

            if (JoinType == QualifiedJoinType.Inner)
                return Math.Min(leftEstimate, rightEstimate);
            else
                return Math.Max(leftEstimate, rightEstimate);
        }

        public override NodeSchema GetSchema(IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes)
        {
            var schema = base.GetSchema(metadata, parameterTypes);

            if (schema.PrimaryKey == null && JoinType == QualifiedJoinType.Inner)
            {
                var leftSchema = LeftSource.GetSchema(metadata, parameterTypes);
                var rightSchema = GetRightSchema(metadata, parameterTypes);

                if (LeftAttribute.GetColumnName() == leftSchema.PrimaryKey)
                    schema.PrimaryKey = rightSchema.PrimaryKey;
                else if (RightAttribute.GetColumnName() == rightSchema.PrimaryKey)
                    schema.PrimaryKey = leftSchema.PrimaryKey;
            }

            return schema;
        }
    }
}
