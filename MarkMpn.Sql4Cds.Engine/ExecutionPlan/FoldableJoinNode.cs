using System;
using System.Collections.Generic;
using System.ComponentModel;
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
    abstract class FoldableJoinNode : BaseJoinNode
    {
        /// <summary>
        /// The attribute in the <see cref="OuterSource"/> to join on
        /// </summary>
        [Category("Join")]
        [Description("The attribute in the outer data source to join on")]
        [DisplayName("Left Attribute")]
        public ColumnReferenceExpression LeftAttribute { get; set; }

        /// <summary>
        /// The attribute in the <see cref="InnerSource"/> to join on
        /// </summary>
        [Category("Join")]
        [Description("The attribute in the inner data source to join on")]
        [DisplayName("Right Attribute")]
        public ColumnReferenceExpression RightAttribute { get; set; }

        /// <summary>
        /// Any additional criteria to apply to the join
        /// </summary>
        [Category("Join")]
        [Description("Any additional criteria to apply to the join")]
        [DisplayName("Additional Join Criteria")]
        public BooleanExpression AdditionalJoinCriteria { get; set; }

        public override IDataExecutionPlanNode FoldQuery(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes)
        {
            LeftSource = LeftSource.FoldQuery(dataSources, options, parameterTypes);
            LeftSource.Parent = this;
            RightSource = RightSource.FoldQuery(dataSources, options, parameterTypes);
            RightSource.Parent = this;

            if (SemiJoin)
                return this;

            var leftSchema = LeftSource.GetSchema(dataSources, parameterTypes);
            var rightSchema = RightSource.GetSchema(dataSources, parameterTypes);

            if (LeftSource is FetchXmlScan leftFetch && RightSource is FetchXmlScan rightFetch)
            {
                // Can't join data from different sources
                if (!leftFetch.DataSource.Equals(rightFetch.DataSource, StringComparison.OrdinalIgnoreCase))
                    return this;

                // If one source is distinct and the other isn't, joining the two won't produce the expected results
                if (leftFetch.FetchXml.distinct ^ rightFetch.FetchXml.distinct)
                    return this;

                // Check that the alias is valid for FetchXML
                if (!FetchXmlScan.IsValidAlias(rightFetch.Alias))
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

                // If the entities are from different virtual entity data providers it's probably not going to work
                if (!dataSources.TryGetValue(leftFetch.DataSource, out var dataSource))
                    throw new NotSupportedQueryFragmentException("Missing datasource " + leftFetch.DataSource);

                if (dataSource.Metadata[leftFetch.Entity.name].DataProviderId != dataSource.Metadata[rightFetch.Entity.name].DataProviderId)
                    return this;

                // If we're doing a right outer join, switch everything round to do a left outer join
                // Also switch join order for inner joins to use N:1 relationships instead of 1:N to avoid problems with paging
                if (JoinType == QualifiedJoinType.RightOuter ||
                    JoinType == QualifiedJoinType.Inner && !rightAttributeParts[0].Equals(rightFetch.Alias, StringComparison.OrdinalIgnoreCase) ||
                    JoinType == QualifiedJoinType.Inner && leftAttribute == leftSchema.PrimaryKey && rightAttribute != rightSchema.PrimaryKey)
                {
                    Swap(ref leftFetch, ref rightFetch);
                    Swap(ref leftEntity, ref rightEntity);
                    Swap(ref leftAttribute, ref rightAttribute);
                    Swap(ref leftAttributeParts, ref rightAttributeParts);
                    Swap(ref leftSchema, ref rightSchema);
                }

                // Must be joining to the root entity of the right source, i.e. not a child link-entity
                if (!rightAttributeParts[0].Equals(rightFetch.Alias, StringComparison.OrdinalIgnoreCase))
                    return this;

                // If there are any additional join criteria, either they must be able to be translated to FetchXml criteria
                // in the new link entity or we must be using an inner join so we can use a post-filter node
                var additionalCriteria = AdditionalJoinCriteria;
                var additionalLinkEntities = new Dictionary<object, List<FetchLinkEntityType>>();

                if (TranslateFetchXMLCriteria(dataSource.Metadata, options, additionalCriteria, rightSchema, rightFetch.Alias, rightEntity.name, rightFetch.Alias, rightEntity.Items, out var filter, additionalLinkEntities))
                {
                    rightEntity.AddItem(filter);

                    foreach (var kvp in additionalLinkEntities)
                    {
                        if (kvp.Key is FetchEntityType e)
                        {
                            foreach (var le in kvp.Value)
                                rightEntity.AddItem(le);
                        }
                        else
                        {
                            foreach (var le in kvp.Value)
                                ((FetchLinkEntityType)kvp.Key).AddItem(le);
                        }
                    }

                    additionalCriteria = null;
                }

                if (additionalCriteria != null && JoinType != QualifiedJoinType.Inner)
                    return this;

                var rightLinkEntity = new FetchLinkEntityType
                {
                    alias = rightFetch.Alias,
                    name = rightEntity.name,
                    linktype = JoinType == QualifiedJoinType.Inner ? "inner" : "outer",
                    from = rightAttributeParts[1].ToLowerInvariant(),
                    to = leftAttributeParts[1].ToLowerInvariant(),
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
                    return new FilterNode { Filter = additionalCriteria, Source = leftFetch }.FoldQuery(dataSources, options, parameterTypes);

                return leftFetch;
            }

            if (LeftSource is MetadataQueryNode leftMeta && RightSource is MetadataQueryNode rightMeta && JoinType == QualifiedJoinType.Inner)
            {
                // Can't join data from different sources
                if (!leftMeta.DataSource.Equals(rightMeta.DataSource, StringComparison.OrdinalIgnoreCase))
                    return this;

                // Check if this is a simple join that the MetadataQueryNode can handle - joining from entity metadata to one of it's children
                if ((leftMeta.MetadataSource & rightMeta.MetadataSource) == 0 && (leftMeta.MetadataSource | rightMeta.MetadataSource).HasFlag(MetadataSource.Entity))
                {
                    // We're joining an entity list with an attribute/relationship list. Check the join is on the entity name fields
                    if (!leftSchema.ContainsColumn(LeftAttribute.GetColumnName(), out var leftKey) ||
                        !rightSchema.ContainsColumn(RightAttribute.GetColumnName(), out var rightKey))
                        return this;

                    var entityMeta = leftMeta.MetadataSource.HasFlag(MetadataSource.Entity) ? leftMeta : rightMeta;
                    var entityKey = entityMeta == leftMeta ? leftKey : rightKey;
                    var otherMeta = entityMeta == leftMeta ? rightMeta : leftMeta;
                    var otherKey = entityMeta == leftMeta ? rightKey : leftKey;

                    if (!entityKey.Equals($"{entityMeta.EntityAlias}.{nameof(EntityMetadata.LogicalName)}", StringComparison.OrdinalIgnoreCase))
                        return this;

                    if (otherMeta.MetadataSource == MetadataSource.Attribute)
                    {
                        if (!otherKey.Equals($"{otherMeta.AttributeAlias}.{nameof(AttributeMetadata.EntityLogicalName)}", StringComparison.OrdinalIgnoreCase))
                            return this;

                        // Move the attribute details into the entity source
                        entityMeta.MetadataSource |= otherMeta.MetadataSource;
                        entityMeta.AttributeAlias = otherMeta.AttributeAlias;
                        entityMeta.Query.AttributeQuery = otherMeta.Query.AttributeQuery;

                        return entityMeta;
                    }

                    if (otherMeta.MetadataSource == MetadataSource.OneToManyRelationship)
                    {
                        if (!otherKey.Equals($"{otherMeta.OneToManyRelationshipAlias}.{nameof(OneToManyRelationshipMetadata.ReferencedEntity)}", StringComparison.OrdinalIgnoreCase))
                            return this;

                        // Move the relationship details into the entity source
                        entityMeta.MetadataSource |= otherMeta.MetadataSource;
                        entityMeta.OneToManyRelationshipAlias = otherMeta.OneToManyRelationshipAlias;
                        entityMeta.Query.RelationshipQuery = otherMeta.Query.RelationshipQuery;

                        return entityMeta;
                    }

                    if (otherMeta.MetadataSource == MetadataSource.ManyToOneRelationship)
                    {
                        if (!otherKey.Equals($"{otherMeta.ManyToOneRelationshipAlias}.{nameof(OneToManyRelationshipMetadata.ReferencingEntity)}", StringComparison.OrdinalIgnoreCase))
                            return this;

                        // Move the relationship details into the entity source
                        entityMeta.MetadataSource |= otherMeta.MetadataSource;
                        entityMeta.ManyToOneRelationshipAlias = otherMeta.ManyToOneRelationshipAlias;
                        entityMeta.Query.RelationshipQuery = otherMeta.Query.RelationshipQuery;

                        return entityMeta;
                    }

                    if (otherMeta.MetadataSource == MetadataSource.ManyToManyRelationship)
                    {
                        if (!otherKey.Equals($"{otherMeta.ManyToManyRelationshipAlias}.{nameof(ManyToManyRelationshipMetadata.Entity1LogicalName)}", StringComparison.OrdinalIgnoreCase) &&
                            !otherKey.Equals($"{otherMeta.ManyToManyRelationshipAlias}.{nameof(ManyToManyRelationshipMetadata.Entity2LogicalName)}", StringComparison.OrdinalIgnoreCase) &&
                            !otherKey.Equals($"{otherMeta.ManyToManyRelationshipAlias}.{nameof(ManyToManyRelationshipMetadata.IntersectEntityName)}", StringComparison.OrdinalIgnoreCase))
                            return this;

                        // Move the relationship details into the entity source
                        entityMeta.MetadataSource |= otherMeta.MetadataSource;
                        entityMeta.ManyToManyRelationshipAlias = otherMeta.ManyToManyRelationshipAlias;
                        entityMeta.ManyToManyRelationshipJoin = otherKey.Split('.')[1];
                        entityMeta.Query.RelationshipQuery = otherMeta.Query.RelationshipQuery;

                        return entityMeta;
                    }
                }
            }

            return this;
        }

        private static void Swap<T>(ref T left, ref T right)
        {
            var temp = left;
            left = right;
            right = temp;
        }

        public override void AddRequiredColumns(IDictionary<string, DataSource> dataSources, IDictionary<string, Type> parameterTypes, IList<string> requiredColumns)
        {
            if (AdditionalJoinCriteria != null)
            {
                foreach (var col in AdditionalJoinCriteria.GetColumns())
                {
                    if (!requiredColumns.Contains(col, StringComparer.OrdinalIgnoreCase))
                        requiredColumns.Add(col);
                }
            }

            // Work out which columns need to be pushed down to which source
            var leftSchema = LeftSource.GetSchema(dataSources, parameterTypes);
            var rightSchema = RightSource.GetSchema(dataSources, parameterTypes);

            var leftColumns = requiredColumns
                .Where(col => leftSchema.ContainsColumn(col, out _))
                .ToList();
            var rightColumns = requiredColumns
                .Where(col => rightSchema.ContainsColumn(col, out _))
                .ToList();

            leftColumns.Add(LeftAttribute.GetColumnName());
            rightColumns.Add(RightAttribute.GetColumnName());

            LeftSource.AddRequiredColumns(dataSources, parameterTypes, leftColumns);
            RightSource.AddRequiredColumns(dataSources, parameterTypes, rightColumns);
        }

        public override int EstimateRowsOut(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes)
        {
            var leftEstimate = LeftSource.EstimateRowsOut(dataSources, options, parameterTypes);
            var rightEstimate = RightSource.EstimateRowsOut(dataSources, options, parameterTypes);

            if (JoinType == QualifiedJoinType.Inner)
                return Math.Min(leftEstimate, rightEstimate);
            else
                return Math.Max(leftEstimate, rightEstimate);
        }

        protected override NodeSchema GetSchema(IDictionary<string, DataSource> dataSources, IDictionary<string, Type> parameterTypes, bool includeSemiJoin)
        {
            var schema = base.GetSchema(dataSources, parameterTypes, includeSemiJoin);

            if (schema.PrimaryKey == null && JoinType == QualifiedJoinType.Inner)
            {
                var leftSchema = LeftSource.GetSchema(dataSources, parameterTypes);
                var rightSchema = GetRightSchema(dataSources, parameterTypes);

                if (LeftAttribute.GetColumnName() == leftSchema.PrimaryKey)
                    schema.PrimaryKey = rightSchema.PrimaryKey;
                else if (RightAttribute.GetColumnName() == rightSchema.PrimaryKey)
                    schema.PrimaryKey = leftSchema.PrimaryKey;
            }

            return schema;
        }
    }
}
