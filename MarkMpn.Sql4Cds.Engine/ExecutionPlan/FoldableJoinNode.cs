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
using Microsoft.Crm.Sdk.Messages;
#if NETCOREAPP
using Microsoft.PowerPlatform.Dataverse.Client;
#else
using Microsoft.Xrm.Tooling.Connector;
#endif

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// Merges two sorted data sets
    /// </summary>
    abstract class FoldableJoinNode : BaseJoinNode
    {
        private static int _nestedLoopCount;

        /// <summary>
        /// The attribute in the <see cref="OuterSource"/> to join on
        /// </summary>
        [Browsable(false)]
        public ColumnReferenceExpression LeftAttribute
        {
            get => LeftAttributes.Count == 1 ? LeftAttributes[0] : null;
            set
            {
                LeftAttributes.Clear();
                LeftAttributes.Add(value);
            }
        }

        /// <summary>
        /// The attributes in the <see cref="OuterSource"/> to join on
        /// </summary>
        [Category("Join")]
        [Description("The attributes in the outer data source to join on")]
        [DisplayName("Left Attributes")]
        public List<ColumnReferenceExpression> LeftAttributes { get; } = new List<ColumnReferenceExpression>();

        /// <summary>
        /// The attribute in the <see cref="InnerSource"/> to join on
        /// </summary>
        [Browsable(false)]
        public ColumnReferenceExpression RightAttribute
        {
            get => RightAttributes.Count == 1 ? RightAttributes[0] : null;
            set
            {
                RightAttributes.Clear();
                RightAttributes.Add(value);
            }
        }

        /// <summary>
        /// The attributes in the <see cref="InnerSource"/> to join on
        /// </summary>
        [Category("Join")]
        [Description("The attributes in the inner data source to join on")]
        [DisplayName("Right Attributes")]
        public List<ColumnReferenceExpression> RightAttributes { get; } = new List<ColumnReferenceExpression>();

        internal List<BooleanComparisonExpression> Expressions { get; } = new List<BooleanComparisonExpression>();

        /// <summary>
        /// The type of comparison that is used for the two inputs
        /// </summary>
        [Category("Join")]
        [Description("The type of comparison that is used for the two inputs")]
        [DisplayName("Comparison Type")]
        public BooleanComparisonType ComparisonType { get; set; } = BooleanComparisonType.Equals;

        /// <summary>
        /// Any additional criteria to apply to the join
        /// </summary>
        [Category("Join")]
        [Description("Any additional criteria to apply to the join")]
        [DisplayName("Additional Join Criteria")]
        public BooleanExpression AdditionalJoinCriteria { get; set; }

        public override IDataExecutionPlanNodeInternal FoldQuery(NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            // For inner joins, additional join criteria are equivalent to doing the join without them and then applying the filter
            // We've already got logic in the Filter node for efficiently folding those queries, so split them out and let it do
            // what it can
            if (JoinType == QualifiedJoinType.Inner && AdditionalJoinCriteria != null)
            {
                var filter = new FilterNode
                {
                    Source = this,
                    Filter = AdditionalJoinCriteria
                };
                AdditionalJoinCriteria = null;
                return filter.FoldQuery(context, hints);
            }

            LeftSource = LeftSource.FoldQuery(context, hints);
            LeftSource.Parent = this;
            RightSource = RightSource.FoldQuery(context, hints);
            RightSource.Parent = this;

            var leftSchema = LeftSource.GetSchema(context);
            var leftCompilationContext = new ExpressionCompilationContext(context, leftSchema, null);
            var rightSchema = RightSource.GetSchema(context);
            var rightCompilationContext = new ExpressionCompilationContext(context, rightSchema, null);

            // Check the types of the comparisons
            for (var i = 0; i < LeftAttributes.Count; i++)
                ValidateComparison(context, leftCompilationContext, rightCompilationContext, i);

            FoldDefinedValues(rightSchema);

            if (SemiJoin)
                return this;

            IDataExecutionPlanNodeInternal folded = null;

            if (LeftAttributes.Count == 1 && ComparisonType == BooleanComparisonType.Equals && JoinType != QualifiedJoinType.FullOuter)
            {
                var leftFilter = JoinType == QualifiedJoinType.Inner || JoinType == QualifiedJoinType.LeftOuter ? LeftSource as FilterNode : null;
                var rightFilter = JoinType == QualifiedJoinType.Inner || JoinType == QualifiedJoinType.RightOuter ? RightSource as FilterNode : null;
                var leftFetch = (leftFilter?.Source ?? LeftSource) as FetchXmlScan;
                var rightFetch = (rightFilter?.Source ?? RightSource) as FetchXmlScan;
                var leftJoin = (leftFilter?.Source ?? LeftSource) as BaseJoinNode;
                var rightJoin = (rightFilter?.Source ?? RightSource) as BaseJoinNode;
                var leftMeta = (leftFilter?.Source ?? LeftSource) as MetadataQueryNode;
                var rightMeta = (rightFilter?.Source ?? RightSource) as MetadataQueryNode;
                var leftOptionSet = (leftFilter?.Source ?? LeftSource) as GlobalOptionSetQueryNode;
                var rightOptionSet = (rightFilter?.Source ?? RightSource) as GlobalOptionSetQueryNode;

                if (leftFetch != null && rightFetch != null && FoldFetchXmlJoin(context, hints, leftFetch, leftSchema, rightFetch, rightSchema, out folded))
                    return PrependFilters(folded, context, hints, leftFilter, rightFilter);

                if (leftJoin != null && rightFetch != null && FoldFetchXmlJoin(context, hints, leftJoin, rightFetch, rightSchema, out folded))
                    return PrependFilters(folded, context, hints, leftFilter, rightFilter);

                if (rightJoin != null && leftFetch != null && FoldFetchXmlJoin(context, hints, rightJoin, leftFetch, leftSchema, out folded))
                    return PrependFilters(folded, context, hints, leftFilter, rightFilter);

                if (leftMeta != null && rightMeta != null && JoinType == QualifiedJoinType.Inner && FoldMetadataJoin(context, hints, leftMeta, leftSchema, rightMeta, rightSchema, out folded))
                {
                    folded = PrependFilters(folded, context, hints, leftFilter, rightFilter);

                    if (AdditionalJoinCriteria != null)
                    {
                        folded = new FilterNode
                        {
                            Source = folded,
                            Filter = AdditionalJoinCriteria
                        }.FoldQuery(context, hints);
                    }

                    return folded;
                }

                if (leftOptionSet != null && rightOptionSet != null && JoinType == QualifiedJoinType.Inner && FoldOptionSetJoin(context, hints, leftOptionSet, leftSchema, rightOptionSet, rightSchema, out folded))
                {
                    folded = PrependFilters(folded, context, hints, leftFilter, rightFilter);

                    if (AdditionalJoinCriteria != null)
                    {
                        folded = new FilterNode
                        {
                            Source = folded,
                            Filter = AdditionalJoinCriteria
                        }.FoldQuery(context, hints);
                    }

                    return folded;
                }
            }

            if (ComparisonType == BooleanComparisonType.Equals)
            {
                // Add not-null filter on join keys
                // Inner join - both must be non-null
                // Left outer join - right key must be non-null
                // Right outer join - left key must be non-null
                if (JoinType == QualifiedJoinType.Inner || JoinType == QualifiedJoinType.RightOuter)
                    LeftSource = AddNotNullFilter(LeftSource, LeftAttribute, context, hints, false);

                if (JoinType == QualifiedJoinType.Inner || JoinType == QualifiedJoinType.LeftOuter)
                    RightSource = AddNotNullFilter(RightSource, RightAttribute, context, hints, false);
            }

            if (FoldSingleRowJoinToNestedLoop(context, hints, leftSchema, rightSchema, out folded))
                return folded;

            return this;
        }

        private void ValidateComparison(NodeCompilationContext context, ExpressionCompilationContext leftCompilationContext, ExpressionCompilationContext rightCompilationContext, int i)
        {
            LeftAttributes[i].GetType(leftCompilationContext, out var leftColType);
            RightAttributes[i].GetType(rightCompilationContext, out var rightColType);

            var expression = i < Expressions.Count ? Expressions[i] : null;
            if (!SqlTypeConverter.CanMakeConsistentTypes(leftColType, rightColType, context.PrimaryDataSource, null, "equals", out var keyType))
                throw new NotSupportedQueryFragmentException(Sql4CdsError.TypeClash(expression, leftColType, rightColType));

            if (keyType is SqlDataTypeReferenceWithCollation keyTypeWithCollation && keyTypeWithCollation.CollationLabel == CollationLabel.NoCollation)
                throw new NotSupportedQueryFragmentException(keyTypeWithCollation.CollationConflictError.ForFragment(expression));

            ValidateComparison(expression, keyType, leftColType, leftCompilationContext, rightColType, rightCompilationContext, i);
        }

        protected virtual void ValidateComparison(BooleanComparisonExpression expression, DataTypeReference keyType, DataTypeReference leftColType, ExpressionCompilationContext leftCompilationContext, DataTypeReference rightColType, ExpressionCompilationContext rightCompilationContext, int i)
        {
        }

        private IDataExecutionPlanNodeInternal PrependFilters(IDataExecutionPlanNodeInternal folded, NodeCompilationContext context, IList<OptimizerHint> hints, params FilterNode[] filters)
        {
            foreach (var filter in filters)
            {
                if (filter == null)
                    continue;

                filter.Source = folded;
                folded = filter.FoldQuery(context, hints);
            }

            return folded;
        }

        protected IDataExecutionPlanNodeInternal AddNotNullFilter(IDataExecutionPlanNodeInternal source, ColumnReferenceExpression attribute, NodeCompilationContext context, IList<OptimizerHint> hints, bool required)
        {
            var schema = source.GetSchema(context);
            if (!schema.ContainsColumn(attribute.GetColumnName(), out var colName))
                return source;

            if (!schema.Schema[colName].IsNullable)
                return source;

            var filter = new FilterNode
            {
                Source = source,
                Filter = new BooleanIsNullExpression
                {
                    Expression = attribute,
                    IsNot = true
                }
            };

            var folded = filter.FoldQuery(context, hints);

            if (required || folded != filter)
            {
                folded.Parent = this;
                return folded;
            }

            return source;
        }

        private bool FoldFetchXmlJoin(NodeCompilationContext context, IList<OptimizerHint> hints, BaseJoinNode join, FetchXmlScan fetch, INodeSchema fetchSchema, out IDataExecutionPlanNodeInternal folded)
        {
            folded = null;

            if (JoinType == QualifiedJoinType.FullOuter)
                return false;

            if (JoinType == QualifiedJoinType.Inner && join.JoinType != QualifiedJoinType.Inner)
                return false;

            if (JoinType == QualifiedJoinType.LeftOuter && fetch == LeftSource)
                return false;

            if (JoinType == QualifiedJoinType.RightOuter && fetch == RightSource)
                return false;

            if ((join.JoinType == QualifiedJoinType.Inner || join.JoinType == QualifiedJoinType.RightOuter) && join.LeftSource is FetchXmlScan leftInnerFetch)
            {
                var leftSource = leftInnerFetch;
                var leftSchema = leftInnerFetch.GetSchema(context);
                var rightSource = fetch;
                var rightSchema = fetchSchema;

                if (fetch == LeftSource)
                {
                    Swap(ref leftSource, ref rightSource);
                    Swap(ref leftSchema, ref rightSchema);
                }

                if (FoldFetchXmlJoin(context, hints, leftSource, leftSchema, rightSource, rightSchema, out folded))
                {
                    folded.Parent = join;
                    join.LeftSource = folded;
                    folded = ConvertManyToManyMergeJoinToHashJoin(join, context, hints);
                    return true;
                }
            }

            if ((join.JoinType == QualifiedJoinType.Inner || join.JoinType == QualifiedJoinType.LeftOuter) && join.RightSource is FetchXmlScan rightInnerFetch)
            {
                var leftSource = rightInnerFetch;
                var leftSchema = rightInnerFetch.GetSchema(context);
                var rightSource = fetch;
                var rightSchema = fetchSchema;

                if (fetch == LeftSource)
                {
                    Swap(ref leftSource, ref rightSource);
                    Swap(ref leftSchema, ref rightSchema);
                }

                if (FoldFetchXmlJoin(context, hints, leftSource, leftSchema, rightSource, rightSchema, out folded))
                {
                    folded.Parent = join;
                    join.RightSource = folded;
                    folded = join;
                    return true;
                }
            }

            return false;
        }

        private IDataExecutionPlanNodeInternal ConvertManyToManyMergeJoinToHashJoin(BaseJoinNode join, NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            // If folding the inner join has caused a one-to-many merge join to become a many-to-many merge join,
            // which we don't currently support, switch it to be a hash join
            if (!(join is MergeJoinNode merge))
                return join;

            var leftSchema = join.LeftSource.GetSchema(context);
            if (leftSchema.ContainsColumn(merge.LeftAttribute.GetColumnName(), out var leftKey) &&
                leftKey == leftSchema.PrimaryKey)
                return join;

            var hash = new HashJoinNode
            {
                AdditionalJoinCriteria = merge.AdditionalJoinCriteria,
                JoinType = merge.JoinType,
                LeftAttribute = merge.LeftAttribute,
                LeftSource = merge.LeftSource,
                Parent = merge.Parent,
                RightAttribute = merge.RightAttribute,
                RightSource = merge.RightSource,
                SemiJoin = merge.SemiJoin
            };

            foreach (var kvp in merge.DefinedValues)
                hash.DefinedValues.Add(kvp);

            // Remove any sorts previously added by the merge join
            if (hash.LeftSource is SortNode leftSort)
                hash.LeftSource = leftSort.Source;
            else if (hash.LeftSource is FetchXmlScan leftFetch)
                leftFetch.RemoveSorts();

            if (hash.RightSource is SortNode rightSort)
                hash.RightSource = rightSort.Source;
            else if (hash.RightSource is FetchXmlScan rightFetch)
                rightFetch.RemoveSorts();

            hash.LeftSource.Parent = hash;
            hash.RightSource.Parent = hash;

            return hash.FoldQuery(context, hints);
        }

        private bool FoldFetchXmlJoin(NodeCompilationContext context, IList<OptimizerHint> hints, FetchXmlScan leftFetch, INodeSchema leftSchema, FetchXmlScan rightFetch, INodeSchema rightSchema, out IDataExecutionPlanNodeInternal folded)
        {
            folded = null;

            // Can't fold joins without key attributes
            if (LeftAttribute == null || RightAttribute == null)
                return false;

            // Can't join data from different sources
            if (!leftFetch.DataSource.Equals(rightFetch.DataSource, StringComparison.OrdinalIgnoreCase))
                return false;

            // Can't join with archived data
            if (leftFetch.FetchXml.DataSource == "retained" || rightFetch.FetchXml.DataSource == "retained")
                return false;

            // Can't join with different schemas
            if (leftFetch.FetchXml.DataSource != rightFetch.FetchXml.DataSource)
                return false;

            // If one source is distinct and the other isn't, joining the two won't produce the expected results
            if (leftFetch.FetchXml.distinct ^ rightFetch.FetchXml.distinct)
                return false;

            // Can't fold joins if a top clause has already been applied
            if (leftFetch.FetchXml.top != null || rightFetch.FetchXml.top != null ||
                leftFetch.FetchXml.page != null || rightFetch.FetchXml.page != null)
                return false;

            // Check that the alias is valid for FetchXML
            if (!FetchXmlScan.IsValidAlias(rightFetch.Alias))
                return false;

            // Can't fold joins if the two FetchXML instances reuse aliases, either for tables or columns
            if (leftFetch.Entity
                .GetLinkEntities()
                .Select(le => le.alias)
                .Concat(new[] { leftFetch.Alias })
                .Intersect(
                    rightFetch.Entity
                    .GetLinkEntities()
                    .Select(le => le.alias)
                    .Concat(new[] { rightFetch.Alias }),
                    StringComparer.OrdinalIgnoreCase)
                .Any())
                return false;

            if (leftFetch.Entity
                .GetLinkEntities()
                .Where(le => le.Items != null)
                .SelectMany(le => le.Items.OfType<FetchAttributeType>())
                .Concat(leftFetch.Entity.Items?.OfType<FetchAttributeType>() ?? Enumerable.Empty<FetchAttributeType>())
                .Select(a => a.alias)
                .Where(alias => alias != null)
                .Intersect(
                    rightFetch.Entity
                    .GetLinkEntities()
                    .Where(le => le.Items != null)
                    .SelectMany(le => le.Items.OfType<FetchAttributeType>())
                    .Concat(rightFetch.Entity.Items?.OfType<FetchAttributeType>() ?? Enumerable.Empty<FetchAttributeType>())
                    .Select(a => a.alias)
                    .Where(alias => alias != null),
                    StringComparer.OrdinalIgnoreCase)
                .Any())
                return false;

            var leftEntity = leftFetch.Entity;
            var rightEntity = rightFetch.Entity;

            // Can't fold joins using explicit collations
            if (LeftAttribute.Collation != null || RightAttribute.Collation != null)
                return false;

            // Check that the join is on columns that are available in the FetchXML
            var leftAttribute = LeftAttribute.GetColumnName();
            if (!leftSchema.ContainsColumn(leftAttribute, out leftAttribute))
                return false;
            var rightAttribute = RightAttribute.GetColumnName();
            if (!rightSchema.ContainsColumn(rightAttribute, out rightAttribute))
                return false;
            var leftAttributeParts = leftAttribute.SplitMultiPartIdentifier();
            var rightAttributeParts = rightAttribute.SplitMultiPartIdentifier();
            if (leftAttributeParts.Length != 2)
                return false;
            if (rightAttributeParts.Length != 2)
                return false;

            // If the entities are from different virtual entity data providers it's probably not going to work
            if (!context.Session.DataSources.TryGetValue(leftFetch.DataSource, out var dataSource))
                throw new NotSupportedQueryFragmentException("Missing datasource " + leftFetch.DataSource);

            if (dataSource.Metadata[leftFetch.Entity.name].DataProviderId != dataSource.Metadata[rightFetch.Entity.name].DataProviderId)
                return false;

            // Elastic tables don't support joins
            if (dataSource.Metadata[leftFetch.Entity.name].DataProviderId == new Guid("1d9bde74-9ebd-4da9-8ff5-aa74945b9f74"))
                return false;

            // Check we're not going to have too many link entities
            var leftLinkCount = leftFetch.Entity.GetLinkEntities().Count();
            var rightLinkCount = rightFetch.Entity.GetLinkEntities().Count() + 1;

            // Max limit of 10 joins, raised to 15 in 9.2.22043
            if (leftLinkCount + rightLinkCount > 10 && (leftLinkCount + rightLinkCount > 15 || dataSource.Connection == null || GetVersion(dataSource.Connection) < new Version("9.2.22043")))
                return false;

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
                Swap(ref leftLinkCount, ref rightLinkCount);
                leftLinkCount--;
                rightLinkCount++;
            }

            // Must be joining to the root entity of the right source, i.e. not a child link-entity
            if (!rightAttributeParts[0].Equals(rightFetch.Alias, StringComparison.OrdinalIgnoreCase))
                return false;

            // Can't join to audit except for single join to systemuser from userid or callinguserid
            if (IsInvalidAuditJoin(leftFetch, leftAttributeParts, rightFetch, rightAttributeParts))
                return false;

            if (IsInvalidAuditJoin(rightFetch, rightAttributeParts, leftFetch, leftAttributeParts))
                return false;

            // If the inner source has any column comparisons to sub-linkentities, we can't fold the join
            // as those filters would then be interpreted as join criteria to the parent table and produce invalid SQL
            // https://github.com/MarkMpn/Sql4Cds/issues/595
            if (HasChildColumnComparisons(rightFetch.Entity))
                return false;

            // If there are any additional join criteria, either they must be able to be translated to FetchXml criteria
            // in the new link entity or we must be using an inner join so we can use a post-filter node
            var additionalCriteria = AdditionalJoinCriteria;

            if (TranslateFetchXMLCriteria(context, dataSource, additionalCriteria, rightSchema, rightFetch.Alias, null, rightEntity.name, rightFetch.Alias, rightEntity.Items, null, null, out var filter))
            {
                rightEntity.AddItem(filter);
                additionalCriteria = null;
            }

            if (additionalCriteria != null && JoinType != QualifiedJoinType.Inner)
                return false;

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
                    return false;

                if (leftLinkEntity.Items == null)
                    leftLinkEntity.Items = new object[] { rightLinkEntity };
                else
                    leftLinkEntity.Items = leftLinkEntity.Items.Concat(new object[] { rightLinkEntity }).ToArray();
            }

            foreach (var alias in rightFetch.HiddenAliases)
                leftFetch.HiddenAliases.Add(alias);

            foreach (var mapping in rightFetch.ColumnMappings)
                leftFetch.ColumnMappings.Add(mapping);

            folded = leftFetch;

            if (additionalCriteria != null)
                folded = new FilterNode { Filter = additionalCriteria, Source = leftFetch }.FoldQuery(context, hints);

            // We might have previously folded a sort to the FetchXML that is no longer valid as we require custom paging
            if (leftFetch.RequiresCustomPaging(context.Session.DataSources))
                leftFetch.RemoveSorts();

            // We might have previously folded a not-null condition that is no longer required as it is implicit in the join
            var unnecessaryNotNullColumns = new List<string>();
            if (JoinType == QualifiedJoinType.Inner || JoinType == QualifiedJoinType.RightOuter)
                unnecessaryNotNullColumns.Add(LeftAttribute.GetColumnName());
            if (JoinType == QualifiedJoinType.Inner || JoinType == QualifiedJoinType.LeftOuter)
                unnecessaryNotNullColumns.Add(RightAttribute.GetColumnName());

            if (unnecessaryNotNullColumns != null)
            {
                var finalSchema = leftFetch.GetSchema(context);

                foreach (var col in unnecessaryNotNullColumns)
                {
                    if (!finalSchema.ContainsColumn(col, out var normalizedCol))
                        continue;

                    var parts = normalizedCol.SplitMultiPartIdentifier();

                    if (parts[0] == leftFetch.Alias)
                    {
                        foreach (var entityFilter in leftFetch.Entity.Items.OfType<filter>())
                        {
                            if (entityFilter.type != filterType.and)
                                continue;

                            foreach (var condition in entityFilter.Items.OfType<condition>().ToList())
                            {
                                if (condition.entityname == null && condition.attribute == parts[1] && condition.@operator == @operator.notnull)
                                    entityFilter.Items = entityFilter.Items.Except(new[] { condition }).ToArray();
                            }
                        }
                    }
                    else
                    {
                        foreach (var entityFilter in leftFetch.Entity.Items.OfType<filter>())
                        {
                            if (entityFilter.type != filterType.and)
                                continue;

                            foreach (var condition in entityFilter.Items.OfType<condition>().ToList())
                            {
                                if (condition.entityname == parts[0] && condition.attribute == parts[1] && condition.@operator == @operator.notnull)
                                    entityFilter.Items = entityFilter.Items.Except(new[] { condition }).ToArray();
                            }
                        }

                        var link = leftFetch.Entity.FindLinkEntity(parts[0]);

                        if (link?.Items != null)
                        {
                            foreach (var linkFilter in link.Items.OfType<filter>())
                            {
                                if (linkFilter.type != filterType.and)
                                    continue;

                                foreach (var condition in linkFilter.Items.OfType<condition>().ToList())
                                {
                                    if (condition.attribute == parts[1] && condition.@operator == @operator.notnull)
                                        linkFilter.Items = linkFilter.Items.Except(new[] { condition }).ToArray();
                                }
                            }
                        }
                    }
                }

                // Re-fold the FetchXML node to remove any filters that are now blank
                leftFetch.FoldQuery(context, hints);
            }

            return true;
        }

        private bool HasChildColumnComparisons(FetchEntityType entity)
        {
            // Check if the entity contains any <condition attribute="myattr" operator="eq" valueof="child.attribute" /> elements
            return entity.GetConditions()
                .Any(c => c.ValueOf != null && c.ValueOf.Contains("."));
        }

        private bool IsInvalidAuditJoin(FetchXmlScan leftFetch, string[] leftAttributeParts, FetchXmlScan rightFetch, string[] rightAttributeParts)
        {
            var isJoinFromAudit = false;

            if (leftAttributeParts[0].Equals(leftFetch.Alias, StringComparison.OrdinalIgnoreCase))
            {
                if (leftFetch.Entity.name == "audit")
                {
                    if (leftFetch.Entity.GetLinkEntities().Any())
                        return true;

                    isJoinFromAudit = true;
                }
            }
            else
            {
                var leftLinkEntity = leftFetch.Entity.GetLinkEntities().FirstOrDefault(l => l.alias.Equals(leftAttributeParts[0], StringComparison.OrdinalIgnoreCase));

                if (leftLinkEntity?.name == "audit")
                {
                    if ((leftLinkEntity.Items ?? Array.Empty<object>()).OfType<FetchLinkEntityType>().Any())
                        return true;

                    isJoinFromAudit = true;
                }
            }

            if (isJoinFromAudit)
            {
                if (!leftAttributeParts[1].Equals("userid", StringComparison.OrdinalIgnoreCase) && !leftAttributeParts[1].Equals("callinguserid", StringComparison.OrdinalIgnoreCase))
                    return true;

                if (rightAttributeParts[0].Equals(rightFetch.Alias, StringComparison.OrdinalIgnoreCase))
                {
                    if (rightFetch.Entity.name != "systemuser")
                        return true;
                }
                else
                {
                    var rightLinkEntity = rightFetch.Entity.GetLinkEntities().FirstOrDefault(l => l.alias.Equals(rightAttributeParts[0], StringComparison.OrdinalIgnoreCase));

                    if (rightLinkEntity?.name != "systemuser")
                        return true;
                }

                if (!rightAttributeParts[1].Equals("systemuserid", StringComparison.OrdinalIgnoreCase))
                    return true;
            }

            return false;
        }

        private Version GetVersion(IOrganizationService org)
        {
#if NETCOREAPP
            if (org is ServiceClient svc)
                return svc.ConnectedOrgVersion;
#else
            if (org is CrmServiceClient svc)
                return svc.ConnectedOrgVersion;
#endif

            var resp = (RetrieveVersionResponse)org.Execute(new RetrieveVersionRequest());
            return new Version(resp.Version);
        }

        private bool FoldMetadataJoin(NodeCompilationContext context, IList<OptimizerHint> hints, MetadataQueryNode leftMeta, INodeSchema leftSchema, MetadataQueryNode rightMeta, INodeSchema rightSchema, out IDataExecutionPlanNodeInternal folded)
        {
            folded = null;

            // Can't join data from different sources
            if (!leftMeta.DataSource.Equals(rightMeta.DataSource, StringComparison.OrdinalIgnoreCase))
                return false;

            // Check if this is a simple join that the MetadataQueryNode can handle - joining from entity metadata to one of it's children
            if ((leftMeta.MetadataSource & rightMeta.MetadataSource) == 0 &&
                (leftMeta.MetadataSource | rightMeta.MetadataSource).HasFlag(MetadataSource.Entity) &&
                !(leftMeta.MetadataSource | rightMeta.MetadataSource).HasFlag(MetadataSource.Value))
            {
                // We're joining an entity list with an attribute/relationship list. Check the join is on the entity name fields
                if (!leftSchema.ContainsColumn(LeftAttribute.GetColumnName(), out var leftKey) ||
                    !rightSchema.ContainsColumn(RightAttribute.GetColumnName(), out var rightKey))
                    return false;

                var entityMeta = leftMeta.MetadataSource.HasFlag(MetadataSource.Entity) ? leftMeta : rightMeta;
                var entityKey = entityMeta == leftMeta ? leftKey : rightKey;
                var otherMeta = entityMeta == leftMeta ? rightMeta : leftMeta;
                var otherKey = entityMeta == leftMeta ? rightKey : leftKey;

                if (!entityKey.Equals($"{entityMeta.EntityAlias}.{nameof(EntityMetadata.LogicalName)}", StringComparison.OrdinalIgnoreCase))
                    return false;

                if (otherMeta.MetadataSource == MetadataSource.Attribute)
                {
                    if (!otherKey.Equals($"{otherMeta.AttributeAlias}.{nameof(AttributeMetadata.EntityLogicalName)}", StringComparison.OrdinalIgnoreCase))
                        return false;

                    // Move the attribute details into the entity source
                    entityMeta.MetadataSource |= otherMeta.MetadataSource;
                    entityMeta.AttributeAlias = otherMeta.AttributeAlias;
                    entityMeta.Query.AttributeQuery = otherMeta.Query.AttributeQuery;

                    folded = entityMeta;
                    return true;
                }

                if (otherMeta.MetadataSource == MetadataSource.OneToManyRelationship)
                {
                    if (!otherKey.Equals($"{otherMeta.OneToManyRelationshipAlias}.{nameof(OneToManyRelationshipMetadata.ReferencedEntity)}", StringComparison.OrdinalIgnoreCase))
                        return false;

                    // Move the relationship details into the entity source
                    entityMeta.MetadataSource |= otherMeta.MetadataSource;
                    entityMeta.OneToManyRelationshipAlias = otherMeta.OneToManyRelationshipAlias;
                    entityMeta.Query.RelationshipQuery = otherMeta.Query.RelationshipQuery;

                    folded = entityMeta;
                    return true;
                }

                if (otherMeta.MetadataSource == MetadataSource.ManyToOneRelationship)
                {
                    if (!otherKey.Equals($"{otherMeta.ManyToOneRelationshipAlias}.{nameof(OneToManyRelationshipMetadata.ReferencingEntity)}", StringComparison.OrdinalIgnoreCase))
                        return false;

                    // Move the relationship details into the entity source
                    entityMeta.MetadataSource |= otherMeta.MetadataSource;
                    entityMeta.ManyToOneRelationshipAlias = otherMeta.ManyToOneRelationshipAlias;
                    entityMeta.Query.RelationshipQuery = otherMeta.Query.RelationshipQuery;

                    folded = entityMeta;
                    return true;
                }

                if (otherMeta.MetadataSource == MetadataSource.ManyToManyRelationship)
                {
                    if (!otherKey.Equals($"{otherMeta.ManyToManyRelationshipAlias}.{nameof(ManyToManyRelationshipMetadata.Entity1LogicalName)}", StringComparison.OrdinalIgnoreCase) &&
                        !otherKey.Equals($"{otherMeta.ManyToManyRelationshipAlias}.{nameof(ManyToManyRelationshipMetadata.Entity2LogicalName)}", StringComparison.OrdinalIgnoreCase) &&
                        !otherKey.Equals($"{otherMeta.ManyToManyRelationshipAlias}.{nameof(ManyToManyRelationshipMetadata.IntersectEntityName)}", StringComparison.OrdinalIgnoreCase))
                        return false;

                    // Move the relationship details into the entity source
                    entityMeta.MetadataSource |= otherMeta.MetadataSource;
                    entityMeta.ManyToManyRelationshipAlias = otherMeta.ManyToManyRelationshipAlias;
                    entityMeta.ManyToManyRelationshipJoin = otherKey.SplitMultiPartIdentifier()[1];
                    entityMeta.Query.RelationshipQuery = otherMeta.Query.RelationshipQuery;

                    folded = entityMeta;
                    return true;
                }

                if (otherMeta.MetadataSource == MetadataSource.Key)
                {
                    if (!otherKey.Equals($"{otherMeta.KeyAlias}.{nameof(EntityKeyMetadata.EntityLogicalName)}", StringComparison.OrdinalIgnoreCase))
                        return false;

                    // Move the key details into the entity source
                    entityMeta.MetadataSource |= otherMeta.MetadataSource;
                    entityMeta.KeyAlias = otherMeta.KeyAlias;
                    entityMeta.Query.KeyQuery = otherMeta.Query.KeyQuery;

                    folded = entityMeta;
                    return true;
                }
            }
            else if ((leftMeta.MetadataSource & rightMeta.MetadataSource) == 0 &&
                (leftMeta.MetadataSource | rightMeta.MetadataSource).HasFlag(MetadataSource.Attribute| MetadataSource.Value))
            {
                // We can also join from attribute to optionset value. Check the join is on the attribute ID fields
                if (!leftSchema.ContainsColumn(LeftAttribute.GetColumnName(), out var leftKey) ||
                    !rightSchema.ContainsColumn(RightAttribute.GetColumnName(), out var rightKey))
                    return false;

                var attributeMeta = leftMeta.MetadataSource.HasFlag(MetadataSource.Attribute) ? leftMeta : rightMeta;
                var attributeKey = attributeMeta == leftMeta ? leftKey : rightKey;
                var otherMeta = attributeMeta == leftMeta ? rightMeta : leftMeta;
                var otherKey = attributeMeta == leftMeta ? rightKey : leftKey;

                if (!attributeKey.Equals($"{attributeMeta.AttributeAlias}.{nameof(AttributeMetadata.MetadataId)}", StringComparison.OrdinalIgnoreCase))
                    return false;

                if (otherMeta.MetadataSource == MetadataSource.Value)
                {
                    if (!otherKey.Equals($"{otherMeta.ValueAlias}.attributeid", StringComparison.OrdinalIgnoreCase))
                        return false;

                    // Move the value details into the attribute source
                    attributeMeta.MetadataSource |= otherMeta.MetadataSource;
                    attributeMeta.ValueAlias = otherMeta.ValueAlias;

                    folded = attributeMeta;
                    return true;
                }
            }

            return false;
        }

        private bool FoldOptionSetJoin(NodeCompilationContext context, IList<OptimizerHint> hints, GlobalOptionSetQueryNode leftOptionSet, INodeSchema leftSchema, GlobalOptionSetQueryNode rightOptionSet, INodeSchema rightSchema, out IDataExecutionPlanNodeInternal folded)
        {
            folded = null;

            // Can't join data from different sources
            if (!leftOptionSet.DataSource.Equals(rightOptionSet.DataSource, StringComparison.OrdinalIgnoreCase))
                return false;

            // Check if this is a simple join that the GlobalOptionSetQueryNode can handle - joining from optionset to its value
            if ((leftOptionSet.MetadataSource & rightOptionSet.MetadataSource) == 0 && (leftOptionSet.MetadataSource | rightOptionSet.MetadataSource) == (OptionSetSource.OptionSet | OptionSetSource.Value))
            {
                // We're joining an optionset list with a value list. Check the join is on the metadataid fields
                if (!leftSchema.ContainsColumn(LeftAttribute.GetColumnName(), out var leftKey) ||
                    !rightSchema.ContainsColumn(RightAttribute.GetColumnName(), out var rightKey))
                    return false;

                var optionset = leftOptionSet.MetadataSource == OptionSetSource.OptionSet ? leftOptionSet : rightOptionSet;
                var entityKey = optionset == leftOptionSet ? leftKey : rightKey;
                var value = optionset == leftOptionSet ? rightOptionSet : leftOptionSet;
                var otherKey = optionset == leftOptionSet ? rightKey : leftKey;

                if (!entityKey.Equals($"{optionset.OptionSetAlias}.{nameof(OptionSetMetadataBase.MetadataId)}", StringComparison.OrdinalIgnoreCase))
                    return false;

                if (!otherKey.Equals($"{value.ValueAlias}.optionsetid", StringComparison.OrdinalIgnoreCase))
                    return false;

                // Move the attribute details into the entity source
                optionset.MetadataSource |= value.MetadataSource;
                optionset.ValueAlias = value.ValueAlias;

                folded = optionset;
                return true;
            }

            return false;
        }

        private bool FoldSingleRowJoinToNestedLoop(NodeCompilationContext context, IList<OptimizerHint> hints, INodeSchema leftSchema, INodeSchema rightSchema, out IDataExecutionPlanNodeInternal folded)
        {
            folded = null;

            if (JoinType == QualifiedJoinType.FullOuter)
                return false;

            // If the inner source has an estimated row count of 1, convert to a nested loop node
            // and remove any previously added sort node.
            var joinType = JoinType;
            var leftSource = LeftSource;
            var rightSource = RightSource;
            var leftAttribute = LeftAttribute;
            var rightAttribute = RightAttribute;
            leftSource.EstimateRowsOut(context);
            rightSource.EstimateRowsOut(context);
            leftSchema.ContainsColumn(leftAttribute.GetColumnName(), out var leftAttr);
            rightSchema.ContainsColumn(rightAttribute.GetColumnName(), out var rightAttr);

            if (joinType == QualifiedJoinType.RightOuter || (joinType == QualifiedJoinType.Inner && leftSource.EstimatedRowsOut > 1))
            {
                Swap(ref leftSource, ref rightSource);
                Swap(ref leftSchema, ref rightSchema);
                Swap(ref leftAttribute, ref rightAttribute);
                Swap(ref leftAttr, ref rightAttr);

                if (joinType == QualifiedJoinType.RightOuter)
                    joinType = QualifiedJoinType.LeftOuter;
            }

            if (leftSource.EstimatedRowsOut > 1)
                return false;
            
            // Add the filter to the inner side of the nested loop
            var outerReference = $"@Cond{++_nestedLoopCount}";
            var filteredRightSource = new FilterNode
            {
                Source = rightSource,
                Filter = new BooleanComparisonExpression
                {
                    FirstExpression = rightAttribute,
                    ComparisonType = BooleanComparisonType.Equals,
                    SecondExpression = new VariableReference { Name = outerReference }
                }
            };
            var contextWithOuterReference = context.CreateChildContext(new Dictionary<string, DataTypeReference>
            {
                [outerReference] = leftSchema.Schema[leftAttr].Type
            });
            var foldedRightSource = filteredRightSource.FoldQuery(contextWithOuterReference, hints);

            // If we can't fold the filter down to the data source, there's no benefit from doing this so stick with the
            // original join type
            if (foldedRightSource == filteredRightSource)
                return false;

            var nestedLoop = new NestedLoopNode
            {
                JoinType = joinType,
                LeftSource = leftSource,
                RightSource = foldedRightSource,
                OuterReferences = new Dictionary<string, string>
                {
                    [leftAttr] = outerReference
                },
                JoinCondition = AdditionalJoinCriteria
            };

            // Merge joins might have added sorts already. They're not needed any longer, so remove them.
            if (nestedLoop.LeftSource is SortNode leftSort)
                nestedLoop.LeftSource = leftSort.Source;
            else if (nestedLoop.LeftSource is FetchXmlScan leftFetch)
                leftFetch.RemoveSorts();

            if (nestedLoop.RightSource is SortNode rightSort)
                nestedLoop.RightSource = rightSort.Source;
            else if (nestedLoop.RightSource is FetchXmlScan rightFetch)
                rightFetch.RemoveSorts();

            folded = nestedLoop.FoldQuery(context, hints);
            return true;
        }

        private static void Swap<T>(ref T left, ref T right)
        {
            var temp = left;
            left = right;
            right = temp;
        }

        public override void AddRequiredColumns(NodeCompilationContext context, IList<string> requiredColumns)
        {
            var criteriaCols = AdditionalJoinCriteria?.GetColumns() ?? Enumerable.Empty<string>();

            // Work out which columns need to be pushed down to which source
            var leftSchema = LeftSource.GetSchema(context);
            var rightSchema = RightSource.GetSchema(context);

            var leftColumns = requiredColumns.Where(col => OutputLeftSchema)
                .Concat(criteriaCols)
                .Where(col => leftSchema.ContainsColumn(col, out _))
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToList();
            var rightColumns = requiredColumns.Where(col => OutputRightSchema)
                .Concat(criteriaCols)
                .Where(col => rightSchema.ContainsColumn(col, out _))
                .Concat(DefinedValues.Values)
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToList();

            if (LeftAttribute != null)
                leftColumns.Add(LeftAttribute.GetColumnName());

            if (RightAttribute != null)
                rightColumns.Add(RightAttribute.GetColumnName());

            LeftSource.AddRequiredColumns(context, leftColumns);
            RightSource.AddRequiredColumns(context, rightColumns);
        }

        protected override RowCountEstimate EstimateRowsOutInternal(NodeCompilationContext context)
        {
            var leftEstimate = LeftSource.EstimateRowsOut(context);
            ParseEstimate(leftEstimate, out var leftMin, out var leftMax, out var leftIsRange);
            var rightEstimate = RightSource.EstimateRowsOut(context);
            ParseEstimate(rightEstimate, out var rightMin, out var rightMax, out var rightIsRange);

            if (JoinType == QualifiedJoinType.LeftOuter && SemiJoin)
                return leftEstimate;

            if (JoinType == QualifiedJoinType.RightOuter && SemiJoin)
                return rightEstimate;

            int estimate;

            var leftSchema = LeftSource.GetSchema(context);
            var rightSchema = GetRightSchema(context);

            if (LeftAttribute != null && LeftAttribute.GetColumnName() == leftSchema.PrimaryKey ||
                RightAttribute != null && RightAttribute.GetColumnName() == rightSchema.PrimaryKey)
            {
                if (JoinType == QualifiedJoinType.Inner)
                    estimate = Math.Min(leftMax, rightMax);
                else
                    estimate = Math.Max(leftMax, rightMax);
            }
            else
            {
                estimate = leftMax * rightMax;

                // Check for overflow
                if (estimate < leftMax || estimate < rightMax)
                    estimate = Int32.MaxValue;
            }

            if (leftIsRange && rightIsRange)
                return new RowCountEstimateDefiniteRange(0, estimate);
            else
                return new RowCountEstimate(estimate);
        }

        protected override string GetPrimaryKey(INodeSchema outerSchema, INodeSchema innerSchema)
        {
            if (JoinType == QualifiedJoinType.Inner)
            {
                if (LeftAttribute.GetColumnName() == outerSchema.PrimaryKey)
                    return innerSchema.PrimaryKey;
                else if (RightAttribute.GetColumnName() == innerSchema.PrimaryKey)
                    return outerSchema.PrimaryKey;
            }

            return base.GetPrimaryKey(outerSchema, innerSchema);
        }

        protected override IEnumerable<string> GetVariablesInternal()
        {
            if (AdditionalJoinCriteria == null)
                return Array.Empty<string>();

            return AdditionalJoinCriteria.GetVariables();
        }
    }
}
