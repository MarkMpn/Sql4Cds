using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.Engine.FetchXml;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Metadata.Query;
using Microsoft.Xrm.Sdk.Query;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// Applies a filter to the data stream
    /// </summary>
    public class FilterNode : BaseDataNode, ISingleSourceExecutionPlanNode
    {
        /// <summary>
        /// The filter to apply
        /// </summary>
        public BooleanExpression Filter { get; set; }

        /// <summary>
        /// The data source to select from
        /// </summary>
        public IDataExecutionPlanNode Source { get; set; }

        protected override IEnumerable<Entity> ExecuteInternal(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues)
        {
            var schema = Source.GetSchema(metadata, parameterTypes);
            var filter = Filter.Compile(schema, parameterTypes);

            foreach (var entity in Source.Execute(org, metadata, options, parameterTypes, parameterValues))
            {
                if (filter(entity, parameterValues))
                    yield return entity;
            }
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            yield return Source;
        }

        public override NodeSchema GetSchema(IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes)
        {
            return Source.GetSchema(metadata, parameterTypes);
        }

        public override IDataExecutionPlanNode FoldQuery(IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes)
        {
            Source = Source.FoldQuery(metadata, options, parameterTypes);
            Source.Parent = this;

            // Find all the data source nodes we could fold this into. Include direct data sources, those from either side of an inner join, or the main side of an outer join
            foreach (var source in GetFoldableSources(Source))
            {
                var schema = source.GetSchema(metadata, parameterTypes);

                if (source is FetchXmlScan fetchXml && !fetchXml.FetchXml.aggregate)
                {
                    if (TranslateFetchXMLCriteria(metadata, options, Filter, schema, null, fetchXml.Entity.name, fetchXml.Alias, fetchXml.Entity.Items, out var fetchFilter))
                    {
                        fetchXml.Entity.AddItem(fetchFilter);
                        return Source;
                    }

                    // If the criteria are ANDed, see if any of the individual conditions can be translated to FetchXML
                    Filter = ExtractFetchXMLFilters(metadata, options, Filter, schema, null, fetchXml.Entity.name, fetchXml.Alias, fetchXml.Entity.Items, out fetchFilter);

                    if (fetchFilter != null)
                        fetchXml.Entity.AddItem(fetchFilter);
                }

                if (source is MetadataQueryNode meta)
                {
                    if (TranslateMetadataCriteria(Filter, meta, out var entityFilter, out var attributeFilter, out var relationshipFilter))
                    {
                        meta.Query.AddFilter(entityFilter);

                        if (attributeFilter != null && meta.Query.AttributeQuery == null)
                            meta.Query.AttributeQuery = new AttributeQueryExpression();

                        meta.Query.AttributeQuery.AddFilter(attributeFilter);

                        if (relationshipFilter != null && meta.Query.RelationshipQuery == null)
                            meta.Query.RelationshipQuery = new RelationshipQueryExpression();

                        meta.Query.RelationshipQuery.AddFilter(relationshipFilter);

                        return Source;
                    }

                    // If the criteria are ANDed, see if any of the individual conditions can be translated to the metadata query
                    Filter = ExtractMetadataFilters(Filter, meta, out entityFilter, out attributeFilter, out relationshipFilter);

                    meta.Query.AddFilter(entityFilter);

                    if (attributeFilter != null && meta.Query.AttributeQuery == null)
                        meta.Query.AttributeQuery = new AttributeQueryExpression();

                    meta.Query.AttributeQuery.AddFilter(attributeFilter);

                    if (relationshipFilter != null && meta.Query.RelationshipQuery == null)
                        meta.Query.RelationshipQuery = new RelationshipQueryExpression();

                    meta.Query.RelationshipQuery.AddFilter(relationshipFilter);
                }
            }

            if (options.JoinOperatorsAvailable.Contains(JoinOperator.Any))
            {
                // Correlated IN filters are created as:
                // Filter: table.column is not null
                // -> MergeJoin (LeftOuter) table.column in RightAttribute
                //    -> FetchXml
                //    -> FetchXml (Distinct) orderby table.column
                var merges = new List<MergeJoinNode>();
                var merge = Source as MergeJoinNode;
                while (merge != null)
                {
                    merges.Add(merge);
                    merge = merge.LeftSource as MergeJoinNode;
                }

                while (merges.Count > 0)
                {
                    merge = merges.Last();

                    if (merge.JoinType != QualifiedJoinType.LeftOuter)
                        break;

                    if (!(merge.LeftSource is FetchXmlScan leftFetch) ||
                        !(merge.RightSource is FetchXmlScan rightFetch))
                        break;

                    if (!rightFetch.FetchXml.distinct)
                        break;

                    var rightSorts = (rightFetch.Entity.Items ?? Array.Empty<object>()).OfType<FetchOrderType>().ToList();

                    if (rightSorts.Count != 1)
                        break;

                    if (!String.IsNullOrEmpty(rightSorts[0].alias))
                        break;

                    var attribute = $"{rightFetch.Alias}.{rightSorts[0].attribute}";

                    if (!merge.RightAttribute.GetColumnName().Equals(attribute, StringComparison.OrdinalIgnoreCase))
                        break;

                    var notNullFilter = FindNotNullFilter(Filter, attribute);
                    if (notNullFilter == null)
                        break;

                    // Remove the filter and replace with an "in" link-entity
                    Filter = Filter.RemoveCondition(notNullFilter);

                    var linkEntity = new FetchLinkEntityType
                    {
                        name = rightFetch.Entity.name,
                        alias = rightFetch.Alias,
                        from = merge.RightAttribute.MultiPartIdentifier.Identifiers.Last().Value,
                        to = merge.LeftAttribute.MultiPartIdentifier.Identifiers.Last().Value,
                        linktype = "in",
                        Items = rightFetch.Entity.Items.Where(i => !(i is FetchOrderType)).ToArray()
                    };

                    leftFetch.Entity.AddItem(linkEntity);
                    merges.Remove(merge);

                    // Remove the sort that has been merged into the left side too
                    leftFetch.Entity.Items = leftFetch.Entity
                        .Items
                        .Where(i => !(i is FetchOrderType sort) || !sort.attribute.Equals(merge.LeftAttribute.MultiPartIdentifier.Identifiers.Last().Value, StringComparison.OrdinalIgnoreCase))
                        .ToArray();

                    if (merges.Count == 0)
                    {
                        Source = leftFetch;
                        leftFetch.Parent = this;
                    }
                    else
                    {
                        merge = merges.Last();
                        merge.LeftSource = leftFetch;
                        leftFetch.Parent = merge;
                    }
                }
            }

            // If we've got a filter matching a column and a variable (key lookup in a nested loop) from a table spool, replace it with a index spool
            if (Source is TableSpoolNode tableSpool)
            {
                var schema = Source.GetSchema(metadata, parameterTypes);

                if (ExtractKeyLookupFilter(Filter, out var filter, out var indexColumn, out var seekVariable) && schema.ContainsColumn(indexColumn, out indexColumn))
                {
                    var spoolSource = tableSpool.Source;

                    // Index spool requires non-null key values
                    if (indexColumn != schema.PrimaryKey)
                    {
                        spoolSource = new FilterNode
                        {
                            Source = tableSpool.Source,
                            Filter = new BooleanIsNullExpression
                            {
                                Expression = indexColumn.ToColumnReference(),
                                IsNot = true
                            }
                        }.FoldQuery(metadata, options, parameterTypes);
                    }

                    Source = new IndexSpoolNode
                    {
                        Source = spoolSource,
                        KeyColumn = indexColumn,
                        SeekValue = seekVariable
                    };

                    if (filter == null)
                        return Source;

                    Filter = filter;
                }
            }

            if (Filter == null)
                return Source;

            return this;
        }

        private BooleanIsNullExpression FindNotNullFilter(BooleanExpression filter, string attribute)
        {
            if (filter is BooleanIsNullExpression isNull &&
                isNull.IsNot &&
                isNull.Expression is ColumnReferenceExpression col &&
                col.GetColumnName().Equals(attribute, StringComparison.OrdinalIgnoreCase))
                return isNull;

            if (filter is BooleanParenthesisExpression paren)
                return FindNotNullFilter(paren.Expression, attribute);

            if (filter is BooleanBinaryExpression bin && bin.BinaryExpressionType == BooleanBinaryExpressionType.And)
                return FindNotNullFilter(bin.FirstExpression, attribute) ?? FindNotNullFilter(bin.SecondExpression, attribute);

            return null;
        }

        private IEnumerable<IDataExecutionPlanNode> GetFoldableSources(IDataExecutionPlanNode source)
        {
            if (source is FetchXmlScan)
            {
                yield return source;
                yield break;
            }

            if (source is MetadataQueryNode)
            {
                yield return source;
                yield break;
            }

            if (source is BaseJoinNode join)
            {
                if (join.JoinType == QualifiedJoinType.Inner || join.JoinType == QualifiedJoinType.LeftOuter)
                {
                    foreach (var subSource in GetFoldableSources(join.LeftSource))
                        yield return subSource;
                }

                if (join.JoinType == QualifiedJoinType.Inner || join.JoinType == QualifiedJoinType.RightOuter)
                {
                    foreach (var subSource in GetFoldableSources(join.RightSource))
                        yield return subSource;
                }

                yield break;
            }

            if (source is HashMatchAggregateNode)
                yield break;

            if (source is TableSpoolNode)
                yield break;

            foreach (var subSource in source.GetSources().OfType<IDataExecutionPlanNode>())
            {
                foreach (var foldableSubSource in GetFoldableSources(subSource))
                    yield return foldableSubSource;
            }
        }

        private bool ExtractKeyLookupFilter(BooleanExpression filter, out BooleanExpression remainingFilter, out string indexColumn, out string seekVariable)
        {
            remainingFilter = null;
            indexColumn = null;
            seekVariable = null;

            if (filter is BooleanComparisonExpression cmp && cmp.ComparisonType == BooleanComparisonType.Equals)
            {
                if (cmp.FirstExpression is ColumnReferenceExpression col1 && 
                    cmp.SecondExpression is VariableReference var2)
                {
                    indexColumn = col1.GetColumnName();
                    seekVariable = var2.Name;
                    return true;
                }
                else if (cmp.FirstExpression is VariableReference var1 &&
                    cmp.SecondExpression is ColumnReferenceExpression col2)
                {
                    indexColumn = col2.GetColumnName();
                    seekVariable = var1.Name;
                    return true;
                }
                else
                {
                    return false;
                }
            }
            else if (filter is BooleanBinaryExpression bin && bin.BinaryExpressionType == BooleanBinaryExpressionType.And)
            {
                if (ExtractKeyLookupFilter(bin.FirstExpression, out remainingFilter, out indexColumn, out seekVariable))
                {
                    if (remainingFilter == null)
                    {
                        remainingFilter = bin.SecondExpression;
                    }
                    else
                    {
                        bin.FirstExpression = remainingFilter;
                        remainingFilter = bin;
                    }

                    return true;
                }
                else if (ExtractKeyLookupFilter(bin.SecondExpression, out remainingFilter, out indexColumn, out seekVariable))
                {
                    if (remainingFilter == null)
                    {
                        remainingFilter = bin.FirstExpression;
                    }
                    else
                    {
                        bin.SecondExpression = remainingFilter;
                        remainingFilter = bin;
                    }

                    return true;
                }
            }

            return false;
        }

        public override void AddRequiredColumns(IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes, IList<string> requiredColumns)
        {
            foreach (var col in Filter.GetColumns())
            {
                if (!requiredColumns.Contains(col, StringComparer.OrdinalIgnoreCase))
                    requiredColumns.Add(col);
            }

            Source.AddRequiredColumns(metadata, parameterTypes, requiredColumns);
        }

        private BooleanExpression ExtractFetchXMLFilters(IAttributeMetadataCache metadata, IQueryExecutionOptions options, BooleanExpression criteria, NodeSchema schema, string allowedPrefix, string targetEntityName, string targetEntityAlias, object[] items, out filter filter)
        {
            if (TranslateFetchXMLCriteria(metadata, options, criteria, schema, allowedPrefix, targetEntityName, targetEntityAlias, items, out filter))
                return null;

            if (!(criteria is BooleanBinaryExpression bin))
                return criteria;

            if (bin.BinaryExpressionType != BooleanBinaryExpressionType.And)
                return criteria;

            bin.FirstExpression = ExtractFetchXMLFilters(metadata, options, bin.FirstExpression, schema, allowedPrefix, targetEntityName, targetEntityAlias, items, out var lhsFilter);
            bin.SecondExpression = ExtractFetchXMLFilters(metadata, options, bin.SecondExpression, schema, allowedPrefix, targetEntityName, targetEntityAlias, items, out var rhsFilter);

            filter = (lhsFilter != null && rhsFilter != null) ? new filter { Items = new object[] { lhsFilter, rhsFilter } } : lhsFilter ?? rhsFilter;

            if (bin.FirstExpression != null && bin.SecondExpression != null)
                return bin;

            return bin.FirstExpression ?? bin.SecondExpression;
        }

        protected BooleanExpression ExtractMetadataFilters(BooleanExpression criteria, MetadataQueryNode meta, out MetadataFilterExpression entityFilter, out MetadataFilterExpression attributeFilter, out MetadataFilterExpression relationshipFilter)
        {
            if (TranslateMetadataCriteria(criteria, meta, out entityFilter, out attributeFilter, out relationshipFilter))
                return null;

            if (!(criteria is BooleanBinaryExpression bin))
                return criteria;

            if (bin.BinaryExpressionType != BooleanBinaryExpressionType.And)
                return criteria;

            bin.FirstExpression = ExtractMetadataFilters(bin.FirstExpression, meta, out var lhsEntityFilter, out var lhsAttributeFilter, out var lhsRelationshipFilter);
            bin.SecondExpression = ExtractMetadataFilters(bin.SecondExpression, meta, out var rhsEntityFilter, out var rhsAttributeFilter, out var rhsRelationshipFilter);

            entityFilter = (lhsEntityFilter != null && rhsEntityFilter != null) ? new MetadataFilterExpression { Filters = { lhsEntityFilter, rhsEntityFilter } } : lhsEntityFilter ?? rhsEntityFilter;
            attributeFilter = (lhsAttributeFilter != null && rhsAttributeFilter != null) ? new MetadataFilterExpression { Filters = { lhsAttributeFilter, rhsAttributeFilter } } : lhsAttributeFilter ?? rhsAttributeFilter;
            relationshipFilter = (lhsRelationshipFilter != null && rhsRelationshipFilter != null) ? new MetadataFilterExpression { Filters = { lhsRelationshipFilter, rhsRelationshipFilter } } : lhsRelationshipFilter ?? rhsRelationshipFilter;

            if (bin.FirstExpression != null && bin.SecondExpression != null)
                return bin;

            return bin.FirstExpression ?? bin.SecondExpression;
        }

        public override int EstimateRowsOut(IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes, ITableSizeCache tableSize)
        {
            return Source.EstimateRowsOut(metadata, parameterTypes, tableSize) * 8 / 10;
        }
    }
}
