using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.Engine.FetchXml;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// Sorts the data in the data stream
    /// </summary>
    public class SortNode : BaseNode, ISingleSourceExecutionPlanNode
    {
        /// <summary>
        /// The sorts to apply
        /// </summary>
        public List<ExpressionWithSortOrder> Sorts { get; } = new List<ExpressionWithSortOrder>();

        /// <summary>
        /// The number of <see cref="Sorts"/> that the input is already sorted by
        /// </summary>
        public int PresortedCount { get; set; }

        /// <summary>
        /// The data source to sort
        /// </summary>
        public IExecutionPlanNode Source { get; set; }

        protected override IEnumerable<Entity> ExecuteInternal(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues)
        {
            var source = Source.Execute(org, metadata, options, parameterTypes, parameterValues);
            var schema = GetSchema(metadata, parameterTypes);

            if (PresortedCount == 0)
            {
                // We haven't been able to fold any of the sort orders down to the source, so we need to apply
                // them all again here
                IOrderedEnumerable<Entity> sortedSource;

                if (Sorts[0].SortOrder == SortOrder.Descending)
                    sortedSource = source.OrderByDescending(e => Sorts[0].Expression.GetValue(e, schema, parameterTypes, parameterValues), CaseInsensitiveObjectComparer.Instance);
                else
                    sortedSource = source.OrderBy(e => Sorts[0].Expression.GetValue(e, schema, parameterTypes, parameterValues), CaseInsensitiveObjectComparer.Instance);

                foreach (var sort in Sorts.Skip(1))
                {
                    if (sort.SortOrder == SortOrder.Descending)
                        sortedSource = sortedSource.ThenByDescending(e => sort.Expression.GetValue(e, schema, parameterTypes, parameterValues), CaseInsensitiveObjectComparer.Instance);
                    else
                        sortedSource = sortedSource.ThenBy(e => sort.Expression.GetValue(e, schema, parameterTypes, parameterValues), CaseInsensitiveObjectComparer.Instance);
                }

                foreach (var entity in sortedSource)
                    yield return entity;
            }
            else
            {
                // We have managed to fold some but not all of the sorts down to the source. Take records
                // from the source that have equal values of the sorts that have been folded, then sort those
                // subsets individually on the remaining sorts
                var subset = new List<Entity>();
                var presortedValues = new List<object>(PresortedCount);
                IEqualityComparer<object> comparer = CaseInsensitiveObjectComparer.Instance;

                foreach (var next in source)
                {
                    // Get the other values to sort on from this next record
                    var nextSortedValues = Sorts
                        .Take(PresortedCount)
                        .Select(sort => sort.Expression.GetValue(next, schema, parameterTypes, parameterValues))
                        .ToList();

                    // If we've already got a subset to work on, check if this fits in the same subset
                    if (subset.Count > 0)
                    {
                        for (var i = 0; i < PresortedCount; i++)
                        {
                            var prevValue = presortedValues[i];
                            var nextValue = nextSortedValues[i];

                            if (prevValue == null ^ nextValue == null ||
                                !comparer.Equals(prevValue, nextValue))
                            {
                                // A value is different, so this record doesn't fit in the same subset. Sort the subset
                                // by the remaining sorts and return the values from it
                                SortSubset(subset, schema, parameterTypes, parameterValues);

                                foreach (var entity in subset)
                                    yield return entity;

                                // Now clear out the previous subset so we can move on to the next
                                subset.Clear();
                                presortedValues.Clear();
                                break;
                            }
                        }
                    }

                    if (subset.Count == 0)
                        presortedValues.AddRange(nextSortedValues);

                    subset.Add(next);
                }

                // Sort and return the final subset
                SortSubset(subset, schema, parameterTypes, parameterValues);

                foreach (var entity in subset)
                    yield return entity;
            }
        }

        private void SortSubset(List<Entity> subset, NodeSchema schema, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues)
        {
            // Simple case if there's no need to do any further sorting
            if (subset.Count <= 1)
                return;

            // Precalculate the sort keys for the remaining sorts
            var sortKeys = subset
                .ToDictionary(entity => entity, entity => Sorts.Skip(PresortedCount).Select(sort => sort.Expression.GetValue(entity, schema, parameterTypes, parameterValues)).ToList());

            // Sort the list according to these sort keys
            subset.Sort((x, y) =>
            {
                var xValues = sortKeys[x];
                var yValues = sortKeys[y];

                for (var i = 0; i < Sorts.Count - PresortedCount; i++)
                {
                    var comparison = CaseInsensitiveObjectComparer.Instance.Compare(xValues[i], yValues[i]);

                    if (comparison == 0)
                        continue;

                    if (Sorts[PresortedCount + i].SortOrder == SortOrder.Descending)
                        return -comparison;

                    return comparison;
                }

                return 0;
            });
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            yield return Source;
        }

        public override NodeSchema GetSchema(IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes)
        {
            return Source.GetSchema(metadata, parameterTypes);
        }

        public override IExecutionPlanNode FoldQuery(IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes)
        {
            Source = Source.FoldQuery(metadata, options, parameterTypes);
            Source.Parent = this;

            if (Source is FetchXmlScan fetchXml)
            {
                // Remove any existing sorts
                if (fetchXml.Entity.Items != null)
                {
                    fetchXml.Entity.Items = fetchXml.Entity.Items.Where(i => !(i is FetchOrderType)).ToArray();

                    foreach (var linkEntity in fetchXml.Entity.GetLinkEntities().Where(le => le.Items != null))
                        linkEntity.Items = linkEntity.Items.Where(i => !(i is FetchOrderType)).ToArray();
                }

                var schema = Source.GetSchema(metadata, parameterTypes);
                var entity = fetchXml.Entity;
                var items = entity.Items;

                foreach (var sortOrder in Sorts)
                {
                    if (!(sortOrder.Expression is ColumnReferenceExpression sortColRef))
                        return this;

                    if (!schema.ContainsColumn(sortColRef.GetColumnName(), out var sortCol))
                        return this;

                    var parts = sortCol.Split('.');
                    var entityName = parts[0];
                    var attrName = parts[1];

                    var fetchSort = new FetchOrderType { attribute = attrName, descending = sortOrder.SortOrder == SortOrder.Descending };
                    if (entityName == fetchXml.Alias)
                    {
                        if (items != entity.Items)
                            return this;

                        entity.AddItem(fetchSort);
                        items = entity.Items;
                    }
                    else
                    {
                        var linkEntity = FetchXmlExtensions.FindLinkEntity(items, entityName);
                        if (linkEntity == null)
                            return this;

                        // Adding sorts to link-entity forces legacy paging which has a maximum record limit of 50K.
                        // Don't add a sort to a link-entity unless there's also a TOP clause of <= 50K
                        var top = Parent as TopNode;
                        var offset = Parent as OffsetFetchNode;

                        if (top == null && offset == null)
                            return this;

                        if (top != null)
                        {
                            if (!IsConstantValueExpression(top.Top, null, out var topLiteral))
                                return this;

                            if (Int32.Parse(topLiteral.Value) > 50000)
                                return this;
                        }
                        else if (offset != null)
                        {
                            if (!IsConstantValueExpression(offset.Offset, null, out var offsetLiteral) ||
                                !IsConstantValueExpression(offset.Fetch, null, out var fetchLiteral))
                                return this;

                            if (Int32.Parse(offsetLiteral.Value) + Int32.Parse(fetchLiteral.Value) > 50000)
                                return this;
                        }

                        linkEntity.AddItem(fetchSort);
                        items = linkEntity.Items;
                    }

                    PresortedCount++;
                }

                return Source;
            }

            return this;
        }

        public override void AddRequiredColumns(IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes, IList<string> requiredColumns)
        {
            var sortColumns = Sorts.SelectMany(s => s.Expression.GetColumns()).Distinct();

            foreach (var col in sortColumns)
            {
                if (!requiredColumns.Contains(col, StringComparer.OrdinalIgnoreCase))
                    requiredColumns.Add(col);
            }

            Source.AddRequiredColumns(metadata, parameterTypes, requiredColumns);
        }

        public override int EstimateRowsOut(IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes, ITableSizeCache tableSize)
        {
            return Source.EstimateRowsOut(metadata, parameterTypes, tableSize);
        }
    }
}
