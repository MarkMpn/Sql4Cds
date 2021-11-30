using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlTypes;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.Engine.FetchXml;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Metadata;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// Sorts the data in the data stream
    /// </summary>
    class SortNode : BaseDataNode, ISingleSourceExecutionPlanNode
    {
        /// <summary>
        /// The sorts to apply
        /// </summary>
        [Category("Sort")]
        [Description("The sorts to apply")]
        public List<ExpressionWithSortOrder> Sorts { get; } = new List<ExpressionWithSortOrder>();

        /// <summary>
        /// The number of <see cref="Sorts"/> that the input is already sorted by
        /// </summary>
        [Category("Sort")]
        [Description("The number of sorts that the input is already sorted by")]
        [DisplayName("Pre-sorted Count")]
        public int PresortedCount { get; set; }

        /// <summary>
        /// The data source to sort
        /// </summary>
        [Browsable(false)]
        public IDataExecutionPlanNode Source { get; set; }

        protected override IEnumerable<Entity> ExecuteInternal(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues)
        {
            var source = Source.Execute(dataSources, options, parameterTypes, parameterValues);
            var schema = GetSchema(dataSources, parameterTypes);
            var expressions = Sorts.Select(sort => sort.Expression.Compile(schema, parameterTypes)).ToList();

            if (PresortedCount == 0)
            {
                // We haven't been able to fold any of the sort orders down to the source, so we need to apply
                // them all again here
                IOrderedEnumerable<Entity> sortedSource;

                if (Sorts[0].SortOrder == SortOrder.Descending)
                    sortedSource = source.OrderByDescending(e => expressions[0](e, parameterValues, options), CaseInsensitiveObjectComparer.Instance);
                else
                    sortedSource = source.OrderBy(e => expressions[0](e, parameterValues, options), CaseInsensitiveObjectComparer.Instance);

                for (var i = 1; i < Sorts.Count; i++)
                {
                    var expr = expressions[i];

                    if (Sorts[i].SortOrder == SortOrder.Descending)
                        sortedSource = sortedSource.ThenByDescending(e => expr(e, parameterValues, options), CaseInsensitiveObjectComparer.Instance);
                    else
                        sortedSource = sortedSource.ThenBy(e => expr(e, parameterValues, options), CaseInsensitiveObjectComparer.Instance);
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
                    var nextSortedValues = expressions
                        .Take(PresortedCount)
                        .Select(expr => expr(next, parameterValues, options))
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
                                SortSubset(subset, schema, parameterTypes, parameterValues, expressions, options);

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
                SortSubset(subset, schema, parameterTypes, parameterValues, expressions, options);

                foreach (var entity in subset)
                    yield return entity;
            }
        }

        private void SortSubset(List<Entity> subset, NodeSchema schema, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues, List<Func<Entity, IDictionary<string, object>, IQueryExecutionOptions, object>> expressions, IQueryExecutionOptions options)
        {
            // Simple case if there's no need to do any further sorting
            if (subset.Count <= 1)
                return;

            // Precalculate the sort keys for the remaining sorts
            var sortKeys = subset
                .ToDictionary(entity => entity, entity => expressions.Skip(PresortedCount).Select(expr => expr(entity, parameterValues, options)).ToList());

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

        public override NodeSchema GetSchema(IDictionary<string, DataSource> dataSources, IDictionary<string, Type> parameterTypes)
        {
            return Source.GetSchema(dataSources, parameterTypes);
        }

        public override IDataExecutionPlanNode FoldQuery(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes)
        {
            Source = Source.FoldQuery(dataSources, options, parameterTypes);

            // These sorts will override any previous sort
            if (Source is SortNode prevSort)
                Source = prevSort.Source;

            Source.Parent = this;

            return FoldSorts(dataSources, options, parameterTypes);
        }

        private IDataExecutionPlanNode FoldSorts(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes)
        {
            if (Source is TryCatchNode tryCatch && tryCatch.TrySource is FetchXmlScan tryFetch && tryFetch.FetchXml.aggregate)
            {
                // We're sorting on the results of an aggregate that will try to go as a single FetchXML request first, then to a separate plan
                // if that fails. Try to fold the sorts in to the aggregate FetchXML first
                var fetchAggregateSort = new SortNode { Source = tryFetch };
                fetchAggregateSort.Sorts.AddRange(Sorts);

                var sortedFetchResult = fetchAggregateSort.FoldSorts(dataSources, options, parameterTypes);

                // If we managed to fold any of the sorts in to the FetchXML, do the same for the non-FetchXML version and remove this node
                if (sortedFetchResult == tryFetch || (sortedFetchResult == fetchAggregateSort && fetchAggregateSort.PresortedCount > 0))
                {
                    tryCatch.TrySource = sortedFetchResult;
                    sortedFetchResult.Parent = tryCatch;

                    var nonFetchAggregateSort = new SortNode { Source = tryCatch.CatchSource };
                    nonFetchAggregateSort.Sorts.AddRange(Sorts);

                    var sortedNonFetchResult = nonFetchAggregateSort.FoldSorts(dataSources, options, parameterTypes);
                    tryCatch.CatchSource = sortedNonFetchResult;
                    sortedNonFetchResult.Parent = tryCatch;
                    return tryCatch;
                }
            }

            // Allow folding sorts around filters
            var fetchXml = Source as FetchXmlScan;

            if (fetchXml == null && Source is FilterNode filter)
                fetchXml = filter.Source as FetchXmlScan;

            if (fetchXml != null)
            {
                if (!dataSources.TryGetValue(fetchXml.DataSource, out var dataSource))
                    throw new QueryExecutionException("Missing datasource " + fetchXml.DataSource);

                // Remove any existing sorts
                if (fetchXml.Entity.Items != null)
                {
                    fetchXml.Entity.Items = fetchXml.Entity.Items.Where(i => !(i is FetchOrderType)).ToArray();

                    foreach (var linkEntity in fetchXml.Entity.GetLinkEntities().Where(le => le.Items != null))
                        linkEntity.Items = linkEntity.Items.Where(i => !(i is FetchOrderType)).ToArray();
                }

                var schema = Source.GetSchema(dataSources, parameterTypes);
                var entity = fetchXml.Entity;
                var items = entity.Items;

                foreach (var sortOrder in Sorts)
                {
                    if (!(sortOrder.Expression is ColumnReferenceExpression sortColRef))
                        return this;

                    if (!schema.ContainsColumn(sortColRef.GetColumnName(), out var sortCol))
                        return this;

                    var parts = sortCol.Split('.');
                    string entityName;
                    string attrName;

                    if (parts.Length == 2)
                    {
                        entityName = parts[0];
                        attrName = parts[1];
                    }
                    else
                    {
                        attrName = parts[0];
                        entityName = FindEntityWithAttributeAlias(fetchXml, attrName);
                    }

                    var fetchSort = new FetchOrderType { attribute = attrName.ToLowerInvariant(), descending = sortOrder.SortOrder == SortOrder.Descending };

                    if (fetchXml.FetchXml.aggregate)
                    {
                        fetchSort.alias = fetchSort.attribute;
                        fetchSort.attribute = null;
                    }

                    if (entityName == fetchXml.Alias)
                    {
                        if (items != entity.Items)
                            return this;

                        if (fetchSort.attribute != null)
                        {
                            var meta = dataSource.Metadata[entity.name];
                            var attribute = meta.Attributes.SingleOrDefault(a => a.LogicalName == fetchSort.attribute && a.AttributeOf == null);

                            // Sorting on a lookup Guid column actually sorts by the associated name field, which isn't what we want
                            if (attribute is LookupAttributeMetadata || attribute is EnumAttributeMetadata || attribute is BooleanAttributeMetadata)
                                return this;

                            // Sorts on the virtual ___name attribute should be applied to the underlying field
                            if (attribute == null && fetchSort.attribute.EndsWith("name") == true)
                            {
                                attribute = meta.Attributes.SingleOrDefault(a => a.LogicalName == fetchSort.attribute.Substring(0, fetchSort.attribute.Length - 4) && a.AttributeOf == null);

                                if (attribute != null)
                                    fetchSort.attribute = attribute.LogicalName;
                            }

                            if (attribute == null)
                                return this;
                        }

                        entity.AddItem(fetchSort);
                        items = entity.Items;
                    }
                    else
                    {
                        var linkEntity = FetchXmlExtensions.FindLinkEntity(items, entityName);
                        if (linkEntity == null)
                            return this;

                        if (fetchSort.attribute != null)
                        {
                            var meta = dataSource.Metadata[linkEntity.name];
                            var attribute = meta.Attributes.SingleOrDefault(a => a.LogicalName == fetchSort.attribute && a.AttributeOf == null);

                            // Sorting on a lookup Guid column actually sorts by the associated name field, which isn't what we want
                            if (attribute is LookupAttributeMetadata || attribute is EnumAttributeMetadata || attribute is BooleanAttributeMetadata)
                                return this;

                            // Sorts on the virtual ___name attribute should be applied to the underlying field
                            if (attribute == null && fetchSort.attribute.EndsWith("name") == true)
                            {
                                attribute = meta.Attributes.SingleOrDefault(a => a.LogicalName == fetchSort.attribute.Substring(0, fetchSort.attribute.Length - 4) && a.AttributeOf == null);

                                if (attribute != null)
                                    fetchSort.attribute = attribute.LogicalName;
                            }

                            if (attribute == null)
                                return this;
                        }

                        // Adding sorts to link-entity forces legacy paging which has a maximum record limit of 50K.
                        // Don't add a sort to a link-entity unless there's also a TOP clause of <= 50K
                        // Doesn't apply to aggregate queries as they can't be paged
                        if (!fetchXml.FetchXml.aggregate)
                        {
                            var top = Parent as TopNode;
                            var offset = Parent as OffsetFetchNode;

                            if (top == null && offset == null)
                                return this;

                            if (top != null)
                            {
                                if (!top.Top.IsConstantValueExpression(null, options, out var topLiteral))
                                    return this;

                                if (Int32.Parse(topLiteral.Value, CultureInfo.InvariantCulture) > 50000)
                                    return this;
                            }
                            else if (offset != null)
                            {
                                if (!offset.Offset.IsConstantValueExpression(null, options, out var offsetLiteral) ||
                                    !offset.Fetch.IsConstantValueExpression(null, options, out var fetchLiteral))
                                    return this;

                                if (Int32.Parse(offsetLiteral.Value, CultureInfo.InvariantCulture) + Int32.Parse(fetchLiteral.Value, CultureInfo.InvariantCulture) > 50000)
                                    return this;
                            }
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

        private string FindEntityWithAttributeAlias(FetchXmlScan fetchXml, string attrName)
        {
            return FindEntityWithAttributeAlias(fetchXml.Alias, fetchXml.Entity.Items, attrName);
        }

        private string FindEntityWithAttributeAlias(string alias, object[] items, string attrName)
        {
            if (items == null)
                return null;

            if (items.OfType<FetchAttributeType>().Any(a => a.alias != null && a.alias.Equals(attrName, StringComparison.OrdinalIgnoreCase)))
                return alias;

            foreach (var linkEntity in items.OfType<FetchLinkEntityType>())
            {
                var entityName = FindEntityWithAttributeAlias(linkEntity.alias, linkEntity.Items, attrName);

                if (entityName != null)
                    return entityName;
            }

            return null;
        }

        public override void AddRequiredColumns(IDictionary<string, DataSource> dataSources, IDictionary<string, Type> parameterTypes, IList<string> requiredColumns)
        {
            var sortColumns = Sorts.SelectMany(s => s.Expression.GetColumns()).Distinct();

            foreach (var col in sortColumns)
            {
                if (!requiredColumns.Contains(col, StringComparer.OrdinalIgnoreCase))
                    requiredColumns.Add(col);
            }

            Source.AddRequiredColumns(dataSources, parameterTypes, requiredColumns);
        }

        public override int EstimateRowsOut(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes)
        {
            return Source.EstimateRowsOut(dataSources, options, parameterTypes);
        }
    }
}
