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
        public IDataExecutionPlanNodeInternal Source { get; set; }

        protected override IEnumerable<Entity> ExecuteInternal(NodeExecutionContext context)
        {
            var source = Source.Execute(context);
            var schema = GetSchema(context);
            var expressionCompilationContext = new ExpressionCompilationContext(context, schema, null);
            var expressionExecutionContext = new ExpressionExecutionContext(context);
            var expressions = Sorts.Select(sort => sort.Expression.Compile(expressionCompilationContext)).ToList();

            if (PresortedCount == 0)
            {
                // We haven't been able to fold any of the sort orders down to the source, so we need to apply
                // them all again here
                IOrderedEnumerable<Entity> sortedSource;

                if (Sorts[0].SortOrder == SortOrder.Descending)
                    sortedSource = source.OrderByDescending(e => { expressionExecutionContext.Entity = e; return expressions[0](expressionExecutionContext); });
                else
                    sortedSource = source.OrderBy(e => { expressionExecutionContext.Entity = e; return expressions[0](expressionExecutionContext); });

                for (var i = 1; i < Sorts.Count; i++)
                {
                    var expr = expressions[i];

                    if (Sorts[i].SortOrder == SortOrder.Descending)
                        sortedSource = sortedSource.ThenByDescending(e => { expressionExecutionContext.Entity = e; return expr(expressionExecutionContext); });
                    else
                        sortedSource = sortedSource.ThenBy(e => { expressionExecutionContext.Entity = e; return expr(expressionExecutionContext); });
                }

                foreach (var entity in sortedSource)
                    yield return entity;
            }
            else
            {
                // We have managed to fold some but not all of the sorts down to the source. Take records
                // from the source that have equal values of the sorts that have been folded, then sort those
                // subsets individually on the remaining sorts
                var preSortedColumns = Sorts
                    .Take(PresortedCount)
                    .Select(s => { schema.ContainsColumn(((ColumnReferenceExpression)s.Expression).GetColumnName(), out var colName); return colName; })
                    .ToList();

                var subset = new List<Entity>();
                var comparer = new DistinctEqualityComparer(preSortedColumns);

                foreach (var next in source)
                {
                    expressionExecutionContext.Entity = next;

                    // Get the other values to sort on from this next record
                    var nextSortedValues = expressions
                        .Take(PresortedCount)
                        .Select(expr => expr(expressionExecutionContext))
                        .ToList();

                    // If we've already got a subset to work on, check if this fits in the same subset
                    if (subset.Count > 0 && !comparer.Equals(subset[0], next))
                    {
                        // A value is different, so this record doesn't fit in the same subset. Sort the subset
                        // by the remaining sorts and return the values from it
                        SortSubset(subset, expressionExecutionContext, expressions);

                        foreach (var entity in subset)
                            yield return entity;

                        // Now clear out the previous subset so we can move on to the next
                        subset.Clear();
                    }

                    subset.Add(next);
                }

                // Sort and return the final subset
                SortSubset(subset, expressionExecutionContext, expressions);

                foreach (var entity in subset)
                    yield return entity;
            }
        }

        private void SortSubset(List<Entity> subset, ExpressionExecutionContext context, List<Func<ExpressionExecutionContext, object>> expressions)
        {
            // Simple case if there's no need to do any further sorting
            if (subset.Count <= 1)
                return;

            // Precalculate the sort keys for the remaining sorts
            var sortKeys = subset
                .ToDictionary(
                    entity => entity,
                    entity => expressions
                        .Skip(PresortedCount)
                        .Select(expr => { context.Entity = entity; return expr(context); })
                        .ToList()
                );

            // Sort the list according to these sort keys
            subset.Sort((x, y) =>
            {
                var xValues = sortKeys[x];
                var yValues = sortKeys[y];

                for (var i = 0; i < Sorts.Count - PresortedCount; i++)
                {
                    var comparison = ((IComparable)xValues[i]).CompareTo(yValues[i]);

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

        public override INodeSchema GetSchema(NodeCompilationContext context)
        {
            var schema = new NodeSchema(Source.GetSchema(context));
            var sortOrder = new List<string>();

            foreach (var sort in Sorts)
            {
                if (!(sort.Expression is ColumnReferenceExpression col))
                    return schema;

                if (!schema.ContainsColumn(col.GetColumnName(), out var colName))
                    return schema;

                sortOrder.Add(colName);
            }

            return new NodeSchema(
                primaryKey: schema.PrimaryKey,
                schema: schema.Schema,
                aliases: schema.Aliases,
                sortOrder: sortOrder);
        }

        public override IDataExecutionPlanNodeInternal FoldQuery(NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            Source = Source.FoldQuery(context, hints);

            // These sorts will override any previous sort
            if (Source is SortNode prevSort)
                Source = prevSort.Source;

            Source.Parent = this;

            return FoldSorts(context);
        }

        private IDataExecutionPlanNodeInternal FoldSorts(NodeCompilationContext context)
        {
            PresortedCount = 0;

            var tryCatch = Source as TryCatchNode;
            
            while (tryCatch?.TrySource is TryCatchNode subTry)
                tryCatch = subTry;

            if (tryCatch != null && tryCatch.TrySource is FetchXmlScan tryFetch && tryFetch.FetchXml.aggregate)
            {
                // We're sorting on the results of an aggregate that will try to go as a single FetchXML request first, then to a separate plan
                // if that fails. Try to fold the sorts in to the aggregate FetchXML first
                var fetchAggregateSort = new SortNode { Source = tryFetch };
                fetchAggregateSort.Sorts.AddRange(Sorts);

                var sortedFetchResult = fetchAggregateSort.FoldSorts(context);

                // If we managed to fold any of the sorts in to the FetchXML, do the same for the non-FetchXML version and remove this node
                if (sortedFetchResult == tryFetch || (sortedFetchResult == fetchAggregateSort && fetchAggregateSort.PresortedCount > 0))
                {
                    tryCatch.TrySource = sortedFetchResult;
                    sortedFetchResult.Parent = tryCatch;

                    while (tryCatch != null)
                    {
                        var nonFetchAggregateSort = new SortNode { Source = tryCatch.CatchSource };
                        nonFetchAggregateSort.Sorts.AddRange(Sorts);

                        var sortedNonFetchResult = nonFetchAggregateSort.FoldSorts(context);
                        tryCatch.CatchSource = sortedNonFetchResult;
                        sortedNonFetchResult.Parent = tryCatch;

                        tryCatch = tryCatch.Parent as TryCatchNode;
                    }

                    return Source;
                }
            }

            // Allow folding sorts around filters and Compute Scalar (so long as sort is not on a calculated field)
            // Can fold to the outer input of a nested loop join
            var source = Source;
            var fetchXml = Source as FetchXmlScan;

            while (source != null && fetchXml == null)
            {
                if (source is FilterNode filter)
                    source = filter.Source;
                else if (source is ComputeScalarNode computeScalar)
                    source = computeScalar.Source;
                else if (source is NestedLoopNode nestedLoop)
                    source = nestedLoop.LeftSource;
                else
                    break;

                fetchXml = source as FetchXmlScan;
            }

            var canFold = fetchXml != null;
            DataSource dataSource = null;

            if (canFold && fetchXml.RequiresCustomPaging(context.DataSources))
                canFold = false;

            if (canFold && !context.DataSources.TryGetValue(fetchXml.DataSource, out dataSource))
                throw new QueryExecutionException("Missing datasource " + fetchXml.DataSource);

            if (canFold && fetchXml.FetchXml.aggregate && (fetchXml.Entity.name == "audit" || dataSource.Metadata[fetchXml.Entity.name].DataProviderId == DataProviders.ElasticDataProvider))
                canFold = false;

            if (canFold)
            {
                fetchXml.RemoveSorts();

                var fetchSchema = fetchXml.GetSchema(context);
                var entity = fetchXml.Entity;
                var items = entity.Items;

                foreach (var sortOrder in Sorts)
                {
                    if (!(sortOrder.Expression is ColumnReferenceExpression sortColRef))
                        return this;

                    if (!fetchSchema.ContainsColumn(sortColRef.GetColumnName(), out var sortCol))
                        return this;

                    var parts = sortCol.SplitMultiPartIdentifier();
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

                    var fetchSort = new FetchOrderType { attribute = attrName, descending = sortOrder.SortOrder == SortOrder.Descending };

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

                            // Sorting on multi-select picklist fields isn't supported in FetchXML
                            if (attribute is MultiSelectPicklistAttributeMetadata)
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

                            // Sorting on multi-select picklist fields isn't supported in FetchXML
                            if (attribute is MultiSelectPicklistAttributeMetadata)
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

                            var expressionCompilationContext = new ExpressionCompilationContext(context, null, null);

                            if (top != null)
                            {
                                if (!top.Top.IsConstantValueExpression(expressionCompilationContext, out var topLiteral))
                                    return this;

                                if (Int32.Parse(topLiteral.Value, CultureInfo.InvariantCulture) > 50000)
                                    return this;
                            }
                            else if (offset != null)
                            {
                                if (!offset.Offset.IsConstantValueExpression(expressionCompilationContext, out var offsetLiteral) ||
                                    !offset.Fetch.IsConstantValueExpression(expressionCompilationContext, out var fetchLiteral))
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

            // Check if the data is already sorted by any prefix of our sorts
            var schema = Source.GetSchema(context);

            for (var i = 0; i < Sorts.Count && i < schema.SortOrder.Count; i++)
            {
                if (!(Sorts[i].Expression is ColumnReferenceExpression col))
                    return this;

                if (!schema.ContainsColumn(col.GetColumnName(), out var colName))
                    return this;

                if (!schema.SortOrder[i].Equals(colName, StringComparison.OrdinalIgnoreCase))
                    return this;

                PresortedCount++;
            }

            if (PresortedCount == Sorts.Count)
                return Source;

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

        public override void AddRequiredColumns(NodeCompilationContext context, IList<string> requiredColumns)
        {
            var sortColumns = Sorts.SelectMany(s => s.Expression.GetColumns()).Distinct();

            foreach (var col in sortColumns)
            {
                if (!requiredColumns.Contains(col, StringComparer.OrdinalIgnoreCase))
                    requiredColumns.Add(col);
            }

            Source.AddRequiredColumns(context, requiredColumns);
        }

        protected override RowCountEstimate EstimateRowsOutInternal(NodeCompilationContext context)
        {
            return Source.EstimateRowsOut(context);
        }

        protected override IEnumerable<string> GetVariablesInternal()
        {
            return Sorts
                .SelectMany(sort => sort.GetVariables())
                .Distinct();
        }

        public override object Clone()
        {
            var clone = new SortNode
            {
                PresortedCount = PresortedCount,
                Source = (IDataExecutionPlanNodeInternal)Source.Clone()
            };

            clone.Sorts.AddRange(Sorts);
            clone.Source.Parent = clone;

            return clone;
        }
    }
}
