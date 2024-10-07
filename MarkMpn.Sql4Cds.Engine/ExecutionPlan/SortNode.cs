using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlTypes;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.Engine.FetchXml;
using Microsoft.Crm.Sdk.Messages;
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
            // Can fold to the outer input of a nested loop join and sources of spools
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
                else if (source is TableSpoolNode tableSpool)
                    source = tableSpool.Source;
                else if (source is IndexSpoolNode indexSpool)
                    source = indexSpool.Source;
                else
                    break;

                fetchXml = source as FetchXmlScan;
            }

            var canFold = fetchXml != null;
            DataSource dataSource = null;

            if (canFold && fetchXml.RequiresCustomPaging(context.Session.DataSources))
                canFold = false;

            if (canFold && !context.Session.DataSources.TryGetValue(fetchXml.DataSource, out dataSource))
                throw new QueryExecutionException("Missing datasource " + fetchXml.DataSource);

            var isAuditOrElastic = fetchXml != null && (fetchXml.Entity.name == "audit" || dataSource != null && dataSource.Metadata[fetchXml.Entity.name].DataProviderId == DataProviders.ElasticDataProvider);

            if (canFold && fetchXml.FetchXml.aggregate && isAuditOrElastic)
                canFold = false;

            if (canFold)
            {
                fetchXml.RemoveSorts();

                // Validate whether the sort orders are applied in a valid order to be added directly to the <entity> and <link-entity>
                // elements, or if we need to use the <order entityname> attribute
                var entityOrder = GetEntityOrder(fetchXml);
                var fetchSchema = fetchXml.GetSchema(context);
                var validOrder = true;
                var currentEntity = 0;
                bool? useRawOrderBy = null;

                foreach (var sortOrder in Sorts)
                {
                    if (!(sortOrder.Expression is ColumnReferenceExpression sortColRef))
                        return this;

                    if (!fetchSchema.ContainsColumn(sortColRef.GetColumnName(), out var sortCol))
                        return this;

                    // Elastic tables can only be sorted by a single field unless a compound key has been manually created
                    // Audit table doesn't throw the same error, but ignores subsequent sorts
                    if (PresortedCount == 1 && isAuditOrElastic)
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

                    if (validOrder)
                    {
                        var entityIndex = entityOrder.IndexOf(entityName);
                        if (entityIndex < currentEntity)
                        {
                            validOrder = false;

                            // We've already added sorts to a subsequent link-entity. We can only add sorts to a previous entity if
                            // we support the <order entityname> attribute
                            if (!dataSource.OrderByEntityNameAvailable)
                                return this;

                            // Existing orders on link-entities need to be moved to the root entity and have their entityname attribute populated
                            foreach (var linkEntity in fetchXml.Entity.GetLinkEntities())
                            {
                                foreach (var sort in linkEntity.Items?.OfType<FetchOrderType>()?.ToArray() ?? Array.Empty<FetchOrderType>())
                                {
                                    sort.entityname = linkEntity.alias;
                                    linkEntity.Items = linkEntity.Items.Except(new[] { sort }).ToArray();
                                    fetchXml.Entity.AddItem(sort);
                                }
                            }
                        }
                        else
                        {
                            currentEntity = entityIndex;
                        }
                    }

                    if (entityName == fetchXml.Alias)
                    {
                        var attributeName = fetchSort.attribute;
                        if (fetchSort.alias != null)
                        {
                            var fetchAttribute = fetchXml.Entity.Items.OfType<FetchAttributeType>().Where(a => a.alias == fetchSort.alias).FirstOrDefault();

                            if (fetchAttribute != null)
                            {
                                attributeName = fetchAttribute.name;

                                if (fetchAttribute.groupbySpecified && fetchAttribute.groupby == FetchBoolType.@true &&
                                    fetchSort.descending)
                                {
                                    // Sorts on groupby columns always seem to be in ascending order, descending flag is ignored
                                    return this;
                                }
                            }
                        }

                        if (attributeName != null)
                        {
                            var meta = dataSource.Metadata[fetchXml.Entity.name];
                            var attribute = meta.Attributes.SingleOrDefault(a => a.LogicalName == attributeName && a.AttributeOf == null);

                            // Sorting on multi-select picklist fields isn't supported in FetchXML
                            if (attribute is MultiSelectPicklistAttributeMetadata)
                                return this;

                            if (isAuditOrElastic)
                            {
                                // Different logic applies to sorting on Cosmos DB backed tables:
                                // Sorting on guids is done in string order, not guid order, so don't fold them
                                if (attribute is LookupAttributeMetadata || attribute?.AttributeType == AttributeTypeCode.Uniqueidentifier)
                                    return this;

                                // Sorts on picklist columns are done on the underlying value, not the name, so we can fold them as normal

                                // We can't sort on some columns in the audit table
                                if (fetchXml.Entity.name == "audit" && attribute?.IsValidForAdvancedFind?.Value == false)
                                    return this;
                            }
                            else
                            {
                                // Sorting on a lookup Guid and picklist column actually sorts by the associated name field, which isn't what we want
                                // Picklist sorting can be controlled by the useraworderby flag though.
                                if (attribute is LookupAttributeMetadata)
                                    return this;
                                
                                if (attribute is EnumAttributeMetadata || attribute is BooleanAttributeMetadata)
                                {
                                    if (useRawOrderBy == false || !dataSource.UseRawOrderByReliable)
                                        return this;

                                    useRawOrderBy = true;
                                }

                                // Sorts on the virtual ___name attribute should be applied to the underlying field
                                if (attribute == null && attributeName.EndsWith("name") == true)
                                {
                                    attribute = meta.Attributes.SingleOrDefault(a => a.LogicalName == attributeName.Substring(0, fetchSort.attribute.Length - 4) && a.AttributeOf == null);

                                    if (attribute != null)
                                    {
                                        if (fetchSort.attribute == null)
                                            return this;

                                        fetchSort.attribute = attribute.LogicalName;

                                        if (attribute is EnumAttributeMetadata || attribute is BooleanAttributeMetadata)
                                        {
                                            if (useRawOrderBy == true)
                                                return this;

                                            useRawOrderBy = false;
                                        }
                                    }
                                }
                            }

                            if (attribute == null)
                                return this;
                        }

                        fetchXml.Entity.AddItem(fetchSort);
                    }
                    else
                    {
                        var linkEntity = fetchXml.Entity.FindLinkEntity(entityName);
                        if (linkEntity == null)
                            return this;

                        var attributeName = fetchSort.attribute;
                        if (fetchSort.alias != null)
                        {
                            var fetchAttribute = linkEntity.Items.OfType<FetchAttributeType>().Where(a => a.alias == fetchSort.alias).FirstOrDefault();

                            if (fetchAttribute != null)
                            {
                                attributeName = fetchAttribute.name;

                                if (fetchAttribute.groupbySpecified && fetchAttribute.groupby == FetchBoolType.@true &&
                                    fetchSort.descending)
                                {
                                    // Sorts on groupby columns always seem to be in ascending order, descending flag is ignored
                                    return this;
                                }
                            }
                        }

                        if (attributeName != null)
                        {
                            var meta = dataSource.Metadata[linkEntity.name];
                            var attribute = meta.Attributes.SingleOrDefault(a => a.LogicalName == attributeName && a.AttributeOf == null);

                            // Sorting on a lookup Guid or picklist column actually sorts by the associated name field, which isn't what we want
                            // Picklist sorting can be controlled by the useraworderby flag though.
                            if (attribute is LookupAttributeMetadata)
                                return this;
                            
                            if (attribute is EnumAttributeMetadata || attribute is BooleanAttributeMetadata)
                            {
                                if (useRawOrderBy == false || !dataSource.UseRawOrderByReliable)
                                    return this;

                                useRawOrderBy = true;
                            }

                            // Sorting on multi-select picklist fields isn't supported in FetchXML
                            if (attribute is MultiSelectPicklistAttributeMetadata)
                                return this;

                            // Sorts on the virtual ___name attribute should be applied to the underlying field
                            if (attribute == null && attributeName.EndsWith("name") == true)
                            {
                                attribute = meta.Attributes.SingleOrDefault(a => a.LogicalName == attributeName.Substring(0, fetchSort.attribute.Length - 4) && a.AttributeOf == null);

                                if (attribute != null)
                                {
                                    if (fetchSort.attribute == null)
                                        return this;

                                    fetchSort.attribute = attribute.LogicalName;

                                    if (attribute is EnumAttributeMetadata || attribute is BooleanAttributeMetadata)
                                    {
                                        if (useRawOrderBy == true)
                                            return this;

                                        useRawOrderBy = false;
                                    }
                                }
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

                        if (validOrder)
                        {
                            linkEntity.AddItem(fetchSort);
                        }
                        else
                        {
                            fetchSort.entityname = entityName;
                            fetchXml.Entity.AddItem(fetchSort);
                        }
                    }

                    PresortedCount++;

                    if (useRawOrderBy == true)
                        fetchXml.FetchXml.UseRawOrderBy = true;
                }

                // Virtual entity providers are unreliable - fold the sorts to the FetchXML but keep this
                // node to resort if required.
                if (fetchXml.IsUnreliableVirtualEntityProvider)
                {
                    PresortedCount = 0;
                    return this;
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

        private List<string> GetEntityOrder(FetchXmlScan fetchXml)
        {
            // Get the list of entities in the order that <order> elements will be applied, i.e. DFS
            var order = new List<string>();
            order.Add(fetchXml.Alias);

            foreach (var linkEntity in fetchXml.Entity.GetLinkEntities())
                order.Add(linkEntity.alias);

            return order;
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
