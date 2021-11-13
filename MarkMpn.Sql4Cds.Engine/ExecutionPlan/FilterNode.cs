using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.Engine.FetchXml;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Metadata;
using Microsoft.Xrm.Sdk.Metadata.Query;
using Microsoft.Xrm.Sdk.Query;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// Applies a filter to the data stream
    /// </summary>
    class FilterNode : BaseDataNode, ISingleSourceExecutionPlanNode
    {
        /// <summary>
        /// The filter to apply
        /// </summary>
        [Category("Filter")]
        [Description("The filter to apply")]
        public BooleanExpression Filter { get; set; }

        /// <summary>
        /// The data source to select from
        /// </summary>
        [Browsable(false)]
        public IDataExecutionPlanNode Source { get; set; }

        protected override IEnumerable<Entity> ExecuteInternal(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues)
        {
            var schema = Source.GetSchema(dataSources, parameterTypes);
            var filter = Filter.Compile(schema, parameterTypes);

            foreach (var entity in Source.Execute(dataSources, options, parameterTypes, parameterValues))
            {
                if (filter(entity, parameterValues, options))
                    yield return entity;
            }
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
            Source.Parent = this;

            // Foldable correlated IN queries "lefttable.column IN (SELECT righttable.column FROM righttable WHERE ...) are created as:
            // Filter: Expr2 is not null
            // -> FoldableJoin (LeftOuter SemiJoin) Expr2 = righttable.column in DefinedValues; righttable.column in RightAttribute
            //    -> FetchXml
            //    -> FetchXml (Distinct) orderby righttable.column

            // Foldable correlated EXISTS filters "EXISTS (SELECT * FROM righttable WHERE righttable.column = lefttable.column AND ...) are created as:
            // Filter - @var2 is not null
            // -> NestedLoop(LeftOuter SemiJoin), null join condition. Outer reference(lefttable.column -> @var1), Defined values(@var2 -> rightttable.primarykey)
            //   -> FetchXml
            //   -> Top 1
            //      -> Index spool, SeekValue @var1, KeyColumn rightttable.column
            //         -> FetchXml
            var joins = new List<BaseJoinNode>();
            var join = Source as BaseJoinNode;
            while (join != null)
            {
                joins.Add(join);

                if (join is MergeJoinNode && join.LeftSource is SortNode sort)
                    join = sort.Source as BaseJoinNode;
                else
                    join = join.LeftSource as BaseJoinNode;
            }

            var addedLinks = new List<FetchLinkEntityType>();
            FetchXmlScan leftFetch;

            if (joins.Count == 0)
            {
                leftFetch = null;
            }
            else
            {
                var lastJoin = joins.Last();
                if (lastJoin is MergeJoinNode && lastJoin.LeftSource is SortNode sort)
                    leftFetch = sort.Source as FetchXmlScan;
                else
                    leftFetch = lastJoin.LeftSource as FetchXmlScan;
            }

            while (leftFetch != null && joins.Count > 0)
            {
                join = joins.Last();

                if (join.JoinType != QualifiedJoinType.LeftOuter ||
                    !join.SemiJoin)
                    break;

                FetchLinkEntityType linkToAdd;
                string leftAlias;

                if (join is FoldableJoinNode merge)
                {
                    // Check we meet all the criteria for a foldable correlated IN query
                    var rightSort = join.RightSource as SortNode;
                    var rightFetch = (rightSort?.Source ?? join.RightSource) as FetchXmlScan;

                    if (rightFetch == null)
                        break;

                    if (!leftFetch.DataSource.Equals(rightFetch.DataSource, StringComparison.OrdinalIgnoreCase))
                        break;

                    // Sorts could be folded into FetchXML or could be in separate node
                    string attribute;

                    if (rightSort != null)
                    {
                        if (rightSort.Sorts.Count != 1)
                            break;

                        if (!(rightSort.Sorts[0].Expression is ColumnReferenceExpression sortCol))
                            break;

                        attribute = sortCol.GetColumnName();
                    }
                    else
                    {
                        var rightSorts = (rightFetch.Entity.Items ?? Array.Empty<object>()).OfType<FetchOrderType>().ToList();

                        if (rightSorts.Count != 1)
                            break;

                        if (!String.IsNullOrEmpty(rightSorts[0].alias))
                            break;

                        attribute = $"{rightFetch.Alias}.{rightSorts[0].attribute}";
                    }

                    if (!merge.RightAttribute.GetColumnName().Equals(attribute, StringComparison.OrdinalIgnoreCase))
                        break;

                    var rightSchema = rightFetch.GetSchema(dataSources, parameterTypes);

                    // Right values need to be distinct - still allowed if it's the primary key
                    if (!rightFetch.FetchXml.distinct && rightSchema.PrimaryKey != attribute)
                        break;

                    var definedValueName = join.DefinedValues.SingleOrDefault(kvp => kvp.Value == attribute).Key;

                    if (definedValueName == null)
                        break;

                    var notNullFilter = FindNotNullFilter(Filter, definedValueName);
                    if (notNullFilter == null)
                        break;

                    // We can fold IN to a simple left outer join where the attribute is the primary key
                    if (!rightFetch.FetchXml.distinct && rightSchema.PrimaryKey == attribute)
                    {
                        // Replace the filter on the defined value name with a filter on the primary key column
                        notNullFilter.Expression = attribute.ToColumnReference();

                        linkToAdd = new FetchLinkEntityType
                        {
                            name = rightFetch.Entity.name,
                            alias = rightFetch.Alias,
                            from = merge.RightAttribute.MultiPartIdentifier.Identifiers.Last().Value,
                            to = merge.LeftAttribute.MultiPartIdentifier.Identifiers.Last().Value,
                            linktype = "outer",
                            Items = rightFetch.Entity.Items.Where(i => !(i is FetchOrderType)).ToArray()
                        };
                    }
                    else
                    {
                        // We need to use an "in" join type - check that's supported
                        if (!options.JoinOperatorsAvailable.Contains(JoinOperator.Any))
                            break;

                        // Remove the filter and replace with an "in" link-entity
                        Filter = Filter.RemoveCondition(notNullFilter);

                        linkToAdd = new FetchLinkEntityType
                        {
                            name = rightFetch.Entity.name,
                            alias = rightFetch.Alias,
                            from = merge.RightAttribute.MultiPartIdentifier.Identifiers.Last().Value,
                            to = merge.LeftAttribute.MultiPartIdentifier.Identifiers.Last().Value,
                            linktype = "in",
                            Items = rightFetch.Entity.Items.Where(i => !(i is FetchOrderType)).ToArray()
                        };
                    }

                    leftAlias = merge.LeftAttribute.MultiPartIdentifier.Identifiers.Reverse().Skip(1).First().Value;

                    // Remove the sort that has been merged into the left side too
                    if (leftFetch.Entity.Items != null)
                    {
                        leftFetch.Entity.Items = leftFetch.Entity
                            .Items
                            .Where(i => !(i is FetchOrderType sort) || !sort.attribute.Equals(merge.LeftAttribute.MultiPartIdentifier.Identifiers.Last().Value, StringComparison.OrdinalIgnoreCase))
                            .ToArray();
                    }
                }
                else if (join is NestedLoopNode loop)
                {
                    // Check we meet all the criteria for a foldable correlated IN query
                    if (!options.JoinOperatorsAvailable.Contains(JoinOperator.Exists))
                        break;

                    if (loop.JoinCondition != null ||
                        loop.OuterReferences.Count != 1 ||
                        loop.DefinedValues.Count != 1)
                        break;

                    if (!(join.RightSource is TopNode top))
                        break;

                    if (!(top.Top is IntegerLiteral topLiteral) ||
                        topLiteral.Value != "1")
                        break;

                    if (!(top.Source is IndexSpoolNode indexSpool))
                        break;

                    if (indexSpool.SeekValue != loop.OuterReferences.Single().Value)
                        break;

                    if (!(indexSpool.Source is FetchXmlScan rightFetch))
                        break;

                    if (indexSpool.KeyColumn.Split('.').Length != 2 ||
                        !indexSpool.KeyColumn.Split('.')[0].Equals(rightFetch.Alias, StringComparison.OrdinalIgnoreCase))
                        break;

                    var notNullFilter = FindNotNullFilter(Filter, loop.DefinedValues.Single().Key);
                    if (notNullFilter == null)
                        break;

                    // Remove the filter and replace with an "exists" link-entity
                    Filter = Filter.RemoveCondition(notNullFilter);

                    linkToAdd = new FetchLinkEntityType
                    {
                        name = rightFetch.Entity.name,
                        alias = rightFetch.Alias,
                        from = indexSpool.KeyColumn.Split('.')[1],
                        to = loop.OuterReferences.Single().Key.Split('.')[1],
                        linktype = "exists",
                        Items = rightFetch.Entity.Items
                    };
                    leftAlias = loop.OuterReferences.Single().Key.Split('.')[0];
                }
                else
                {
                    // This isn't a type of join we can fold as a correlated IN/EXISTS join
                    break;
                }

                // Remove any attributes from the new linkentity
                var tempEntity = new FetchEntityType { Items = new object[] { linkToAdd } };

                foreach (var link in tempEntity.GetLinkEntities())
                    link.Items = (link.Items ?? Array.Empty<object>()).Where(i => !(i is FetchAttributeType) && !(i is allattributes)).ToArray();

                if (leftAlias.Equals(leftFetch.Alias, StringComparison.OrdinalIgnoreCase))
                    leftFetch.Entity.AddItem(linkToAdd);
                else
                    leftFetch.Entity.FindLinkEntity(leftAlias).AddItem(linkToAdd);

                addedLinks.Add(linkToAdd);

                joins.Remove(join);

                if (joins.Count == 0)
                {
                    Source = leftFetch;
                    leftFetch.Parent = this;
                }
                else
                {
                    join = joins.Last();

                    if (join is MergeJoinNode && join.LeftSource is SortNode sort)
                    {
                        sort.Source = leftFetch;
                        leftFetch.Parent = sort;
                    }
                    else
                    {
                        join.LeftSource = leftFetch;
                        leftFetch.Parent = join;
                    }
                }
            }

            // If we've got a filter matching a column and a variable (key lookup in a nested loop) from a table spool, replace it with a index spool
            if (Source is TableSpoolNode tableSpool)
            {
                var schema = Source.GetSchema(dataSources, parameterTypes);

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
                        }.FoldQuery(dataSources, options, parameterTypes);
                    }

                    Source = new IndexSpoolNode
                    {
                        Source = spoolSource,
                        KeyColumn = indexColumn,
                        SeekValue = seekVariable
                    };

                    Filter = filter;
                }
            }

            // Find all the data source nodes we could fold this into. Include direct data sources, those from either side of an inner join, or the main side of an outer join
            foreach (var source in GetFoldableSources(Source))
            {
                var schema = source.GetSchema(dataSources, parameterTypes);

                if (source is FetchXmlScan fetchXml && !fetchXml.FetchXml.aggregate)
                {
                    if (!dataSources.TryGetValue(fetchXml.DataSource, out var dataSource))
                        throw new NotSupportedQueryFragmentException("Missing datasource " + fetchXml.DataSource);

                    var additionalLinkEntities = new Dictionary<object, List<FetchLinkEntityType>>();

                    // If the criteria are ANDed, see if any of the individual conditions can be translated to FetchXML
                    Filter = ExtractFetchXMLFilters(dataSource.Metadata, options, Filter, schema, null, fetchXml.Entity.name, fetchXml.Alias, fetchXml.Entity.Items, out var fetchFilter, additionalLinkEntities);

                    if (fetchFilter != null)
                    {
                        fetchXml.Entity.AddItem(fetchFilter);

                        foreach (var kvp in additionalLinkEntities)
                        {
                            if (kvp.Key is FetchEntityType e)
                            {
                                foreach (var le in kvp.Value)
                                    fetchXml.Entity.AddItem(le);
                            }
                            else
                            {
                                foreach (var le in kvp.Value)
                                    ((FetchLinkEntityType)kvp.Key).AddItem(le);
                            }
                        }
                    }
                }

                if (source is MetadataQueryNode meta)
                {
                    // If the criteria are ANDed, see if any of the individual conditions can be translated to the metadata query
                    Filter = ExtractMetadataFilters(Filter, meta, options, out var entityFilter, out var attributeFilter, out var relationshipFilter);

                    meta.Query.AddFilter(entityFilter);

                    if (attributeFilter != null && meta.Query.AttributeQuery == null)
                        meta.Query.AttributeQuery = new AttributeQueryExpression();

                    meta.Query.AttributeQuery.AddFilter(attributeFilter);

                    if (relationshipFilter != null && meta.Query.RelationshipQuery == null)
                        meta.Query.RelationshipQuery = new RelationshipQueryExpression();

                    meta.Query.RelationshipQuery.AddFilter(relationshipFilter);
                }
            }

            foreach (var addedLink in addedLinks)
                addedLink.SemiJoin = true;

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

        public override void AddRequiredColumns(IDictionary<string, DataSource> dataSources, IDictionary<string, Type> parameterTypes, IList<string> requiredColumns)
        {
            var schema = Source.GetSchema(dataSources, parameterTypes);

            foreach (var col in Filter.GetColumns())
            {
                if (!schema.ContainsColumn(col, out var normalized))
                    continue;

                if (!requiredColumns.Contains(normalized, StringComparer.OrdinalIgnoreCase))
                    requiredColumns.Add(normalized);
            }

            Source.AddRequiredColumns(dataSources, parameterTypes, requiredColumns);
        }

        private BooleanExpression ExtractFetchXMLFilters(IAttributeMetadataCache metadata, IQueryExecutionOptions options, BooleanExpression criteria, NodeSchema schema, string allowedPrefix, string targetEntityName, string targetEntityAlias, object[] items, out filter filter, IDictionary<object, List<FetchLinkEntityType>> additionalLinkEntities)
        {
            if (TranslateFetchXMLCriteria(metadata, options, criteria, schema, allowedPrefix, targetEntityName, targetEntityAlias, items, out filter, additionalLinkEntities))
                return null;

            if (!(criteria is BooleanBinaryExpression bin))
                return criteria;

            if (bin.BinaryExpressionType != BooleanBinaryExpressionType.And)
                return criteria;

            bin.FirstExpression = ExtractFetchXMLFilters(metadata, options, bin.FirstExpression, schema, allowedPrefix, targetEntityName, targetEntityAlias, items, out var lhsFilter, additionalLinkEntities);
            bin.SecondExpression = ExtractFetchXMLFilters(metadata, options, bin.SecondExpression, schema, allowedPrefix, targetEntityName, targetEntityAlias, items, out var rhsFilter, additionalLinkEntities);

            filter = (lhsFilter != null && rhsFilter != null) ? new filter { Items = new object[] { lhsFilter, rhsFilter } } : lhsFilter ?? rhsFilter;

            if (bin.FirstExpression != null && bin.SecondExpression != null)
                return bin;

            return bin.FirstExpression ?? bin.SecondExpression;
        }

        protected BooleanExpression ExtractMetadataFilters(BooleanExpression criteria, MetadataQueryNode meta, IQueryExecutionOptions options, out MetadataFilterExpression entityFilter, out MetadataFilterExpression attributeFilter, out MetadataFilterExpression relationshipFilter)
        {
            if (TranslateMetadataCriteria(criteria, meta, options, out entityFilter, out attributeFilter, out relationshipFilter))
                return null;

            if (!(criteria is BooleanBinaryExpression bin))
                return criteria;

            if (bin.BinaryExpressionType != BooleanBinaryExpressionType.And)
                return criteria;

            bin.FirstExpression = ExtractMetadataFilters(bin.FirstExpression, meta, options, out var lhsEntityFilter, out var lhsAttributeFilter, out var lhsRelationshipFilter);
            bin.SecondExpression = ExtractMetadataFilters(bin.SecondExpression, meta, options, out var rhsEntityFilter, out var rhsAttributeFilter, out var rhsRelationshipFilter);

            entityFilter = (lhsEntityFilter != null && rhsEntityFilter != null) ? new MetadataFilterExpression { Filters = { lhsEntityFilter, rhsEntityFilter } } : lhsEntityFilter ?? rhsEntityFilter;
            attributeFilter = (lhsAttributeFilter != null && rhsAttributeFilter != null) ? new MetadataFilterExpression { Filters = { lhsAttributeFilter, rhsAttributeFilter } } : lhsAttributeFilter ?? rhsAttributeFilter;
            relationshipFilter = (lhsRelationshipFilter != null && rhsRelationshipFilter != null) ? new MetadataFilterExpression { Filters = { lhsRelationshipFilter, rhsRelationshipFilter } } : lhsRelationshipFilter ?? rhsRelationshipFilter;

            if (bin.FirstExpression != null && bin.SecondExpression != null)
                return bin;

            return bin.FirstExpression ?? bin.SecondExpression;
        }
        protected bool TranslateMetadataCriteria(BooleanExpression criteria, MetadataQueryNode meta, IQueryExecutionOptions options, out MetadataFilterExpression entityFilter, out MetadataFilterExpression attributeFilter, out MetadataFilterExpression relationshipFilter)
        {
            entityFilter = null;
            attributeFilter = null;
            relationshipFilter = null;

            if (criteria is BooleanBinaryExpression binary)
            {
                if (!TranslateMetadataCriteria(binary.FirstExpression, meta, options, out var lhsEntityFilter, out var lhsAttributeFilter, out var lhsRelationshipFilter))
                    return false;
                if (!TranslateMetadataCriteria(binary.SecondExpression, meta, options, out var rhsEntityFilter, out var rhsAttributeFilter, out var rhsRelationshipFilter))
                    return false;

                if (binary.BinaryExpressionType == BooleanBinaryExpressionType.Or)
                {
                    // Can only do OR filters within a single type
                    var typeCount = 0;

                    if (lhsEntityFilter != null || rhsEntityFilter != null)
                        typeCount++;

                    if (lhsAttributeFilter != null || rhsAttributeFilter != null)
                        typeCount++;

                    if (lhsRelationshipFilter != null || rhsRelationshipFilter != null)
                        typeCount++;

                    if (typeCount > 1)
                        return false;
                }

                entityFilter = lhsEntityFilter;
                attributeFilter = lhsAttributeFilter;
                relationshipFilter = lhsRelationshipFilter;

                if (rhsEntityFilter != null)
                {
                    if (entityFilter == null)
                        entityFilter = rhsEntityFilter;
                    else
                        entityFilter = new MetadataFilterExpression { Filters = { lhsEntityFilter, rhsEntityFilter }, FilterOperator = binary.BinaryExpressionType == BooleanBinaryExpressionType.And ? LogicalOperator.And : LogicalOperator.Or };
                }

                if (rhsAttributeFilter != null)
                {
                    if (attributeFilter == null)
                        attributeFilter = rhsAttributeFilter;
                    else
                        attributeFilter = new MetadataFilterExpression { Filters = { lhsAttributeFilter, rhsAttributeFilter }, FilterOperator = binary.BinaryExpressionType == BooleanBinaryExpressionType.And ? LogicalOperator.And : LogicalOperator.Or };
                }

                if (rhsRelationshipFilter != null)
                {
                    if (relationshipFilter == null)
                        relationshipFilter = rhsRelationshipFilter;
                    else
                        relationshipFilter = new MetadataFilterExpression { Filters = { lhsRelationshipFilter, rhsRelationshipFilter }, FilterOperator = binary.BinaryExpressionType == BooleanBinaryExpressionType.And ? LogicalOperator.And : LogicalOperator.Or };
                }

                return true;
            }

            if (criteria is BooleanComparisonExpression comparison)
            {
                if (comparison.ComparisonType != BooleanComparisonType.Equals &&
                    comparison.ComparisonType != BooleanComparisonType.NotEqualToBrackets &&
                    comparison.ComparisonType != BooleanComparisonType.NotEqualToExclamation &&
                    comparison.ComparisonType != BooleanComparisonType.LessThan &&
                    comparison.ComparisonType != BooleanComparisonType.GreaterThan)
                    return false;

                var col = comparison.FirstExpression as ColumnReferenceExpression;
                var literal = comparison.SecondExpression as Literal;

                if (col == null && literal == null)
                {
                    col = comparison.SecondExpression as ColumnReferenceExpression;
                    literal = comparison.FirstExpression as Literal;
                }

                if (col == null || literal == null)
                    return false;

                var schema = meta.GetSchema(null, null);
                if (!schema.ContainsColumn(col.GetColumnName(), out var colName))
                    return false;

                var parts = colName.Split('.');

                if (parts.Length != 2)
                    return false;

                MetadataConditionOperator op;

                switch (comparison.ComparisonType)
                {
                    case BooleanComparisonType.Equals:
                        op = MetadataConditionOperator.Equals;
                        break;

                    case BooleanComparisonType.NotEqualToBrackets:
                    case BooleanComparisonType.NotEqualToExclamation:
                        op = MetadataConditionOperator.NotEquals;
                        break;

                    case BooleanComparisonType.LessThan:
                        op = MetadataConditionOperator.LessThan;
                        break;

                    case BooleanComparisonType.GreaterThan:
                        op = MetadataConditionOperator.GreaterThan;
                        break;

                    default:
                        throw new InvalidOperationException();
                }

                var condition = new MetadataConditionExpression(parts[1], op, literal.Compile(null, null)(null, null, options));

                return TranslateMetadataCondition(condition, parts[0], meta, out entityFilter, out attributeFilter, out relationshipFilter);
            }

            if (criteria is InPredicate inPred)
            {
                var col = inPred.Expression as ColumnReferenceExpression;

                if (col == null)
                    return false;

                var schema = meta.GetSchema(null, null);
                if (!schema.ContainsColumn(col.GetColumnName(), out var colName))
                    return false;

                var parts = colName.Split('.');

                if (parts.Length != 2)
                    return false;

                if (inPred.Values.Any(val => !(val is Literal)))
                    return false;

                var condition = new MetadataConditionExpression(parts[1], inPred.NotDefined ? MetadataConditionOperator.NotIn : MetadataConditionOperator.In, inPred.Values.Select(val => val.Compile(null, null)(null, null, options)).ToArray());

                return TranslateMetadataCondition(condition, parts[0], meta, out entityFilter, out attributeFilter, out relationshipFilter);
            }

            if (criteria is BooleanIsNullExpression isNull)
            {
                var col = isNull.Expression as ColumnReferenceExpression;

                if (col == null)
                    return false;

                var schema = meta.GetSchema(null, null);
                if (!schema.ContainsColumn(col.GetColumnName(), out var colName))
                    return false;

                var parts = colName.Split('.');

                if (parts.Length != 2)
                    return false;

                var condition = new MetadataConditionExpression(parts[1], isNull.IsNot ? MetadataConditionOperator.NotEquals : MetadataConditionOperator.Equals, null);

                return TranslateMetadataCondition(condition, parts[0], meta, out entityFilter, out attributeFilter, out relationshipFilter);
            }

            return false;
        }

        private bool TranslateMetadataCondition(MetadataConditionExpression condition, string alias, MetadataQueryNode meta, out MetadataFilterExpression entityFilter, out MetadataFilterExpression attributeFilter, out MetadataFilterExpression relationshipFilter)
        {
            entityFilter = null;
            attributeFilter = null;
            relationshipFilter = null;

            // Translate queries on attribute.EntityLogicalName to entity.LogicalName for better performance
            var isEntityFilter = alias.Equals(meta.EntityAlias, StringComparison.OrdinalIgnoreCase);
            var isAttributeFilter = alias.Equals(meta.AttributeAlias, StringComparison.OrdinalIgnoreCase);
            var isRelationshipFilter = alias.Equals(meta.OneToManyRelationshipAlias, StringComparison.OrdinalIgnoreCase) || alias.Equals(meta.ManyToOneRelationshipAlias, StringComparison.OrdinalIgnoreCase) || alias.Equals(meta.ManyToManyRelationshipAlias, StringComparison.OrdinalIgnoreCase);

            if (isAttributeFilter &&
                condition.PropertyName.Equals(nameof(AttributeMetadata.EntityLogicalName), StringComparison.OrdinalIgnoreCase))
            {
                condition.PropertyName = nameof(EntityMetadata.LogicalName);
                isAttributeFilter = false;
                isEntityFilter = true;
            }

            if (alias.Equals(meta.OneToManyRelationshipAlias, StringComparison.OrdinalIgnoreCase) &&
                condition.PropertyName.Equals(nameof(OneToManyRelationshipMetadata.ReferencedEntity), StringComparison.OrdinalIgnoreCase))
            {
                condition.PropertyName = nameof(EntityMetadata.LogicalName);
                isRelationshipFilter = false;
                isEntityFilter = true;
            }

            if (alias.Equals(meta.ManyToOneRelationshipAlias, StringComparison.OrdinalIgnoreCase) &&
                condition.PropertyName.Equals(nameof(OneToManyRelationshipMetadata.ReferencingEntity), StringComparison.OrdinalIgnoreCase))
            {
                condition.PropertyName = nameof(EntityMetadata.LogicalName);
                isRelationshipFilter = false;
                isEntityFilter = true;
            }

            var filter = new MetadataFilterExpression { Conditions = { condition } };

            // Attributes & relationships are polymorphic, but filters can only be applied to the base type. Check the property
            // we're filtering on is valid to be folded
            var targetType = isEntityFilter ? typeof(EntityMetadata) : isAttributeFilter ? typeof(AttributeMetadata) : typeof(RelationshipMetadataBase);
            var prop = targetType.GetProperty(condition.PropertyName, BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase);

            if (prop == null)
                return false;

            // Only properties that represent simple data types, enumerations, BooleanManagedProperty or AttributeRequiredLevelManagedProperty types can be used in a MetadataFilterExpression. When a BooleanManagedProperty or AttributeRequiredLevelManagedProperty is specified, only the Value property is evaluated.
            // https://docs.microsoft.com/en-us/dynamics365/customerengagement/on-premises/developer/retrieve-detect-changes-metadata#specify-your-filter-criteria

            var targetValueType = prop.PropertyType;

            // Managed properties and nullable types are handled through their Value property
            if (targetValueType.BaseType != null &&
                targetValueType.BaseType.IsGenericType &&
                targetValueType.BaseType.GetGenericTypeDefinition() == typeof(ManagedProperty<>))
                targetValueType = targetValueType.BaseType.GetGenericArguments()[0];

            if (targetValueType.IsGenericType &&
                targetValueType.GetGenericTypeDefinition() == typeof(Nullable<>))
                targetValueType = targetValueType.GetGenericArguments()[0];

            if (!targetValueType.IsPrimitive &&
                !targetValueType.IsEnum &&
                targetValueType != typeof(string) &&
                targetValueType != typeof(decimal) &&
                targetValueType != typeof(Guid))
                return false;

            // Convert the property name to the correct case
            filter.Conditions[0].PropertyName = prop.Name;

            // Convert the value to the expected type
            if (filter.Conditions[0].Value != null)
            {
                var propertyType = MetadataQueryNode.GetPropertyType(targetValueType);

                if (filter.Conditions[0].ConditionOperator == MetadataConditionOperator.In ||
                    filter.Conditions[0].ConditionOperator == MetadataConditionOperator.NotIn)
                {
                    var array = (Array)filter.Conditions[0].Value;
                    var targetArray = Array.CreateInstance(targetValueType, array.Length);

                    for (var i = 0; i < array.Length; i++)
                        targetArray.SetValue(SqlTypeConverter.ChangeType(SqlTypeConverter.ChangeType(array.GetValue(i), propertyType), targetValueType), i);

                    filter.Conditions[0].Value = targetArray;
                }
                else
                {
                    filter.Conditions[0].Value = SqlTypeConverter.ChangeType(SqlTypeConverter.ChangeType(filter.Conditions[0].Value, propertyType), targetValueType);
                }
            }

            if (isEntityFilter)
            {
                entityFilter = filter;
                return true;
            }

            if (isAttributeFilter)
            {
                attributeFilter = filter;
                return true;
            }

            if (isRelationshipFilter)
            {
                relationshipFilter = filter;
                return true;
            }

            return false;
        }

        public override int EstimateRowsOut(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes)
        {
            return Source.EstimateRowsOut(dataSources, options, parameterTypes) * 8 / 10;
        }
    }
}
