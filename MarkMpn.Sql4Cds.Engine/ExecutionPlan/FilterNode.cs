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
        public IDataExecutionPlanNodeInternal Source { get; set; }

        protected override IEnumerable<Entity> ExecuteInternal(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, DataTypeReference> parameterTypes, IDictionary<string, object> parameterValues)
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

        public override INodeSchema GetSchema(IDictionary<string, DataSource> dataSources, IDictionary<string, DataTypeReference> parameterTypes)
        {
            var schema = Source.GetSchema(dataSources, parameterTypes);
            var notNullColumns = new HashSet<string>(schema.NotNullColumns, StringComparer.OrdinalIgnoreCase);
            AddNotNullColumns(schema, notNullColumns, Filter, false);

            return new NodeSchema(
                primaryKey: schema.PrimaryKey,
                schema: schema.Schema,
                aliases: schema.Aliases,
                notNullColumns: notNullColumns.ToList(),
                sortOrder: schema.SortOrder);
        }

        private void AddNotNullColumns(INodeSchema schema, HashSet<string> notNullColumns, BooleanExpression filter, bool not)
        {
            if (filter is BooleanBinaryExpression binary)
            {
                if (binary.BinaryExpressionType == BooleanBinaryExpressionType.Or)
                    return;

                AddNotNullColumns(schema, notNullColumns, binary.FirstExpression, not);
                AddNotNullColumns(schema, notNullColumns, binary.SecondExpression, not);
            }

            if (filter is BooleanIsNullExpression isNull)
            {
                if (not ^ isNull.IsNot)
                    AddNotNullColumn(schema, notNullColumns, isNull.Expression);
            }

            if (!not && filter is BooleanComparisonExpression cmp)
            {
                AddNotNullColumn(schema, notNullColumns, cmp.FirstExpression);
                AddNotNullColumn(schema, notNullColumns, cmp.SecondExpression);
            }

            if (filter is BooleanParenthesisExpression paren)
            {
                AddNotNullColumns(schema, notNullColumns, paren.Expression, not);
            }

            if (filter is BooleanNotExpression n)
            {
                AddNotNullColumns(schema, notNullColumns, n.Expression, !not);
            }

            if (!not && filter is InPredicate inPred)
            {
                AddNotNullColumn(schema, notNullColumns, inPred.Expression);
            }

            if (!not && filter is LikePredicate like)
            {
                AddNotNullColumn(schema, notNullColumns, like.FirstExpression);
                AddNotNullColumn(schema, notNullColumns, like.SecondExpression);
            }

            if (!not && filter is FullTextPredicate fullText)
            {
                foreach (var col in fullText.Columns)
                    AddNotNullColumn(schema, notNullColumns, col);
            }
        }

        private void AddNotNullColumn(INodeSchema schema, HashSet<string> notNullColumns, ScalarExpression expr)
        {
            if (!(expr is ColumnReferenceExpression col))
                return;

            if (!schema.ContainsColumn(col.GetColumnName(), out var colName))
                return;

            notNullColumns.Add(colName);
        }

        public override IDataExecutionPlanNodeInternal FoldQuery(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, DataTypeReference> parameterTypes, IList<OptimizerHint> hints)
        {
            Source = Source.FoldQuery(dataSources, options, parameterTypes, hints);
            Source.Parent = this;

            var foldedFilters = false;

            foldedFilters |= FoldNestedLoopFiltersToJoins(dataSources, options, parameterTypes, hints);
            foldedFilters |= FoldInExistsToFetchXml(dataSources, options, parameterTypes, hints, out var addedLinks);
            foldedFilters |= FoldTableSpoolToIndexSpool(dataSources, options, parameterTypes, hints);
            foldedFilters |= FoldFiltersToDataSources(dataSources, options, parameterTypes);

            foreach (var addedLink in addedLinks)
            {
                addedLink.Key.SemiJoin = true;
                addedLink.Value.ResetSchemaCache();
            }

            // Some of the filters have been folded into the source. Fold the sources again as the filter can have changed estimated row
            // counts and lead to a better execution plan.
            if (foldedFilters)
                Source = Source.FoldQuery(dataSources, options, parameterTypes, hints);

            // All the filters have been folded into the source. 
            if (Filter == null)
                return Source;

            return this;
        }

        private bool FoldNestedLoopFiltersToJoins(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, DataTypeReference> parameterTypes, IList<OptimizerHint> hints)
        {
            // Queries like "FROM table1, table2 WHERE table1.col = table2.col" are created as:
            // Filter: table1.col = table2.col
            // -> NestedLoop (Inner), null join condition, no defined values
            //    -> FetchXml
            //    -> Table Spool
            //       -> FetchXml
            if (FoldNestedLoopFiltersToJoins(Source as BaseJoinNode, dataSources, options, parameterTypes, hints, out var foldedJoin))
            {
                Source = foldedJoin;
                return true;
            }

            return false;
        }

        private bool FoldNestedLoopFiltersToJoins(BaseJoinNode join, IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, DataTypeReference> parameterTypes, IList<OptimizerHint> hints, out FoldableJoinNode foldedJoin)
        {
            foldedJoin = null;

            if (join == null)
                return false;

            var foldedFilters = false;

            if (join.JoinType == QualifiedJoinType.Inner &&
                !join.SemiJoin &&
                join.DefinedValues.Count == 0 &&
                join is NestedLoopNode loop &&
                (loop.OuterReferences == null || loop.OuterReferences.Count == 0))
            {
                var leftSchema = join.LeftSource.GetSchema(dataSources, parameterTypes);
                var rightSchema = join.RightSource.GetSchema(dataSources, parameterTypes);

                if (ExtractJoinCondition(Filter, loop, leftSchema, rightSchema, out foldedJoin, out var removedCondition))
                {
                    Filter = Filter.RemoveCondition(removedCondition);
                    foldedFilters = true;
                    join = foldedJoin;
                }
            }

            if (FoldNestedLoopFiltersToJoins(join.LeftSource as BaseJoinNode, dataSources, options, parameterTypes, hints, out var foldedLeftSource))
            {
                join.LeftSource = foldedLeftSource;
                foldedFilters = true;
            }

            if (FoldNestedLoopFiltersToJoins(join.RightSource as BaseJoinNode, dataSources, options, parameterTypes, hints, out var foldedRightSource))
            {
                join.RightSource = foldedRightSource;
                foldedFilters = true;
            }

            return foldedFilters;
        }

        private bool ExtractJoinCondition(BooleanExpression filter, NestedLoopNode join, INodeSchema leftSchema, INodeSchema rightSchema, out FoldableJoinNode foldedJoin, out BooleanExpression removedCondition)
        {
            if (filter is BooleanComparisonExpression cmp &&
                cmp.ComparisonType == BooleanComparisonType.Equals &&
                cmp.FirstExpression is ColumnReferenceExpression col1 &&
                cmp.SecondExpression is ColumnReferenceExpression col2)
            {
                var leftSource = join.LeftSource;
                var rightSource = join.RightSource;

                // Equality expression may be written in the opposite order to the join - swap the tables if necessary
                if (rightSchema.ContainsColumn(col1.GetColumnName(), out _) &&
                    leftSchema.ContainsColumn(col2.GetColumnName(), out _))
                {
                    (leftSource, leftSchema, rightSource, rightSchema) = (rightSource, rightSchema, leftSource, leftSchema);
                }

                if (leftSchema.ContainsColumn(col1.GetColumnName(), out var leftCol) &&
                    rightSchema.ContainsColumn(col2.GetColumnName(), out var rightCol))
                {
                    if (leftSource is TableSpoolNode leftSpool)
                    {
                        leftSpool.Source.Parent = leftSource.Parent;
                        leftSource = leftSpool.Source;
                    }

                    if (rightSource is TableSpoolNode rightSpool)
                    {
                        rightSpool.Source.Parent = rightSource.Parent;
                        rightSource = rightSpool.Source;
                    }

                    // Prefer to use a merge join if either of the join keys are the primary key.
                    // Swap the tables if necessary to use the primary key from the right source.
                    if (leftSchema.PrimaryKey != leftCol && rightSchema.PrimaryKey == rightCol)
                        (leftSource, leftSchema, leftCol, rightSource, rightSchema, rightCol) = (rightSource, rightSchema, rightCol, leftSource, leftSchema, leftCol);

                    if (leftSchema.PrimaryKey == leftCol)
                        foldedJoin = new MergeJoinNode();
                    else
                        foldedJoin = new HashJoinNode();

                    foldedJoin.LeftSource = leftSource;
                    foldedJoin.LeftAttribute = leftCol.ToColumnReference();
                    foldedJoin.RightSource = rightSource;
                    foldedJoin.RightAttribute = rightCol.ToColumnReference();
                    foldedJoin.JoinType = QualifiedJoinType.Inner;
                    foldedJoin.Parent = join.Parent;

                    leftSource.Parent = foldedJoin;
                    rightSource.Parent = foldedJoin;

                    removedCondition = filter;
                    return true;
                }
            }
            else if (filter is BooleanBinaryExpression bin &&
                bin.BinaryExpressionType == BooleanBinaryExpressionType.And)
            {
                // Recurse into ANDs but not into ORs
                if (ExtractJoinCondition(bin.FirstExpression, join, leftSchema, rightSchema, out foldedJoin, out removedCondition) ||
                    ExtractJoinCondition(bin.SecondExpression, join, leftSchema, rightSchema, out foldedJoin, out removedCondition))
                {
                    return true;
                }
            }

            foldedJoin = null;
            removedCondition = null;
            return false;
        }

        private bool FoldInExistsToFetchXml(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, DataTypeReference> parameterTypes, IList<OptimizerHint> hints, out Dictionary<FetchLinkEntityType, FetchXmlScan> addedLinks)
        {
            var foldedFilters = false;

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

            addedLinks = new Dictionary<FetchLinkEntityType, FetchXmlScan>();
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
                bool semiJoin;
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

                    var rightSchema = rightFetch.GetSchema(dataSources, parameterTypes);

                    if (!rightSchema.ContainsColumn(merge.RightAttribute.GetColumnName(), out var attribute))
                        break;

                    // Right values need to be distinct - still allowed if it's the primary key
                    if (!rightFetch.FetchXml.distinct && rightSchema.PrimaryKey != attribute)
                        break;

                    var definedValueName = join.DefinedValues.SingleOrDefault(kvp => kvp.Value == attribute).Key;

                    if (definedValueName == null)
                        break;

                    var notNullFilter = FindNotNullFilter(Filter, definedValueName, out var notNullFilterRemovable);
                    if (notNullFilter == null)
                        break;

                    // If IN query is on matching primary keys (SELECT name FROM account WHERE accountid IN (SELECT accountid FROM account WHERE ...))
                    // we can eliminate the left join and rewrite as SELECT name FROM account WHERE ....
                    // Can't do this if there is any conflict in join aliases
                    if (notNullFilterRemovable &&
                        leftFetch.Entity.name == rightFetch.Entity.name &&
                        merge.LeftAttribute.GetColumnName() == leftFetch.Alias + "." + dataSources[leftFetch.DataSource].Metadata[leftFetch.Entity.name].PrimaryIdAttribute &&
                        merge.RightAttribute.GetColumnName() == rightFetch.Alias + "." + dataSources[rightFetch.DataSource].Metadata[rightFetch.Entity.name].PrimaryIdAttribute &&
                        !leftFetch.Entity.GetLinkEntities().Select(l => l.alias).Intersect(rightFetch.Entity.GetLinkEntities().Select(l => l.alias), StringComparer.OrdinalIgnoreCase).Any() &&
                        (leftFetch.FetchXml.top == null || rightFetch.FetchXml.top == null))
                    {
                        if (rightFetch.Entity.Items != null)
                        {
                            // Remove any attributes from the subquery. Mark all joins as semi joins so no more attributes will
                            // be added to them using a SELECT * query
                            rightFetch.Entity.Items = rightFetch.Entity.Items.Where(i => !(i is FetchAttributeType || i is allattributes)).ToArray();

                            foreach (var link in rightFetch.Entity.GetLinkEntities())
                            {
                                link.SemiJoin = true;

                                if (link.Items != null)
                                    link.Items = link.Items.Where(i => !(i is FetchAttributeType || i is allattributes)).ToArray();
                            }

                            foreach (var item in rightFetch.Entity.Items)
                                leftFetch.Entity.AddItem(item);
                        }

                        if (rightFetch.FetchXml.top != null)
                            leftFetch.FetchXml.top = rightFetch.FetchXml.top;

                        Filter = Filter.RemoveCondition(notNullFilter);
                        foldedFilters = true;

                        linkToAdd = null;
                        semiJoin = false;
                    }
                    // We can fold IN to a simple join where the attribute is the primary key. Can't do this
                    // if there are any inner joins though, unless the new join will itself be an inner join.
                    else if (!rightFetch.FetchXml.distinct &&
                        rightSchema.PrimaryKey == attribute &&
                        (notNullFilterRemovable || !rightFetch.Entity.GetLinkEntities().Any(link => link.linktype == "inner")))
                    {
                        // Replace the filter on the defined value name with a filter on the primary key column
                        notNullFilter.Expression = attribute.ToColumnReference();

                        linkToAdd = new FetchLinkEntityType
                        {
                            name = rightFetch.Entity.name,
                            alias = rightFetch.Alias,
                            from = merge.RightAttribute.MultiPartIdentifier.Identifiers.Last().Value,
                            to = merge.LeftAttribute.MultiPartIdentifier.Identifiers.Last().Value,
                            Items = rightFetch.Entity.Items.Where(i => !(i is FetchOrderType)).ToArray()
                        };

                        if (notNullFilterRemovable)
                        {
                            // IN filter is combined with AND, so all matching records must satisfy the filter
                            // We can use an inner join to simplify the FetchXML, and mark it as a semi join so
                            // none of the attributes in the joined table are exposed in the schema
                            linkToAdd.linktype = "inner";
                            semiJoin = true;

                            // The inner join is now doing the job of the filter, so we can remove the filter
                            Filter = Filter.RemoveCondition(notNullFilter);
                            foldedFilters = true;
                        }
                        else
                        {
                            // IN filter is combined with OR, so we may get matching records that don't satisfy
                            // this join. We need to use an outer join and return some attributes in the schema
                            // for the filter to be able to work, but require them to be fully qualified so we
                            // don't cause conflicts.
                            linkToAdd.linktype = "outer";
                            linkToAdd.RequireTablePrefix = true;
                            semiJoin = false;
                        }
                    }
                    // We need to use an "in" join type - check that's supported
                    else if (notNullFilterRemovable &&
                        options.JoinOperatorsAvailable.Contains(JoinOperator.Any))
                    {
                        // Remove the filter and replace with an "in" link-entity
                        Filter = Filter.RemoveCondition(notNullFilter);
                        foldedFilters = true;

                        linkToAdd = new FetchLinkEntityType
                        {
                            name = rightFetch.Entity.name,
                            alias = rightFetch.Alias,
                            from = merge.RightAttribute.MultiPartIdentifier.Identifiers.Last().Value,
                            to = merge.LeftAttribute.MultiPartIdentifier.Identifiers.Last().Value,
                            linktype = "in",
                            Items = rightFetch.Entity.Items.Where(i => !(i is FetchOrderType)).ToArray()
                        };
                        semiJoin = true;
                    }
                    else
                    {
                        break;
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
                    // Check we meet all the criteria for a foldable correlated EXISTS query
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

                    var notNullFilter = FindNotNullFilter(Filter, loop.DefinedValues.Single().Key, out var notNullFilterRemovable);
                    if (notNullFilter == null || !notNullFilterRemovable)
                        break;

                    // Remove the filter and replace with an "exists" link-entity
                    Filter = Filter.RemoveCondition(notNullFilter);
                    foldedFilters = true;

                    linkToAdd = new FetchLinkEntityType
                    {
                        name = rightFetch.Entity.name,
                        alias = rightFetch.Alias,
                        from = indexSpool.KeyColumn.Split('.')[1],
                        to = loop.OuterReferences.Single().Key.Split('.')[1],
                        linktype = "exists",
                        Items = rightFetch.Entity.Items
                    };
                    semiJoin = true;
                    leftAlias = loop.OuterReferences.Single().Key.Split('.')[0];
                }
                else
                {
                    // This isn't a type of join we can fold as a correlated IN/EXISTS join
                    break;
                }

                if (linkToAdd != null)
                {
                    // Can't add the link if it's got any filter conditions with an entityname
                    if (linkToAdd.GetConditions().Any(c => !String.IsNullOrEmpty(c.entityname)))
                        break;

                    // Remove any attributes from the new linkentity
                    var tempEntity = new FetchEntityType { Items = new object[] { linkToAdd } };

                    foreach (var link in tempEntity.GetLinkEntities())
                        link.Items = (link.Items ?? Array.Empty<object>()).Where(i => !(i is FetchAttributeType) && !(i is allattributes)).ToArray();

                    if (leftAlias.Equals(leftFetch.Alias, StringComparison.OrdinalIgnoreCase))
                        leftFetch.Entity.AddItem(linkToAdd);
                    else
                        leftFetch.Entity.FindLinkEntity(leftAlias).AddItem(linkToAdd);

                    if (semiJoin)
                    {
                        // Join needs to be a semi-join, but don't set it yet as the columns won't be visible in the schema to allow
                        // the filter to be folded into it later. Keep track of which links we've added now so we can set the semi-join
                        // flag on them later.
                        addedLinks.Add(linkToAdd, leftFetch);
                    }
                }

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

            return foldedFilters;
        }

        private bool FoldTableSpoolToIndexSpool(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, DataTypeReference> parameterTypes, IList<OptimizerHint> hints)
        {
            // If we've got a filter matching a column and a variable (key lookup in a nested loop) from a table spool, replace it with a index spool
            if (!(Source is TableSpoolNode tableSpool))
                return false;

            var schema = Source.GetSchema(dataSources, parameterTypes);

            if (!ExtractKeyLookupFilter(Filter, out var filter, out var indexColumn, out var seekVariable) || !schema.ContainsColumn(indexColumn, out indexColumn))
                return false;

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
                };
            }

            Source = new IndexSpoolNode
            {
                Source = spoolSource,
                KeyColumn = indexColumn,
                SeekValue = seekVariable
            }.FoldQuery(dataSources, options, parameterTypes, hints);

            Filter = filter;
            return true;
        }

        private bool FoldFiltersToDataSources(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, DataTypeReference> parameterTypes)
        {
            var foldedFilters = false;

            // Find all the data source nodes we could fold this into. Include direct data sources, those from either side of an inner join, or the main side of an outer join
            foreach (var source in GetFoldableSources(Source))
            {
                var schema = source.GetSchema(dataSources, parameterTypes);

                if (source is FetchXmlScan fetchXml && !fetchXml.FetchXml.aggregate)
                {
                    if (!dataSources.TryGetValue(fetchXml.DataSource, out var dataSource))
                        throw new NotSupportedQueryFragmentException("Missing datasource " + fetchXml.DataSource);

                    // If the criteria are ANDed, see if any of the individual conditions can be translated to FetchXML
                    Filter = ExtractFetchXMLFilters(dataSource.Metadata, options, Filter, schema, null, fetchXml.Entity.name, fetchXml.Alias, fetchXml.Entity.Items, parameterTypes, out var fetchFilter);

                    if (fetchFilter != null)
                    {
                        fetchXml.Entity.AddItem(fetchFilter);
                        foldedFilters = true;

                        // If we're re-adding a filter that was previously extracted to an IndexSpoolNode, remove the not-null condition that's
                        // also been added
                        if (Filter == null && fetchFilter.Items.Length == 1 && fetchFilter.Items[0] is condition c &&
                            c.@operator == @operator.eq && c.IsVariable)
                        {
                            var notNull = fetchXml.Entity.Items
                                .OfType<filter>()
                                .Where(f => f.Items.Length == 1 && f.Items[0] is condition nn && nn.entityname == c.entityname && nn.attribute == c.attribute && nn.@operator == @operator.notnull)
                                .ToList();

                            fetchXml.Entity.Items = fetchXml.Entity.Items.Except(notNull).ToArray();
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

                    if (entityFilter != null || attributeFilter != null || relationshipFilter != null)
                        foldedFilters = true;
                }
            }

            return foldedFilters;
        }

        private BooleanIsNullExpression FindNotNullFilter(BooleanExpression filter, string attribute, out bool and)
        {
            if (filter is BooleanIsNullExpression isNull &&
                isNull.IsNot &&
                isNull.Expression is ColumnReferenceExpression col &&
                col.GetColumnName().Equals(attribute, StringComparison.OrdinalIgnoreCase))
            {
                and = true;
                return isNull;
            }

            if (filter is BooleanParenthesisExpression paren)
                return FindNotNullFilter(paren.Expression, attribute, out and);

            if (filter is BooleanBinaryExpression bin)
            {
                var notNull = FindNotNullFilter(bin.FirstExpression, attribute, out and) ?? FindNotNullFilter(bin.SecondExpression, attribute, out and);

                if (notNull != null)
                {
                    and &= bin.BinaryExpressionType == BooleanBinaryExpressionType.And;
                    return notNull;
                }
            }

            and = false;
            return null;
        }

        private IEnumerable<IDataExecutionPlanNodeInternal> GetFoldableSources(IDataExecutionPlanNodeInternal source)
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

            foreach (var subSource in source.GetSources().OfType<IDataExecutionPlanNodeInternal>())
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

        public override void AddRequiredColumns(IDictionary<string, DataSource> dataSources, IDictionary<string, DataTypeReference> parameterTypes, IList<string> requiredColumns)
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

        private BooleanExpression ExtractFetchXMLFilters(IAttributeMetadataCache metadata, IQueryExecutionOptions options, BooleanExpression criteria, INodeSchema schema, string allowedPrefix, string targetEntityName, string targetEntityAlias, object[] items, IDictionary<string, DataTypeReference> parameterTypes, out filter filter)
        {
            if (TranslateFetchXMLCriteria(metadata, options, criteria, schema, allowedPrefix, targetEntityName, targetEntityAlias, items, parameterTypes, out filter))
                return null;

            if (!(criteria is BooleanBinaryExpression bin))
                return criteria;

            if (bin.BinaryExpressionType != BooleanBinaryExpressionType.And)
                return criteria;

            bin.FirstExpression = ExtractFetchXMLFilters(metadata, options, bin.FirstExpression, schema, allowedPrefix, targetEntityName, targetEntityAlias, items, parameterTypes, out var lhsFilter);
            bin.SecondExpression = ExtractFetchXMLFilters(metadata, options, bin.SecondExpression, schema, allowedPrefix, targetEntityName, targetEntityAlias, items, parameterTypes, out var rhsFilter);

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

            // String comparisons will be executed case-sensitively, but all other comparisons are case-insensitive. For consistency, don't allow
            // comparisons on string properties except those where we know the expected case.
            Func<object, object> valueConverter = (o) => o;

            if (targetValueType == typeof(string))
            {
                if (prop.DeclaringType == typeof(EntityMetadata) && (prop.Name == nameof(EntityMetadata.LogicalName) || prop.Name == nameof(EntityMetadata.LogicalCollectionName)) ||
                    prop.DeclaringType == typeof(AttributeMetadata) && (prop.Name == nameof(AttributeMetadata.LogicalName) || prop.Name == nameof(AttributeMetadata.EntityLogicalName)))
                {
                    valueConverter = (o) => ((string)o)?.ToLowerInvariant();
                }
                else
                {
                    return false;
                }
            }

            // Convert the property name to the correct case
            filter.Conditions[0].PropertyName = prop.Name;

            // Convert the value to the expected type
            if (filter.Conditions[0].Value != null)
            {
                var propertyType = MetadataQueryNode.GetPropertyType(targetValueType).ToNetType(out _);

                if (filter.Conditions[0].ConditionOperator == MetadataConditionOperator.In ||
                    filter.Conditions[0].ConditionOperator == MetadataConditionOperator.NotIn)
                {
                    var array = (Array)filter.Conditions[0].Value;
                    var targetArray = Array.CreateInstance(targetValueType, array.Length);

                    for (var i = 0; i < array.Length; i++)
                        targetArray.SetValue(valueConverter(SqlTypeConverter.ChangeType(SqlTypeConverter.ChangeType(array.GetValue(i), propertyType), targetValueType)), i);

                    filter.Conditions[0].Value = targetArray;
                }
                else
                {
                    filter.Conditions[0].Value = valueConverter(SqlTypeConverter.ChangeType(SqlTypeConverter.ChangeType(filter.Conditions[0].Value, propertyType), targetValueType));
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

        protected override RowCountEstimate EstimateRowsOutInternal(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, DataTypeReference> parameterTypes)
        {
            return new RowCountEstimate(Source.EstimateRowsOut(dataSources, options, parameterTypes).Value * 8 / 10);
        }

        protected override IEnumerable<string> GetVariablesInternal()
        {
            return Filter.GetVariables();
        }

        public override object Clone()
        {
            var clone = new FilterNode
            {
                Filter = Filter,
                Source = (IDataExecutionPlanNodeInternal)Source.Clone()
            };

            clone.Source.Parent = clone;
            return clone;
        }
    }
}
