﻿using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.Engine.FetchXml;
using MarkMpn.Sql4Cds.Engine.Visitors;
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
        /// Indicates if the filter should be evaluated during startup only
        /// </summary>
        [Category("Filter")]
        [DisplayName("Startup Expression")]
        [Description("Indicates if the filter shold be evaluated during startup only")]
        public bool StartupExpression { get; set; }

        /// <summary>
        /// The data source to select from
        /// </summary>
        [Browsable(false)]
        public IDataExecutionPlanNodeInternal Source { get; set; }

        protected override IEnumerable<Entity> ExecuteInternal(NodeExecutionContext context)
        {
            var schema = Source.GetSchema(context);
            var expressionCompilationContext = new ExpressionCompilationContext(context, schema, null);
            var filter = Filter.Compile(expressionCompilationContext);
            var expressionContext = new ExpressionExecutionContext(context);

            if (StartupExpression && !filter(expressionContext))
                yield break;

            foreach (var entity in Source.Execute(context))
            {
                expressionContext.Entity = entity;

                if (filter(expressionContext))
                    yield return entity;
            }
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            yield return Source;
        }

        public override INodeSchema GetSchema(NodeCompilationContext context)
        {
            var schema = Source.GetSchema(context);
            var notNullSchema = new NodeSchema(schema);
            AddNotNullColumns(notNullSchema, Filter, false);

            return notNullSchema;
        }

        private void AddNotNullColumns(NodeSchema schema, BooleanExpression filter, bool not)
        {
            if (filter is BooleanBinaryExpression binary)
            {
                if (not ^ binary.BinaryExpressionType == BooleanBinaryExpressionType.Or)
                    return;

                AddNotNullColumns(schema, binary.FirstExpression, not);
                AddNotNullColumns(schema, binary.SecondExpression, not);
            }

            if (filter is BooleanIsNullExpression isNull)
            {
                if (not ^ isNull.IsNot)
                    AddNotNullColumn(schema, isNull.Expression);
            }

            if (!not && filter is BooleanComparisonExpression cmp)
            {
                AddNotNullColumn(schema, cmp.FirstExpression);
                AddNotNullColumn(schema, cmp.SecondExpression);
            }

            if (filter is BooleanParenthesisExpression paren)
            {
                AddNotNullColumns(schema, paren.Expression, not);
            }

            if (filter is BooleanNotExpression n)
            {
                AddNotNullColumns(schema, n.Expression, !not);
            }

            if (!not && filter is InPredicate inPred && !inPred.NotDefined)
            {
                AddNotNullColumn(schema, inPred.Expression);
            }

            if (!not && filter is LikePredicate like && !like.NotDefined)
            {
                AddNotNullColumn(schema, like.FirstExpression);
                AddNotNullColumn(schema, like.SecondExpression);
            }

            if (!not && filter is FullTextPredicate fullText)
            {
                foreach (var col in fullText.Columns)
                    AddNotNullColumn(schema, col);
            }
        }

        private void AddNotNullColumn(NodeSchema schema, ScalarExpression expr)
        {
            if (!(expr is ColumnReferenceExpression col))
                return;

            if (!schema.ContainsColumn(col.GetColumnName(), out var colName))
                return;

            ((ColumnList)schema.Schema)[colName] = schema.Schema[colName].NotNull();
        }

        public override IDataExecutionPlanNodeInternal FoldQuery(NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            // Swap filter to come after sort
            if (Source is SortNode sort)
            {
                Source = sort.Source;
                sort.Source = this;
                return sort.FoldQuery(context, hints);
            }

            Filter = FoldNotIsNullToIsNotNull(Filter);

            // If we have a filter which implies a non-null value for a column that is generated by an outer join,
            // we can convert the join to an inner join *before folding the join* to give more options on how that
            // join can be folded.
            ConvertOuterJoinsWithNonNullFiltersToInnerJoins(context);

            Source = Source.FoldQuery(context, hints);
            Source.Parent = this;

            var foldedFilters = false;

            foldedFilters |= FoldConsecutiveFilters();
            foldedFilters |= RemoveTautologyAndContradiction(context, hints);
            foldedFilters |= FoldNestedLoopFiltersToJoins(context, hints);
            foldedFilters |= FoldInExistsToFetchXml(context, hints, out var addedLinks, out var subqueryConditions);
            foldedFilters |= FoldTableSpoolToIndexSpool(context, hints);
            foldedFilters |= ExpandFiltersOnColumnComparisons(context);
            foldedFilters |= FoldFiltersToDataSources(context, hints, subqueryConditions, out var dynamicValuesLoop);
            foldedFilters |= FoldFiltersToInnerJoinSources(context, hints);
            foldedFilters |= FoldFiltersToSpoolSource(context, hints);
            foldedFilters |= FoldFiltersToNestedLoopCondition(context, hints);

            foreach (var addedLink in addedLinks)
            {
                addedLink.Key.SemiJoin = true;
                addedLink.Value.ResetSchemaCache();
            }

            if (dynamicValuesLoop != null)
            {
                var innerLoop = dynamicValuesLoop;
                while (innerLoop.RightSource != null)
                    innerLoop = (NestedLoopNode)innerLoop.RightSource;

                innerLoop.RightSource = Source;
                Source = dynamicValuesLoop;
            }

            // Some of the filters have been folded into the source. Fold the sources again as the filter can have changed estimated row
            // counts and lead to a better execution plan.
            if (foldedFilters)
                Source = Source.FoldQuery(context, hints);

            // All the filters have been folded into the source. 
            if (Filter == null)
                return Source;

            if (FoldScalarSubqueries(context, out var subqueryLoop))
                return subqueryLoop.FoldQuery(context, hints);

            // Check if we can apply the filter during startup instead of per-record
            StartupExpression = CheckStartupExpression();

            return this;
        }

        private bool RemoveTautologyAndContradiction(NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            if (Filter == null)
                return false;

            var modified = false;
            var schema = Source.GetSchema(context);
            Filter = RemoveTautologyAndContradiction(Filter, context, schema, ref modified, out var result);

            if (result == true)
            {
                Debug.Assert(Filter == null);
            }
            else if (result == false)
            {
                Filter = null;

                var constantScan = new ConstantScanNode();
                foreach (var col in schema.Schema)
                    constantScan.Schema[col.Key] = col.Value;

                Source = constantScan;
            }

            return modified;
        }

        private BooleanExpression RemoveTautologyAndContradiction(BooleanExpression filter, NodeCompilationContext context, INodeSchema schema, ref bool modified, out bool? result)
        {
            result = null;

            if (filter is BooleanParenthesisExpression paren)
            {
                paren.Expression = RemoveTautologyAndContradiction(paren.Expression, context, schema, ref modified, out result);
                if (paren.Expression == null)
                    return null;
            }

            if (filter is BooleanBinaryExpression bin)
            {
                bin.FirstExpression = RemoveTautologyAndContradiction(bin.FirstExpression, context, schema, ref modified, out var firstResult);
                bin.SecondExpression = RemoveTautologyAndContradiction(bin.SecondExpression, context, schema, ref modified, out var secondResult);

                if (bin.BinaryExpressionType == BooleanBinaryExpressionType.And)
                {
                    if (firstResult == false || secondResult == false)
                    {
                        result = false;
                        return null;
                    }

                    if (firstResult == true)
                        return bin.SecondExpression;

                    if (secondResult == true)
                        return bin.FirstExpression;
                }
                else if (bin.BinaryExpressionType == BooleanBinaryExpressionType.Or)
                {
                    if (firstResult == true || secondResult == true)
                    {
                        result = true;
                        return null;
                    }

                    if (firstResult == false)
                        return bin.SecondExpression;

                    if (secondResult == false)
                        return bin.FirstExpression;
                }
            }

            if (filter is BooleanIsNullExpression isNull)
            {
                if (isNull.Expression is ColumnReferenceExpression colRef &&
                    schema.ContainsColumn(colRef.GetColumnName(), out var colName))
                {
                    var schemaCol = schema.Schema[colName];

                    // IS NOT NULL on a non-nullable column is always true,
                    // IS NULL on a non-nullable column is always false
                    if (!schemaCol.IsNullable)
                    {
                        result = isNull.IsNot;
                        modified = true;
                        return null;
                    }
                }
            }

            if (filter is BooleanComparisonExpression cmp)
            {
                if (cmp.FirstExpression is ColumnReferenceExpression colRef1 &&
                    cmp.SecondExpression is ColumnReferenceExpression colRef2 &&
                    schema.ContainsColumn(colRef1.GetColumnName(), out var colName1) &&
                    schema.ContainsColumn(colRef2.GetColumnName(), out var colName2) &&
                    colName1 == colName2)
                {
                    if ((cmp.ComparisonType == BooleanComparisonType.Equals || cmp.ComparisonType == BooleanComparisonType.LessThanOrEqualTo || cmp.ComparisonType == BooleanComparisonType.GreaterThanOrEqualTo) && !schema.Schema[colName1].IsNullable)
                    {
                        // a = a (or a <= a, a >= a) is always true so long as the column is not nullable
                        result = true;
                        modified = true;
                        return null;
                    }
                    else if (cmp.ComparisonType == BooleanComparisonType.IsNotDistinctFrom)
                    {
                        // a IS NOT DISTINCT FROM a is always true even if the column is nullable
                        result = true;
                        modified = true;
                        return null;
                    }
                    else if ((cmp.ComparisonType == BooleanComparisonType.NotEqualToExclamation || cmp.ComparisonType == BooleanComparisonType.NotEqualToBrackets) && !schema.Schema[colName1].IsNullable)
                    {
                        // a <> a is always false so long as the column is not nullable
                        result = false;
                        modified = true;
                        return null;
                    }
                    else if (cmp.ComparisonType == BooleanComparisonType.IsDistinctFrom)
                    {
                        // a IS DISTINCT FROM a is always false even if the column is nullable
                        result = false;
                        modified = true;
                        return null;
                    }
                    else if (cmp.ComparisonType == BooleanComparisonType.LessThan || cmp.ComparisonType == BooleanComparisonType.GreaterThan)
                    {
                        // a < a (or a > a) is always false
                        result = false;
                        modified = true;
                        return null;
                    }
                }
            }

            if (filter.IsConstantValueExpression(new ExpressionCompilationContext(context, schema, null), out var value))
            {
                result = value;
                modified = true;
                return null;
            }

            return filter;
        }

        private bool FoldFiltersToNestedLoopCondition(NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            if (Filter == null)
                return false;

            if (!(Source is NestedLoopNode loop) || loop.JoinType != QualifiedJoinType.Inner)
                return false;

            // Can't move the filter to the loop condition if we're using any of the defined values created by the loop
            if (Filter.GetColumns().Any(c => loop.DefinedValues.ContainsKey(c)))
                return false;

            if (loop.JoinCondition == null)
            {
                loop.JoinCondition = Filter;
            }
            else
            {
                loop.JoinCondition = new BooleanBinaryExpression
                {
                    FirstExpression = loop.JoinCondition,
                    BinaryExpressionType = BooleanBinaryExpressionType.And,
                    SecondExpression = Filter
                };
            }

            Filter = null;
            return true;
        }

        private bool FoldFiltersToSpoolSource(NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            if (Filter == null)
                return false;

            if (!(Source is TableSpoolNode spool))
                return false;

            var usesVariables = Filter.GetVariables().Any();

            if (usesVariables)
                return false;

            spool.Source = new FilterNode
            {
                Source = spool.Source,
                Filter = Filter
            };

            Filter = null;

            return true;
        }

        private bool FoldFiltersToInnerJoinSources(NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            if (Filter == null)
                return false;

            if (!(Source is BaseJoinNode join) || join.JoinType != QualifiedJoinType.Inner)
                return false;

            var folded = false;
            var leftSchema = join.LeftSource.GetSchema(context);
            Filter = ExtractChildFilters(Filter, leftSchema, col => leftSchema.ContainsColumn(col, out _), out var leftFilter);

            if (leftFilter != null)
            {
                join.LeftSource = new FilterNode
                {
                    Source = join.LeftSource,
                    Filter = leftFilter
                }.FoldQuery(context, hints);
                join.LeftSource.Parent = join;

                folded = true;
            }

            if (Filter == null)
                return true;

            var rightContext = context;

            if (join is NestedLoopNode loop && loop.OuterReferences != null)
            {
                var innerParameterTypes = loop.OuterReferences
                    .Select(or => new KeyValuePair<string, DataTypeReference>(or.Value, leftSchema.Schema[or.Key].Type))
                    .ToDictionary(p => p.Key, p => p.Value, StringComparer.OrdinalIgnoreCase);

                rightContext = context.CreateChildContext(innerParameterTypes);
            }

            var rightSchema = join.RightSource.GetSchema(rightContext);
            Filter = ExtractChildFilters(Filter, rightSchema, col => rightSchema.ContainsColumn(col, out _) || join.DefinedValues.ContainsKey(col), out var rightFilter);

            if (rightFilter != null)
            {
                if (join.DefinedValues.Count > 0)
                {
                    var rewrite = new RewriteVisitor(join.DefinedValues.ToDictionary(kvp => (ScalarExpression)kvp.Key.ToColumnReference(), kvp => (ScalarExpression)kvp.Value.ToColumnReference()));
                    rightFilter.Accept(rewrite);
                }

                join.RightSource = new FilterNode
                {
                    Source = join.RightSource,
                    Filter = rightFilter
                }.FoldQuery(rightContext, hints);
                join.RightSource.Parent = join;

                folded = true;
            }

            if (folded)
            {
                // Re-fold the join
                Source = Source.FoldQuery(context, hints);
                Source.Parent = this;
            }

            return folded;
        }

        private bool CheckStartupExpression()
        {
            // We only need to apply the filter expression to individual rows if it references any fields
            if (Filter.GetColumns().Any())
                return false;

            return true;
        }

        private BooleanExpression FoldNotIsNullToIsNotNull(BooleanExpression filter)
        {
            var visitor = new RefactorNotIsNullVisitor(filter);
            return visitor.Replacement;
        }

        private bool ExpandFiltersOnColumnComparisons(NodeCompilationContext context)
        {
            // Expand filters so if we have:
            // col1 < col2 AND col1 = 5
            // we also know:
            // col2 > 5
            // We can often fold these additional filters down to the data sources to reduce the number of records we need to retrieve
            // before performing the column comparison in memory

            // Start by getting the graph of column comparisons from the filter
            var schema = Source.GetSchema(context);
            var columns = new Dictionary<string, Column>(StringComparer.OrdinalIgnoreCase);
            FindColumnComparisons(columns, Filter, schema);

            // Add more column comparisons from any source joins
            FindColumnComparisons(columns, Source, context);

            var removeConditions = new List<BooleanExpression>();
            BooleanExpression additionalFilter = null;
            ExpandFiltersOnColumnComparisons(columns, Filter, schema, removeConditions, ref additionalFilter);

            if (additionalFilter == null)
                return false;

            foreach (var condition in removeConditions)
                Filter = Filter.RemoveCondition(condition);

            Filter = Filter.And(additionalFilter);

            return true;
        }

        private void FindColumnComparisons(Dictionary<string, Column> columns, IDataExecutionPlanNodeInternal source, NodeCompilationContext context)
        {
            if (source == null)
                return;

            if (source is BaseJoinNode join && join.JoinType == QualifiedJoinType.Inner)
            {
                var schema = join.GetSchema(context);

                if (join is NestedLoopNode loop)
                {
                    FindColumnComparisons(columns, loop.JoinCondition, schema);
                }
                else if (join is FoldableJoinNode foldable)
                {
                    FindColumnComparisons(columns, new BooleanComparisonExpression { FirstExpression = foldable.LeftAttribute, ComparisonType = BooleanComparisonType.Equals, SecondExpression = foldable.RightAttribute }, schema);
                    FindColumnComparisons(columns, foldable.AdditionalJoinCriteria, schema);
                }

                FindColumnComparisons(columns, join.LeftSource, context);
                FindColumnComparisons(columns, join.RightSource, context);
            }
        }

        private void ExpandFiltersOnColumnComparisons(Dictionary<string, Column> columns, BooleanExpression filter, INodeSchema schema, List<BooleanExpression> removeConditions, ref BooleanExpression additionalFilter)
        {
            if (filter is BooleanBinaryExpression bin && bin.BinaryExpressionType == BooleanBinaryExpressionType.And)
            {
                ExpandFiltersOnColumnComparisons(columns, bin.FirstExpression, schema, removeConditions, ref additionalFilter);
                ExpandFiltersOnColumnComparisons(columns, bin.SecondExpression, schema, removeConditions, ref additionalFilter);
            }
            else if (filter is BooleanParenthesisExpression paren)
            {
                ExpandFiltersOnColumnComparisons(columns, paren.Expression, schema, removeConditions, ref additionalFilter);
            }
            else if (filter is BooleanComparisonExpression cmp)
            {
                var colRef1 = cmp.FirstExpression as ColumnReferenceExpression;
                var colRef2 = cmp.SecondExpression as ColumnReferenceExpression;

                if (colRef1 != null && colRef2 == null &&
                    schema.ContainsColumn(colRef1.GetColumnName(), out var colName1) &&
                    columns.TryGetValue(colName1, out var col1))
                {
                    ExpandFiltersOnColumnComparisons(new HashSet<Column> { col1 }, col1, cmp.ComparisonType, cmp.SecondExpression, removeConditions, ref additionalFilter);
                }
                else if (colRef1 == null && colRef2 != null &&
                    schema.ContainsColumn(colRef2.GetColumnName(), out var colName2) &&
                    columns.TryGetValue(colName2, out var col2))
                {
                    ExpandFiltersOnColumnComparisons(new HashSet<Column> { col2 }, col2, cmp.ComparisonType.TransitiveComparison(), cmp.FirstExpression, removeConditions, ref additionalFilter);
                }
            }
            else if (filter is InPredicate @in)
            {
                var colRef = @in.Expression as ColumnReferenceExpression;

                if (colRef != null &&
                    schema.ContainsColumn(colRef.GetColumnName(), out var colName) &&
                    columns.TryGetValue(colName, out var col))
                {
                    ExpandFiltersOnColumnComparisons(new HashSet<Column> { col }, col, @in, ref additionalFilter);
                }
            }
        }

        private void ExpandFiltersOnColumnComparisons(HashSet<Column> addedColumns, Column col, BooleanComparisonType comparisonType, ScalarExpression value, List<BooleanExpression> removeConditions, ref BooleanExpression additionalFilter)
        {
            foreach (var comparison in col.Comparisons)
            {
                if (comparison.Comparison != BooleanComparisonType.Equals && comparisonType != BooleanComparisonType.Equals)
                    continue;

                if (!addedColumns.Add(comparison.Column2))
                    continue;

                additionalFilter = additionalFilter.And(new BooleanComparisonExpression
                {
                    FirstExpression = comparison.Column2.ColumnName.ToColumnReference(),
                    ComparisonType = comparisonType == BooleanComparisonType.Equals ? comparison.Comparison : comparisonType,
                    SecondExpression = value
                });

                if (comparison.Comparison == BooleanComparisonType.Equals && comparisonType == BooleanComparisonType.Equals)
                {
                    // If we knew "a = b AND a = 1" and we've now added " AND b = 1", we can also remove "a = b"
                    if (comparison.Expression != null)
                        removeConditions.Add(comparison.Expression);

                    ExpandFiltersOnColumnComparisons(addedColumns, comparison.Column2, comparisonType, value, removeConditions, ref additionalFilter);
                }
            }
        }

        private void ExpandFiltersOnColumnComparisons(HashSet<Column> addedColumns, Column col, InPredicate @in, ref BooleanExpression additionalFilter)
        {
            foreach (var comparison in col.Comparisons)
            {
                if (comparison.Comparison != BooleanComparisonType.Equals)
                    continue;

                if (!addedColumns.Add(comparison.Column2))
                    continue;

                var newIn = new InPredicate
                {
                    Expression = comparison.Column2.ColumnName.ToColumnReference(),
                    NotDefined = @in.NotDefined,
                    Subquery = @in.Subquery
                };

                foreach (var val in @in.Values)
                    newIn.Values.Add(val);

                additionalFilter = additionalFilter.And(newIn);

                ExpandFiltersOnColumnComparisons(addedColumns, comparison.Column2, @in, ref additionalFilter);
            }
        }

        private void FindColumnComparisons(Dictionary<string, Column> columns, BooleanExpression filter, INodeSchema schema)
        {
            if (filter is BooleanBinaryExpression bin && bin.BinaryExpressionType == BooleanBinaryExpressionType.And)
            {
                FindColumnComparisons(columns, bin.FirstExpression, schema);
                FindColumnComparisons(columns, bin.SecondExpression, schema);
            }
            else if (filter is BooleanParenthesisExpression paren)
            {
                FindColumnComparisons(columns, paren.Expression, schema);
            }
            else if (filter is BooleanComparisonExpression cmp &&
                cmp.FirstExpression is ColumnReferenceExpression colRef1 &&
                cmp.SecondExpression is ColumnReferenceExpression colRef2 &&
                schema.ContainsColumn(colRef1.GetColumnName(), out var colName1) &&
                schema.ContainsColumn(colRef2.GetColumnName(), out var colName2))
            {
                if (!columns.TryGetValue(colName1, out var col1))
                {
                    col1 = new Column { ColumnName = colName1 };
                    columns.Add(colName1, col1);
                }

                if (!columns.TryGetValue(colName2, out var col2))
                {
                    col2 = new Column { ColumnName = colName2 };
                    columns.Add(colName2, col2);
                }

                col1.Comparisons.Add(new ColumnComparison { Comparison = cmp.ComparisonType, Column2 = col2, Expression = cmp });
                col2.Comparisons.Add(new ColumnComparison { Comparison = cmp.ComparisonType.TransitiveComparison(), Column2 = col1, Expression = cmp });
            }
        }

        class Column
        {
            public string ColumnName { get; set; }

            public List<ColumnComparison> Comparisons { get; } = new List<ColumnComparison>();
        }

        class ColumnComparison
        {
            public BooleanComparisonType Comparison { get; set; }
            public Column Column2 { get; set; }
            public BooleanExpression Expression { get; set; }
        }

        private bool FoldScalarSubqueries(NodeCompilationContext context, out NestedLoopNode nestedLoop)
        {
            // If we have a filter condition that uses a non-correlated scalar subquery, e.g.
            //
            // SELECT * FROM account WHERE primarycontactid = (SELECT contactid FROM contact WHERE firstname = 'Mark')
            //
            // this will produce a query plan that looks like:
            //
            // SELECT ━━ FILTER ━━ NESTED LOOP ━━ FETCHXML (account)
            //                          ┕━━ TABLE SPOOL ━━ ASSERT ━━ STREAM AGGREGATE ━━ FETCHXML (contact)
            //
            // Because the subquery is uncorrelated, we can move the subquery to the outer query and use the result to
            // fold the filter down to the source. This will produce a query plan that looks like:
            //
            // SELECT ━━ NESTED LOOP ━━ ASSERT ━━ STREAM AGGREGATE ━━ FETCHXML (contact)
            //                ┕━━ FILTER ━━ FETCHXML (account)
            //
            // The filter may then be able to be folded into the source FetchXML

            nestedLoop = null;

            if (Source is NestedLoopNode sourceLoop &&
                (sourceLoop.JoinType == QualifiedJoinType.LeftOuter || sourceLoop.JoinType == QualifiedJoinType.Inner) &&
                sourceLoop.SemiJoin &&
                sourceLoop.JoinCondition == null &&
                sourceLoop.OuterReferences.Count == 0 &&
                sourceLoop.DefinedValues.Count == 1 &&
                Filter.GetColumns().Contains(sourceLoop.DefinedValues.Single().Key) &&
                sourceLoop.RightSource.EstimateRowsOut(context) is RowCountEstimateDefiniteRange subqueryEstimate &&
                subqueryEstimate.Maximum == 1)
            {
                var scalarSubqueryCol = sourceLoop.DefinedValues.Single().Key;
                var scalarSubquerySource = sourceLoop.DefinedValues.Single().Value;

                // The filter would previously have been e.g. "primarycontactid = Expr1"
                // Expr1 will now be provided to the inner loop as a parameter, so we need to rewrite the filter to use it as
                // "primarycontactid = @Expr2"
                var rewrite = new RewriteVisitor(new Dictionary<ScalarExpression, ScalarExpression>
                {
                    { new ColumnReferenceExpression { MultiPartIdentifier = new MultiPartIdentifier { Identifiers = { new Identifier { Value = scalarSubqueryCol } } } }, new VariableReference { Name = "@" + scalarSubqueryCol } }
                });
                var filter = Filter.Clone();
                filter.Accept(rewrite);

                nestedLoop = new NestedLoopNode
                {
                    JoinType = QualifiedJoinType.Inner,
                    OuterReferences = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
                    {
                        { scalarSubquerySource, "@" + scalarSubqueryCol }
                    },
                    LeftSource = sourceLoop.RightSource,
                    RightSource = filter == null ? sourceLoop.LeftSource : new FilterNode
                    {
                        Filter = filter,
                        Source = sourceLoop.LeftSource
                    }
                };

                if (nestedLoop.LeftSource is TableSpoolNode subquerySpool)
                    nestedLoop.LeftSource = subquerySpool.Source;

                return true;
            }

            return false;
        }

        private void ConvertOuterJoinsWithNonNullFiltersToInnerJoins(NodeCompilationContext context)
        {
            var schema = Source.GetSchema(context);
            var nullableColumns = new ColumnList();

            foreach (var col in schema.Schema.Where(col => col.Value.IsNullable))
                nullableColumns.Add(col.Key, col.Value);

            var notNullSchema = new NodeSchema(nullableColumns, null, null, null);
            AddNotNullColumns(notNullSchema, Filter, false);

            var notNullColumns = notNullSchema.Schema
                .Where(col => !col.Value.IsNullable)
                .Select(col => col.Key)
                .ToList();

            if (notNullColumns.Any())
                ConvertOuterJoinsWithNonNullFiltersToInnerJoins(context, Source, notNullColumns);
        }

        private void ConvertOuterJoinsWithNonNullFiltersToInnerJoins(NodeCompilationContext context, IDataExecutionPlanNodeInternal source, List<string> notNullColumns)
        {
            if (source is BaseJoinNode join && !join.SemiJoin)
            {
                IDataExecutionPlanNodeInternal outerSource = null;

                if (join.JoinType == QualifiedJoinType.LeftOuter)
                    outerSource = join.OutputRightSchema ? join.RightSource : null;
                else if (join.JoinType == QualifiedJoinType.RightOuter)
                    outerSource = join.OutputLeftSchema ? join.LeftSource : null;

                if (outerSource != null)
                {
                    // If we are enforcing a non-null constraint on the outer source, we can convert the join to an inner join
                    // To get the schema of the outer source, we need to include any outer references that are used in the join condition
                    var outerContext = context;
                    
                    if (join.JoinType == QualifiedJoinType.LeftOuter && join is NestedLoopNode loop && loop.OuterReferences != null && loop.OuterReferences.Count > 0)
                    {
                        var leftSchema = join.LeftSource.GetSchema(context);

                        var innerParameterTypes = loop.OuterReferences
                            .Select(or => new KeyValuePair<string, DataTypeReference>(or.Value, leftSchema.Schema[or.Key].Type))
                            .ToDictionary(p => p.Key, p => p.Value, StringComparer.OrdinalIgnoreCase);

                        outerContext = context.CreateChildContext(innerParameterTypes);
                    }

                    var outerSchema = outerSource.GetSchema(outerContext);

                    if (notNullColumns.Any(col => outerSchema.ContainsColumn(col, out _)))
                        join.JoinType = QualifiedJoinType.Inner;
                }
            }

            foreach (var child in source.GetSources().OfType<IDataExecutionPlanNodeInternal>())
                ConvertOuterJoinsWithNonNullFiltersToInnerJoins(context, child, notNullColumns);
        }

        private bool FoldConsecutiveFilters()
        {
            if (Source is FilterNode filter)
            {
                filter.Filter = filter.Filter.And(Filter);
                Filter = null;
                return true;
            }

            return false;
        }

        private bool FoldNestedLoopFiltersToJoins(NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            // Queries like "FROM table1, table2 WHERE table1.col = table2.col" are created as:
            // Filter: table1.col = table2.col
            // -> NestedLoop (Inner), null join condition, no defined values
            //    -> FetchXml
            //    -> Table Spool
            //       -> FetchXml
            if (FoldNestedLoopFiltersToJoins(Source as BaseJoinNode, context, hints, out var foldedJoin))
            {
                Source = foldedJoin;
                return true;
            }

            return false;
        }

        private bool FoldNestedLoopFiltersToJoins(BaseJoinNode join, NodeCompilationContext context, IList<OptimizerHint> hints, out FoldableJoinNode foldedJoin)
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
                var leftSchema = join.LeftSource.GetSchema(context);
                var rightSchema = join.RightSource.GetSchema(context);

                if (ExtractJoinCondition(Filter, loop, context, leftSchema, rightSchema, out foldedJoin, out var removedCondition))
                {
                    Filter = Filter.RemoveCondition(removedCondition);
                    foldedFilters = true;
                    join = foldedJoin;
                }
            }

            if (FoldNestedLoopFiltersToJoins(join.LeftSource as BaseJoinNode, context, hints, out var foldedLeftSource))
            {
                join.LeftSource = foldedLeftSource;
                foldedFilters = true;
            }

            if (FoldNestedLoopFiltersToJoins(join.RightSource as BaseJoinNode, context, hints, out var foldedRightSource))
            {
                join.RightSource = foldedRightSource;
                foldedFilters = true;
            }

            return foldedFilters;
        }

        private bool ExtractJoinCondition(BooleanExpression filter, NestedLoopNode join, NodeCompilationContext context, INodeSchema leftSchema, INodeSchema rightSchema, out FoldableJoinNode foldedJoin, out BooleanExpression removedCondition)
        {
            if (filter is BooleanComparisonExpression cmp &&
                cmp.ComparisonType == BooleanComparisonType.Equals)
            {
                var leftSource = join.LeftSource;
                var rightSource = join.RightSource;
                var col1 = cmp.FirstExpression as ColumnReferenceExpression;
                var col2 = cmp.SecondExpression as ColumnReferenceExpression;

                // If join is not directly on a.col = b.col, it may be something that we can calculate such as
                // a.col1 + a.col2 = left(b.col3, 10)
                // Create a ComputeScalar node for each side so the join can work on a single column
                // This only works if each side of the equality expression references columns only from one side of the join
                var originalLeftSource = leftSource;
                var originalRightSource = rightSource;

                if (col1 == null)
                    col1 = ComputeColumn(context, cmp.FirstExpression, ref leftSource, ref leftSchema, ref rightSource, ref rightSchema);

                if (col2 == null)
                    col2 = ComputeColumn(context, cmp.SecondExpression, ref leftSource, ref leftSchema, ref rightSource, ref rightSchema);

                // Equality expression may be written in the opposite order to the join - swap the tables if necessary
                if (col1 != null &&
                    col2 != null &&
                    rightSchema.ContainsColumn(col1.GetColumnName(), out _) &&
                    leftSchema.ContainsColumn(col2.GetColumnName(), out _))
                {
                    Swap(ref leftSource, ref rightSource);
                    Swap(ref leftSchema, ref rightSchema);
                }

                if (col1 != null &&
                    col2 != null &&
                    leftSchema.ContainsColumn(col1.GetColumnName(), out var leftCol) &&
                    rightSchema.ContainsColumn(col2.GetColumnName(), out var rightCol))
                {
                    leftSource = RemoveTableSpool(leftSource);
                    rightSource = RemoveTableSpool(rightSource);

                    // Prefer to use a merge join if either of the join keys are the primary key.
                    // Swap the tables if necessary to use the primary key from the right source.
                    if (leftSchema.PrimaryKey != leftCol && rightSchema.PrimaryKey == rightCol)
                    {
                        Swap(ref leftSource, ref rightSource);
                        Swap(ref leftSchema, ref rightSchema);
                        Swap(ref col1, ref col2);
                        Swap(ref leftCol, ref rightCol);
                    }

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
                if (ExtractJoinCondition(bin.FirstExpression, join, context, leftSchema, rightSchema, out foldedJoin, out removedCondition) ||
                    ExtractJoinCondition(bin.SecondExpression, join, context, leftSchema, rightSchema, out foldedJoin, out removedCondition))
                {
                    return true;
                }
            }

            foldedJoin = null;
            removedCondition = null;
            return false;
        }

        private IDataExecutionPlanNodeInternal RemoveTableSpool(IDataExecutionPlanNodeInternal source)
        {
            if (source is TableSpoolNode spool)
            {
                spool.Source.Parent = source.Parent;
                return spool.Source;
            }

            if (source is ComputeScalarNode computeScalar && computeScalar.Source is TableSpoolNode computeScalarSpool)
            {
                computeScalarSpool.Source.Parent = computeScalar;
                computeScalar.Source = computeScalarSpool.Source;
            }

            return source;
        }

        private ColumnReferenceExpression ComputeColumn(NodeCompilationContext context, ScalarExpression expression, ref IDataExecutionPlanNodeInternal leftSource, ref INodeSchema leftSchema, ref IDataExecutionPlanNodeInternal rightSource, ref INodeSchema rightSchema)
        {
            return ComputeColumn(context, expression, ref leftSource, ref leftSchema) ?? ComputeColumn(context, expression, ref rightSource, ref rightSchema);
        }

        private ColumnReferenceExpression ComputeColumn(NodeCompilationContext context, ScalarExpression expression, ref IDataExecutionPlanNodeInternal source, ref INodeSchema schema)
        {
            var columns = expression.GetColumns().ToList();
            var s = schema;

            if (columns.Count == 0 || !columns.All(c => s.ContainsColumn(c, out _)))
                return null;

            var exprName = context.GetExpressionName();
            var computeScalar = new ComputeScalarNode
            {
                Source = source,
                Columns =
                {
                    [exprName] = expression
                }
            };

            source = computeScalar;
            schema = computeScalar.GetSchema(context);

            return exprName.ToColumnReference();
        }

        private void Swap<T>(ref T first, ref T second)
        {
            var temp = first;
            first = second;
            second = temp;
        }

        private bool FoldInExistsToFetchXml(NodeCompilationContext context, IList<OptimizerHint> hints, out Dictionary<FetchLinkEntityType, FetchXmlScan> addedLinks, out Dictionary<BooleanExpression, ConvertedSubquery> subqueryConditions)
        {
            var foldedFilters = false;

            // Foldable correlated IN queries "lefttable.column IN (SELECT righttable.column FROM righttable WHERE ...) are created as:
            // Filter: Expr2 is [not] null
            // -> FoldableJoin (LeftOuter SemiJoin) Expr2 = righttable.column in DefinedValues; righttable.column in RightAttribute
            //    -> FetchXml
            //    -> FetchXml (Distinct) orderby righttable.column

            // Foldable correlated EXISTS filters "EXISTS (SELECT * FROM righttable WHERE righttable.column = lefttable.column AND ...) are created as:
            // Filter - [not] @var2 is not null
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
            subqueryConditions = new Dictionary<BooleanExpression, ConvertedSubquery>();

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

                // Only supported on regular database tables
                var leftMeta = context.Session.DataSources[leftFetch.DataSource].Metadata[leftFetch.Entity.name];
                if (leftMeta.DataProviderId != null)
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

                    // Can't fold queries using explicit collations
                    if (merge.LeftAttribute.Collation != null || merge.RightAttribute.Collation != null)
                        break;

                    if (!leftFetch.DataSource.Equals(rightFetch.DataSource, StringComparison.OrdinalIgnoreCase))
                        break;

                    var rightSchema = rightFetch.GetSchema(context);

                    if (!rightSchema.ContainsColumn(merge.RightAttribute.GetColumnName(), out var attribute))
                        break;

                    // Can't fold queries using unsupported aliases
                    if (!FetchXmlScan.IsValidAlias(rightFetch.Alias))
                        break;

                    // Right values need to be distinct - still allowed if it's the primary key
                    if (!rightFetch.FetchXml.distinct && rightSchema.PrimaryKey != attribute)
                        break;

                    var definedValueName = join.DefinedValues.SingleOrDefault(kvp => kvp.Value == attribute).Key;

                    if (definedValueName == null)
                        break;

                    var nullFilter = FindNullFilter(Filter, definedValueName, out var nullFilterRemovable);
                    if (nullFilter == null)
                        break;

                    leftAlias = merge.LeftAttribute.MultiPartIdentifier.Identifiers.Reverse().Skip(1).First().Value;

                    // If IN query is on matching primary keys (SELECT name FROM account WHERE accountid IN (SELECT accountid FROM account WHERE ...))
                    // we can eliminate the left join and rewrite as SELECT name FROM account WHERE ....
                    // Can't do this if there is any conflict in join aliases
                    if (nullFilterRemovable &&
                        nullFilter.IsNot &&
                        leftFetch.Entity.name == rightFetch.Entity.name &&
                        merge.LeftAttribute.GetColumnName() == leftFetch.Alias + "." + context.Session.DataSources[leftFetch.DataSource].Metadata[leftFetch.Entity.name].PrimaryIdAttribute &&
                        merge.RightAttribute.GetColumnName() == rightFetch.Alias + "." + context.Session.DataSources[rightFetch.DataSource].Metadata[rightFetch.Entity.name].PrimaryIdAttribute &&
                        !leftFetch.Entity.GetLinkEntities().Select(l => l.alias).Intersect(rightFetch.Entity.GetLinkEntities().Select(l => l.alias), StringComparer.OrdinalIgnoreCase).Any() &&
                        (leftFetch.FetchXml.top == null || rightFetch.FetchXml.top == null))
                    {
                        // Remove any attributes from the subquery. Mark all joins as semi joins so no more attributes will
                        // be added to them using a SELECT * query
                        rightFetch.Entity.RemoveAttributes();

                        if (rightFetch.Entity.Items != null)
                        {
                            foreach (var link in rightFetch.Entity.GetLinkEntities())
                                link.SemiJoin = true;

                            foreach (var item in rightFetch.Entity.Items)
                                leftFetch.Entity.AddItem(item);
                        }

                        if (rightFetch.FetchXml.top != null)
                            leftFetch.FetchXml.top = rightFetch.FetchXml.top;

                        Filter = Filter.RemoveCondition(nullFilter);
                        foldedFilters = true;

                        linkToAdd = null;
                        semiJoin = false;
                    }
                    // We can fold IN to a simple join where the attribute is the primary key. Can't do this
                    // if there are any inner joins though, unless the new join will itself be an inner join.
                    else if (!rightFetch.FetchXml.distinct &&
                        rightSchema.PrimaryKey == attribute &&
                        nullFilter.IsNot &&
                        (nullFilterRemovable || !rightFetch.Entity.GetLinkEntities().Any(link => link.linktype == "inner")) &&
                        rightFetch.FetchXml.top == null)
                    {
                        linkToAdd = new FetchLinkEntityType
                        {
                            name = rightFetch.Entity.name,
                            alias = rightFetch.Alias,
                            from = merge.RightAttribute.MultiPartIdentifier.Identifiers.Last().Value,
                            to = merge.LeftAttribute.MultiPartIdentifier.Identifiers.Last().Value,
                            Items = rightFetch.Entity.Items
                        };

                        if (!CanAddLink(linkToAdd))
                            break;

                        // Replace the filter on the defined value name with a filter on the primary key column
                        nullFilter.Expression = attribute.ToColumnReference();

                        linkToAdd.RemoveSorts();

                        if (nullFilterRemovable)
                        {
                            // IN filter is combined with AND, so all matching records must satisfy the filter
                            // We can use an inner join to simplify the FetchXML, and mark it as a semi join so
                            // none of the attributes in the joined table are exposed in the schema
                            linkToAdd.linktype = "inner";
                            semiJoin = true;

                            // The inner join is now doing the job of the filter, so we can remove the filter
                            Filter = Filter.RemoveCondition(nullFilter);
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
                    else if (nullFilterRemovable &&
                        nullFilter.IsNot &&
                        context.Session.DataSources[rightFetch.DataSource].JoinOperatorsAvailable.Contains(JoinOperator.In) &&
                        rightFetch.FetchXml.top == null)
                    {
                        // Remove the filter and replace with an "in" link-entity
                        linkToAdd = new FetchLinkEntityType
                        {
                            name = rightFetch.Entity.name,
                            from = merge.RightAttribute.MultiPartIdentifier.Identifiers.Last().Value,
                            to = merge.LeftAttribute.MultiPartIdentifier.Identifiers.Last().Value,
                            linktype = "in",
                            Items = rightFetch.Entity.Items
                        };

                        if (!CanAddLink(linkToAdd))
                            break;

                        Filter = Filter.RemoveCondition(nullFilter);
                        foldedFilters = true;

                        linkToAdd.RemoveSorts();

                        semiJoin = true;
                    }
                    // We can use an "any" join type - check that's supported
                    else if (!nullFilterRemovable &&
                        nullFilter.IsNot &&
                        context.Session.DataSources[rightFetch.DataSource].JoinOperatorsAvailable.Contains(JoinOperator.Any) &&
                        rightFetch.FetchXml.top == null)
                    {
                        if (subqueryConditions.ContainsKey(nullFilter))
                            break; // We've already processed this subquery

                        // We can't remove the filter yet - store the details for use later
                        var clone = (FetchXmlScan)rightFetch.Clone();
                        clone.RemoveSorts();
                        clone.RemoveAttributes();
                        subqueryConditions[nullFilter] = new ConvertedSubquery
                        {
                            JoinNode = join,
                            Condition = new FetchLinkEntityType
                            {
                                name = clone.Entity.name,
                                from = merge.RightAttribute.MultiPartIdentifier.Identifiers.Last().Value,
                                to = merge.LeftAttribute.MultiPartIdentifier.Identifiers.Last().Value,
                                linktype = "any",
                                Items = clone.Entity.Items
                            },
                            LinkEntity = leftFetch.Entity.FindLinkEntity(leftAlias)
                        };

                        // Move this join to the start of the list and continue on to the next join
                        joins.Remove(join);
                        joins.Insert(0, join);
                        continue;
                    }
                    // We can fold NOT IN to a left outer join, using similar rules to IN on the primary key
                    else if (!nullFilter.IsNot && rightFetch.FetchXml.top == null)
                    {
                        // Can't add an outer join if the right side includes an inner join
                        // https://github.com/MarkMpn/Sql4Cds/issues/382
                        if (rightFetch.Entity.GetLinkEntities().Any(l => l.linktype == "inner"))
                            break;

                        linkToAdd = new FetchLinkEntityType
                        {
                            name = rightFetch.Entity.name,
                            alias = rightFetch.Alias,
                            from = merge.RightAttribute.MultiPartIdentifier.Identifiers.Last().Value,
                            to = merge.LeftAttribute.MultiPartIdentifier.Identifiers.Last().Value,
                            linktype = "outer",
                            Items = rightFetch.Entity.Items
                        };

                        if (!CanAddLink(linkToAdd))
                            break;

                        // Replace the filter on the defined value name with a filter on the primary key column
                        nullFilter.Expression = (rightFetch.Alias + "." + context.Session.DataSources[rightFetch.DataSource].Metadata[rightFetch.Entity.name].PrimaryIdAttribute).ToColumnReference();

                        linkToAdd.RemoveSorts(true);

                        if (nullFilterRemovable)
                        {
                            // NOT IN filter is combined with AND, so all matching records must satisfy the filter
                            // Mark the join as a semi join so none of the attributes in the joined table are exposed
                            // in the schema
                            semiJoin = true;
                        }
                        else
                        {
                            // NOT IN filter is combined with OR, so we may get matching records that don't satisfy
                            // this join. We need to return the primary key attribute from the join for the filter to
                            // be able to work, but require it to be fully qualified so we don't cause conflicts
                            linkToAdd.RequireTablePrefix = true;
                            semiJoin = false;
                        }
                    }
                    else
                    {
                        break;
                    }

                    // Remove the sort that has been merged into the left side too
                    if (leftFetch.FetchXml.top == null)
                        leftFetch.RemoveSorts();
                }
                else if (join is NestedLoopNode loop)
                {
                    // Check we meet all the criteria for a foldable correlated EXISTS query
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

                    var keyParts = indexSpool.KeyColumn.SplitMultiPartIdentifier();
                    var outerReferenceParts = loop.OuterReferences.Single().Key.SplitMultiPartIdentifier();

                    if (keyParts.Length != 2 ||
                        !keyParts[0].Equals(rightFetch.Alias, StringComparison.OrdinalIgnoreCase))
                        break;

                    var notNullFilter = FindNullFilter(Filter, loop.DefinedValues.Single().Key, out var notNullFilterRemovable);
                    if (notNullFilter == null)
                        break;
                    
                    leftAlias = outerReferenceParts[0];

                    if (context.Session.DataSources[rightFetch.DataSource].JoinOperatorsAvailable.Contains(JoinOperator.Exists) &&
                        notNullFilter.IsNot &&
                        notNullFilterRemovable)
                    {
                        // Remove the filter and replace with an "exists" link-entity
                        linkToAdd = new FetchLinkEntityType
                        {
                            name = rightFetch.Entity.name,
                            from = keyParts[1],
                            to = outerReferenceParts[1],
                            linktype = "exists",
                            Items = rightFetch.Entity.Items
                        };

                        if (!CanAddLink(linkToAdd))
                            break;

                        Filter = Filter.RemoveCondition(notNullFilter);
                        foldedFilters = true;

                        semiJoin = true;
                    }
                    else if (context.Session.DataSources[rightFetch.DataSource].JoinOperatorsAvailable.Contains(JoinOperator.Any) && notNullFilter.IsNot ||
                        context.Session.DataSources[rightFetch.DataSource].JoinOperatorsAvailable.Contains(JoinOperator.NotAny) && !notNullFilter.IsNot)
                    {
                        if (subqueryConditions.ContainsKey(notNullFilter))
                            break; // We've already processed this subquery

                        // We can't remove the filter yet - store the details for use later
                        var clone = (FetchXmlScan)rightFetch.Clone();
                        clone.RemoveSorts();
                        clone.RemoveAttributes();
                        subqueryConditions[notNullFilter] = new ConvertedSubquery
                        {
                            JoinNode = join,
                            Condition = new FetchLinkEntityType
                            {
                                name = clone.Entity.name,
                                from = keyParts[1],
                                to = outerReferenceParts[1],
                                linktype = notNullFilter.IsNot ? "any" : "not any",
                                Items = clone.Entity.Items
                            }.RemoveNotNullJoinCondition(),
                            LinkEntity = leftFetch.Entity.FindLinkEntity(leftAlias)
                        };

                        // Move this join to the start of the list and continue on to the next join
                        joins.Remove(join);
                        joins.Insert(0, join);
                        continue;
                    }
                    else
                    {
                        break;
                    }
                }
                else
                {
                    // This isn't a type of join we can fold as a correlated IN/EXISTS join
                    break;
                }

                if (linkToAdd != null)
                {
                    // Remove any attributes from the new linkentity
                    var tempEntity = new FetchEntityType { Items = new object[] { linkToAdd } };
                    tempEntity.RemoveAttributes();

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

        private static bool CanAddLink(FetchLinkEntityType linkToAdd)
        {
            // Can't add the link if it's got any filter conditions with an entityname
            if (linkToAdd.GetConditions().Any(c => !String.IsNullOrEmpty(c.entityname)))
                return false;

            return true;
        }

        private bool FoldTableSpoolToIndexSpool(NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            // If we've got a filter matching a column and a variable (key lookup in a nested loop) from a table spool, replace it with a index spool
            if (!(Source is TableSpoolNode tableSpool))
                return false;

            var schema = Source.GetSchema(context);

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
            }.FoldQuery(context, hints);

            Filter = filter;
            return true;
        }

        private bool FoldFiltersToDataSources(NodeCompilationContext context, IList<OptimizerHint> hints, Dictionary<BooleanExpression, ConvertedSubquery> subqueryExpressions, out NestedLoopNode nestedLoop)
        {
            nestedLoop = null;

            if (Filter == null)
                return false;

            var foldedFilters = false;

            // Find all the data source nodes we could fold this into. Include direct data sources, those from either side of an inner join, or the main side of an outer join
            // CTEs or other query-derived tables can contain a table with the same alias as a table in the main query. Identify these and prefer using the table
            // from the outer query.
            var ignoreAliasesByNode = GetIgnoreAliasesByNode(context);

            foreach (var sourceAndContext in GetFoldableSources(Source, context))
            {
                var source = sourceAndContext.Key;
                var foldableContext = sourceAndContext.Value;
                var schema = source.GetSchema(foldableContext);

                if (source is FetchXmlScan fetchXml && !fetchXml.FetchXml.aggregate)
                {
                    if (!foldableContext.Session.DataSources.TryGetValue(fetchXml.DataSource, out var dataSource))
                        throw new NotSupportedQueryFragmentException("Missing datasource " + fetchXml.DataSource);

                    // If the criteria are ANDed, see if any of the individual conditions can be translated to FetchXML
                    var originalFilter = fetchXml.IsUnreliableVirtualEntityProvider ? Filter.Clone() : null;

                    Filter = ExtractFetchXMLFilters(
                        foldableContext,
                        dataSource,
                        Filter,
                        schema,
                        null,
                        ignoreAliasesByNode[fetchXml],
                        fetchXml,
                        subqueryExpressions,
                        out var fetchFilter);

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

                    // Virtual entity providers are unreliable - fold the filters to the FetchXML but keep this
                    // node to filter again if necessary
                    if (originalFilter != null)
                        Filter = originalFilter;
                }

                if (source is MetadataQueryNode meta)
                {
                    // If the criteria are ANDed, see if any of the individual conditions can be translated to the metadata query
                    Filter = ExtractMetadataFilters(foldableContext, Filter, meta, out var entityFilter, out var attributeFilter, out var relationshipFilter, out var keyFilter);

                    meta.Query.AddFilter(entityFilter);

                    if (attributeFilter != null && meta.Query.AttributeQuery == null)
                        meta.Query.AttributeQuery = new AttributeQueryExpression();

                    meta.Query.AttributeQuery.AddFilter(attributeFilter);

                    if (relationshipFilter != null && meta.Query.RelationshipQuery == null)
                        meta.Query.RelationshipQuery = new RelationshipQueryExpression();

                    meta.Query.RelationshipQuery.AddFilter(relationshipFilter);

                    if (keyFilter != null && meta.Query.KeyQuery == null)
                        meta.Query.KeyQuery = new EntityKeyQueryExpression();

                    meta.Query.KeyQuery.AddFilter(keyFilter);

                    if (entityFilter != null || attributeFilter != null || relationshipFilter != null || keyFilter != null)
                        foldedFilters = true;
                }

                if (source is AliasNode alias)
                {
                    // Extract filters on this alias and rewrite them to the source columns
                    // Move these filters to within the alias node and re-fold them
                    var escapedAlias = alias.Alias.EscapeIdentifier();
                    var aliasColumns = alias.GetColumnMappings(context);

                    Filter = ExtractChildFilters(Filter, schema, colName => aliasColumns.ContainsKey(colName), out var aliasFilter);

                    if (aliasFilter != null)
                    {
                        var aliasColumnReplacements = alias.GetColumnMappings(context)
                            .ToDictionary(kvp => (ScalarExpression)kvp.Key.ToColumnReference(), col => col.Value);

                        var aliasFilterNode = new FilterNode
                        {
                            Source = alias.Source,
                            Filter = ReplaceColumnNames(aliasFilter, aliasColumnReplacements)
                        };
                        alias.Source = aliasFilterNode;

                        foldedFilters = true;
                    }
                }

                if (source is ConcatenateNode concat)
                {
                    // Duplicate the filters and rewrite them to the source columns of each input
                    // Place these filters within the inputs and re-fold them
                    Filter = ExtractChildFilters(Filter, schema, colName => concat.ColumnSet.Any(col => col.OutputColumn.Equals(colName, StringComparison.OrdinalIgnoreCase)), out var concatFilter);

                    if (concatFilter != null)
                    {
                        for (var i = 0; i < concat.Sources.Count; i++)
                        {
                            var concatFilterNode = new FilterNode
                            {
                                Source = concat.Sources[i],
                                Filter = ReplaceColumnNames(concatFilter, concat.ColumnSet.ToDictionary(col => (ScalarExpression)col.OutputColumn.ToColumnReference(), col => col.SourceColumns[i]))
                            };
                            concat.Sources[i] = concatFilterNode.FoldQuery(foldableContext, hints);
                        }

                        foldedFilters = true;
                    }
                }

                if (Filter == null)
                    break;
            }

            return foldedFilters;
        }

        private void RemoveJoin(BaseJoinNode joinNode)
        {
            // We've consumed the subquery implemented by this join - remove it from the execution plan
            if (joinNode.Parent == this)
            {
                Source = joinNode.LeftSource;
                joinNode.LeftSource.Parent = this;
            }
            else if (joinNode.Parent is BaseJoinNode parentJoin)
            {
                parentJoin.LeftSource = joinNode.LeftSource;
                joinNode.LeftSource.Parent = parentJoin;
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        private Dictionary<FetchXmlScan,HashSet<string>> GetIgnoreAliasesByNode(NodeCompilationContext context)
        {
            var fetchXmlSources = GetFoldableSources(Source, context)
                .Select(sourceAndContext => sourceAndContext.Key)
                .OfType<FetchXmlScan>()
                .ToList();

            // Build a full list of where we see each alias
            var nodesByAlias = new Dictionary<string, List<FetchXmlScan>>(StringComparer.OrdinalIgnoreCase);

            foreach (var fetchXml in fetchXmlSources)
            {
                foreach (var alias in GetAliases(fetchXml))
                {
                    if (!nodesByAlias.TryGetValue(alias, out var nodes))
                    {
                        nodes = new List<FetchXmlScan>();
                        nodesByAlias.Add(alias, nodes);
                    }

                    nodes.Add(fetchXml);
                }
            }

            var ignoreAliases = fetchXmlSources
                .ToDictionary(f => f, f => new HashSet<string>(StringComparer.OrdinalIgnoreCase));

            // Aliases should be ignored if they appear as visible in another node, or they are hidden in all nodes
            foreach (var alias in nodesByAlias)
            {
                if (alias.Value.Count < 2)
                    continue;

                var hiddenNodes = alias.Value
                    .Where(n => n.HiddenAliases.Contains(alias.Key, StringComparer.OrdinalIgnoreCase))
                    .ToList();

                var visibleNodes = alias.Value
                    .Except(hiddenNodes)
                    .ToList();

                foreach (var node in alias.Value)
                {
                    if (visibleNodes.Count == 0 || visibleNodes.Any(n => n != node))
                        ignoreAliases[node].Add(alias.Key);
                }
            }

            return ignoreAliases;
        }

        private IEnumerable<string> GetAliases(FetchXmlScan fetchXml)
        {
            yield return fetchXml.Alias;

            foreach (var linkEntity in fetchXml.Entity.GetLinkEntities())
            {
                if (linkEntity.alias != null)
                    yield return linkEntity.alias;
            }
        }

        private BooleanExpression ReplaceColumnNames(BooleanExpression filter, Dictionary<ScalarExpression, string> replacements)
        {
            filter = filter.Clone();
            filter.Accept(new RewriteVisitor(replacements));
            return filter;
        }

        private BooleanIsNullExpression FindNullFilter(BooleanExpression filter, string attribute, out bool and)
        {
            if (filter is BooleanIsNullExpression isNull &&
                isNull.Expression is ColumnReferenceExpression col &&
                col.GetColumnName().Equals(attribute, StringComparison.OrdinalIgnoreCase))
            {
                and = true;
                return isNull;
            }

            if (filter is BooleanParenthesisExpression paren)
                return FindNullFilter(paren.Expression, attribute, out and);

            if (filter is BooleanBinaryExpression bin)
            {
                var @null = FindNullFilter(bin.FirstExpression, attribute, out and) ?? FindNullFilter(bin.SecondExpression, attribute, out and);

                if (@null != null)
                {
                    and &= bin.BinaryExpressionType == BooleanBinaryExpressionType.And;
                    return @null;
                }
            }

            and = false;
            return null;
        }

        private IEnumerable<KeyValuePair<IDataExecutionPlanNodeInternal,NodeCompilationContext>> GetFoldableSources(IDataExecutionPlanNodeInternal source, NodeCompilationContext context)
        {
            if (source is FetchXmlScan ||
                source is MetadataQueryNode ||
                source is AliasNode ||
                source is ConcatenateNode)
            {
                yield return new KeyValuePair<IDataExecutionPlanNodeInternal, NodeCompilationContext>(source, context);
                yield break;
            }

            if (source is BaseJoinNode join)
            {
                if (join.JoinType == QualifiedJoinType.Inner || join.JoinType == QualifiedJoinType.LeftOuter)
                {
                    foreach (var subSource in GetFoldableSources(join.LeftSource, context))
                        yield return subSource;
                }

                if (join.JoinType == QualifiedJoinType.Inner || join.JoinType == QualifiedJoinType.RightOuter)
                {
                    var childContext = context;

                    if (join is NestedLoopNode loop && loop.OuterReferences != null && loop.OuterReferences.Count > 0)
                    {
                        var leftSchema = join.LeftSource.GetSchema(context);
                        var innerParameterTypes = loop.OuterReferences
                            .Select(or => new KeyValuePair<string,DataTypeReference>(or.Value, leftSchema.Schema[or.Key].Type))
                            .ToDictionary(p => p.Key, p => p.Value, StringComparer.OrdinalIgnoreCase);

                        childContext = context.CreateChildContext(innerParameterTypes);
                    }

                    foreach (var subSource in GetFoldableSources(join.RightSource, childContext))
                        yield return subSource;
                }

                yield break;
            }

            if (source is HashMatchAggregateNode)
                yield break;

            if (source is TableSpoolNode || source is IndexSpoolNode)
                yield break;

            foreach (var subSource in source.GetSources().OfType<IDataExecutionPlanNodeInternal>())
            {
                foreach (var foldableSubSource in GetFoldableSources(subSource, context))
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
                    cmp.SecondExpression is VariableReference var2 &&
                    col1.Collation == null &&
                    var2.Collation == null)
                {
                    indexColumn = col1.GetColumnName();
                    seekVariable = var2.Name;
                    return true;
                }
                else if (cmp.FirstExpression is VariableReference var1 &&
                    cmp.SecondExpression is ColumnReferenceExpression col2 &&
                    var1.Collation == null &&
                    col2.Collation == null)
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

        public override void AddRequiredColumns(NodeCompilationContext context, IList<string> requiredColumns)
        {
            var schema = Source.GetSchema(context);

            foreach (var col in Filter.GetColumns())
            {
                if (!schema.ContainsColumn(col, out var normalized))
                    continue;

                if (!requiredColumns.Contains(normalized, StringComparer.OrdinalIgnoreCase))
                    requiredColumns.Add(normalized);
            }

            Source.AddRequiredColumns(context, requiredColumns);
        }

        private BooleanExpression ExtractFetchXMLFilters(NodeCompilationContext context, DataSource dataSource, BooleanExpression criteria, INodeSchema schema, string allowedPrefix, HashSet<string> barredPrefixes, FetchXmlScan fetchXmlScan, Dictionary<BooleanExpression, ConvertedSubquery> subqueryExpressions, out filter filter)
        {
            var targetEntityName = fetchXmlScan.Entity.name;
            var targetEntityAlias = fetchXmlScan.Alias;
            var items = fetchXmlScan.Entity.Items;

            var subqueryConditions = new HashSet<BooleanExpression>();
            var result = ExtractFetchXMLFilters(context, dataSource, criteria, schema, allowedPrefix, barredPrefixes, targetEntityName, targetEntityAlias, items, subqueryExpressions, subqueryConditions, out filter);

            if (result == criteria)
                return result;

            // If we've used any subquery expressions we need to make sure all the conditions are for the same entity as we
            // can't specify an entityname attribute for the subquery filter.
            if (subqueryConditions.Count == 0)
                return result;

            var subqueryLinks = subqueryConditions
                .Select(c => subqueryExpressions[c].LinkEntity?.alias ?? targetEntityAlias)
                .Distinct()
                .ToList();

            if (subqueryLinks.Count > 1)
            {
                filter = null;
                return criteria;
            }

            foreach (var condition in filter.GetConditions())
            {
                if ((condition.entityname ?? targetEntityAlias) != subqueryLinks[0])
                {
                    filter = null;
                    return criteria;
                }

                condition.entityname = null;
            }

            foreach (var subqueryCondition in subqueryConditions)
                RemoveJoin(subqueryExpressions[subqueryCondition].JoinNode);

            // If the criteria are to be applied to the root entity, no need to do any further processing
            if (subqueryLinks[0] == targetEntityAlias)
                return result;

            // Otherwise, add the filter directly to the link entity
            var linkEntity = fetchXmlScan.Entity.FindLinkEntity(subqueryLinks[0]);
            linkEntity.AddItem(filter);
            filter = null;
            return result;
        }

        private BooleanExpression ExtractFetchXMLFilters(NodeCompilationContext context, DataSource dataSource, BooleanExpression criteria, INodeSchema schema, string allowedPrefix, HashSet<string> barredPrefixes, string targetEntityName, string targetEntityAlias, object[] items, Dictionary<BooleanExpression, ConvertedSubquery> subqueryExpressions, HashSet<BooleanExpression> replacedSubqueryExpressions, out filter filter)
        {
            if (TranslateFetchXMLCriteria(context, dataSource, criteria, schema, allowedPrefix, barredPrefixes, targetEntityName, targetEntityAlias, items, subqueryExpressions, replacedSubqueryExpressions, out filter))
                return null;

            if (!(criteria is BooleanBinaryExpression bin))
                return criteria;

            if (bin.BinaryExpressionType != BooleanBinaryExpressionType.And)
                return criteria;

            bin.FirstExpression = ExtractFetchXMLFilters(context, dataSource, bin.FirstExpression, schema, allowedPrefix, barredPrefixes, targetEntityName, targetEntityAlias, items, subqueryExpressions, replacedSubqueryExpressions, out var lhsFilter);
            bin.SecondExpression = ExtractFetchXMLFilters(context, dataSource, bin.SecondExpression, schema, allowedPrefix, barredPrefixes, targetEntityName, targetEntityAlias, items, subqueryExpressions, replacedSubqueryExpressions, out var rhsFilter);

            filter = (lhsFilter != null && rhsFilter != null) ? new filter { Items = new object[] { lhsFilter, rhsFilter } } : lhsFilter ?? rhsFilter;

            if (bin.FirstExpression != null && bin.SecondExpression != null)
                return bin;

            return bin.FirstExpression ?? bin.SecondExpression;
        }

        protected BooleanExpression ExtractMetadataFilters(NodeCompilationContext context, BooleanExpression criteria, MetadataQueryNode meta, out MetadataFilterExpression entityFilter, out MetadataFilterExpression attributeFilter, out MetadataFilterExpression relationshipFilter, out MetadataFilterExpression keyFilter)
        {
            if (TranslateMetadataCriteria(context, criteria, meta, out entityFilter, out attributeFilter, out relationshipFilter, out keyFilter))
                return null;

            if (!(criteria is BooleanBinaryExpression bin))
                return criteria;

            if (bin.BinaryExpressionType != BooleanBinaryExpressionType.And)
                return criteria;

            bin.FirstExpression = ExtractMetadataFilters(context, bin.FirstExpression, meta, out var lhsEntityFilter, out var lhsAttributeFilter, out var lhsRelationshipFilter, out var lhsKeyFilter);
            bin.SecondExpression = ExtractMetadataFilters(context, bin.SecondExpression, meta, out var rhsEntityFilter, out var rhsAttributeFilter, out var rhsRelationshipFilter, out var rhsKeyFilter);

            entityFilter = (lhsEntityFilter != null && rhsEntityFilter != null) ? new MetadataFilterExpression { Filters = { lhsEntityFilter, rhsEntityFilter } } : lhsEntityFilter ?? rhsEntityFilter;
            attributeFilter = (lhsAttributeFilter != null && rhsAttributeFilter != null) ? new MetadataFilterExpression { Filters = { lhsAttributeFilter, rhsAttributeFilter } } : lhsAttributeFilter ?? rhsAttributeFilter;
            relationshipFilter = (lhsRelationshipFilter != null && rhsRelationshipFilter != null) ? new MetadataFilterExpression { Filters = { lhsRelationshipFilter, rhsRelationshipFilter } } : lhsRelationshipFilter ?? rhsRelationshipFilter;
            keyFilter = (lhsKeyFilter != null && rhsKeyFilter != null) ? new MetadataFilterExpression { Filters = { lhsKeyFilter, rhsKeyFilter } } : lhsKeyFilter ?? rhsKeyFilter;

            if (bin.FirstExpression != null && bin.SecondExpression != null)
                return bin;

            return bin.FirstExpression ?? bin.SecondExpression;
        }

        private BooleanExpression ExtractChildFilters(BooleanExpression criteria, INodeSchema schema, Func<string, bool> columnPredicate, out BooleanExpression childFilter)
        {
            if (TranslateChildFilters(criteria, schema, columnPredicate, out childFilter))
                return null;

            if (!(criteria is BooleanBinaryExpression bin))
                return criteria;

            if (bin.BinaryExpressionType != BooleanBinaryExpressionType.And)
                return criteria;

            bin.FirstExpression = ExtractChildFilters(bin.FirstExpression, schema, columnPredicate, out var lhsFilter);
            bin.SecondExpression = ExtractChildFilters(bin.SecondExpression, schema, columnPredicate, out var rhsFilter);

            childFilter = lhsFilter.And(rhsFilter);

            if (bin.FirstExpression != null && bin.SecondExpression != null)
                return bin;

            return bin.FirstExpression ?? bin.SecondExpression;
        }

        private bool TranslateChildFilters(BooleanExpression criteria, INodeSchema schema, Func<string, bool> columnPredicate, out BooleanExpression childFilter)
        {
            childFilter = null;

            // If all the columns are from the target child we can move the whole filter to that child
            var columns = criteria.GetColumns().ToList();

            if (columns.Count == 0)
                return false;

            if (columns.All(col => columnPredicate(col)))
            {
                childFilter = criteria;
                return true;
            }

            return false;
        }

        protected bool TranslateMetadataCriteria(NodeCompilationContext context, BooleanExpression criteria, MetadataQueryNode meta, out MetadataFilterExpression entityFilter, out MetadataFilterExpression attributeFilter, out MetadataFilterExpression relationshipFilter, out MetadataFilterExpression keyFilter)
        {
            entityFilter = null;
            attributeFilter = null;
            relationshipFilter = null;
            keyFilter = null;

            var expressionCompilationContext = new ExpressionCompilationContext(context, null, null);
            var expressionExecutionContext = new ExpressionExecutionContext(expressionCompilationContext);

            if (criteria is BooleanBinaryExpression binary)
            {
                if (!TranslateMetadataCriteria(context, binary.FirstExpression, meta, out var lhsEntityFilter, out var lhsAttributeFilter, out var lhsRelationshipFilter, out var lhsKeyFilter))
                    return false;
                if (!TranslateMetadataCriteria(context, binary.SecondExpression, meta, out var rhsEntityFilter, out var rhsAttributeFilter, out var rhsRelationshipFilter, out var rhsKeyFilter))
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
                keyFilter = lhsKeyFilter;

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

                if (rhsKeyFilter != null)
                {
                    if (keyFilter == null)
                        keyFilter = rhsKeyFilter;
                    else
                        keyFilter = new MetadataFilterExpression { Filters = { lhsKeyFilter, rhsKeyFilter }, FilterOperator = binary.BinaryExpressionType == BooleanBinaryExpressionType.And ? LogicalOperator.And : LogicalOperator.Or };
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
                var literal = comparison.SecondExpression;

                if (col == null || literal.GetColumns().Any())
                {
                    col = comparison.SecondExpression as ColumnReferenceExpression;
                    literal = comparison.FirstExpression;
                }

                if (col == null || literal.GetColumns().Any())
                    return false;

                var schema = meta.GetSchema(context);
                if (!schema.ContainsColumn(col.GetColumnName(), out var colName))
                    return false;

                var parts = colName.SplitMultiPartIdentifier();

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

                var condition = new MetadataConditionExpression(parts[1], op, literal);

                return TranslateMetadataCondition(condition, parts[0], meta, out entityFilter, out attributeFilter, out relationshipFilter, out keyFilter);
            }

            if (criteria is InPredicate inPred)
            {
                var col = inPred.Expression as ColumnReferenceExpression;

                if (col == null)
                    return false;

                var schema = meta.GetSchema(context);
                if (!schema.ContainsColumn(col.GetColumnName(), out var colName))
                    return false;

                var parts = colName.SplitMultiPartIdentifier();

                if (parts.Length != 2)
                    return false;

                if (inPred.Values.Any(val => val.GetColumns().Any()))
                    return false;

                var condition = new MetadataConditionExpression(parts[1], inPred.NotDefined ? MetadataConditionOperator.NotIn : MetadataConditionOperator.In, inPred.Values.ToArray());

                return TranslateMetadataCondition(condition, parts[0], meta, out entityFilter, out attributeFilter, out relationshipFilter, out keyFilter);
            }

            if (criteria is BooleanIsNullExpression isNull)
            {
                var col = isNull.Expression as ColumnReferenceExpression;

                if (col == null)
                    return false;

                var schema = meta.GetSchema(context);
                if (!schema.ContainsColumn(col.GetColumnName(), out var colName))
                    return false;

                var parts = colName.SplitMultiPartIdentifier();

                if (parts.Length != 2)
                    return false;

                var condition = new MetadataConditionExpression(parts[1], isNull.IsNot ? MetadataConditionOperator.NotEquals : MetadataConditionOperator.Equals, null);

                return TranslateMetadataCondition(condition, parts[0], meta, out entityFilter, out attributeFilter, out relationshipFilter, out keyFilter);
            }

            return false;
        }

        private bool TranslateMetadataCondition(MetadataConditionExpression condition, string alias, MetadataQueryNode meta, out MetadataFilterExpression entityFilter, out MetadataFilterExpression attributeFilter, out MetadataFilterExpression relationshipFilter, out MetadataFilterExpression keyFilter)
        {
            entityFilter = null;
            attributeFilter = null;
            relationshipFilter = null;
            keyFilter = null;

            // Translate queries on attribute.EntityLogicalName to entity.LogicalName for better performance
            var isEntityFilter = alias.Equals(meta.EntityAlias, StringComparison.OrdinalIgnoreCase);
            var isAttributeFilter = alias.Equals(meta.AttributeAlias, StringComparison.OrdinalIgnoreCase);
            var isRelationshipFilter = alias.Equals(meta.OneToManyRelationshipAlias, StringComparison.OrdinalIgnoreCase) || alias.Equals(meta.ManyToOneRelationshipAlias, StringComparison.OrdinalIgnoreCase) || alias.Equals(meta.ManyToManyRelationshipAlias, StringComparison.OrdinalIgnoreCase);
            var isKeyFilter = alias.Equals(meta.KeyAlias, StringComparison.OrdinalIgnoreCase);

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

            if (isKeyFilter &&
                condition.PropertyName.Equals(nameof(EntityKeyMetadata.EntityLogicalName), StringComparison.OrdinalIgnoreCase))
            {
                condition.PropertyName = nameof(EntityMetadata.LogicalName);
                isKeyFilter = false;
                isEntityFilter = true;
            }

            var filter = new MetadataFilterExpression { Conditions = { condition } };

            // Attributes & relationships are polymorphic, but filters can only be applied to the base type. Check the property
            // we're filtering on is valid to be folded
            var targetType = isEntityFilter ? typeof(EntityMetadata) : isAttributeFilter ? typeof(AttributeMetadata) : isRelationshipFilter ? typeof(RelationshipMetadataBase) : typeof(EntityKeyMetadata);
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

            // Filtering on enum types only works for = and <>
            if (targetValueType.IsEnum &&
                condition.ConditionOperator != MetadataConditionOperator.Equals &&
                condition.ConditionOperator != MetadataConditionOperator.NotEquals &&
                condition.ConditionOperator != MetadataConditionOperator.In &&
                condition.ConditionOperator != MetadataConditionOperator.NotIn)
                return false;

            // Filtering on IsArchivalEnabled is not supported
            if (prop.DeclaringType == typeof(EntityMetadata) &&
                (
                    prop.Name == nameof(EntityMetadata.IsRetentionEnabled) ||
                    prop.Name == nameof(EntityMetadata.IsArchivalEnabled)
                ))
                return false;

            // Filtering on SourceType is not supported
            // https://github.com/MicrosoftDocs/powerapps-docs/issues/4608
            if (prop.DeclaringType == typeof(AttributeMetadata) &&
                prop.Name == nameof(AttributeMetadata.SourceType))
                return false;

            // Filtering on types that the KnownTypesResolver can't handle is not supported
            // https://github.com/MarkMpn/Sql4Cds/issues/534
            if (!new KnownTypesResolver().ResolvedTypes.ContainsKey(targetValueType.Name))
                return false;

            // String comparisons will be executed case-sensitively, but all other comparisons are case-insensitive. For consistency, don't allow
            // comparisons on string properties except those where we know the expected case.
            if (targetValueType == typeof(string))
            {
                if (prop.DeclaringType == typeof(EntityMetadata) && (prop.Name == nameof(EntityMetadata.LogicalName) || prop.Name == nameof(EntityMetadata.LogicalCollectionName)) ||
                    prop.DeclaringType == typeof(AttributeMetadata) && (prop.Name == nameof(AttributeMetadata.LogicalName) || prop.Name == nameof(AttributeMetadata.EntityLogicalName)))
                {
                    var toLower = (Func<ScalarExpression, ScalarExpression>)((ScalarExpression o) => new FunctionCall
                    {
                        FunctionName = new Identifier { Value = "LOWER" },
                        Parameters = { o }
                    });

                    if (condition.Value is ScalarExpression expr)
                        condition.Value = toLower(expr);
                    else if (condition.Value is IList<ScalarExpression> exprs)
                        condition.Value = exprs.Select(e => toLower(e)).ToList();
                }
                else
                {
                    return false;
                }
            }

            // Convert the property name to the correct case
            filter.Conditions[0].PropertyName = prop.Name;

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

            if (isKeyFilter)
            {
                keyFilter = filter;
                return true;
            }

            return false;
        }

        protected override RowCountEstimate EstimateRowsOutInternal(NodeCompilationContext context)
        {
            return new RowCountEstimate(Source.EstimateRowsOut(context).Value * 8 / 10);
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
                StartupExpression = StartupExpression,
                Source = (IDataExecutionPlanNodeInternal)Source.Clone()
            };

            clone.Source.Parent = clone;
            return clone;
        }
    }
}
