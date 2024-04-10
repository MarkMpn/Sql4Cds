using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace MarkMpn.Sql4Cds.Engine.Visitors
{
    /// <summary>
    /// Checks the properties of a CTE to ensure it is valid
    /// </summary>
    /// <remarks>
    /// https://learn.microsoft.com/en-us/sql/t-sql/queries/with-common-table-expression-transact-sql?view=sql-server-ver16
    /// </remarks>
    class CteValidatorVisitor : TSqlConcreteFragmentVisitor
    {
        private int _cteReferenceCount;
        private FunctionCall _scalarAggregate;
        private ScalarSubquery _subquery;
        private QualifiedJoin _outerJoin;
        private int _nestedQueryDepth;
        private CommonTableExpression _root;

        public string Name { get; private set; }

        public bool IsRecursive { get; private set; }

        public QueryExpression AnchorQuery { get; private set; }

        public List<QueryExpression> RecursiveQueries { get; } = new List<QueryExpression>();

        public override void Visit(CommonTableExpression node)
        {
            _root = node;
            Name = node.ExpressionName.Value;

            base.Visit(node);
        }

        public override void Visit(FromClause node)
        {
            _cteReferenceCount = 0;

            base.Visit(node);

            // The FROM clause of a recursive member must refer only one time to the CTE expression_name.
            if (_cteReferenceCount > 1)
                throw new NotSupportedQueryFragmentException(Sql4CdsError.CteMultipleRecursiveMembers(node, Name));
        }

        public override void Visit(NamedTableReference node)
        {
            if (node.SchemaObject.Identifiers.Count == 1 &&
                node.SchemaObject.BaseIdentifier.Value.Equals(Name, StringComparison.OrdinalIgnoreCase))
            {
                IsRecursive = true;
                _cteReferenceCount++;

                // The following items aren't allowed in the CTE_query_definition of a recursive member:
                // A hint applied to a recursive reference to a CTE inside a CTE_query_definition.
                if (node.TableHints.Count > 0)
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.CteNotAllowedHintsInRecursivePart(node.TableHints[0], Name));
            }

            base.Visit(node);
        }

        public override void Visit(QualifiedJoin node)
        {
            base.Visit(node);

            if (node.QualifiedJoinType != QualifiedJoinType.Inner)
                _outerJoin = node;
        }

        public override void ExplicitVisit(BinaryQueryExpression node)
        {
            base.ExplicitVisit(node);

            if (!IsRecursive)
                AnchorQuery = node;

            // UNION ALL is the only set operator allowed between the last anchor member and first recursive member, and when combining multiple recursive members.
            if (IsRecursive && (node.BinaryQueryExpressionType != BinaryQueryExpressionType.Union || !node.All))
                throw new NotSupportedQueryFragmentException(Sql4CdsError.CteRecursiveMemberWithoutUnionAll(node, Name));
        }

        public override void ExplicitVisit(QueryParenthesisExpression node)
        {
            _nestedQueryDepth++;

            base.ExplicitVisit(node);

            _nestedQueryDepth--;

            if (_nestedQueryDepth == 0)
            {
                if (!IsRecursive)
                    AnchorQuery = node;
                else
                    RecursiveQueries.Add(node);
            }
        }

        public override void ExplicitVisit(QuerySpecification node)
        {
            _nestedQueryDepth++;

            _scalarAggregate = null;
            _subquery = null;
            _outerJoin = null;

            base.ExplicitVisit(node);

            _nestedQueryDepth--;

            if (_nestedQueryDepth == 0)
            {
                // The following clauses can't be used in the CTE_query_definition:
                // ORDER BY (except when a TOP clause is specified)
                if (node.OrderByClause != null && node.TopRowFilter == null)
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.OrderByWithoutTop(node.OrderByClause));

                // FOR BROWSE
                if (node.ForClause is BrowseForClause forBrowse)
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.ForBrowseNotSupported(forBrowse));

                if (IsRecursive)
                {
                    // The following items aren't allowed in the CTE_query_definition of a recursive member:
                    // SELECT DISTINCT
                    if (node.UniqueRowFilter == UniqueRowFilter.Distinct)
                        throw new NotSupportedQueryFragmentException(Sql4CdsError.CteNotAllowedDistinct(node, Name));

                    // GROUP BY
                    if (node.GroupByClause != null)
                        throw new NotSupportedQueryFragmentException(Sql4CdsError.CteNotAllowedGroupByInRecursivePart(node.GroupByClause, Name));

                    // TODO: PIVOT

                    // HAVING
                    if (node.HavingClause != null)
                        throw new NotSupportedQueryFragmentException(Sql4CdsError.CteNotAllowedGroupByInRecursivePart(node.HavingClause, Name));

                    // Scalar aggregation
                    if (_scalarAggregate != null)
                        throw new NotSupportedQueryFragmentException(Sql4CdsError.CteNotAllowedGroupByInRecursivePart(_scalarAggregate, Name));

                    // TOP
                    if (node.TopRowFilter != null || node.OffsetClause != null)
                        throw new NotSupportedQueryFragmentException(Sql4CdsError.CteNotAllowedTopOffsetInRecursivePart((TSqlFragment)node.TopRowFilter ?? node.OffsetClause, Name));

                    // LEFT, RIGHT, OUTER JOIN (INNER JOIN is allowed)
                    if (_outerJoin != null)
                        throw new NotSupportedQueryFragmentException(Sql4CdsError.CteNotAllowedOuterJoinInRecursivePart(_outerJoin, Name));

                    // Subqueries
                    if (_subquery != null)
                        throw new NotSupportedQueryFragmentException(Sql4CdsError.CteNotAllowedRecursiveReferenceInSubquery(_subquery));
                }

                if (!IsRecursive)
                    AnchorQuery = node;
                else if (AnchorQuery == null)
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.CteNoAnchorMember(_root, Name));
                else
                    RecursiveQueries.Add(node);
            }
        }

        public override void Visit(SelectStatement node)
        {
            base.Visit(node);

            // The following clauses can't be used in the CTE_query_definition:
            // INTO
            if (node.Into != null)
                throw new NotSupportedQueryFragmentException(Sql4CdsError.SyntaxErrorKeyword(node.Into, "INTO")) { Suggestion = "INTO is not supported in CTEs" };

            // OPTION clause with query hints
            if (node.OptimizerHints.Count > 0)
                throw new NotSupportedQueryFragmentException(Sql4CdsError.SyntaxErrorKeyword(node.OptimizerHints[0], "OPTION")) { Suggestion = "Optimizer hints are not supported in CTEs" };
        }

        public override void ExplicitVisit(ScalarSubquery node)
        {
            var count = _cteReferenceCount;

            base.ExplicitVisit(node);

            if (_cteReferenceCount > count)
                _subquery = node;
        }

        public override void Visit(FunctionCall node)
        {
            base.Visit(node);

            switch (node.FunctionName.Value.ToLowerInvariant())
            {
                case "approx_count_distinct":
                case "avg":
                case "checksum_agg":
                case "count":
                case "count_big":
                case "grouping":
                case "grouping_id":
                case "max":
                case "min":
                case "stdev":
                case "stdevp":
                case "string_agg":
                case "sum":
                case "var":
                case "varp":
                    _scalarAggregate = node;
                    break;
            }
        }
    }
}
