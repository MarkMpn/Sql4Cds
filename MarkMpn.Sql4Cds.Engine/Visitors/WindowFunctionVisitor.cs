using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace MarkMpn.Sql4Cds.Engine.Visitors
{
    /// <summary>
    /// Provides methods to identify the details of window functions used within a SELECT or ORDER BY clause
    /// </summary>
    class WindowFunctionVisitor : TSqlConcreteFragmentVisitor
    {
        private readonly HashSet<string> _rankingFunctions;
        private readonly HashSet<string> _aggregateFunctions;
        private readonly HashSet<string> _analyticFunctions;

        private bool _inSelectClause;
        private bool _inOrderByClause;

        public WindowFunctionVisitor()
        {
            // https://learn.microsoft.com/en-us/sql/t-sql/functions/ranking-functions-transact-sql?view=sql-server-ver16
            _rankingFunctions = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
            {
                "ROW_NUMBER",
                "RANK",
                "DENSE_RANK",
                "NTILE"
            };

            // https://learn.microsoft.com/en-us/sql/t-sql/functions/aggregate-functions-transact-sql?view=sql-server-ver16
            _aggregateFunctions = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
            {
                "AVG",
                "COUNT",
                "COUNT_BIG",
                "MAX",
                "MIN",
                "STDEV",
                "STDEVP",
                "SUM",
                "VAR",
                "VARP"
            };

            // https://learn.microsoft.com/en-us/sql/t-sql/functions/analytic-functions-transact-sql?view=sql-server-ver16
            _analyticFunctions = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
            {
                "CUME_DIST",
                "FIRST_VALUE",
                "LAG",
                "LAST_VALUE",
                "LEAD",
                "PERCENT_RANK",
                "PERCENTILE_CONT",
                "PERCENTILE_DISC"
            };
        }

        public List<FunctionCall> WindowFunctions { get; } = new List<FunctionCall>();

        public List<FunctionCall> OutOfPlaceWindowFunctions { get; } = new List<FunctionCall>();

        public override void ExplicitVisit(FunctionCall node)
        {
            base.ExplicitVisit(node);

            if (_rankingFunctions.Contains(node.FunctionName.Value))
            {
                // This is a ranking function - we must have an OVER clause with an ORDER BY clause
                if (node.OverClause == null)
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.OverClauseRequired(node));

                if (node.OverClause.OrderByClause == null)
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.OverClauseRequiresOrderBy(node));

                // Ranking functions don't support the ROWS/RANGE window frame definition
                if (node.OverClause.WindowFrameClause != null)
                    throw new Sql4CdsException(Sql4CdsError.WindowFrameNotSupported(node));

                if (!_inSelectClause && !_inOrderByClause)
                {
                    // Ranking functions can only appear in the SELECT or ORDER BY clause
                    OutOfPlaceWindowFunctions.Add(node);
                }
                else
                {
                    WindowFunctions.Add(node);
                }
            }
            else if (_aggregateFunctions.Contains(node.FunctionName.Value) && node.OverClause != null)
            {
                // Aggregate function with the optional OVER clause
                if (!_inSelectClause && !_inOrderByClause)
                {
                    // Window aggregate functions can only appear in the SELECT or ORDER BY clause
                    OutOfPlaceWindowFunctions.Add(node);
                }
                else
                {
                    WindowFunctions.Add(node);
                }

                // DISTINCT isn't supported with the OVER clause
                if (node.UniqueRowFilter == UniqueRowFilter.Distinct)
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.DistinctNotSupportedWithOver(node));

                // ROWS/RANGE requires an ORDER BY clause
                if (node.OverClause.WindowFrameClause != null && node.OverClause.OrderByClause == null)
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.WindowFrameRequiresOrderBy(node.OverClause.WindowFrameClause));
            }
            else if (_analyticFunctions.Contains(node.FunctionName.Value))
            {
                // Analytic function - we must have an OVER clause
                if (node.OverClause == null)
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.OverClauseRequired(node));

                if (!_inSelectClause && !_inOrderByClause)
                {
                    // Window analytic functions can only appear in the SELECT or ORDER BY clause
                    OutOfPlaceWindowFunctions.Add(node);
                }
                else
                {
                    WindowFunctions.Add(node);
                }
            }
            else if (node.OverClause != null)
            {
                throw new NotSupportedQueryFragmentException(Sql4CdsError.OverClauseNotSupported(node));
            }
        }

        public override void ExplicitVisit(SelectScalarExpression node)
        {
            _inSelectClause = true;
            base.ExplicitVisit(node);
            _inSelectClause = false;
        }

        public override void ExplicitVisit(OrderByClause node)
        {
            _inOrderByClause = true;
            base.ExplicitVisit(node);
            _inOrderByClause = false;
        }

        public override void ExplicitVisit(ScalarSubquery node)
        {
            // Do not recurse into subqueries
        }

        public override void ExplicitVisit(QueryDerivedTable node)
        {
            // Do not recurse into subqueries
        }
    }
}
