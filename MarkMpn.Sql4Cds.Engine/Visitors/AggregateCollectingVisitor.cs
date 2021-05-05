using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Generic;
using System.Linq;

namespace MarkMpn.Sql4Cds.Engine.Visitors
{
    /// <summary>
    /// Collects all the aggregate functions used throughout a query
    /// </summary>
    class AggregateCollectingVisitor : TSqlFragmentVisitor
    {
        private readonly ISet<string> _aggregates = new HashSet<string>();

        /// <summary>
        /// Returns the list of aggregate functions used directly within the SELECT clause and not combined as part of a larger calculation
        /// </summary>
        public IList<SelectScalarExpression> SelectAggregates { get; } = new List<SelectScalarExpression>();

        /// <summary>
        /// Returns a list of all other aggregates used in the query but not as the only expression in a SELECT clause
        /// </summary>
        public IList<FunctionCall> Aggregates { get; } = new List<FunctionCall>();

        /// <summary>
        /// Finds the aggregate functions used in a query
        /// </summary>
        /// <param name="querySpec">The query to get the aggregate functions from</param>
        public void GetAggregates(QuerySpecification querySpec)
        {
            // Extract the aggregates used directly in the SELECT clause separately to keep the associated aliases
            foreach (var element in querySpec.SelectElements.OfType<SelectScalarExpression>())
            {
                if (element.Expression is FunctionCall func && IsAggregate(func))
                {
                    SelectAggregates.Add(element);
                    _aggregates.Add(func.ToSql());
                }
                else
                {
                    element.Expression.Accept(this);
                }
            }

            // Then handle the rest of the query
            querySpec.Accept(this);
        }

        public override void ExplicitVisit(FunctionCall node)
        {
            base.ExplicitVisit(node);

            // Generate a unique list of the aggregates that are required
            // Even if the query references the same aggregate twice, we only need to calculate it once
            if (IsAggregate(node) && _aggregates.Add(node.ToSql()))
                Aggregates.Add(node);
        }

        public override void ExplicitVisit(SelectScalarExpression node)
        {
            // Do not recuse into SELECT clauses as we've already handled them separately
        }

        public override void ExplicitVisit(ScalarSubquery node)
        {
            // Do not recurse into subqueries - they'll be handled separately
        }

        private bool IsAggregate(FunctionCall func)
        {
            if (func.FunctionName.Value.Equals("SUM", StringComparison.OrdinalIgnoreCase) ||
                func.FunctionName.Value.Equals("MIN", StringComparison.OrdinalIgnoreCase) ||
                func.FunctionName.Value.Equals("MAX", StringComparison.OrdinalIgnoreCase) ||
                func.FunctionName.Value.Equals("COUNT", StringComparison.OrdinalIgnoreCase) ||
                func.FunctionName.Value.Equals("AVG", StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }

            return false;
        }
    }
}
