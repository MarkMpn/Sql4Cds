using Microsoft.SqlServer.TransactSql.ScriptDom;
using System.Collections.Generic;
using System.Linq;

namespace MarkMpn.Sql4Cds.Engine.Visitors
{
    /// <summary>
    /// Replaces expressions with the equivalent column names
    /// </summary>
    /// <remarks>
    /// During the post-processing of aggregate queries, a new schema is produced where the aggregates are stored in new
    /// columns. To make the processing of the remainder of the query easier, this class replaces any references to those
    /// aggregate functions with references to the calculated column name, e.g.
    /// SELECT firstname, count(*) FROM contact HAVING count(*) > 2
    /// would become
    /// SELECT firstname, agg1 FROM contact HAVING agg1 > 2
    /// 
    /// During query execution the agg1 column is generated from the aggregate query and allows the rest of the query execution
    /// to proceed without knowledge of how the aggregate was derived.
    /// </remarks>
    class RewriteVisitor : RewriteVisitorBase
    {
        private readonly IDictionary<string, ScalarExpression> _mappings;

        public RewriteVisitor(IDictionary<ScalarExpression,string> rewrites)
        {
            _mappings = rewrites
                .GroupBy(kvp => kvp.Key.ToSql())
                .ToDictionary(g => g.Key, g => (ScalarExpression) new ColumnReferenceExpression { MultiPartIdentifier = new MultiPartIdentifier { Identifiers = { new Identifier { Value = g.First().Value } } } });
        }

        public RewriteVisitor(IDictionary<ScalarExpression,ScalarExpression> rewrites)
        {
            _mappings = rewrites
                .GroupBy(kvp => kvp.Key.ToSql())
                .ToDictionary(g => g.Key, g => g.First().Value);
        }

        protected override ScalarExpression ReplaceExpression(ScalarExpression expression, out string name)
        {
            name = null;

            if (expression == null)
                return null;

            if (_mappings.TryGetValue(expression.ToSql(), out var column))
            {
                name = (column as ColumnReferenceExpression)?.MultiPartIdentifier?.Identifiers?.Last()?.Value;
                return column;
            }

            return expression;
        }

        protected override BooleanExpression ReplaceExpression(BooleanExpression expression)
        {
            return expression;
        }
    }
}
