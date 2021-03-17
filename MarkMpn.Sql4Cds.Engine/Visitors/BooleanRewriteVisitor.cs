using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace MarkMpn.Sql4Cds.Engine.Visitors
{
    class BooleanRewriteVisitor : TSqlFragmentVisitor
    {
        private readonly IDictionary<BooleanExpression, BooleanExpression> _mappings;

        public BooleanRewriteVisitor(IDictionary<BooleanExpression, BooleanExpression> rewrites)
        {
            _mappings = rewrites;
        }

        private void Rewrite<TSource>(TSource source, Expression<Func<TSource, BooleanExpression>> expr) where TSource:TSqlFragment
        {
            var prop = (PropertyInfo) ((MemberExpression) expr.Body).Member;

            var value = (BooleanExpression) prop.GetValue(source, null);
            if (_mappings.TryGetValue(value, out var mapped))
            {
                prop.SetValue(source, mapped);
                source.ScriptTokenStream = null;
            }
        }

        public override void ExplicitVisit(WhereClause node)
        {
            Rewrite(node, n => n.SearchCondition);
            base.ExplicitVisit(node);
        }

        public override void ExplicitVisit(HavingClause node)
        {
            Rewrite(node, n => n.SearchCondition);
            base.ExplicitVisit(node);
        }

        public override void ExplicitVisit(BooleanBinaryExpression node)
        {
            Rewrite(node, n => n.FirstExpression);
            Rewrite(node, n => n.SecondExpression);
            base.ExplicitVisit(node);
        }

        public override void ExplicitVisit(BooleanParenthesisExpression node)
        {
            Rewrite(node, n => n.Expression);
            base.ExplicitVisit(node);
        }

        public override void ExplicitVisit(BooleanNotExpression node)
        {
            Rewrite(node, n => n.Expression);
            base.ExplicitVisit(node);
        }
    }
}
