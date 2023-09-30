using System;
using System.Globalization;
using System.Linq.Expressions;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace MarkMpn.Sql4Cds.Engine.Visitors
{
    /// <summary>
    /// Replaces NOT IS [NOT] NULL with IS [NOT] NULL
    /// </summary>
    class RefactorNotIsNullVisitor : BooleanRewriteVisitor
    {
        private BooleanExpression _root;

        public RefactorNotIsNullVisitor(BooleanExpression root) : base(null)
        {
            _root = ReplaceExpression(root);
            _root.Accept(this);
        }

        public BooleanExpression Replacement => _root;

        protected override BooleanExpression ReplaceExpression(BooleanExpression expr)
        {
            if (expr is BooleanNotExpression not && not.Expression is BooleanIsNullExpression isNull)
            {
                isNull.IsNot = !isNull.IsNot;
                return isNull;
            }

            return expr;
        }
    }
}
