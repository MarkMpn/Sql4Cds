using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace MarkMpn.Sql4Cds.Engine.Visitors
{
    /// <summary>
    /// Provides a standard method for replacing individual scalar expressions within the SQL DOM
    /// </summary>
    abstract class RewriteVisitorBase : TSqlFragmentVisitor
    {
        public ScalarExpression ReplaceExpression(ScalarExpression expression)
        {
            var directReplaced = ReplaceExpression(expression, out _);

            if (directReplaced != expression)
                return directReplaced;

            expression.Accept(this);
            return expression;
        }

        private string ReplaceExpression<T>(T target, Expression<Func<T, ScalarExpression>> selector) where T : TSqlFragment
        {
            var property = (PropertyInfo) ((MemberExpression)selector.Body).Member;
            var expression = (ScalarExpression) property.GetValue(target);
            var replaced = ReplaceExpression(expression, out var name);

            if (expression != replaced)
            {
                property.SetValue(target, replaced);
                target.ScriptTokenStream = null;
            }

            return name;
        }

        protected abstract ScalarExpression ReplaceExpression(ScalarExpression expression, out string name);

        private void ReplaceExpression<T>(T target, Expression<Func<T, BooleanExpression>> selector) where T:TSqlFragment
        {
            var property = (PropertyInfo)((MemberExpression)selector.Body).Member;
            var expression = (BooleanExpression)property.GetValue(target);
            property.SetValue(target, ReplaceExpression(expression));
        }

        protected abstract BooleanExpression ReplaceExpression(BooleanExpression expression);

        public override void ExplicitVisit(SelectScalarExpression node)
        {
            base.ExplicitVisit(node);
            var originalColumn = node.Expression as ColumnReferenceExpression;
            var name = ReplaceExpression(node, n => n.Expression);
            if (name != null && node.ColumnName == null)
            {
                var alias = name;

                if (originalColumn != null)
                    alias = originalColumn.MultiPartIdentifier.Identifiers.Last().Value;

                node.ColumnName = new IdentifierOrValueExpression { Identifier = new Identifier { Value = alias } };
            }
        }

        public override void ExplicitVisit(ExpressionWithSortOrder node)
        {
            base.ExplicitVisit(node);
            ReplaceExpression(node, n => n.Expression);
        }

        public override void ExplicitVisit(BooleanComparisonExpression node)
        {
            base.ExplicitVisit(node);
            ReplaceExpression(node, n => n.FirstExpression);
            ReplaceExpression(node, n => n.SecondExpression);
        }

        public override void ExplicitVisit(BooleanIsNullExpression node)
        {
            base.ExplicitVisit(node);
            ReplaceExpression(node, n => n.Expression);
        }

        public override void ExplicitVisit(LikePredicate node)
        {
            base.ExplicitVisit(node);
            ReplaceExpression(node, n => n.FirstExpression);
            ReplaceExpression(node, n => n.SecondExpression);
        }

        public override void ExplicitVisit(InPredicate node)
        {
            base.ExplicitVisit(node);
            ReplaceExpression(node, n => n.Expression);

            for (var i = 0; i < node.Values.Count; i++)
                node.Values[i] = ReplaceExpression(node.Values[i], out _);
        }

        public override void ExplicitVisit(Microsoft.SqlServer.TransactSql.ScriptDom.BinaryExpression node)
        {
            base.ExplicitVisit(node);
            ReplaceExpression(node, n => n.FirstExpression);
            ReplaceExpression(node, n => n.SecondExpression);
        }

        public override void ExplicitVisit(Microsoft.SqlServer.TransactSql.ScriptDom.UnaryExpression node)
        {
            base.ExplicitVisit(node);
            ReplaceExpression(node, n => n.Expression);
        }

        public override void ExplicitVisit(FunctionCall node)
        {
            base.ExplicitVisit(node);

            for (var i = 0; i < node.Parameters.Count; i++)
                node.Parameters[i] = ReplaceExpression(node.Parameters[i], out _);
        }

        public override void ExplicitVisit(CaseExpression node)
        {
            base.ExplicitVisit(node);
            ReplaceExpression(node, n => n.ElseExpression);
        }

        public override void ExplicitVisit(SimpleCaseExpression node)
        {
            base.ExplicitVisit(node);
            ReplaceExpression(node, n => n.InputExpression);
        }

        public override void ExplicitVisit(WhenClause node)
        {
            base.ExplicitVisit(node);
            ReplaceExpression(node, n => n.ThenExpression);
        }

        public override void ExplicitVisit(SimpleWhenClause node)
        {
            base.ExplicitVisit(node);
            ReplaceExpression(node, n => n.WhenExpression);
        }

        public override void ExplicitVisit(AssignmentSetClause node)
        {
            base.ExplicitVisit(node);
            ReplaceExpression(node, n => n.NewValue);
        }

        public override void ExplicitVisit(BooleanTernaryExpression node)
        {
            base.ExplicitVisit(node);
            ReplaceExpression(node, n => n.FirstExpression);
            ReplaceExpression(node, n => n.SecondExpression);
            ReplaceExpression(node, n => n.ThirdExpression);
        }

        public override void ExplicitVisit(BooleanBinaryExpression node)
        {
            base.ExplicitVisit(node);
            ReplaceExpression(node, n => n.FirstExpression);
            ReplaceExpression(node, n => n.SecondExpression);
        }

        public override void ExplicitVisit(BooleanNotExpression node)
        {
            base.ExplicitVisit(node);
            ReplaceExpression(node, n => n.Expression);
        }

        public override void ExplicitVisit(BooleanParenthesisExpression node)
        {
            base.ExplicitVisit(node);
            ReplaceExpression(node, n => n.Expression);
        }

        public override void ExplicitVisit(WhereClause node)
        {
            base.ExplicitVisit(node);
            ReplaceExpression(node, n => n.SearchCondition);
        }

        public override void ExplicitVisit(CastCall node)
        {
            base.ExplicitVisit(node);
            ReplaceExpression(node, n => n.Parameter);
        }

        public override void ExplicitVisit(ConvertCall node)
        {
            base.ExplicitVisit(node);
            ReplaceExpression(node, n => n.Parameter);
        }

        public override void ExplicitVisit(ExpressionGroupingSpecification node)
        {
            base.ExplicitVisit(node);
            ReplaceExpression(node, n => n.Expression);
        }
    }
}
