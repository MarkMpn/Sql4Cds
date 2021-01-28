using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.Engine.Visitors;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    static class ExpressionExtensions
    {
        public static object GetValue(this TSqlFragment expr, Entity entity)
        {
            var visitor = new ExpressionEvaluatorVisitor(entity);
            expr.Accept(visitor);

            return visitor.Value;
        }

        public static bool GetValue(this BooleanExpression expr, Entity entity)
        {
            return (bool)GetValue((TSqlFragment)expr, entity);
        }

        public static BooleanExpression RemoveCondition(this BooleanExpression expr, BooleanExpression remove)
        {
            if (expr == remove)
                return null;

            if (expr is BooleanBinaryExpression binary)
            {
                if (binary.FirstExpression == remove)
                    return binary.SecondExpression;

                if (binary.SecondExpression == remove)
                    return binary.FirstExpression;

                var clone = new BooleanBinaryExpression
                {
                    BinaryExpressionType = binary.BinaryExpressionType,
                    FirstExpression = binary.FirstExpression.RemoveCondition(remove),
                    SecondExpression = binary.SecondExpression.RemoveCondition(remove)
                };

                return clone;
            }

            if (expr is BooleanParenthesisExpression paren)
            {
                if (paren.Expression == remove)
                    return null;

                return new BooleanParenthesisExpression { Expression = paren.Expression.RemoveCondition(remove) };
            }

            return expr;
        }

        public static string GetColumnName(this ColumnReferenceExpression col)
        {
            return String.Join(".", col.MultiPartIdentifier.Identifiers.Select(id => id.Value));
        }

        public static IEnumerable<string> GetColumns(this TSqlFragment fragment)
        {
            var visitor = new ColumnCollectingVisitor();
            fragment.Accept(visitor);

            return visitor.Columns
                .Select(col => col.GetColumnName())
                .Distinct();
        }
    }
}
