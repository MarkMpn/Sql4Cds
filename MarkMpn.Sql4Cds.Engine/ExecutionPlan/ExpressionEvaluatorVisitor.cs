using System;
using System.Collections;
using System.Linq;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    internal class ExpressionEvaluatorVisitor : TSqlConcreteFragmentVisitor
    {
        private readonly Entity _entity;

        public ExpressionEvaluatorVisitor(Entity entity)
        {
            _entity = entity;
        }

        public object Value { get; private set; }

        public override void ExplicitVisit(ColumnReferenceExpression node)
        {
            var name = String.Join(".", node.MultiPartIdentifier.Identifiers.Select(id => id.Value));

            _entity.Attributes.TryGetValue(name, out var value);
            Value = value;
        }

        public override void ExplicitVisit(IdentifierLiteral node)
        {
            Value = new Guid(node.Value);
        }

        public override void ExplicitVisit(IntegerLiteral node)
        {
            Value = Int32.Parse(node.Value);
        }

        public override void ExplicitVisit(MoneyLiteral node)
        {
            Value = Decimal.Parse(node.Value);
        }

        public override void ExplicitVisit(NullLiteral node)
        {
            Value = null;
        }

        public override void ExplicitVisit(NumericLiteral node)
        {
            Value = Decimal.Parse(node.Value);
        }

        public override void ExplicitVisit(RealLiteral node)
        {
            Value = Single.Parse(node.Value);
        }

        public override void ExplicitVisit(StringLiteral node)
        {
            Value = node.Value;
        }

        public override void ExplicitVisit(BooleanComparisonExpression node)
        {
            var lhs = node.FirstExpression.GetValue(_entity);
            var rhs = node.SecondExpression.GetValue(_entity);

            if (lhs == null || rhs == null)
            {
                Value = false;
                return;
            }

            SqlTypeConverter.MakeConsistentTypes(ref lhs, ref rhs);

            var comparison = CaseInsensitiveComparer.Default.Compare(lhs, rhs);

            switch (node.ComparisonType)
            {
                case BooleanComparisonType.Equals:
                    Value = comparison == 0;
                    break;

                case BooleanComparisonType.GreaterThan:
                    Value = comparison > 0;
                    break;

                case BooleanComparisonType.GreaterThanOrEqualTo:
                case BooleanComparisonType.NotLessThan:
                    Value = comparison >= 0;
                    break;

                case BooleanComparisonType.LessThan:
                    Value = comparison < 0;
                    break;

                case BooleanComparisonType.LessThanOrEqualTo:
                case BooleanComparisonType.NotGreaterThan:
                    Value = comparison <= 0;
                    break;

                case BooleanComparisonType.NotEqualToBrackets:
                case BooleanComparisonType.NotEqualToExclamation:
                    Value = comparison != 0;
                    break;

                default:
                    throw new QueryExecutionException(node, "Unknown comparison type");
            }
        }
    }
}