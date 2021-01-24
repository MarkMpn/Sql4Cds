using System;
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

            var comparableLhs = lhs as IComparable;
            var comparableRhs = rhs as IComparable;

            if (comparableLhs == null)
                throw new QueryExecutionException(node, "Comparison expression cannot be evaluated");

            var comparison = comparableLhs.CompareTo(comparableRhs);

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