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
        private readonly NodeSchema _schema;

        public ExpressionEvaluatorVisitor(Entity entity, NodeSchema schema)
        {
            _entity = entity;
            _schema = schema;
        }

        public object Value { get; private set; }

        public Type Type { get; private set; }

        public override void ExplicitVisit(ColumnReferenceExpression node)
        {
            var name = node.GetColumnName();

            if (!_schema.ContainsColumn(name, out name))
                return;

            _entity.Attributes.TryGetValue(name, out var value);
            Value = value;
            Type = _schema.Schema[name];
        }

        public override void ExplicitVisit(IdentifierLiteral node)
        {
            Value = new Guid(node.Value);
            Type = typeof(Guid);
        }

        public override void ExplicitVisit(IntegerLiteral node)
        {
            Value = Int32.Parse(node.Value);
            Type = typeof(int);
        }

        public override void ExplicitVisit(MoneyLiteral node)
        {
            Value = Decimal.Parse(node.Value);
            Type = typeof(decimal);
        }

        public override void ExplicitVisit(NullLiteral node)
        {
            Value = null;
            Type = null;
        }

        public override void ExplicitVisit(NumericLiteral node)
        {
            Value = Decimal.Parse(node.Value);
            Type = typeof(decimal);
        }

        public override void ExplicitVisit(RealLiteral node)
        {
            Value = Single.Parse(node.Value);
            Type = typeof(float);
        }

        public override void ExplicitVisit(StringLiteral node)
        {
            Value = node.Value;
            Type = typeof(string);
        }

        public override void ExplicitVisit(BooleanComparisonExpression node)
        {
            var lhs = node.FirstExpression.GetValue(_entity, _schema);
            var rhs = node.SecondExpression.GetValue(_entity, _schema);

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