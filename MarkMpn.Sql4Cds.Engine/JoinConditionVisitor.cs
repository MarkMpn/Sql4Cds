using System;
using System.Linq;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace MarkMpn.Sql4Cds.Engine
{
    internal class JoinConditionVisitor : TSqlConcreteFragmentVisitor
    {
        private readonly NodeSchema _lhs;
        private readonly NodeSchema _rhs;

        public JoinConditionVisitor(NodeSchema lhs, NodeSchema rhs)
        {
            _lhs = lhs;
            _rhs = rhs;
        }

        public bool AllowExpressions { get; set; }

        public ColumnReferenceExpression LhsKey { get; set; }

        public ScalarExpression LhsExpression { get; set; }

        public ColumnReferenceExpression RhsKey { get; set; }

        public ScalarExpression RhsExpression { get; set; }

        public BooleanComparisonExpression JoinCondition { get; private set; }

        public override void Visit(BooleanComparisonExpression node)
        {
            base.Visit(node);

            if (node.FirstExpression is ColumnReferenceExpression lhsCol &&
                node.SecondExpression is ColumnReferenceExpression rhsCol &&
                node.ComparisonType == BooleanComparisonType.Equals)
            {
                var lhsName = lhsCol.GetColumnName();
                var rhsName = rhsCol.GetColumnName();

                if (_lhs.ContainsColumn(lhsName, out _) && _rhs.ContainsColumn(rhsName, out _))
                {
                    LhsKey = lhsCol;
                    RhsKey = rhsCol;
                }
                else if (_lhs.ContainsColumn(rhsName, out _) && _rhs.ContainsColumn(lhsName, out _))
                {
                    LhsKey = rhsCol;
                    RhsKey = lhsCol;
                }

                JoinCondition = node;
                return;
            }

            if (AllowExpressions &&
                node.ComparisonType == BooleanComparisonType.Equals)
            {
                // Check each expression includes at least one column reference and relates entirely to one source
                var firstColumns = node.FirstExpression.GetColumns().ToList();
                var secondColumns = node.SecondExpression.GetColumns().ToList();

                if (firstColumns.Count == 0 || secondColumns.Count == 0)
                    return;

                var firstIsLhs = firstColumns.Any(c => _lhs.ContainsColumn(c, out _));
                var firstIsRhs = firstColumns.Any(c => _rhs.ContainsColumn(c, out _));
                var secondIsLhs = secondColumns.Any(c => _lhs.ContainsColumn(c, out _));
                var secondIsRhs = secondColumns.Any(c => _rhs.ContainsColumn(c, out _));

                if (firstIsLhs && !firstIsRhs && !secondIsLhs && secondIsRhs)
                {
                    LhsExpression = node.FirstExpression;
                    RhsExpression = node.SecondExpression;
                }
                else if (!firstIsLhs && firstIsRhs && secondIsLhs && !secondIsRhs)
                {
                    LhsExpression = node.SecondExpression;
                    RhsExpression = node.FirstExpression;
                }
                else
                {
                    return;
                }

                LhsKey = LhsExpression as ColumnReferenceExpression;
                RhsKey = RhsExpression as ColumnReferenceExpression;
            }
        }

        public override void ExplicitVisit(BooleanBinaryExpression node)
        {
            if (node.BinaryExpressionType == BooleanBinaryExpressionType.And && JoinCondition == null)
                base.ExplicitVisit(node);
        }
    }
}