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

        public ColumnReferenceExpression LhsKey { get; private set; }

        public ColumnReferenceExpression RhsKey { get; private set; }

        public BooleanComparisonExpression JoinCondition { get; private set; }

        public override void Visit(BooleanComparisonExpression node)
        {
            base.Visit(node);

            if (node.FirstExpression is ColumnReferenceExpression lhsCol &&
                node.SecondExpression is ColumnReferenceExpression rhsCol &&
                node.ComparisonType == BooleanComparisonType.Equals)
            {
                var lhsName = String.Join(".", lhsCol.MultiPartIdentifier.Identifiers.Select(id => id.Value));
                var rhsName = String.Join(".", rhsCol.MultiPartIdentifier.Identifiers.Select(id => id.Value));

                if (_lhs.Schema.ContainsKey(lhsName) && _rhs.Schema.ContainsKey(rhsName))
                {
                    LhsKey = lhsCol;
                    RhsKey = rhsCol;
                }
                else if (_lhs.Schema.ContainsKey(rhsName) && _rhs.Schema.ContainsKey(lhsName))
                {
                    LhsKey = rhsCol;
                    RhsKey = lhsCol;
                }

                JoinCondition = node;
            }
        }

        public override void ExplicitVisit(BooleanBinaryExpression node)
        {
            if (node.BinaryExpressionType == BooleanBinaryExpressionType.And && JoinCondition == null)
                base.ExplicitVisit(node);
        }
    }
}