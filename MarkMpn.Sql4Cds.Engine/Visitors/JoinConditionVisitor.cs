using System;
using System.Collections.Generic;
using System.Linq;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace MarkMpn.Sql4Cds.Engine.Visitors
{
    internal class JoinConditionVisitor : TSqlConcreteFragmentVisitor
    {
        private readonly INodeSchema _lhs;
        private readonly INodeSchema _rhs;
        private readonly HashSet<string> _fixedValueColumns;
        private string _lhsColName;
        private string _rhsColName;

        public JoinConditionVisitor(INodeSchema lhs, INodeSchema rhs, HashSet<string> fixedValueColumns)
        {
            _lhs = lhs;
            _rhs = rhs;
            _fixedValueColumns = fixedValueColumns;
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

                if (_lhs.ContainsColumn(lhsName, out var lhsColName) && _rhs.ContainsColumn(rhsName, out var rhsColName))
                {
                }
                else if (_lhs.ContainsColumn(rhsName, out rhsColName) && _rhs.ContainsColumn(lhsName, out lhsColName))
                {
                    (lhsCol, rhsCol) = (rhsCol, lhsCol);
                    (lhsColName, rhsName) = (rhsName, lhsColName);
                }
                else
                {
                    return;
                }

                // Use this join key if we don't already have one or this is better (prefer joining on primary/foreign key vs. other fields,
                // and prefer using columns that aren't being filtered on separately - we can apply them as secondary filters on the joined
                // table as well rather than using them as the join key).
                if (JoinCondition == null ||
                    _lhs.PrimaryKey == lhsColName ||
                    _rhs.PrimaryKey == rhsColName ||
                    _fixedValueColumns.Contains(_lhsColName) ||
                    _fixedValueColumns.Contains(_rhsColName))
                {
                    LhsKey = lhsCol;
                    RhsKey = rhsCol;
                    _lhsColName = lhsColName;
                    _rhsColName = rhsColName;
                    JoinCondition = node;
                }

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
            if (node.BinaryExpressionType == BooleanBinaryExpressionType.And)
                base.ExplicitVisit(node);
        }
    }
}