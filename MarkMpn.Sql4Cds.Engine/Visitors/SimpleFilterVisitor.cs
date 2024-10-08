using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using MarkMpn.Sql4Cds.Engine.FetchXml;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace MarkMpn.Sql4Cds.Engine.Visitors
{
    /// <summary>
    /// Flattens a nested AND/OR filter into a simple list of conditions
    /// </summary>
    class SimpleFilterVisitor : TSqlFragmentVisitor
    {
        private List<condition> _conditions;
        private BooleanBinaryExpressionType _binaryType;
        private bool _invalid;
        private bool _setType;

        public SimpleFilterVisitor()
        {
            _conditions = new List<condition>();
            _binaryType = BooleanBinaryExpressionType.And;
        }

        public override void Visit(BooleanBinaryExpression node)
        {
            base.Visit(node);

            if (_setType && _binaryType != node.BinaryExpressionType)
                _invalid = true;

            _binaryType = node.BinaryExpressionType;
            _setType = true;
        }

        public override void Visit(BooleanExpression node)
        {
            base.Visit(node);

            if (node is BooleanBinaryExpression)
            {
                // NOOP
            }
            else if (node is BooleanComparisonExpression cmp)
            {
                if (cmp.FirstExpression is ColumnReferenceExpression col1 && cmp.SecondExpression is ValueExpression lit2)
                {
                    if (!cmp.ComparisonType.TryConvertToFetchXml(out var op))
                        _invalid = true;

                    _conditions.Add(new condition
                    {
                        attribute = col1.MultiPartIdentifier.Identifiers.Last().Value,
                        @operator = op,
                        value = GetValue(lit2),
                        IsVariable = !(lit2 is Literal)
                    });
                }
                else if (cmp.FirstExpression is ValueExpression lit1 && cmp.SecondExpression is ColumnReferenceExpression col2)
                {
                    if (!cmp.ComparisonType.TransitiveComparison().TryConvertToFetchXml(out var op))
                        _invalid = true;

                    _conditions.Add(new condition
                    {
                        attribute = col2.MultiPartIdentifier.Identifiers.Last().Value,
                        @operator = op,
                        value = GetValue(lit1),
                        IsVariable = !(lit1 is Literal)
                    });
                }
                else
                {
                    // Unsupported
                    _invalid = true;
                }
            }
            else if (node is InPredicate @in && @in.Expression is ColumnReferenceExpression inCol && @in.Subquery == null && @in.Values.All(v => v is ValueExpression))
            {
                _conditions.Add(new condition
                {
                    attribute = inCol.MultiPartIdentifier.Identifiers.Last().Value,
                    @operator = @operator.@in,
                    Items = @in.Values.Cast<ValueExpression>().Select(l => new conditionValue { Value = GetValue(l), IsVariable = !(l is Literal) }).ToArray()
                });
            }
            else
            {
                // Unsupported
                _invalid = true;
            }
        }

        private string GetValue(ValueExpression expr)
        {
            if (expr is Literal lit)
                return lit.Value;

            if (expr is GlobalVariableExpression g)
                return g.Name;

            if (expr is VariableReference v)
                return v.Name;

            throw new NotSupportedException("Unknown value type");
        }

        /// <summary>
        /// The type of comparison used to combine the conditions. <see langword="null"/> if the filter is not .
        /// </summary>
        public BooleanBinaryExpressionType? BinaryType => _invalid ? null : (BooleanBinaryExpressionType?)_binaryType;

        public IEnumerable<condition> Conditions => _conditions;
    }
}
