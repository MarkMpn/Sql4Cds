using System;
using System.Globalization;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace MarkMpn.Sql4Cds.Engine.Visitors
{
    /// <summary>
    /// Replaces certain "special" functions with standard function calls for easier handling
    /// </summary>
    /// <remarks>
    /// Some functions are handled in a special way by the SQL parser and produce a different type of object in the DOM that
    /// does not inherit from <see cref="FunctionCall"/>. This makes handling them generically throughout the rest of the conversion
    /// process more difficult. To simplify things, this visitor replaces those DOM objects with equivalent <see cref="FunctionCall"/>
    /// instances.
    /// </remarks>
    /// <see href="https://docs.microsoft.com/en-us/dotnet/api/microsoft.sqlserver.transactsql.scriptdom.primaryexpression?view=sql-dacfx-140.3881.1"/>
    class ReplacePrimaryFunctionsVisitor : RewriteVisitorBase
    {
        protected override ScalarExpression ReplaceExpression(ScalarExpression expression, out string name)
        {
            name = null;

            if (expression is CoalesceExpression coalesce)
            {
                var caseExpr = new SearchedCaseExpression();

                foreach (var expr in coalesce.Expressions)
                    caseExpr.WhenClauses.Add(new SearchedWhenClause
                    {
                        WhenExpression = new BooleanIsNullExpression { Expression = expr, IsNot = true },
                        ThenExpression = expr
                    });

                return caseExpr;
            }

            if (expression is IIfCall iif)
            {
                var caseExpr = new SearchedCaseExpression
                {
                    WhenClauses =
                    {
                        new SearchedWhenClause
                        {
                            WhenExpression = iif.Predicate,
                            ThenExpression = iif.ThenExpression
                        }
                    },
                    ElseExpression = iif.ElseExpression
                };

                return caseExpr;
            }

            if (expression is LeftFunctionCall left)
            {
                var leftFunc = new FunctionCall
                {
                    FunctionName = new Identifier { Value = "LEFT" }
                };

                foreach (var param in left.Parameters)
                    leftFunc.Parameters.Add(param);

                return leftFunc;
            }

            if (expression is NullIfExpression nullif)
            {
                var caseExpr = new SearchedCaseExpression
                {
                    WhenClauses =
                    {
                        new SearchedWhenClause
                        {
                            WhenExpression = new BooleanComparisonExpression
                            {
                                ComparisonType = BooleanComparisonType.Equals,
                                FirstExpression = nullif.FirstExpression,
                                SecondExpression = nullif.SecondExpression
                            },
                            ThenExpression = new NullLiteral()
                        }
                    },
                    ElseExpression = nullif.FirstExpression
                };

                return caseExpr;
            }

            if (expression is RightFunctionCall right)
            {
                var rightFunc = new FunctionCall
                {
                    FunctionName = new Identifier { Value = "RIGHT" }
                };

                foreach (var param in right.Parameters)
                    rightFunc.Parameters.Add(param);

                return rightFunc;
            }

            if (expression is UnaryExpression unary && unary.Expression is IntegerLiteral integer && unary.UnaryExpressionType != UnaryExpressionType.BitwiseNot)
            {
                if (unary.UnaryExpressionType == UnaryExpressionType.Negative)
                {
                    var value = Int32.Parse(integer.Value, CultureInfo.InvariantCulture);
                    integer.Value = (-value).ToString();
                    return integer;
                }
            }

            return expression;
        }

        protected override BooleanExpression ReplaceExpression(BooleanExpression expression)
        {
            if (expression is BooleanTernaryExpression between)
            {
                // field BETWEEN x AND y is equivalent to (field >= x AND field <= y)
                // field NOT BETWEEN x AND y is equivalent to (field < x OR field > y)
                var converted = new BooleanParenthesisExpression
                {
                    Expression = new BooleanBinaryExpression
                    {
                        FirstExpression = new BooleanComparisonExpression
                        {
                            FirstExpression = between.FirstExpression,
                            ComparisonType = between.TernaryExpressionType == BooleanTernaryExpressionType.Between ? BooleanComparisonType.GreaterThanOrEqualTo : BooleanComparisonType.LessThan,
                            SecondExpression = between.SecondExpression,

                            FirstTokenIndex = between.FirstTokenIndex,
                            LastTokenIndex = between.LastTokenIndex,
                            ScriptTokenStream = between.ScriptTokenStream
                        },
                        BinaryExpressionType = between.TernaryExpressionType == BooleanTernaryExpressionType.Between ? BooleanBinaryExpressionType.And : BooleanBinaryExpressionType.Or,
                        SecondExpression = new BooleanComparisonExpression
                        {
                            FirstExpression = between.FirstExpression,
                            ComparisonType = between.TernaryExpressionType == BooleanTernaryExpressionType.Between ? BooleanComparisonType.LessThanOrEqualTo : BooleanComparisonType.GreaterThan,
                            SecondExpression = between.ThirdExpression,

                            FirstTokenIndex = between.FirstTokenIndex,
                            LastTokenIndex = between.LastTokenIndex,
                            ScriptTokenStream = between.ScriptTokenStream
                        },

                        FirstTokenIndex = between.FirstTokenIndex,
                        LastTokenIndex = between.LastTokenIndex,
                        ScriptTokenStream = between.ScriptTokenStream
                    },

                    FirstTokenIndex = between.FirstTokenIndex,
                    LastTokenIndex = between.LastTokenIndex,
                    ScriptTokenStream = between.ScriptTokenStream
                };

                return converted;
            }

            return expression;
        }
    }
}
