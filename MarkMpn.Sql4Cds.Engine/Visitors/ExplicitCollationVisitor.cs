using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace MarkMpn.Sql4Cds.Engine.Visitors
{
    /// <summary>
    /// Replaces explicit collations with a function call to make further processing simpler
    /// </summary>
    class ExplicitCollationVisitor : RewriteVisitorBase
    {
        protected override ScalarExpression ReplaceExpression(ScalarExpression expression, out string name)
        {
            name = null;

            if (expression is PrimaryExpression primary &&
                primary.Collation != null)
            {
                return new FunctionCall
                {
                    FunctionName = new Identifier { Value = "ExplicitCollation" },
                    Parameters =
                    {
                        expression
                    }
                };
            }

            return expression;
        }

        protected override BooleanExpression ReplaceExpression(BooleanExpression expression)
        {
            return expression;
        }
    }
}
