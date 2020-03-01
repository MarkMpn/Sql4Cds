using MarkMpn.Sql4Cds.Engine.QueryExtensions;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace MarkMpn.Sql4Cds.Engine
{
    class ReplacePrimaryFunctionsVisitor : RewriteVisitorBase
    {
        protected override ScalarExpression ReplaceExpression(ScalarExpression expression, out string name)
        {
            name = null;

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

            return expression;
        }
    }
}
