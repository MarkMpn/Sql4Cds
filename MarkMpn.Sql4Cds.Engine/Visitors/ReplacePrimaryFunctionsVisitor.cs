using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace MarkMpn.Sql4Cds.Engine
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

            // TODO: Handle other primary functions

            return expression;
        }
    }
}
