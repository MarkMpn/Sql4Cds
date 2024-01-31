using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Text;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    class CompiledExpression
    {
        public CompiledExpression(ScalarExpression expression, Func<ExpressionExecutionContext, object> compiled)
        {
            Expression = expression;
            Compiled = compiled;
        }

        public ScalarExpression Expression { get; }

        [Browsable(false)]
        public Func<ExpressionExecutionContext, object> Compiled { get; }
    }

    class CompiledExpressionList : List<CompiledExpression>
    {
        public CompiledExpressionList(IEnumerable<CompiledExpression> expressions) : base(expressions)
        {
        }

        public Type ElementType { get; set; }
    }
}
