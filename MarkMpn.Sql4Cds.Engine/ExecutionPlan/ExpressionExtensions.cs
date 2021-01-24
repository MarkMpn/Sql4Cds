using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    static class ExpressionExtensions
    {
        public static object GetValue(this TSqlFragment expr, Entity entity)
        {
            var visitor = new ExpressionEvaluatorVisitor(entity);
            expr.Accept(visitor);

            return visitor.Value;
        }

        public static bool GetValue(this BooleanExpression expr, Entity entity)
        {
            return (bool)GetValue((TSqlFragment)expr, entity);
        }
    }
}
