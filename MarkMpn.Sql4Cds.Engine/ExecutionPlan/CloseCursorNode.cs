using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    class CloseCursorNode : BaseCursorNode, IDmlQueryExecutionPlanNode
    {
        public void Execute(NodeExecutionContext context, out int recordsAffected, out string message)
        {
            var cursor = GetCursor(context);

            cursor.Close(context);

            recordsAffected = 0;
            message = null;
        }

        public override object Clone()
        {
            return new CloseCursorNode
            {
                Index = Index,
                Length = Length,
                LineNumber = LineNumber,
                Sql = Sql,
                CursorName = CursorName
            };
        }
    }
}
