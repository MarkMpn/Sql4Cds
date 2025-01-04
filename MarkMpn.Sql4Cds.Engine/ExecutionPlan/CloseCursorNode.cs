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
            try
            {
                var cursor = GetCursor(context);

                cursor.Close(context);

                recordsAffected = 0;
                message = null;
            }
            catch (QueryExecutionException ex)
            {
                if (ex.Node == null)
                    ex.Node = this;

                throw;
            }
            catch (Exception ex)
            {
                throw new QueryExecutionException(Sql4CdsError.InternalError(ex.Message), ex) { Node = this };
            }
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
