using System;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    class DeallocateCursorNode : BaseCursorNode, IDmlQueryExecutionPlanNode
    {
        public void Execute(NodeExecutionContext context, out int recordsAffected, out string message)
        {
            try
            {
                var cursor = GetCursor(context);

                if (context.Cursors.TryGetValue(CursorName, out var cursorDeclaration) && cursorDeclaration == cursor)
                    context.Cursors.Remove(CursorName);
                else if (context.Session.Cursors.TryGetValue(CursorName, out cursorDeclaration) && cursorDeclaration == cursor)
                    context.Session.Cursors.Remove(CursorName);
                else
                    throw new QueryExecutionException(Sql4CdsError.InvalidCursorName(CursorName));

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
            return new DeallocateCursorNode
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
