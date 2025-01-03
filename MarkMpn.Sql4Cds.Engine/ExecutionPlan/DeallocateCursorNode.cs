namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    class DeallocateCursorNode : BaseCursorNode, IDmlQueryExecutionPlanNode
    {
        public void Execute(NodeExecutionContext context, out int recordsAffected, out string message)
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
