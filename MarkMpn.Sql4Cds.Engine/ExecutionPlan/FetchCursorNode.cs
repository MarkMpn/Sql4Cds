using System.Collections.Generic;
using System.Data;
using System.Data.Common;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    class FetchCursorNode : BaseCursorNode, IDataReaderExecutionPlanNode
    {
        private CursorDeclarationBaseNode _cursor;

        public DbDataReader Execute(NodeExecutionContext context, CommandBehavior behavior)
        {
            _cursor = GetCursor(context);

            var fetchQuery = _cursor.Fetch(context);

            return fetchQuery.Execute(context, behavior);
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            if (_cursor?.FetchQuery != null)
                yield return _cursor.FetchQuery;
        }

        public override object Clone()
        {
            return new FetchCursorNode
            {
                CursorName = CursorName,
                Index = Index,
                Length = Length,
                LineNumber = LineNumber,
                Sql = Sql
            };
        }
    }
}
