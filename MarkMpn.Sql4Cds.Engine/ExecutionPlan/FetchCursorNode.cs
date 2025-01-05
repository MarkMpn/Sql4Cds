using System;
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
            try
            {
                _cursor = GetCursor(context);

                var fetchQuery = _cursor.Fetch(context);

                return new FetchStatusDataReader(fetchQuery.Execute(context, behavior), context);
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
