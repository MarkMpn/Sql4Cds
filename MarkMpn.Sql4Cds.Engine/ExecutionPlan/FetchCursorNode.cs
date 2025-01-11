using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    class FetchCursorNode : BaseCursorNode, IDataReaderExecutionPlanNode
    {
        private CursorDeclarationBaseNode _cursor;
        private Func<ExpressionExecutionContext, object> _rowOffset;

        public FetchOrientation Orientation { get; set; }

        public ScalarExpression RowOffset { get; set; }

        public override IRootExecutionPlanNodeInternal[] FoldQuery(NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            if (RowOffset != null)
            {
                var ecc = new ExpressionCompilationContext(context, null, null);
                _rowOffset = RowOffset.Compile(ecc);
            }

            return base.FoldQuery(context, hints);
        }

        public DbDataReader Execute(NodeExecutionContext context, CommandBehavior behavior)
        {
            try
            {
                _cursor = GetCursor(context);

                var fetchQuery = _cursor.Fetch(context, Orientation, _rowOffset);

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
                Sql = Sql,
                Orientation = Orientation,
                RowOffset = RowOffset,
                _rowOffset = _rowOffset,
            };
        }
    }
}
