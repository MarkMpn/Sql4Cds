using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    class FetchCursorNode : BaseCursorNode, IDataReaderExecutionPlanNode
    {
        private IDataReaderExecutionPlanNode _fetchQuery;
        private Func<ExpressionExecutionContext, object> _rowOffset;
        private readonly Timer _timer = new Timer();

        public FetchOrientation Orientation { get; set; }

        public ScalarExpression RowOffset { get; set; }

        public override TimeSpan Duration => base.Duration + _timer.Duration;

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
            var reader = Execute(() =>
            {
                var cursor = GetCursor(context);

                _fetchQuery = cursor.Fetch(context, Orientation, _rowOffset);

                return _fetchQuery.Execute(context, behavior);
            });

            return new FetchStatusDataReader(reader, context, _timer.Run());
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            if (_fetchQuery != null)
                yield return _fetchQuery;
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
