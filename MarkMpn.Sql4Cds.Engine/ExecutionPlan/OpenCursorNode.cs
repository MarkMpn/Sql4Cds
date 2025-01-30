using System;
using System.Collections.Generic;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    class OpenCursorNode : BaseCursorNode, IDmlQueryExecutionPlanNode
    {
        private IDmlQueryExecutionPlanNode _populationQuery;

        public void Execute(NodeExecutionContext context, out int recordsAffected, out string message)
        {
            Execute(() =>
            {
                var cursor = GetCursor(context);

                _populationQuery = cursor.Open(context);

                if (_populationQuery != null)
                    _populationQuery.Execute(context, out _, out _);

            });

            recordsAffected = -1;
            message = null;
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            if (_populationQuery != null)
                yield return _populationQuery;
        }

        public override object Clone()
        {
            return new OpenCursorNode
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
