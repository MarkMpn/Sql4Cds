using System.Collections.Generic;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    class OpenCursorNode : BaseCursorNode, IDmlQueryExecutionPlanNode
    {
        private CursorDeclarationBaseNode _cursor;

        public void Execute(NodeExecutionContext context, out int recordsAffected, out string message)
        {
            _cursor = GetCursor(context);

            var populationQuery = _cursor.Open(context);

            if (populationQuery != null)
            {
                populationQuery.Execute(context, out recordsAffected, out message);
            }
            else
            {
                recordsAffected = 0;
                message = null;
            }
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            if (_cursor?.PopulationQuery != null)
                yield return _cursor.PopulationQuery;
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
