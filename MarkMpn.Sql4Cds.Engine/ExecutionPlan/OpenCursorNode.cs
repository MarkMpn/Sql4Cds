using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    class OpenCursorNode : BaseNode, IRootExecutionPlanNodeInternal
    {
        public string Sql { get; set; }
        public int Index { get; set; }
        public int Length { get; set; }
        public int LineNumber { get; set; }
        public override int ExecutionCount => 0;

        public override TimeSpan Duration => TimeSpan.Zero;

        public string CursorName { get; set; }

        public override void AddRequiredColumns(NodeCompilationContext context, IList<string> requiredColumns)
        {
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            return Enumerable.Empty<IExecutionPlanNode>();
        }

        public object Clone()
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

        public IRootExecutionPlanNodeInternal[] FoldQuery(NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            return new[] { this };
        }
    }
}
