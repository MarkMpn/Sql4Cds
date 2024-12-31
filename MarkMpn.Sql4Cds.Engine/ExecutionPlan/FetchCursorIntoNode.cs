using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    class FetchCursorIntoNode : BaseNode, ISingleSourceExecutionPlanNode, IDmlQueryExecutionPlanNode
    {
        public override int ExecutionCount => throw new NotImplementedException();

        public override TimeSpan Duration => throw new NotImplementedException();

        public string Sql { get; set; }

        public int Index { get; set; }

        public int Length { get; set; }

        public int LineNumber { get; set; }

        public IDataExecutionPlanNodeInternal Source { get; set; }

        public string CursorName { get; set; }

        public IList<VariableReference> Variables { get; set; }

        public override void AddRequiredColumns(NodeCompilationContext context, IList<string> requiredColumns)
        {
            if (Source != null)
                Source.AddRequiredColumns(context, requiredColumns);
        }

        public void Execute(NodeExecutionContext context, out int recordsAffected, out string message)
        {
            throw new NotImplementedException();
        }

        public IRootExecutionPlanNodeInternal[] FoldQuery(NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            Source = Source?.FoldQuery(context, hints);

            return new[] { this };
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            if (Source != null)
                yield return Source;
        }

        public object Clone()
        {
            return new FetchCursorIntoNode
            {
                Index = Index,
                Length = Length,
                LineNumber = LineNumber,
                Sql = Sql,
                CursorName = CursorName,
                Variables = Variables?.Select(v => v.Clone()).Cast<VariableReference>().ToList(),
                Source = (IDataExecutionPlanNodeInternal)Source?.Clone()
            };
        }
    }
}
