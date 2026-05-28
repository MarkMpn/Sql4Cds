using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    internal class ExecuteSqlNode : IRootExecutionPlanNodeInternal
    {
        public ExecuteStatement Statement { get; set; }

        public string Sql { get; set; }
        
        public int Index { get; set; }
        
        public int Length { get; set; }
        
        public int LineNumber { get; set; }
        
        public IExecutionPlanNode Parent { get; set; }

        public int ExecutionCount => 0;

        public TimeSpan Duration => TimeSpan.Zero;

        public void AddRequiredColumns(NodeCompilationContext context, IList<string> requiredColumns)
        {
        }

        public void FinishedFolding(NodeCompilationContext context)
        {
        }

        public IRootExecutionPlanNodeInternal[] FoldQuery(NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            return new[] { this };
        }

        public IEnumerable<IExecutionPlanNode> GetSources()
        {
            return Enumerable.Empty<IExecutionPlanNode>();
        }

        public object Clone()
        {
            return new ExecuteSqlNode
            {
                Statement = Statement.Clone(),
                Sql = Sql,
                Index = Index,
                Length = Length,
                LineNumber = LineNumber
            };
        }
    }
}
