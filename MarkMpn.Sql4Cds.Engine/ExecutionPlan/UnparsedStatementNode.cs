using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Text;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    class UnparsedStatementNode : IJitStatement
    {
        public TSqlStatement Statement { get; set; }

        public ExecutionPlanBuilder Compiler { get; set; }

        public ExecutionPlanOptimizer Optimizer { get; set; }

        public string Sql { get; set; }

        public int Index { get; set; }

        public int Length { get; set; }

        public int LineNumber { get; set; }

        public IExecutionPlanNode Parent { get; set; }

        public int ExecutionCount => 0;

        public TimeSpan Duration => TimeSpan.Zero;

        public IEnumerable<IExecutionPlanNode> GetSources()
        {
            return Array.Empty<IExecutionPlanNode>();
        }

        public void AddRequiredColumns(NodeCompilationContext context, IList<string> requiredColumns)
        {
        }

        public object Clone()
        {
            return new UnparsedStatementNode
            {
                Statement = Statement,
                Compiler = Compiler,
                Optimizer = Optimizer,
                Sql = Sql,
                Index = Index,
                Length = Length,
                LineNumber = LineNumber,
            };
        }

        public IRootExecutionPlanNodeInternal[] FoldQuery(NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            return new[] { this };
        }

        public void FinishedFolding(NodeCompilationContext context)
        {
        }

        public IRootExecutionPlanNodeInternal[] Compile()
        {
            return Compiler.ConvertStatementInternal(Statement, Optimizer);
        }
    }
}
