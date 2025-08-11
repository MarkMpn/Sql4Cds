using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlTypes;
using System.Linq;
using System.Text;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    class UnparsedGoToNode : IJitStatement
    {
        private NodeCompilationContext _context;
        private IList<OptimizerHint> _hints;

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

        public string Label { get; set; }

        public BooleanExpression Predicate { get; set; }

        public IEnumerable<IExecutionPlanNode> GetSources()
        {
            return Array.Empty<IExecutionPlanNode>();
        }

        public void AddRequiredColumns(NodeCompilationContext context, IList<string> requiredColumns)
        {
        }

        public object Clone()
        {
            return new UnparsedGoToNode
            {
                Statement = Statement,
                Compiler = Compiler,
                Optimizer = Optimizer,
                Sql = Sql,
                Index = Index,
                Length = Length,
                LineNumber = LineNumber,
                Label = Label,
                Predicate = Predicate.Clone(),
                _context = _context,
                _hints = _hints
            };
        }

        public IRootExecutionPlanNodeInternal[] FoldQuery(NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            _context = context;
            _hints = hints;

            return new[] { this };
        }

        public void FinishedFolding(NodeCompilationContext context)
        {
        }

        public IRootExecutionPlanNodeInternal[] Compile()
        {
            var hasQuery = Compiler.ConvertPredicateQuery(Predicate, out var predicateSource, out var sourceCol);

            return new GoToNode
            {
                Condition = hasQuery ? null : Predicate,
                Label = Label,
                Source = predicateSource,
                SourceColumn = sourceCol
            }.FoldQuery(_context, _hints);
        }
    }
}
