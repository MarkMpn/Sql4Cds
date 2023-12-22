using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    class UnparsedConditionalNode : IRootExecutionPlanNodeInternal
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

        public ConditionalNodeType Type { get; set; }

        public BooleanExpression Predicate { get; set; }

        public List<IRootExecutionPlanNodeInternal> TrueStatements { get; } = new List<IRootExecutionPlanNodeInternal>();

        public List<IRootExecutionPlanNodeInternal> FalseStatements { get; } = new List<IRootExecutionPlanNodeInternal>();

        internal string LoopStartLabel { get; private set; }

        internal string TrueLabel { get; private set; }

        internal string FalseLabel { get; private set; }

        public IEnumerable<IExecutionPlanNode> GetSources()
        {
            return TrueStatements.Concat(FalseStatements);
        }

        public void AddRequiredColumns(NodeCompilationContext context, IList<string> requiredColumns)
        {
        }

        public object Clone()
        {
            var clone = new UnparsedConditionalNode
            {
                Statement = Statement,
                Compiler = Compiler,
                Optimizer = Optimizer,
                Sql = Sql,
                Index = Index,
                Length = Length,
                LineNumber = LineNumber,
                Predicate = Predicate.Clone(),
                LoopStartLabel = LoopStartLabel,
                TrueLabel = TrueLabel,
                FalseLabel = FalseLabel,
                Type = Type,
            };

            clone.TrueStatements.AddRange(TrueStatements);
            clone.FalseStatements.AddRange(FalseStatements);

            return clone;
        }

        public IRootExecutionPlanNodeInternal[] FoldQuery(NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            LoopStartLabel = Guid.NewGuid().ToString();
            TrueLabel = Guid.NewGuid().ToString();
            FalseLabel = Guid.NewGuid().ToString();

            var statements = new List<IRootExecutionPlanNodeInternal>();

            if (Type == ConditionalNodeType.While)
                statements.Add(new GotoLabelNode { Label = LoopStartLabel });

            statements.AddRange(
                new UnparsedGoToNode
                {
                    Label = TrueLabel,
                    Predicate = Predicate,
                    Compiler = Compiler,
                    Optimizer = Optimizer,
                }.FoldQuery(context, hints));

            statements.AddRange(
                new GoToNode
                {
                    Label = FalseLabel
                }.FoldQuery(context, hints));

            statements.Add(new GotoLabelNode { Label = TrueLabel });

            statements.AddRange(TrueStatements.SelectMany(stmt => stmt.FoldQuery(context, hints)));

            if (FalseStatements.Count == 0)
            {
                if (Type == ConditionalNodeType.While)
                {
                    statements.AddRange(
                        new GoToNode
                        {
                            Label = LoopStartLabel
                        }.FoldQuery(context, hints));
                }

                statements.Add(new GotoLabelNode { Label = FalseLabel });
            }
            else
            {
                var endLabel = Guid.NewGuid().ToString();
                statements.AddRange(
                    new GoToNode
                    {
                        Label = endLabel
                    }.FoldQuery(context, hints));

                statements.Add(new GotoLabelNode { Label = FalseLabel });

                statements.AddRange(FalseStatements.SelectMany(stmt => stmt.FoldQuery(context, hints)));

                statements.Add(new GotoLabelNode { Label = endLabel });
            }

            return statements.ToArray();
        }

        public void FinishedFolding()
        {
        }
    }
}
