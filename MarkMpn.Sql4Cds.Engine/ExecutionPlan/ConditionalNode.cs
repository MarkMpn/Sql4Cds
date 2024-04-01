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
    class ConditionalNode : BaseNode, IGoToNode
    {
        public override int ExecutionCount => 0;

        public override TimeSpan Duration => TimeSpan.Zero;

        [Browsable(false)]
        public string Sql { get; set; }

        [Browsable(false)]
        public int Index { get; set; }

        [Browsable(false)]
        public int Length { get; set; }

        [Browsable(false)]
        public int LineNumber { get; set; }

        [Browsable(false)]
        public ConditionalNodeType Type { get; set; }

        [Category("Conditional")]
        [Description("The condition that must be true for execution to continue")]
        public BooleanExpression Condition { get; set; }

        [Browsable(false)]
        public IDataExecutionPlanNodeInternal Source { get; set; }

        [Browsable(false)]
        public string SourceColumn { get; set; }

        [Browsable(false)]
        public IRootExecutionPlanNodeInternal[] TrueStatements { get; set; }

        [Browsable(false)]
        public IRootExecutionPlanNodeInternal[] FalseStatements { get; set; }

        public override void AddRequiredColumns(NodeCompilationContext context, IList<string> requiredColumns)
        {
            if (Source != null)
                Source.AddRequiredColumns(context, new List<string>(requiredColumns));

            foreach (var node in TrueStatements)
                node.AddRequiredColumns(context, new List<string>(requiredColumns));

            if (FalseStatements != null)
            {
                foreach (var node in FalseStatements)
                    node.AddRequiredColumns(context, new List<string>(requiredColumns));
            }
        }

        public string Execute(NodeExecutionContext context)
        {
            throw new NotSupportedException("Conditional node should have been converted to GOTO during query plan building");
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

            foreach (var stmt in TrueStatements)
                yield return stmt;

            if (FalseStatements != null)
            {
                foreach (var stmt in FalseStatements)
                    yield return stmt;
            }
        }

        public override string ToString()
        {
            var name = base.ToString();

            if (Source != null)
                name += "\r\n(With Query)";

            return name;
        }

        public object Clone()
        {
            var clone = new ConditionalNode
            {
                Sql = Sql,
                Index = Index,
                Length = Length,
                LineNumber = LineNumber,
                Type = Type,
                Condition = Condition,
                Source = (IDataExecutionPlanNodeInternal) Source?.Clone(),
                SourceColumn = SourceColumn,
                TrueStatements = TrueStatements.Select(s => s.Clone()).Cast<IRootExecutionPlanNodeInternal>().ToArray(),
                FalseStatements = FalseStatements?.Select(s => s.Clone()).Cast<IRootExecutionPlanNodeInternal>().ToArray()
            };

            foreach (var s in clone.TrueStatements)
                s.Parent = clone;

            if (clone.FalseStatements != null)
            {
                foreach (var s in clone.FalseStatements)
                    s.Parent = clone;
            }

            if (clone.Source != null)
                clone.Source.Parent = clone;

            return clone;
        }

        internal class DoNotCompileConditionsHint : OptimizerHint
        {
        }
    }

    enum ConditionalNodeType
    {
        If,
        While
    }
}
