using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Text;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    class GotoLabelNode : BaseNode, IRootExecutionPlanNodeInternal
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

        [Category("Label")]
        public string Label { get; set; }

        [Browsable(false)]
        internal LabelStatement Statement { get; set; }

        public override void AddRequiredColumns(NodeCompilationContext context, IList<string> requiredColumns)
        {
        }

        public object Clone()
        {
            return new GotoLabelNode
            {
                Sql = Sql,
                Index = Index,
                Length = Length,
                LineNumber = LineNumber,
                Label = Label,
                Statement = Statement
            };
        }

        public IRootExecutionPlanNodeInternal[] FoldQuery(NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            return new[] { this };
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            return Array.Empty<IExecutionPlanNode>();
        }

        public override string ToString()
        {
            return "LABEL\r\n" + Label;
        }
    }
}
