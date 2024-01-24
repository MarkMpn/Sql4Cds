using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Text;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    abstract class TryCatchNodeBase : BaseNode, IRootExecutionPlanNodeInternal
    {
        [Browsable(false)]
        public string Sql { get; set; }

        [Browsable(false)]
        public int Index { get; set; }

        [Browsable(false)]
        public int Length { get; set; }

        [Browsable(false)]
        public int LineNumber { get; set; }

        public override int ExecutionCount => 0;

        public override TimeSpan Duration => TimeSpan.Zero;

        public override void AddRequiredColumns(NodeCompilationContext context, IList<string> requiredColumns)
        {
        }

        public abstract object Clone();

        public IRootExecutionPlanNodeInternal[] FoldQuery(NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            return new[] { this };
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            return Array.Empty<IExecutionPlanNode>();
        }
    }

    class BeginTryNode : TryCatchNodeBase
    {
        public override object Clone()
        {
            return new BeginTryNode();
        }
    }

    class EndTryNode : TryCatchNodeBase
    {
        public override object Clone()
        {
            return new EndTryNode();
        }
    }

    class BeginCatchNode : TryCatchNodeBase
    {
        public override object Clone()
        {
            return new BeginCatchNode();
        }
    }

    class EndCatchNode : TryCatchNodeBase
    {
        public override object Clone()
        {
            return new EndCatchNode();
        }
    }
}
