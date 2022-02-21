using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    class ContinueBreakNode : BaseNode, IGoToNode
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
        public ContinueBreakNodeType Type { get; set; }

        public override void AddRequiredColumns(IDictionary<string, DataSource> dataSources, IDictionary<string, DataTypeReference> parameterTypes, IList<string> requiredColumns)
        {
        }

        public object Clone()
        {
            return new ContinueBreakNode
            {
                Sql = Sql,
                Index = Index,
                Length = Length,
                Type = Type
            };
        }

        public string Execute(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, DataTypeReference> parameterTypes, IDictionary<string, object> parameterValues)
        {
            throw new NotSupportedException(Type.ToString() + " node should have been converted to GOTO during query plan building");
        }

        public IRootExecutionPlanNodeInternal[] FoldQuery(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, DataTypeReference> parameterTypes, IList<OptimizerHint> hints)
        {
            if (hints != null && hints.OfType<ConditionalNode.DoNotCompileConditionsHint>().Any())
                return new[] { this };

            var parent = Parent;

            while (parent != null && (!(parent is ConditionalNode c) || c.Type != ConditionalNodeType.While))
                parent = parent.Parent;

            if (parent == null)
                return new[] { this };

            var loopNode = (ConditionalNode)parent;

            var statements = new List<IRootExecutionPlanNodeInternal>();

            if (Type == ContinueBreakNodeType.Continue)
            {
                statements.AddRange(
                    new GoToNode
                    {
                        Label = loopNode.TrueLabel,
                        Condition = loopNode.Condition,
                        Source = loopNode.Source,
                        SourceColumn = loopNode.SourceColumn
                    }.FoldQuery(dataSources, options, parameterTypes, hints));
            }

            statements.AddRange(
                new GoToNode
                {
                    Label = loopNode.FalseLabel
                }.FoldQuery(dataSources, options, parameterTypes, hints));

            return statements.ToArray();
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            return Array.Empty<IExecutionPlanNode>();
        }

        public override string ToString()
        {
            return Type.ToString().ToUpper();
        }
    }

    enum ContinueBreakNodeType
    {
        Continue,
        Break
    }
}
