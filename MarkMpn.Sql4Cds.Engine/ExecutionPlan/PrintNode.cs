using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlTypes;
using System.Text;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    class PrintNode : BaseNode, IDmlQueryExecutionPlanNode
    {
        private int _executionCount;
        private readonly Timer _timer = new Timer();
        private Func<ExpressionExecutionContext, object> _expression;

        public override int ExecutionCount => _executionCount;

        public override TimeSpan Duration => _timer.Duration;

        [Browsable(false)]
        public string Sql { get; set; }

        [Browsable(false)]
        public int Index { get; set; }

        [Browsable(false)]
        public int Length { get; set; }

        [Browsable(false)]
        public int LineNumber { get; set; }

        [Category("Print")]
        [Description("The value to print")]
        public ScalarExpression Expression { get; set; }

        public override void AddRequiredColumns(NodeCompilationContext context, IList<string> requiredColumns)
        {
        }

        public object Clone()
        {
            return new PrintNode
            {
                Expression = Expression,
                _expression = _expression,
                Index = Index,
                Length = Length,
                Sql = Sql,
                LineNumber = LineNumber,
            };
        }

        public void Execute(NodeExecutionContext context, out int recordsAffected, out string message)
        {
            _executionCount++;
            recordsAffected = -1;
            message = null;

            using (_timer.Run())
            {
                var value = (SqlString)_expression(new ExpressionExecutionContext(context));

                if (!value.IsNull)
                    context.Log(new Sql4CdsError(0, LineNumber, 0, null, null, 0, value.Value, null));
            }
        }

        public IRootExecutionPlanNodeInternal[] FoldQuery(NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            _expression = Expression.Compile(new ExpressionCompilationContext(context, null, null));
            return new[] { this };
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            return Array.Empty<IExecutionPlanNode>();
        }
    }
}
