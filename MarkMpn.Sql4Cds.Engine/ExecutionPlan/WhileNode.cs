using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    class WhileNode : BaseNode, IControlOfFlowNode
    {
        private int _executionCount;
        private readonly Timer _timer = new Timer();

        public override int ExecutionCount => _executionCount;

        public override TimeSpan Duration => _timer.Duration;

        [Browsable(false)]
        public string Sql { get; set; }

        [Browsable(false)]
        public int Index { get; set; }

        [Browsable(false)]
        public int Length { get; set; }

        [Category("While")]
        [Description("The condition that must be true for execution to continue")]
        public BooleanExpression Condition { get; set; }

        [Browsable(false)]
        public IRootExecutionPlanNodeInternal[] Statements { get; set; }
        
        public override void AddRequiredColumns(IDictionary<string, DataSource> dataSources, IDictionary<string, DataTypeReference> parameterTypes, IList<string> requiredColumns)
        {
            foreach (var node in Statements)
                node.AddRequiredColumns(dataSources, parameterTypes, new List<string>(requiredColumns));
        }

        public IRootExecutionPlanNodeInternal[] Execute(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, DataTypeReference> parameterTypes, IDictionary<string, object> parameterValues, out bool rerun)
        {
            var expr = Condition.Compile(null, parameterTypes);

            if (expr(null, parameterValues, options))
            {
                rerun = true;
                return Statements;
            }
            else
            {
                rerun = false;
                return null;
            }
        }

        public IRootExecutionPlanNodeInternal FoldQuery(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, DataTypeReference> parameterTypes, IList<OptimizerHint> hints)
        {
            for (var i = 0; i < Statements.Length; i++)
                Statements[i] = Statements[i].FoldQuery(dataSources, options, parameterTypes, hints);

            return this;
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            return Statements;
        }
    }
}
