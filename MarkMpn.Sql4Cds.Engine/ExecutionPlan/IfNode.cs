using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    class IfNode : BaseNode, IControlOfFlowNode
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

        [Category("If")]
        [Description("The condition that must be true for execution to continue")]
        public BooleanExpression Condition { get; set; }

        [Browsable(false)]
        public IRootExecutionPlanNodeInternal[] TrueStatements { get; set; }

        [Browsable(false)]
        public IRootExecutionPlanNodeInternal[] FalseStatements { get; set; }

        public override void AddRequiredColumns(IDictionary<string, DataSource> dataSources, IDictionary<string, DataTypeReference> parameterTypes, IList<string> requiredColumns)
        {
            foreach (var node in TrueStatements)
                node.AddRequiredColumns(dataSources, parameterTypes, new List<string>(requiredColumns));

            if (FalseStatements != null)
            {
                foreach (var node in FalseStatements)
                    node.AddRequiredColumns(dataSources, parameterTypes, new List<string>(requiredColumns));
            }
        }

        public IRootExecutionPlanNodeInternal[] Execute(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, DataTypeReference> parameterTypes, IDictionary<string, object> parameterValues, out bool rerun)
        {
            rerun = false;
            var expr = Condition.Compile(null, parameterTypes);

            if (expr(null, parameterValues, options))
                return TrueStatements;
            else
                return FalseStatements;
        }

        public IRootExecutionPlanNodeInternal FoldQuery(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, DataTypeReference> parameterTypes, IList<OptimizerHint> hints)
        {
            for (var i = 0; i < TrueStatements.Length; i++)
                TrueStatements[i] = TrueStatements[i].FoldQuery(dataSources, options, parameterTypes, hints);

            if (FalseStatements != null)
            {
                for (var i = 0; i < FalseStatements.Length; i++)
                    FalseStatements[i] = FalseStatements[i].FoldQuery(dataSources, options, parameterTypes, hints);
            }

            return this;
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            if (FalseStatements == null)
                return TrueStatements;

            return TrueStatements.Concat(FalseStatements);
        }
    }
}
