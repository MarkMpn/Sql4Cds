using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    class DeclareVariablesNode : BaseNode, IDmlQueryExecutionPlanNode
    {
        private int _executionCount;
        private readonly Timer _timer = new Timer();

        /// <summary>
        /// The SQL string that the query was converted from
        /// </summary>
        [Browsable(false)]
        public string Sql { get; set; }

        /// <summary>
        /// The position of the SQL query within the entire query text
        /// </summary>
        [Browsable(false)]
        public int Index { get; set; }

        /// <summary>
        /// The length of the SQL query within the entire query text
        /// </summary>
        [Browsable(false)]
        public int Length { get; set; }

        public override int ExecutionCount => _executionCount;

        public override TimeSpan Duration => _timer.Duration;

        [Category("Declare Variables")]
        [Description("The name and type of each variable to be declared")]
        public IDictionary<string, DataTypeReference> Variables { get; } = new Dictionary<string, DataTypeReference>();

        public override void AddRequiredColumns(NodeCompilationContext context, IList<string> requiredColumns)
        {
        }

        public void Execute(NodeExecutionContext context, out int recordsAffected)
        {
            _executionCount++;

            using (_timer.Run())
            {
                foreach (var variable in Variables)
                {
                    context.ParameterTypes[variable.Key] = variable.Value;
                    context.ParameterValues[variable.Key] = SqlTypeConverter.GetNullValue(variable.Value.ToNetType(out _));
                }
            }

            recordsAffected = -1;
        }

        public IRootExecutionPlanNodeInternal[] FoldQuery(NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            return new[] { this };
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            return Array.Empty<IExecutionPlanNode>();
        }

        public object Clone()
        {
            var clone = new DeclareVariablesNode
            {
                Sql = Sql,
                Index = Index,
                Length = Length
            };

            foreach (var kvp in Variables)
                clone.Variables.Add(kvp);

            return clone;
        }
    }
}
