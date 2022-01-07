using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
        public IDictionary<string, Type> Variables { get; } = new Dictionary<string, Type>();

        public override void AddRequiredColumns(IDictionary<string, DataSource> dataSources, IDictionary<string, Type> parameterTypes, IList<string> requiredColumns)
        {
        }

        public string Execute(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues)
        {
            _executionCount++;

            using (_timer.Run())
            {
                foreach (var variable in Variables)
                {
                    parameterTypes[variable.Key] = variable.Value;
                    parameterValues[variable.Key] = Activator.CreateInstance(variable.Value);
                }
            }

            return null;
        }

        public IRootExecutionPlanNode FoldQuery(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes)
        {
            return this;
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            return Array.Empty<IExecutionPlanNode>();
        }
    }
}
