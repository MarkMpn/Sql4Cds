using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    class AssignVariablesNode : BaseDmlNode
    {
        private int _executionCount;
        private readonly Timer _timer = new Timer();

        [Browsable(false)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override string DataSource { get => base.DataSource; set => base.DataSource = value; }

        [Category("Assign Variables")]
        public List<VariableAssignment> Variables { get; } = new List<VariableAssignment>();

        public override int ExecutionCount => _executionCount;

        public override TimeSpan Duration => _timer.Duration;

        public override string Execute(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, DataTypeReference> parameterTypes, IDictionary<string, object> parameterValues)
        {
            _executionCount++;

            using (_timer.Run())
            {
                foreach (var entity in GetDmlSourceEntities(dataSources, options, parameterTypes, parameterValues, out _))
                {
                    foreach (var variable in Variables)
                        parameterValues[variable.VariableName] = entity[variable.SourceColumn];
                }
            }

            return null;
        }

        public override void AddRequiredColumns(IDictionary<string, DataSource> dataSources, IDictionary<string, DataTypeReference> parameterTypes, IList<string> requiredColumns)
        {
            foreach (var variable in Variables)
            {
                if (!requiredColumns.Contains(variable.SourceColumn, StringComparer.OrdinalIgnoreCase))
                    requiredColumns.Add(variable.SourceColumn);
            }

            Source.AddRequiredColumns(dataSources, parameterTypes, requiredColumns);
        }
    }

    class VariableAssignment
    {
        public string VariableName { get; set; }

        public string SourceColumn { get; set; }
    }
}
