using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlTypes;
using System.Linq;
using System.Text;
using System.Threading;
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

        public override string Execute(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, DataTypeReference> parameterTypes, IDictionary<string, object> parameterValues, out int recordsAffected, CancellationToken cancellationToken)
        {
            _executionCount++;

            using (_timer.Run())
            {
                var count = 0;

                foreach (var entity in GetDmlSourceEntities(dataSources, options, parameterTypes, parameterValues, out _, cancellationToken))
                {
                    foreach (var variable in Variables)
                        parameterValues[variable.VariableName] = entity[variable.SourceColumn];

                    count++;
                }

                parameterValues["@@ROWCOUNT"] = (SqlInt32)count;
            }

            recordsAffected = -1;
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

        public override object Clone()
        {
            var clone = new AssignVariablesNode
            {
                DataSource = DataSource,
                Index = Index,
                Length = Length,
                Source = (IExecutionPlanNodeInternal)Source.Clone(),
                Sql = Sql
            };

            clone.Source.Parent = clone;
            clone.Variables.AddRange(Variables);

            return clone;
        }
    }

    class VariableAssignment
    {
        public string VariableName { get; set; }

        public string SourceColumn { get; set; }
    }
}
