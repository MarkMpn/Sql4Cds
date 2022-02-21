using System;
using System.Collections.Generic;
using System.Text;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;

namespace MarkMpn.Sql4Cds.Engine
{
    /// <summary>
    /// The argument for the <see cref="Sql4CdsCommand.StatementCompleted"/> event
    /// </summary>
    public class StatementCompletedEventArgs
    {
        internal StatementCompletedEventArgs(IRootExecutionPlanNode node, int recordsAffected)
        {
            Statement = node;
            RecordsAffected = recordsAffected;
        }

        /// <summary>
        /// The execution plan for the statement that has completed
        /// </summary>
        public IRootExecutionPlanNode Statement { get; }

        /// <summary>
        /// The number of records that were affected by the completed statement
        /// </summary>
        public int RecordsAffected { get; }
    }
}
