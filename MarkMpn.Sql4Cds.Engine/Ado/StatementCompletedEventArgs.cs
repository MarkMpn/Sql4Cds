using System;
using System.Collections.Generic;
using System.Text;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;

namespace MarkMpn.Sql4Cds.Engine
{
    public class StatementCompletedEventArgs
    {
        public StatementCompletedEventArgs(IRootExecutionPlanNode node, int recordsAffected)
        {
            Statement = node;
            RecordsAffected = recordsAffected;
        }

        public IRootExecutionPlanNode Statement { get; }

        public int RecordsAffected { get; }
    }
}
