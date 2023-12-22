using System;
using System.Collections.Generic;
using System.Text;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;

namespace MarkMpn.Sql4Cds.Engine
{
    public class StatementCompletedEventArgs
    {
        public StatementCompletedEventArgs(IRootExecutionPlanNode node, int recordsAffected, string message)
        {
            Statement = node;
            RecordsAffected = recordsAffected;
            Message = message;
        }

        public IRootExecutionPlanNode Statement { get; }

        public int RecordsAffected { get; }

        public string Message { get; }
    }
}
