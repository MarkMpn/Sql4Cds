using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;

namespace MarkMpn.Sql4Cds.Engine
{
    public interface ISql4CdsDataReader : IDataReader
    {
        IRootExecutionPlanNode CurrentResultQuery { get; }

        DataTable GetCurrentDataTable();
    }
}
