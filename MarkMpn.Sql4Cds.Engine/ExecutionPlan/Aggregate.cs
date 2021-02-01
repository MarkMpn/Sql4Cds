using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    public enum AggregateType
    {
        Min,
        Max,
        Count,
        CountStar,
        Sum,
        Average
    }

    public class Aggregate
    {
        public AggregateType AggregateType { get; set; }

        public bool Distinct { get; set; }

        public ScalarExpression Expression { get; set; }
    }
}
