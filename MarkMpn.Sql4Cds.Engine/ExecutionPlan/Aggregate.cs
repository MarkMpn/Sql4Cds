using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    public enum AggregateType
    {
        Min,
        Max,
        Count,
        CountStar,
        Sum,
        Average,
        First
    }

    public class Aggregate
    {
        public AggregateType AggregateType { get; set; }

        public bool Distinct { get; set; }

        public ScalarExpression SqlExpression { get; set; }

        [Browsable(false)]
        public Func<Entity, IDictionary<string, object>, object> Expression { get; set; }

        [Browsable(false)]
        public Type ExpressionType { get; set; }
    }
}
