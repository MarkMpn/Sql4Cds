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
    /// <summary>
    /// Describes the type of aggregate function
    /// </summary>
    enum AggregateType
    {
        Min,
        Max,
        Count,
        CountStar,
        Sum,
        Average,
        First
    }

    /// <summary>
    /// Holds the details of an aggregate to calculate
    /// </summary>
    class Aggregate
    {
        /// <summary>
        /// The type of aggregate to calculate
        /// </summary>
        [Description("The type of aggregate to calculate")]
        public AggregateType AggregateType { get; set; }

        /// <summary>
        /// Indicates if only distinct values should be used in the calculation
        /// </summary>
        [Description("Indicates if only distinct values should be used in the calculation")]
        public bool Distinct { get; set; }

        /// <summary>
        /// The expression that the aggregate is calculated from
        /// </summary>
        [Description("The expression that the aggregate is calculated from")]
        public ScalarExpression SqlExpression { get; set; }

        /// <summary>
        /// A compiled version of the <see cref="SqlExpression"/> that takes the row values and parameter values and returns the value to add to the aggregate
        /// </summary>
        [Browsable(false)]
        public Func<Entity, IDictionary<string, object>, object> Expression { get; set; }

        /// <summary>
        /// The type of value produced by the aggregate function
        /// </summary>
        [Browsable(false)]
        public Type ExpressionType { get; set; }
    }
}
