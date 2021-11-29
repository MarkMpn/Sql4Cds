using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlTypes;
using System.Linq.Expressions;
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
        public Func<Entity, IDictionary<string, object>, IQueryExecutionOptions, object> Expression { get; set; }

        /// <summary>
        /// The type of value produced by the aggregate function
        /// </summary>
        [Browsable(false)]
        public Type ReturnType { get; set; }
    }

    /// <summary>
    /// Base class for calculating aggregate values
    /// </summary>
    abstract class AggregateFunction
    {
        private readonly Func<Entity, object> _selector;

        /// <summary>
        /// Creates a new <see cref="AggregateFunction"/>
        /// </summary>
        /// <param name="selector">The function that returns the value to aggregate from the source entity</param>
        public AggregateFunction(Func<Entity, object> selector)
        {
            _selector = selector;
        }

        /// <summary>
        /// Updates the aggregate function state based on the next <see cref="Entity"/> in the sequence
        /// </summary>
        /// <param name="entity">The <see cref="Entity"/> to take the value from and apply to this aggregation</param>
        public virtual void NextRecord(Entity entity)
        {
            var value = _selector == null ? entity : _selector(entity);
            Update(value);
        }

        /// <summary>
        /// Updates the aggregation state based on a value extracted from the source <see cref="Entity"/>
        /// </summary>
        /// <param name="value"></param>
        protected abstract void Update(object value);

        /// <summary>
        /// Returns the current value of this aggregation
        /// </summary>
        public object Value { get; protected set; }

        /// <summary>
        /// Returns the name of the column that will store the result of this aggregation in the aggregated dataset
        /// </summary>
        public string OutputName { get; set; }

        /// <summary>
        /// Returns the SQL fragment that this aggregate was converted from
        /// </summary>
        public ScalarExpression SqlExpression { get; set; }

        /// <summary>
        /// Returns the <see cref="SqlExpression"/> being aggregated converted to a <see cref="System.Linq.Expressions.Expression"/>
        /// </summary>
        public Expression Expression { get; set; }

        /// <summary>
        /// Returns the type of value that will be produced by this aggregation
        /// </summary>
        public abstract Type Type { get; }

        /// <summary>
        /// Resets this aggregation ready for the next group
        /// </summary>
        public virtual void Reset()
        {
            Value = null;
        }
    }

    /// <summary>
    /// Calculates the mean value
    /// </summary>
    class Average : AggregateFunction
    {
        private SqlDecimal _sum;
        private int _count;
        private Func<SqlDecimal, object> _valueSelector;

        /// <summary>
        /// Creates a new <see cref="Average"/>
        /// </summary>
        /// <param name="selector">A function that extracts the value to calculate the average from</param>
        public Average(Func<Entity, object> selector, Type type) : base(selector)
        {
            Type = type;

            var valueParam = Expression.Parameter(typeof(SqlDecimal));
            var conversion = SqlTypeConverter.Convert(valueParam, Type);
            conversion = Expr.Box(conversion);
            _valueSelector = (Func<SqlDecimal, object>)Expression.Lambda(conversion, valueParam).Compile();
        }

        protected override void Update(object value)
        {
            var d = (SqlDecimal)value;

            if (d.IsNull)
                return;

            _sum += d;
            _count++;

            Value = _valueSelector(_sum / _count);
        }

        public override Type Type { get; }

        public override void Reset()
        {
            _sum = 0;
            _count = 0;
            base.Reset();
        }
    }

    /// <summary>
    /// Counts all records
    /// </summary>
    class Count : AggregateFunction
    {
        /// <summary>
        /// Creates a new <see cref="Count"/>
        /// </summary>
        /// <param name="selector">Unused</param>
        public Count(Func<Entity, object> selector) : base(selector)
        {
        }

        protected override void Update(object value)
        {
            Value = (SqlInt32)Value + 1;
        }

        public override Type Type => typeof(SqlInt32);

        public override void Reset()
        {
            Value = new SqlInt32(0);
        }
    }

    /// <summary>
    /// Counts non-null values
    /// </summary>
    class CountColumn : AggregateFunction
    {
        /// <summary>
        /// Creates a new <see cref="CountColumn"/>
        /// </summary>
        /// <param name="selector">A function that extracts the value to count non-null instances of</param>
        public CountColumn(Func<Entity, object> selector) : base(selector)
        {
        }

        protected override void Update(object value)
        {
            if (value == null || (value is INullable nullable && nullable.IsNull))
                return;

            Value = (SqlInt32)Value + 1;
        }

        public override Type Type => typeof(int);

        public override void Reset()
        {
            Value = new SqlInt32(0);
        }
    }

    /// <summary>
    /// Identifies the maximum value
    /// </summary>
    class Max : AggregateFunction
    {
        /// <summary>
        /// Creates a new <see cref="Max"/>
        /// </summary>
        /// <param name="selector">A function that extracts the value to find the maximum value of</param>
        public Max(Func<Entity, object> selector, Type type) : base(selector)
        {
            Type = type;
        }

        protected override void Update(object value)
        {
            if (value == null || (value is INullable nullable && nullable.IsNull))
                return;

            if (!(value is IComparable))
                throw new InvalidOperationException("MAX is not valid for values of type " + value.GetType().Name);

            if (Value == null || ((IComparable)Value).CompareTo(value) < 0)
                Value = value;
        }

        public override Type Type { get; }
    }

    /// <summary>
    /// Identifies the minimum value
    /// </summary>
    class Min : AggregateFunction
    {
        /// <summary>
        /// Creates a new <see cref="Min"/>
        /// </summary>
        /// <param name="selector">A function that extracts the value to find the minimum value of</param>
        public Min(Func<Entity, object> selector, Type type) : base(selector)
        {
            Type = type;
        }

        protected override void Update(object value)
        {
            if (value == null || (value is INullable nullable && nullable.IsNull))
                return;

            if (!(value is IComparable))
                throw new InvalidOperationException("MAX is not valid for values of type " + value.GetType().Name);

            if (Value == null || ((IComparable)Value).CompareTo(value) > 0)
                Value = value;
        }

        public override Type Type { get; }
    }

    /// <summary>
    /// Calculates the sum
    /// </summary>
    class Sum : AggregateFunction
    {
        private SqlDecimal _sumDecimal;
        private Func<SqlDecimal, object> _valueSelector;

        /// <summary>
        /// Creates a new <see cref="Sum"/>
        /// </summary>
        /// <param name="selector">A function that extracts the value to sum</param>
        public Sum(Func<Entity, object> selector, Type type) : base(selector)
        {
            Type = type;

            var valueParam = Expression.Parameter(typeof(SqlDecimal));
            var conversion = SqlTypeConverter.Convert(valueParam, Type);
            conversion = Expr.Box(conversion);
            _valueSelector = (Func<SqlDecimal, object>) Expression.Lambda(conversion, valueParam).Compile();
        }

        protected override void Update(object value)
        {
            var d = (SqlDecimal)value;

            if (d.IsNull)
                return;

            _sumDecimal += d;

            Value = _valueSelector(_sumDecimal);
        }

        public override Type Type { get; }

        public override void Reset()
        {
            base.Reset();
            _sumDecimal = 0;
            Value = _valueSelector(_sumDecimal);
        }
    }

    class First : AggregateFunction
    {
        private bool _done;

        /// <summary>
        /// Creates a new <see cref="Sum"/>
        /// </summary>
        /// <param name="selector">A function that extracts the value to sum</param>
        public First(Func<Entity, object> selector, Type type) : base(selector)
        {
            Type = type;
        }

        protected override void Update(object value)
        {
            if (_done)
                return;

            Value = value;
        }

        public override Type Type { get; }

        public override void Reset()
        {
            base.Reset();
            _done = false;
        }
    }

    class DistinctAggregate : AggregateFunction
    {
        private readonly HashSet<object> _distinct;
        private readonly AggregateFunction _func;
        private readonly Func<Entity, object> _selector;

        public DistinctAggregate(AggregateFunction func, Func<Entity, object> selector) : base(selector)
        {
            _func = func;
            _distinct = new HashSet<object>(CaseInsensitiveObjectComparer.Instance);
            _selector = selector;

            Expression = func.Expression;
            OutputName = func.OutputName;
            SqlExpression = func.SqlExpression;
        }

        public override Type Type => _func.Type;

        public override void NextRecord(Entity entity)
        {
            var value = _selector(entity);

            if (_distinct.Add(value))
            {
                _func.NextRecord(entity);
                Value = _func.Value;
            }
        }

        protected override void Update(object value)
        {
            throw new NotImplementedException();
        }

        public override void Reset()
        {
            _distinct.Clear();
            _func.Reset();
            Value = _func.Value;
        }
    }
}
