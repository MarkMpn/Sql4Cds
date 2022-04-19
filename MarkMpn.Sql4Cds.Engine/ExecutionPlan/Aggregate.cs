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
        public DataTypeReference ReturnType { get; set; }
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
        /// <param name="state">The current state of the aggregation</param>
        /// <returns>The new state of the aggregation</returns>
        public virtual void NextRecord(Entity entity, object state)
        {
            var value = _selector == null ? entity : _selector(entity);
            Update(value, state);
        }

        /// <summary>
        /// Updates the aggregate function state based on the aggregate values for a partition
        /// </summary>
        /// <param name="entity">The <see cref="Entity"/> that contains aggregated values from a partition of the available records</param>
        /// <param name="state">The current state of the aggregation</param>
        /// <returns>The new state of the aggregation</returns>
        public virtual void NextPartition(Entity entity, object state)
        {
            var value = _selector(entity);
            UpdatePartition(value, state);
        }

        /// <summary>
        /// Updates the aggregation state based on a value extracted from the source <see cref="Entity"/>
        /// </summary>
        /// <param name="value"></param>
        /// <param name="state">The current state of the aggregation</param>
        protected abstract void Update(object value, object state);

        /// <summary>
        /// Updates the aggregation state based on a value extracted from the partition <see cref="Entity"/>
        /// </summary>
        /// <param name="value"></param>
        /// <param name="state">The current state of the aggregation</param>
        protected abstract void UpdatePartition(object value, object state);

        /// <summary>
        /// Returns the current value of this aggregation
        /// </summary>
        /// <param name="state">The current state of the aggregation</param>
        /// <returns>The value of the aggregation</returns>
        public abstract object GetValue(object state);

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
        public abstract DataTypeReference Type { get; }

        /// <summary>
        /// Resets this aggregation ready for the next group
        /// </summary>
        /// <returns>The initial state of the aggregation</returns>
        public abstract object Reset();
    }

    /// <summary>
    /// Calculates the mean value
    /// </summary>
    class Average : AggregateFunction
    {
        class State
        {
            public SqlDecimal Sum { get; set; }
            public int Count { get; set; }
        }

        private readonly Func<SqlDecimal, object> _valueSelector;

        /// <summary>
        /// Creates a new <see cref="Average"/>
        /// </summary>
        /// <param name="selector">A function that extracts the value to calculate the average from</param>
        public Average(Func<Entity, object> selector, DataTypeReference type) : base(selector)
        {
            Type = type;

            var valueParam = Expression.Parameter(typeof(SqlDecimal));
            var conversion = SqlTypeConverter.Convert(valueParam, Type);
            conversion = Expr.Box(conversion);
            _valueSelector = (Func<SqlDecimal, object>)Expression.Lambda(conversion, valueParam).Compile();
        }

        protected override void Update(object value, object state)
        {
            var d = (SqlDecimal)value;

            if (d.IsNull)
                return;

            var s = (State)state;
            s.Sum += d;
            s.Count++;
        }

        protected override void UpdatePartition(object value, object state)
        {
            throw new InvalidOperationException();
        }

        public override object GetValue(object state)
        {
            var s = (State)state;

            if (s.Count == 0)
                return _valueSelector(SqlDecimal.Null);

            return _valueSelector(s.Sum / s.Count);
        }

        public override DataTypeReference Type { get; }

        public override object Reset()
        {
            return new State();
        }
    }

    /// <summary>
    /// Counts all records
    /// </summary>
    class Count : AggregateFunction
    {
        private static readonly DataTypeReference _type = new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.Int };

        class State
        {
            public SqlInt32 Value { get; set; }
        }

        /// <summary>
        /// Creates a new <see cref="Count"/>
        /// </summary>
        /// <param name="selector">Unused</param>
        public Count(Func<Entity, object> selector) : base(selector)
        {
        }

        protected override void Update(object value, object state)
        {
            var s = (State)state;
            s.Value = s.Value + 1;
        }

        protected override void UpdatePartition(object value, object state)
        {
            var s = (State)state;
            s.Value = s.Value + (SqlInt32)value;
        }

        public override object GetValue(object state)
        {
            var s = (State)state;
            return s.Value;
        }

        public override DataTypeReference Type => _type;

        public override object Reset()
        {
            return new State { Value = 0 };
        }
    }

    /// <summary>
    /// Counts non-null values
    /// </summary>
    class CountColumn : AggregateFunction
    {
        private static readonly DataTypeReference _type = new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.Int };

        class State
        {
            public SqlInt32 Value { get; set; }
        }

        /// <summary>
        /// Creates a new <see cref="CountColumn"/>
        /// </summary>
        /// <param name="selector">A function that extracts the value to count non-null instances of</param>
        public CountColumn(Func<Entity, object> selector) : base(selector)
        {
        }

        protected override void Update(object value, object state)
        {
            if (value == null || (value is INullable nullable && nullable.IsNull))
                return;

            var s = (State)state;
            s.Value = s.Value + 1;
        }

        protected override void UpdatePartition(object value, object state)
        {
            var s = (State)state;
            s.Value = s.Value + (SqlInt32)value;
        }

        public override object GetValue(object state)
        {
            var s = (State)state;
            return s.Value;
        }

        public override DataTypeReference Type => _type;

        public override object Reset()
        {
            return new State { Value = 0 };
        }
    }

    /// <summary>
    /// Identifies the maximum value
    /// </summary>
    class Max : AggregateFunction
    {
        class State
        {
            public IComparable Value { get; set; }
        }

        /// <summary>
        /// Creates a new <see cref="Max"/>
        /// </summary>
        /// <param name="selector">A function that extracts the value to find the maximum value of</param>
        public Max(Func<Entity, object> selector, DataTypeReference type) : base(selector)
        {
            Type = type;
        }

        protected override void Update(object value, object state)
        {
            if (value == null || (value is INullable nullable && nullable.IsNull))
                return;

            if (!(value is IComparable cmp))
                throw new InvalidOperationException("MAX is not valid for values of type " + value.GetType().Name);

            var s = (State)state;
            if (s.Value == null || s.Value.CompareTo(cmp) < 0)
                s.Value = cmp;
        }

        protected override void UpdatePartition(object value, object state)
        {
            Update(value, state);
        }

        public override object GetValue(object state)
        {
            var s = (State)state;
            return s.Value;
        }

        public override DataTypeReference Type { get; }

        public override object Reset()
        {
            return new State();
        }
    }

    /// <summary>
    /// Identifies the minimum value
    /// </summary>
    class Min : AggregateFunction
    {
        class State
        {
            public IComparable Value { get; set; }
        }

        /// <summary>
        /// Creates a new <see cref="Min"/>
        /// </summary>
        /// <param name="selector">A function that extracts the value to find the minimum value of</param>
        public Min(Func<Entity, object> selector, DataTypeReference type) : base(selector)
        {
            Type = type;
        }

        protected override void Update(object value, object state)
        {
            if (value == null || (value is INullable nullable && nullable.IsNull))
                return;

            if (!(value is IComparable cmp))
                throw new InvalidOperationException("MAX is not valid for values of type " + value.GetType().Name);

            var s = (State)state;

            if (s.Value == null || s.Value.CompareTo(cmp) > 0)
                s.Value = cmp;
        }

        protected override void UpdatePartition(object value, object state)
        {
            Update(value, state);
        }

        public override object GetValue(object state)
        {
            var s = (State)state;
            return s.Value;
        }

        public override DataTypeReference Type { get; }

        public override object Reset()
        {
            return new State();
        }
    }

    /// <summary>
    /// Calculates the sum
    /// </summary>
    class Sum : AggregateFunction
    {
        class State
        {
            public SqlDecimal Value { get; set; }
        }

        private Func<SqlDecimal, object> _valueSelector;

        /// <summary>
        /// Creates a new <see cref="Sum"/>
        /// </summary>
        /// <param name="selector">A function that extracts the value to sum</param>
        public Sum(Func<Entity, object> selector, DataTypeReference type) : base(selector)
        {
            Type = type;

            var valueParam = Expression.Parameter(typeof(SqlDecimal));
            var conversion = SqlTypeConverter.Convert(valueParam, Type);
            conversion = Expr.Box(conversion);
            _valueSelector = (Func<SqlDecimal, object>) Expression.Lambda(conversion, valueParam).Compile();
        }

        protected override void Update(object value, object state)
        {
            var d = (SqlDecimal)value;

            if (d.IsNull)
                return;

            var s = (State)state;
            s.Value += d;
        }

        protected override void UpdatePartition(object value, object state)
        {
            Update(value, state);
        }

        public override object GetValue(object state)
        {
            return _valueSelector(((State)state).Value);
        }

        public override DataTypeReference Type { get; }

        public override object Reset()
        {
            return new State { Value = 0 };
        }
    }

    class First : AggregateFunction
    {
        class State
        {
            public bool Done { get; set; }

            public object Value { get; set; }
        }

        /// <summary>
        /// Creates a new <see cref="Sum"/>
        /// </summary>
        /// <param name="selector">A function that extracts the value to sum</param>
        public First(Func<Entity, object> selector, DataTypeReference type) : base(selector)
        {
            Type = type;
        }

        protected override void Update(object value, object state)
        {
            var s = (State)state;
            if (s.Done)
                return;

            s.Value = value;
        }

        protected override void UpdatePartition(object value, object state)
        {
            throw new InvalidOperationException();
        }

        public override object GetValue(object state)
        {
            var s = (State)state;

            return s.Value;
        }

        public override DataTypeReference Type { get; }

        public override object Reset()
        {
            return new State();
        }
    }

    class DistinctAggregate : AggregateFunction
    {
        class State
        {
            public HashSet<object> Distinct { get; set; }
            public object InnerState { get; set; }
        }

        private readonly AggregateFunction _func;
        private readonly Func<Entity, object> _selector;

        public DistinctAggregate(AggregateFunction func, Func<Entity, object> selector) : base(selector)
        {
            _func = func;
            _selector = selector;

            Expression = func.Expression;
            OutputName = func.OutputName;
            SqlExpression = func.SqlExpression;
        }

        public override DataTypeReference Type => _func.Type;

        public override void NextRecord(Entity entity, object state)
        {
            var value = _selector(entity);
            var s = (State)state;

            if (s.Distinct.Add(value))
                _func.NextRecord(entity, s.InnerState);
        }

        protected override void UpdatePartition(object value, object state)
        {
            throw new InvalidOperationException();
        }

        protected override void Update(object value, object state)
        {
            throw new NotImplementedException();
        }

        public override object GetValue(object state)
        {
            var s = (State)state;
            return _func.GetValue(s.InnerState);
        }

        public override object Reset()
        {
            return new State
            {
                Distinct = new HashSet<object>(),
                InnerState = _func.Reset()
            };
        }
    }
}
