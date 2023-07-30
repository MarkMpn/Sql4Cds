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
        First,
        StringAgg
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
        public Func<ExpressionExecutionContext, object> Expression { get; set; }

        /// <summary>
        /// The type of value produced by the <see cref="Expression"/>
        /// </summary>
        [Browsable(false)]
        public DataTypeReference SourceType { get; set; }

        /// <summary>
        /// The type of value produced by the aggregate function
        /// </summary>
        [Browsable(false)]
        public DataTypeReference ReturnType { get; set; }

        /// <summary>
        /// The separator to apply to STRING_AGG aggregates
        /// </summary>
        [Browsable(false)]
        public SqlString Separator { get; internal set; }
    }

    /// <summary>
    /// Base class for calculating aggregate values
    /// </summary>
    abstract class AggregateFunction
    {
        private readonly Func<object> _selector;

        /// <summary>
        /// Creates a new <see cref="AggregateFunction"/>
        /// </summary>
        /// <param name="selector">The function that returns the value to aggregate from the source entity</param>
        public AggregateFunction(Func<object> selector)
        {
            _selector = selector;
        }

        /// <summary>
        /// Updates the aggregate function state based on the next <see cref="Entity"/> in the sequence
        /// </summary>
        /// <param name="state">The current state of the aggregation</param>
        /// <returns>The new state of the aggregation</returns>
        public virtual void NextRecord(object state)
        {
            var value = _selector();
            Update(value, state);
        }

        /// <summary>
        /// Updates the aggregate function state based on the aggregate values for a partition
        /// </summary>
        /// <param name="state">The current state of the aggregation</param>
        /// <returns>The new state of the aggregation</returns>
        public virtual void NextPartition(object state)
        {
            var value = _selector();
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
            public State(object sumState, object countState)
            {
                SumState = sumState;
                CountState = countState;
            }

            public object SumState { get; set; }
            public object CountState { get; set; }
        }

        private readonly Sum _sum;
        private readonly CountColumn _count;

        /// <summary>
        /// Creates a new <see cref="Average"/>
        /// </summary>
        /// <param name="selector">A function that extracts the value to calculate the average from</param>
        public Average(Func<object> selector, DataTypeReference sourceType, DataTypeReference returnType) : base(selector)
        {
            Type = returnType;

            _sum = new Sum(selector, sourceType, returnType);
            _count = new CountColumn(selector);
        }

        public override void NextRecord(object state)
        {
            var s = (State)state;

            _sum.NextRecord(s.SumState);
            _count.NextRecord(s.CountState);
        }

        protected override void Update(object value, object state)
        {
            throw new NotImplementedException();
        }

        protected override void UpdatePartition(object value, object state)
        {
            throw new InvalidOperationException();
        }

        public override object GetValue(object state)
        {
            var s = (State)state;

            var count = (SqlInt32) _count.GetValue(s.CountState);
            var sum = _sum.GetValue(s.SumState);

            if (sum is SqlInt32 i32)
                return count == 0 ? SqlInt32.Null : i32 / count;

            if (sum is SqlInt64 i64)
                return count == 0 ? SqlInt64.Null : i64 / count;

            if (sum is SqlDecimal dec)
                return count == 0 ? SqlDecimal.Null : dec / count;

            if (sum is SqlMoney money)
                return count == 0 ? SqlMoney.Null : money / count;

            if (sum is SqlDouble dbl)
                return count == 0 ? SqlDouble.Null : dbl / count;

            throw new InvalidOperationException();
        }

        public override DataTypeReference Type { get; }

        public override object Reset()
        {
            return new State(_sum.Reset(), _count.Reset());
        }
    }

    /// <summary>
    /// Counts all records
    /// </summary>
    class Count : AggregateFunction
    {
        class State
        {
            public SqlInt32 Value { get; set; } = 0;
        }

        /// <summary>
        /// Creates a new <see cref="Count"/>
        /// </summary>
        /// <param name="selector">Unused</param>
        public Count(Func<object> selector) : base(selector)
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

        public override DataTypeReference Type => DataTypeHelpers.Int;

        public override object Reset()
        {
            return new State();
        }
    }

    /// <summary>
    /// Counts non-null values
    /// </summary>
    class CountColumn : AggregateFunction
    {
        class State
        {
            public SqlInt32 Value { get; set; } = 0;
        }

        /// <summary>
        /// Creates a new <see cref="CountColumn"/>
        /// </summary>
        /// <param name="selector">A function that extracts the value to count non-null instances of</param>
        public CountColumn(Func<object> selector) : base(selector)
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

        public override DataTypeReference Type => DataTypeHelpers.Int;

        public override object Reset()
        {
            return new State();
        }
    }

    /// <summary>
    /// Identifies the maximum value
    /// </summary>
    class Max : AggregateFunction
    {
        class State
        {
            public State(IComparable value)
            {
                Value = value;
            }

            public IComparable Value { get; set; }
        }

        /// <summary>
        /// Creates a new <see cref="Max"/>
        /// </summary>
        /// <param name="selector">A function that extracts the value to find the maximum value of</param>
        public Max(Func<object> selector, DataTypeReference type) : base(selector)
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
            if (((INullable)s.Value).IsNull || s.Value.CompareTo(cmp) < 0)
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
            return new State((IComparable)SqlTypeConverter.GetNullValue(Type.ToNetType(out _)));
        }
    }

    /// <summary>
    /// Identifies the minimum value
    /// </summary>
    class Min : AggregateFunction
    {
        class State
        {
            public State(IComparable value)
            {
                Value = value;
            }

            public IComparable Value { get; set; }
        }

        /// <summary>
        /// Creates a new <see cref="Min"/>
        /// </summary>
        /// <param name="selector">A function that extracts the value to find the minimum value of</param>
        public Min(Func<object> selector, DataTypeReference type) : base(selector)
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

            if (((INullable)s.Value).IsNull || s.Value.CompareTo(cmp) > 0)
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
            return new State((IComparable)SqlTypeConverter.GetNullValue(Type.ToNetType(out _)));
        }
    }

    /// <summary>
    /// Calculates the sum
    /// </summary>
    class Sum : AggregateFunction
    {
        class State
        {
            public SqlInt32 Int32Value { get; set; } = 0;
            public SqlInt64 Int64Value { get; set; } = 0;
            public SqlDecimal DecimalValue { get; set; } = 0;
            public SqlMoney MoneyValue { get; set; } = 0;
            public SqlDouble FloatValue { get; set; } = 0;
        }

        private readonly SqlDataTypeOption _type;
        private readonly Func<object, object> _valueSelector;

        /// <summary>
        /// Creates a new <see cref="Sum"/>
        /// </summary>
        /// <param name="selector">A function that extracts the value to sum</param>
        public Sum(Func<object> selector, DataTypeReference sourceType, DataTypeReference returnType) : base(selector)
        {
            Type = returnType;

            _type = ((SqlDataTypeReference)returnType).SqlDataTypeOption;

            var valueParam = Expression.Parameter(typeof(object));
            var unboxed = Expression.Unbox(valueParam, sourceType.ToNetType(out _));
            var conversion = SqlTypeConverter.Convert(unboxed, sourceType, returnType);
            conversion = Expr.Box(conversion);
            _valueSelector = (Func<object, object>) Expression.Lambda(conversion, valueParam).Compile();
        }

        protected override void Update(object value, object state)
        {
            var d = (INullable)_valueSelector(value);

            if (d.IsNull)
                return;

            var s = (State)state;

            switch (_type)
            {
                case SqlDataTypeOption.Int:
                    s.Int32Value += (SqlInt32)d;
                    break;

                case SqlDataTypeOption.BigInt:
                    s.Int64Value += (SqlInt64)d;
                    break;

                case SqlDataTypeOption.Decimal:
                    s.DecimalValue += (SqlDecimal)d;
                    break;

                case SqlDataTypeOption.Money:
                    s.MoneyValue += (SqlMoney)d;
                    break;

                case SqlDataTypeOption.Float:
                    s.FloatValue += (SqlDouble)d;
                    break;

                default:
                    throw new InvalidOperationException();
            }
        }

        protected override void UpdatePartition(object value, object state)
        {
            Update(value, state);
        }

        public override object GetValue(object state)
        {
            switch (_type)
            {
                case SqlDataTypeOption.Int:
                    return ((State)state).Int32Value;

                case SqlDataTypeOption.BigInt:
                    return ((State)state).Int64Value;

                case SqlDataTypeOption.Decimal:
                    return ((State)state).DecimalValue;

                case SqlDataTypeOption.Money:
                    return ((State)state).MoneyValue;

                case SqlDataTypeOption.Float:
                    return ((State)state).FloatValue;

                default:
                    throw new InvalidOperationException();
            }
        }

        public override DataTypeReference Type { get; }

        public override object Reset()
        {
            return new State();
        }
    }

    class First : AggregateFunction
    {
        class State
        {
            public State(object value)
            {
                Value = value;
            }

            public bool Done { get; set; }

            public object Value { get; set; }
        }

        /// <summary>
        /// Creates a new <see cref="Sum"/>
        /// </summary>
        /// <param name="selector">A function that extracts the value to sum</param>
        public First(Func<object> selector, DataTypeReference type) : base(selector)
        {
            Type = type;
        }

        protected override void Update(object value, object state)
        {
            var s = (State)state;
            if (s.Done)
                return;

            s.Value = value;
            s.Done = true;
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
            return new State(SqlTypeConverter.GetNullValue(Type.ToNetType(out _)));
        }
    }

    class StringAgg : AggregateFunction
    {
        class State
        {
            public State()
            {
                Value = SqlString.Null;
            }

            public SqlString Value { get; set; }
        }

        private Func<object, SqlString> _valueSelector;

        /// <summary>
        /// Creates a new <see cref="Sum"/>
        /// </summary>
        /// <param name="selector">A function that extracts the value to sum</param>
        public StringAgg(Func<object> selector, DataTypeReference sourceType, DataTypeReference returnType) : base(selector)
        {
            Type = returnType;

            var valueParam = Expression.Parameter(typeof(object));
            var unboxed = Expression.Unbox(valueParam, sourceType.ToNetType(out _));
            var conversion = SqlTypeConverter.Convert(unboxed, sourceType, returnType);
            conversion = Expr.Convert(conversion, typeof(SqlString));
            _valueSelector = (Func<object, SqlString>)Expression.Lambda(conversion, valueParam).Compile();
        }

        protected override void Update(object value, object state)
        {
            var s = (State)state;

            var str = _valueSelector(value);

            if (str.IsNull)
                return;

            if (s.Value.IsNull)
                s.Value = str;
            else
                s.Value += Separator + str;
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

        public SqlString Separator { get; set; }

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
        private readonly Func<object> _selector;

        public DistinctAggregate(AggregateFunction func, Func<object> selector) : base(selector)
        {
            _func = func;
            _selector = selector;

            Expression = func.Expression;
            OutputName = func.OutputName;
        }

        public override DataTypeReference Type => _func.Type;

        public override void NextRecord(object state)
        {
            var value = _selector();
            var s = (State)state;

            if (s.Distinct.Add(value))
                _func.NextRecord(s.InnerState);
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
