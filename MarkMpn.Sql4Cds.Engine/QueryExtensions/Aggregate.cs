using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace MarkMpn.Sql4Cds.Engine.QueryExtensions
{
    /// <summary>
    /// Group the records and apply aggregate functions
    /// </summary>
    class Aggregate : IQueryExtension
    {
        private readonly IList<Grouping> _groupings;
        private readonly IList<AggregateFunction> _aggregates;

        public Aggregate(IList<Grouping> groupings, IList<AggregateFunction> aggregates)
        {
            _groupings = groupings;
            _aggregates = aggregates;
        }

        /// <summary>
        /// Groups entities and calculates aggregates within a sorted sequence of entities
        /// </summary>
        /// <param name="source">The sequence of entities to group, sorted by the grouping attributes</param>
        /// <param name="options">The options to apply to the query execution</param>
        /// <returns>A sequence of entities representing the groups found within the <paramref name="source"/></returns>
        /// <remarks>
        /// This method assumes that the <paramref name="source"/> is already sorted by the grouping attributes. If the list is not correctly
        /// sorted then there may be duplicate groups produced in the output
        /// </remarks>
        public IEnumerable<Entity> ApplyTo(IEnumerable<Entity> source, IQueryExecutionOptions options)
        {
            var groupByValues = new object[_groupings.Count];
            var first = true;
            string entityName = null;

            foreach (var entity in source)
            {
                if (options.Cancelled)
                    throw new OperationCanceledException();

                // If this is the first record in the sequence, start a new group without producing an empty group
                if (first)
                {
                    foreach (var aggregate in _aggregates)
                        aggregate.Reset();

                    entityName = entity.LogicalName;

                    for (var i = 0; i < _groupings.Count; i++)
                        groupByValues[i] = _groupings[i].Selector(entity);

                    first = false;
                }
                else
                {
                    // Check if the value of any of the grouping attributes have changed. If so, output the previous group and start a new one
                    for (var i = 0; i < _groupings.Count; i++)
                    {
                        if (!Equal(groupByValues[i], _groupings[i].Selector(entity)))
                        {
                            var group = new Entity(entityName);

                            for (var j = 0; j < _groupings.Count; j++)
                                group[_groupings[j].OutputName] = groupByValues[j];

                            foreach (var aggregate in _aggregates)
                                group[aggregate.OutputName] = aggregate.Value;

                            yield return group;

                            for (var j = 0; j < _groupings.Count; j++)
                                groupByValues[j] = _groupings[j].Selector(entity);

                            foreach (var aggregate in _aggregates)
                                aggregate.Reset();

                            break;
                        }
                    }
                }

                // Update the aggregate values in the current group based on this record
                foreach (var aggregate in _aggregates)
                    aggregate.NextRecord(entity);
            }

            // Return the final group
            if (!first)
            {
                var finalGroup = new Entity(entityName);

                for (var j = 0; j < _groupings.Count; j++)
                    finalGroup[_groupings[j].OutputName] = groupByValues[j];

                foreach (var aggregate in _aggregates)
                    finalGroup[aggregate.OutputName] = aggregate.Value;

                yield return finalGroup;
            }
        }

        /// <summary>
        /// Checks if two values are equal
        /// </summary>
        /// <param name="x">The first value to check</param>
        /// <param name="y">The second value to check</param>
        /// <returns><c>true</c> if the values are equal, or <c>false</c> otherwise</returns>
        private static bool Equal(object x, object y)
        {
            if (x == y)
                return true;

            if (x == null ^ y == null)
                return false;

            if (x is string xs && y is string ys)
                return StringComparer.OrdinalIgnoreCase.Equals(xs, ys);

            return x.Equals(y);
        }
    }

    /// <summary>
    /// Contains the details of how the data should be grouped
    /// </summary>
    class Grouping
    {
        /// <summary>
        /// A function that returns the grouping key from an entity
        /// </summary>
        public Func<Entity,object> Selector { get; set; }

        /// <summary>
        /// Indicates whether the data source has already been sorted by this grouping key
        /// </summary>
        public bool Sorted { get; set; }

        /// <summary>
        /// Contains the name of the column in the aggregate dataset that will contain this grouping key
        /// </summary>
        public string OutputName { get; set; }

        /// <summary>
        /// Returns the SQL fragment that gave the grouping key details
        /// </summary>
        public ScalarExpression SqlExpression { get; set; }

        /// <summary>
        /// Returns the <see cref="SqlExpression"/> converted to an <see cref="System.Linq.Expressions.Expression"/>
        /// </summary>
        public Expression Expression { get; set; }
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
        public AggregateFunction(Func<Entity,object> selector)
        {
            _selector = selector;
        }

        /// <summary>
        /// Updates the aggregate function state based on the next <see cref="Entity"/> in the sequence
        /// </summary>
        /// <param name="entity">The <see cref="Entity"/> to take the value from and apply to this aggregation</param>
        public void NextRecord(Entity entity)
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
        /// Returns the scalar expression being aggregated converted to a <see cref="System.Linq.Expressions.Expression"/>
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
        private decimal _sum;
        private int _count;

        /// <summary>
        /// Creates a new <see cref="Average"/>
        /// </summary>
        /// <param name="selector">A function that extracts the value to calculate the average from</param>
        public Average(Func<Entity,object> selector) : base(selector)
        {
        }

        protected override void Update(object value)
        {
            if (value == null)
                return;

            _sum += Convert.ToDecimal(value);
            _count++;

            Value = _sum / _count;
        }

        public override Type Type => typeof(decimal);

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
            if (Value == null)
                Value = 1;
            else
                Value = (int)Value + 1;
        }

        public override Type Type => typeof(int);

        public override void Reset()
        {
            Value = 0;
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
            if (value == null)
                return;

            if (Value == null)
                Value = 1;
            else
                Value = (int)Value + 1;
        }

        public override Type Type => typeof(int);

        public override void Reset()
        {
            Value = 0;
        }
    }

    /// <summary>
    /// Counts distinct values
    /// </summary>
    class CountColumnDistinct : AggregateFunction
    {
        private HashSet<object> _values = new HashSet<object>();
        private HashSet<string> _stringValues = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        /// <summary>
        /// Creates a new <see cref="CountColumnDistinct"/>
        /// </summary>
        /// <param name="selector">A function that extracts the values to count non-null distinct instances of</param>
        public CountColumnDistinct(Func<Entity, object> selector) : base(selector)
        {
        }

        protected override void Update(object value)
        {
            if (value == null)
                return;

            if (value is string s)
                _stringValues.Add(s);
            else
                _values.Add(value);

            Value = _values.Count + _stringValues.Count;
        }

        public override Type Type => typeof(int);

        public override void Reset()
        {
            _values.Clear();
            _stringValues.Clear();
            Value = 0;
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
            if (value == null)
                return;

            if (!(value is IComparable))
                throw new InvalidOperationException("MAX is not valid for values of type " + value.GetType().Name);

            if (Value == null || ((IComparable) Value).CompareTo(value) < 0)
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
            if (value == null)
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
        private decimal _sumDecimal;

        /// <summary>
        /// Creates a new <see cref="Sum"/>
        /// </summary>
        /// <param name="selector">A function that extracts the value to sum</param>
        public Sum(Func<Entity, object> selector, Type type) : base(selector)
        {
            Type = type;
        }

        protected override void Update(object value)
        {
            if (value == null)
                return;

            var d = Convert.ToDecimal(value);
            _sumDecimal += d;

            var targetType = Type;

            if (targetType.IsGenericType && targetType.GetGenericTypeDefinition() == typeof(Nullable<>))
                targetType = targetType.GetGenericArguments()[0];

            Value = Convert.ChangeType(_sumDecimal, targetType);
        }

        public override Type Type { get; }

        public override void Reset()
        {
            base.Reset();
            _sumDecimal = 0;
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
}
