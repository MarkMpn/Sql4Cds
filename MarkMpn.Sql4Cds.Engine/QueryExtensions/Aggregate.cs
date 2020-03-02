using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace MarkMpn.Sql4Cds.Engine.QueryExtensions
{
    /// <summary>
    /// Second - group the records and apply aggregate functions
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
        /// <param name="list">The sequence of entities to group, sorted by the grouping attributes</param>
        /// <param name="groupByAttributes">The names of the attributes to group by</param>
        /// <param name="aggregates">The names of the aggregates to produce, mapped to the calculations to apply to generate the aggregates within each group</param>
        /// <param name="options">The options to apply to the query execution</param>
        /// <returns>A sequence of entities representing the groups found within the <paramref name="list"/></returns>
        /// <remarks>
        /// This method assumes that the <paramref name="list"/> is already sorted by the <paramref name="groupByAttributes"/>. If the list is not correctly
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
            var finalGroup = new Entity(entityName);

            for (var j = 0; j < _groupings.Count; j++)
                finalGroup[_groupings[j].OutputName] = groupByValues[j];

            foreach (var aggregate in _aggregates)
                finalGroup[aggregate.OutputName] = aggregate.Value;

            yield return finalGroup;
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

            return x.Equals(y);
        }
    }

    class Grouping
    {
        public Func<Entity,object> Selector { get; set; }

        public bool Sorted { get; set; }

        public string OutputName { get; set; }

        public ScalarExpression SqlExpression { get; set; }

        public Expression Expression { get; set; }
    }

    /// <summary>
    /// Base class for calculating aggregate values
    /// </summary>
    abstract class AggregateFunction
    {
        private readonly Func<Entity, object> _selector;
        
        public AggregateFunction(Func<Entity,object> selector)
        {
            _selector = selector;
        }

        public void NextRecord(Entity entity)
        {
            var value = _selector == null ? entity : _selector(entity);
            Update(value);
        }

        protected abstract void Update(object value);

        public object Value { get; protected set; }

        public string OutputName { get; set; }

        public ScalarExpression SqlExpression { get; set; }

        public Expression Expression { get; set; }

        public abstract Type Type { get; }

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
        private decimal? _maxDecimal;

        public Max(Func<Entity, object> selector) : base(selector)
        {
        }

        protected override void Update(object value)
        {
            if (value == null)
                return;

            var d = Convert.ToDecimal(value);

            if (Value == null || _maxDecimal < d)
                Value = value;
        }

        public override Type Type => Expression.Type;

        public override void Reset()
        {
            base.Reset();
            _maxDecimal = null;
        }
    }

    /// <summary>
    /// Identifies the minimum value
    /// </summary>
    class Min : AggregateFunction
    {
        private decimal? _minDecimal;

        public Min(Func<Entity, object> selector) : base(selector)
        {
        }

        protected override void Update(object value)
        {
            if (value == null)
                return;

            var d = Convert.ToDecimal(value);

            if (Value == null || _minDecimal > d)
                Value = value;
        }

        public override Type Type => Expression.Type;

        public override void Reset()
        {
            base.Reset();
            _minDecimal = null;
        }
    }

    /// <summary>
    /// Calculates the sum
    /// </summary>
    class Sum : AggregateFunction
    {
        private decimal _sumDecimal;

        public Sum(Func<Entity, object> selector) : base(selector)
        {
        }

        protected override void Update(object value)
        {
            if (value == null)
                return;

            var d = Convert.ToDecimal(value);
            _sumDecimal += d;

            Value = Convert.ChangeType(_sumDecimal, Expression.Type);
        }

        public override Type Type => Expression.Type;

        public override void Reset()
        {
            base.Reset();
            _sumDecimal = 0;
        }
    }
}
