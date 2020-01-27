using MarkMpn.Sql4Cds.Engine.FetchXml;
using Microsoft.Xrm.Sdk;
using System;
using System.Collections.Generic;
using System.Linq;

namespace MarkMpn.Sql4Cds.Engine
{
    /// <summary>
    /// Base class for calculating aggregate values
    /// </summary>
    abstract class Aggregate
    {
        public abstract void Update(object value);

        public object Value { get; protected set; }

        public virtual void Reset()
        {
            Value = null;
        }
    }

    /// <summary>
    /// Calculates the mean value
    /// </summary>
    class Average : Aggregate
    {
        private decimal _sum;
        private int _count;

        public override void Update(object value)
        {
            if (value == null)
                return;

            _sum += Convert.ToDecimal(value);
            _count++;

            Value = _sum / _count;
        }

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
    class Count : Aggregate
    {
        public override void Update(object value)
        {
            if (Value == null)
                Value = 1;
            else
                Value = (int)Value + 1;
        }

        public override void Reset()
        {
            Value = 0;
        }
    }

    /// <summary>
    /// Counts non-null values
    /// </summary>
    class CountColumn : Aggregate
    {
        public override void Update(object value)
        {
            if (value == null)
                return;

            if (Value == null)
                Value = 1;
            else
                Value = (int)Value + 1;
        }

        public override void Reset()
        {
            Value = 0;
        }
    }

    /// <summary>
    /// Counts distinct values
    /// </summary>
    class CountColumnDistinct : Aggregate
    {
        private HashSet<object> _values = new HashSet<object>();
        private HashSet<string> _stringValues = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        public override void Update(object value)
        {
            if (value == null)
                return;

            if (value is string s)
                _stringValues.Add(s);
            else
                _values.Add(value);

            Value = _values.Count + _stringValues.Count;
        }

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
    class Max : Aggregate
    {
        public override void Update(object value)
        {
            if (value == null)
                return;

            var d = Convert.ToDecimal(value);

            if (Value == null || (decimal)Value < d)
                Value = d;
        }
    }

    /// <summary>
    /// Identifies the minimum value
    /// </summary>
    class Min : Aggregate
    {
        public override void Update(object value)
        {
            if (value == null)
                return;

            var d = Convert.ToDecimal(value);

            if (Value == null || (decimal)Value > d)
                Value = d;
        }
    }

    /// <summary>
    /// Calculates the sum
    /// </summary>
    class Sum : Aggregate
    {
        public override void Update(object value)
        {
            if (value == null)
                return;

            var d = Convert.ToDecimal(value);

            Value = (decimal)(Value ?? 0M) + d;
        }
    }

    /// <summary>
    /// Implements grouping and sorting on a sequence of entities
    /// </summary>
    static class AggregateExtensions
    {
        /// <summary>
        /// Gets the value of an attribute
        /// </summary>
        /// <param name="entity">The entity to get the value from</param>
        /// <param name="attribute">The name of the attribute to get the value of</param>
        /// <returns></returns>
        private static object GetValue(Entity entity, string attribute)
        {
            if (!entity.Contains(attribute))
                return null;

            var value = entity[attribute];

            if (value is AliasedValue alias)
                value = alias.Value;

            return value;
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
        public static IEnumerable<Entity> AggregateGroupBy(this IEnumerable<Entity> list, IList<string> groupByAttributes, IDictionary<string,Aggregate> aggregates, IQueryExecutionOptions options)
        {
            var groupByValues = new object[groupByAttributes.Count];
            var first = true;
            string entityName = null;

            foreach (var entity in list)
            {
                if (options.Cancelled)
                    throw new OperationCanceledException();

                // If this is the first record in the sequence, start a new group without producing an empty group
                if (first)
                {
                    foreach (var aggregate in aggregates)
                        aggregate.Value.Reset();

                    entityName = entity.LogicalName;

                    for (var i = 0; i < groupByAttributes.Count; i++)
                        groupByValues[i] = GetValue(entity, groupByAttributes[i]);

                    first = false;
                }
                else
                {
                    // Check if the value of any of the grouping attributes have changed. If so, output the previous group and start a new one
                    for (var i = 0; i < groupByAttributes.Count; i++)
                    {
                        if (!Equal(groupByValues[i], GetValue(entity, groupByAttributes[i])))
                        {
                            var group = new Entity(entityName);

                            for (var j = 0; j < groupByAttributes.Count; j++)
                                group[groupByAttributes[j]] = groupByValues[j];

                            foreach (var aggregate in aggregates)
                                group[aggregate.Key] = aggregate.Value.Value;

                            yield return group;

                            for (var j = 0; j < groupByAttributes.Count; j++)
                                groupByValues[j] = GetValue(entity, groupByAttributes[j]);

                            foreach (var aggregate in aggregates)
                                aggregate.Value.Reset();

                            break;
                        }
                    }
                }

                // Update the aggregate values in the current group based on this record
                foreach (var aggregate in aggregates)
                    aggregate.Value.Update(GetValue(entity, aggregate.Key));
            }

            // Return the final group
            var finalGroup = new Entity(entityName);

            for (var j = 0; j < groupByAttributes.Count; j++)
                finalGroup[groupByAttributes[j]] = groupByValues[j];

            foreach (var aggregate in aggregates)
                finalGroup[aggregate.Key] = aggregate.Value.Value;

            yield return finalGroup;
        }

        /// <summary>
        /// Sort a sequence of entities
        /// </summary>
        /// <param name="list">The sequence of entities to sort</param>
        /// <param name="sorts">The sorts to apply</param>
        /// <returns>The sorted sequence of entities</returns>
        public static IOrderedEnumerable<Entity> OrderBy(this IEnumerable<Entity> list, FetchOrderType[] sorts)
        {
            IOrderedEnumerable<Entity> sorted = null;

            foreach (var sort in sorts)
            {
                if (sorted == null)
                {
                    if (sort.descending)
                        sorted = list.OrderByDescending(e => GetValue(e, sort.alias ?? sort.attribute));
                    else
                        sorted = list.OrderBy(e => GetValue(e, sort.alias ?? sort.attribute));
                }
                else
                {
                    if (sort.descending)
                        sorted = sorted.ThenByDescending(e => GetValue(e, sort.alias ?? sort.attribute));
                    else
                        sorted = sorted.ThenBy(e => GetValue(e, sort.alias ?? sort.attribute));
                }
            }

            return sorted;
        }
    }
}
