using MarkMpn.Sql4Cds.Engine.FetchXml;
using Microsoft.Xrm.Sdk;
using System;
using System.Collections.Generic;
using System.Linq;

namespace MarkMpn.Sql4Cds.Engine
{
    abstract class Aggregate
    {
        public abstract void Update(object value);

        public object Value { get; protected set; }

        public virtual void Reset()
        {
            Value = null;
        }
    }

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

    static class AggregateExtensions
    {
        private static object GetValue(Entity entity, string attribute)
        {
            if (!entity.Contains(attribute))
                return null;

            var value = entity[attribute];

            if (value is AliasedValue alias)
                value = alias.Value;

            return value;
        }

        private static bool Equal(object x, object y)
        {
            if (x == y)
                return true;

            if (x == null ^ y == null)
                return false;

            return x.Equals(y);
        }

        public static IEnumerable<Entity> AggregateGroupBy(this IEnumerable<Entity> list, IList<string> groupByAttributes, IDictionary<string,Aggregate> aggregates, IQueryExecutionOptions options)
        {
            var groupByValues = new object[groupByAttributes.Count];
            var first = true;
            string entityName = null;

            foreach (var entity in list)
            {
                if (options.Cancelled)
                    throw new OperationCanceledException();

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
