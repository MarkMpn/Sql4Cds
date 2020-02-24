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
}
