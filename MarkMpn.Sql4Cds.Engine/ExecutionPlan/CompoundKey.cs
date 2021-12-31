using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Linq;
using System.Text;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// Compares key fields for sorting, grouping or distinctness. null values are treated as equal
    /// </summary>
    class CompoundKey
    {
        private readonly Lazy<int> _hashCode;

        /// <summary>
        /// Extracts a compound key from a <see cref="Entity"/>
        /// </summary>
        /// <param name="entity">The <see cref="Entity"/> to extract the compound key from</param>
        /// <param name="columns">The columns that form the compound key</param>
        public CompoundKey(Entity entity, List<string> columns)
        {
            Values = new object[columns.Count];

            for (var i = 0; i < columns.Count; i++)
                Values[i] = entity[columns[i]];

            _hashCode = new Lazy<int>(() =>
            {
                var hashCode = 0;

                foreach (var value in Values)
                {
                    if (value == null)
                        continue;

                    hashCode ^= value.GetHashCode();
                }

                return hashCode;
            });
        }

        public object[] Values { get; }

        public override int GetHashCode() => _hashCode.Value;

        public override bool Equals(object obj)
        {
            var other = (CompoundKey)obj;

            for (var i = 0; i < Values.Length; i++)
            {
                var xNullable = (INullable)Values[i];
                var yNullable = (INullable)other.Values[i];
                if (xNullable.IsNull && yNullable.IsNull)
                    continue;

                if (xNullable.IsNull || yNullable.IsNull)
                    return false;

                var xComparable = (IComparable)Values[i];
                if (xComparable.CompareTo(other.Values[i]) != 0)
                    return false;
            }

            return true;
        }
    }
}
