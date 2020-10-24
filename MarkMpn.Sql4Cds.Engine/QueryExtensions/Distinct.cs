using Microsoft.Xrm.Sdk;
using System;
using System.Collections.Generic;

namespace MarkMpn.Sql4Cds.Engine.QueryExtensions
{
    /// <summary>
    /// Filters the data so only unique records are returned
    /// </summary>
    class Distinct : IQueryExtension
    {
        class EntityKey
        {
            private readonly string[] _fields;
            private readonly Entity _entity;
            private readonly int _hashCode;

            public EntityKey(Entity entity, string[] fields)
            {
                _entity = entity;
                _fields = fields;

                _hashCode = 0;

                foreach (var field in fields)
                {
                    _hashCode ^= field.GetHashCode();

                    if (entity.Attributes.TryGetValue(field, out var value) && value != null)
                        _hashCode ^= value.GetHashCode();
                }
            }

            public override bool Equals(object obj)
            {
                var other = (EntityKey)obj;

                foreach (var field in _fields)
                {
                    _entity.Attributes.TryGetValue(field, out var thisValue);
                    other._entity.Attributes.TryGetValue(field, out var otherValue);

                    if (!Equals(thisValue, otherValue))
                        return false;
                }

                return true;
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

            public override int GetHashCode()
            {
                return _hashCode;
            }
        }

        private readonly string[] _fields;

        public Distinct(string[] fields)
        {
            _fields = fields;
        }

        public IEnumerable<Entity> ApplyTo(IEnumerable<Entity> source, IQueryExecutionOptions options)
        {
            var unique = new HashSet<EntityKey>();

            foreach (var entity in source)
            {
                if (options.Cancelled)
                    throw new OperationCanceledException();

                var key = new EntityKey(entity, _fields);

                if (unique.Add(key))
                    yield return entity;
            }
        }
    }
}
