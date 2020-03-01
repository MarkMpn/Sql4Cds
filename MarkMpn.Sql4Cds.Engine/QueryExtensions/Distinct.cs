using Microsoft.Xrm.Sdk;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MarkMpn.Sql4Cds.Engine.QueryExtensions
{
    class Distinct : IQueryExtension
    {
        class EntityKey
        {
            private readonly Entity _entity;
            private readonly int _hashCode;

            public EntityKey(Entity entity)
            {
                _entity = entity;

                _hashCode = 0;

                foreach (var attr in _entity.Attributes)
                {
                    _hashCode ^= attr.Key.GetHashCode();

                    if (attr.Value != null)
                        _hashCode ^= attr.Value.GetHashCode();
                }
            }

            public override bool Equals(object obj)
            {
                var other = (EntityKey)obj;

                if (_entity.Attributes.Count != other._entity.Attributes.Count)
                    return false;

                foreach (var attr in _entity.Attributes)
                {
                    if (!other._entity.Contains(attr.Key))
                        return false;

                    var thisValue = attr.Value;
                    var otherValue = other._entity[attr.Key];

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

        public IEnumerable<Entity> ApplyTo(IEnumerable<Entity> source, IQueryExecutionOptions options)
        {
            var unique = new HashSet<EntityKey>();

            foreach (var entity in source)
            {
                if (options.Cancelled)
                    throw new OperationCanceledException();

                var key = new EntityKey(entity);

                if (unique.Add(key))
                    yield return entity;
            }
        }
    }
}
