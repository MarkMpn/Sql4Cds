using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Text;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    class DistinctEqualityComparer : EqualityComparer<Entity>
    {
        private readonly List<string> _columns;

        public DistinctEqualityComparer(List<string> columns)
        {
            _columns = columns;
        }

        public override bool Equals(Entity x, Entity y)
        {
            foreach (var col in _columns)
            {
                var xVal = (INullable)x[col];
                var yVal = (INullable)y[col];

                if (xVal.IsNull && yVal.IsNull)
                    continue;

                if (xVal.IsNull || yVal.IsNull)
                    return false;

                if (!xVal.Equals(yVal))
                    return false;
            }

            return true;
        }

        public override int GetHashCode(Entity obj)
        {
            var hashCode = 0;

            foreach (var col in _columns)
                hashCode ^= obj[col].GetHashCode();

            return hashCode;
        }
    }
}
