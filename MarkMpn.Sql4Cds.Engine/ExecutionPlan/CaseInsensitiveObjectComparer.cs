using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    class CaseInsensitiveObjectComparer : IComparer<object>, IEqualityComparer<object>
    {
        public int Compare(object x, object y)
        {
            if (x is string s1 && y is string s2)
                return StringComparer.CurrentCultureIgnoreCase.Compare(s1, s2);

            return Comparer<object>.Default.Compare(x, y);
        }

        bool IEqualityComparer<object>.Equals(object x, object y)
        {
            return Compare(x, y) == 0;
        }

        public int GetHashCode(object obj)
        {
            return StringComparer.CurrentCultureIgnoreCase.GetHashCode(obj);
        }

        public static CaseInsensitiveObjectComparer Instance { get; } = new CaseInsensitiveObjectComparer();
    }
}
