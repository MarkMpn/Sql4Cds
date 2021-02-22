using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    class CaseInsensitiveObjectComparer : IComparer<object>
    {
        public int Compare(object x, object y)
        {
            if (x is string s1 && y is string s2)
                return StringComparer.CurrentCultureIgnoreCase.Compare(s1, s2);

            return Comparer<object>.Default.Compare(x, y);
        }

        public static CaseInsensitiveObjectComparer Instance { get; } = new CaseInsensitiveObjectComparer();
    }
}
