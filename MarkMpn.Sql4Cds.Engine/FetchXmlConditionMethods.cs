using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MarkMpn.Sql4Cds.Engine
{
    /// <summary>
    /// Contains implementations of FetchXML-specific conditions
    /// </summary>
    static class FetchXmlConditionMethods
    {
        public static bool LastXDays(DateTime? field, int value)
        {
            throw CreateException();
        }

        private static Exception CreateException()
        {
            return new NotImplementedException("Custom FetchXML filter conditions must only be used where they can be folded into a FetchXML Scan operator");
        }
    }
}
