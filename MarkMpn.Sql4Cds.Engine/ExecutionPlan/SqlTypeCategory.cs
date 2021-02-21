using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// Groups of related types, ref: https://docs.microsoft.com/en-us/sql/t-sql/data-types/data-types-transact-sql?view=sql-server-ver15
    /// </summary>
    enum SqlTypeCategory
    {
        ExactNumerics,
        ApproximateNumerics,
        DateTime,
        Strings,
        UnicodeStrings,
        BinaryStrings,
        Other
    }
}
