using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// Provides access to the number of records in each table
    /// </summary>
    public interface ITableSizeCache
    {
        /// <summary>
        /// Returns the number of records in a table
        /// </summary>
        /// <param name="logicalName">The name of the table to get the number of records for</param>
        /// <returns>The number of records in the table</returns>
        int this[string logicalName] { get; }
    }
}
