using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    public interface ITableSizeCache
    {
        int this[string logicalName] { get; }
    }
}
