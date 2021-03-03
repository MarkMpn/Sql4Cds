using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;

namespace MarkMpn.Sql4Cds.Engine.Tests
{
    class StubTableSizeCache : ITableSizeCache
    {
        public int this[string logicalName]
        {
            get
            {
                switch (logicalName)
                {
                    case "account": return 30000;
                    case "contact": return 100000;
                    default: throw new ArgumentOutOfRangeException(nameof(logicalName));
                }
            }
        }
    }
}
