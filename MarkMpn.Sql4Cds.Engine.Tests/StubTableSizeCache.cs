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
                    case "systemuser": return 100;
                    case "new_customentity": return 1000;
                    case "audit": return 1000000;
                    default: throw new ArgumentOutOfRangeException(nameof(logicalName), "Unknown entity name " + logicalName);
                }
            }
        }
    }
}
