using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using McTools.Xrm.Connection;

namespace MarkMpn.Sql4Cds
{
    class DataSource : MarkMpn.Sql4Cds.Engine.DataSource
    {
        public ConnectionDetail ConnectionDetail { get; set; }
    }
}
