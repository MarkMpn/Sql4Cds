using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.Engine;
using McTools.Xrm.Connection;

namespace MarkMpn.Sql4Cds
{
    class XtbDataSource : DataSource
    {
        public ConnectionDetail ConnectionDetail { get; set; }
    }
}
