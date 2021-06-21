using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using McTools.Xrm.Connection;

namespace MarkMpn.Sql4Cds
{
    interface IDocumentWindow
    {
        void Format();

        void Save();

        TabContent GetSessionDetails();

        void RestoreSessionDetails(TabContent tab);
    }
}
