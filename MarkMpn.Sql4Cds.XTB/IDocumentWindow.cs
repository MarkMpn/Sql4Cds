using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using McTools.Xrm.Connection;

namespace MarkMpn.Sql4Cds.XTB
{
    interface IDocumentWindow
    {
        TabContent GetSessionDetails();

        void RestoreSessionDetails(TabContent tab);

        void SettingsChanged();
    }

    interface ISaveableDocumentWindow : IDocumentWindow
    {
        string Filter { get; }

        void Save(string filename);

        void Open(string filename);

        string Filename { get; }

        string DisplayName { get; }

        bool Modified { get; }
    }

    interface IFormatableDocumentWindow : IDocumentWindow
    {
        void Format();
    }
}
