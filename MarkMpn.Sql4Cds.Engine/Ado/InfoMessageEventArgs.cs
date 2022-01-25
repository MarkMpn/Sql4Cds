using System;
using System.Collections.Generic;
using System.Text;

namespace MarkMpn.Sql4Cds.Engine
{
    public class InfoMessageEventArgs : EventArgs
    {
        public InfoMessageEventArgs(string message)
        {
            Message = message;
        }

        public string Message { get; }
    }
}
