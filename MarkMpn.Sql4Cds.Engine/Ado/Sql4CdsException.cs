using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;

namespace MarkMpn.Sql4Cds.Engine
{
    public class Sql4CdsException : DbException
    {
        public Sql4CdsException(string message) : base(message)
        {
        }

        public Sql4CdsException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}
