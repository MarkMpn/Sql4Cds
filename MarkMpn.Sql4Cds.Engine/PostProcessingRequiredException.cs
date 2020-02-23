using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MarkMpn.Sql4Cds.Engine
{
    class PostProcessingRequiredException : NotSupportedQueryFragmentException
    {
        public PostProcessingRequiredException(string message, TSqlFragment fragment) : base(message, fragment)
        {
        }
    }
}
