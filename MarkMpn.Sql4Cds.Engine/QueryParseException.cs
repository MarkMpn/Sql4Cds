using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Linq;

namespace MarkMpn.Sql4Cds.Engine
{
    public class QueryParseException : NotSupportedException
    {
        public QueryParseException(ParseError error) : base(error.Message)
        {
            Error = error;
        }

        public ParseError Error { get; set; }
    }
}
