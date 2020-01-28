using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;

namespace MarkMpn.Sql4Cds.Engine
{
    /// <summary>
    /// Indicates that there was an error parsing the SQL query
    /// </summary>
    public class QueryParseException : NotSupportedException
    {
        /// <summary>
        /// Creates a new <see cref="QueryParseException"/>
        /// </summary>
        /// <param name="error">The error encountered during parsing</param>
        public QueryParseException(ParseError error) : base(error.Message)
        {
            Error = error;
        }

        /// <summary>
        /// Returns the error encountered during parsing
        /// </summary>
        public ParseError Error { get; }
    }
}
