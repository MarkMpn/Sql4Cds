using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Runtime.Serialization;

namespace MarkMpn.Sql4Cds.Engine
{
    /// <summary>
    /// Indicates that there was an error parsing the SQL query
    /// </summary>
    [Serializable]
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

        public QueryParseException()
        {
        }

        public QueryParseException(string message) : base(message)
        {
        }

        public QueryParseException(string message, Exception innerException) : base(message, innerException)
        {
        }

        protected QueryParseException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }

        /// <summary>
        /// Returns the error encountered during parsing
        /// </summary>
        public ParseError Error { get; }
    }
}
