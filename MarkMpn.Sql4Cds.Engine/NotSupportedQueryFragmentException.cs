using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Linq;

namespace MarkMpn.Sql4Cds.Engine
{
    /// <summary>
    /// Indicates that a fragment of a SQL query cannot be converted to a FetchXML query
    /// </summary>
    public class NotSupportedQueryFragmentException : NotSupportedException
    {
        /// <summary>
        /// Creates a new <see cref="NotSupportedQueryFragmentException"/>
        /// </summary>
        /// <param name="message">The error message to display</param>
        /// <param name="fragment">The fragment of the query that caused the error</param>
        public NotSupportedQueryFragmentException(string message, TSqlFragment fragment) : base(message + ": " + GetText(fragment))
        {
            Error = message;
            Fragment = fragment;
        }

        /// <summary>
        /// The error message to display
        /// </summary>
        public string Error { get; set; }

        /// <summary>
        /// The fragment of the query that caused the error
        /// </summary>
        public TSqlFragment Fragment { get; set; }

        /// <summary>
        /// Gets the text of the SQL fragment that caused the error
        /// </summary>
        /// <param name="fragment">A SQL fragment</param>
        /// <returns>The string that the fragment was parsed from</returns>
        private static string GetText(TSqlFragment fragment)
        {
            return String.Join("",
                fragment.ScriptTokenStream
                    .Skip(fragment.FirstTokenIndex)
                    .Take(fragment.LastTokenIndex - fragment.FirstTokenIndex + 1)
                    .Select(t => t.Text));
        }
    }
}
