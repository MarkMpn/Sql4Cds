using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace MarkMpn.Sql4Cds.Engine
{
    /// <summary>
    /// Indicates that a fragment of a SQL query cannot be converted to a FetchXML query, but may be able
    /// to be handled by a post-processing operation
    /// </summary>
    class PostProcessingRequiredException : NotSupportedQueryFragmentException
    {
        /// <summary>
        /// Creates a new <see cref="PostProcessingRequiredException"/>
        /// </summary>
        /// <param name="message">The error message to display</param>
        /// <param name="fragment">The fragment of the query that caused the error</param>
        public PostProcessingRequiredException(string message, TSqlFragment fragment) : base(message, fragment)
        {
        }
    }
}
