using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;

namespace MarkMpn.Sql4Cds.Engine
{
    /// <summary>
    /// Indicates that a fragment of a SQL query cannot be converted to a FetchXML query
    /// </summary>
    [Serializable]
    public class NotSupportedQueryFragmentException : NotSupportedException, ISql4CdsErrorException
    {
        /// <summary>
        /// Creates a new <see cref="NotSupportedQueryFragmentException"/>
        /// </summary>
        /// <param name="message">The error message to display</param>
        /// <param name="fragment">The fragment of the query that caused the error</param>
        public NotSupportedQueryFragmentException(string message, TSqlFragment fragment) : this(message, fragment, null)
        {
        }

        /// <summary>
        /// Creates a new <see cref="NotSupportedQueryFragmentException"/>
        /// </summary>
        /// <param name="message">The error message to display</param>
        /// <param name="fragment">The fragment of the query that caused the error</param>
        /// <param name="innerException">The original exception</param>
        public NotSupportedQueryFragmentException(string message, TSqlFragment fragment, Exception innerException) : base(message, innerException)
        {
            Fragment = fragment;
            Errors = new[] { Sql4CdsError.InternalError(message, fragment) };
        }

        /// <summary>
        /// Creates a new <see cref="NotSupportedQueryFragmentException"/>
        /// </summary>
        /// <param name="error">The error to return</param>
        public NotSupportedQueryFragmentException(Sql4CdsError error) : this(error, null)
        {
        }

        /// <summary>
        /// Creates a new <see cref="NotSupportedQueryFragmentException"/>
        /// </summary>
        /// <param name="error">The error to return</param>
        /// <param name="innerException">The original exception</param>
        public NotSupportedQueryFragmentException(Sql4CdsError error, Exception innerException) : base(error.Message, innerException)
        {
            Fragment = error.Fragment;
            Errors = new[] { error };
        }

        public NotSupportedQueryFragmentException()
        {
        }

        public NotSupportedQueryFragmentException(string message) : base(message)
        {
        }

        public NotSupportedQueryFragmentException(string message, Exception innerException) : base(message, innerException)
        {
        }

        protected NotSupportedQueryFragmentException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }

        /// <summary>
        /// The fragment of the query that caused the error
        /// </summary>
        public TSqlFragment Fragment { get; set; }

        /// <summary>
        /// Returns or sets an optional suggestion to resolve the error
        /// </summary>
        public string Suggestion { get; set; }

        /// <inheritdoc cref="ISql4CdsErrorException.Errors"/>
        public IReadOnlyList<Sql4CdsError> Errors { get; }
    }
}
