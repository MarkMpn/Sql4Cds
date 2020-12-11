using System;

namespace MarkMpn.Sql4Cds.Engine
{
    /// <summary>
    /// An exception thrown when a query has partially completed, to expose the results available so far
    /// </summary>
    [Serializable]
    public class PartialSuccessException : Exception
    {
        /// <summary>
        /// Creates a new <see cref="PartialSuccessException"/>
        /// </summary>
        public PartialSuccessException()
        {
        }

        /// <summary>
        /// Creates a new <see cref="PartialSuccessException"/>
        /// </summary>
        /// <param name="message"></param>
        public PartialSuccessException(string message) : base(message)
        {
        }

        /// <summary>
        /// Creates a new <see cref="PartialSuccessException"/>
        /// </summary>
        /// <param name="message"></param>
        /// <param name="innerException"></param>
        public PartialSuccessException(string message, Exception innerException) : base(message, innerException)
        {
            Result = message;
        }

        /// <summary>
        /// Creates a new <see cref="PartialSuccessException"/>
        /// </summary>
        /// <param name="result">The results available up to the point of the error</param>
        /// <param name="ex">The details of the error that stopped the query</param>
        public PartialSuccessException(object result, Exception ex) : base("Query partially succeeded", ex)
        {
            Result = result;
        }

        protected PartialSuccessException(System.Runtime.Serialization.SerializationInfo serializationInfo, System.Runtime.Serialization.StreamingContext streamingContext)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// The results of the query up to the point of the error
        /// </summary>
        public object Result { get; }
    }
}
