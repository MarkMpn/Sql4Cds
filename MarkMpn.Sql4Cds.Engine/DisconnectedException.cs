using System;

namespace MarkMpn.Sql4Cds.Engine
{
    /// <summary>
    /// An exception that is thrown when the conversion process requires a connnection to the CDS instance but it is disconnected
    /// </summary>
    [Serializable]
    public class DisconnectedException : ApplicationException
    {
        /// <summary>
        /// Creates a new <see cref="DisconnectedException"/>
        /// </summary>
        public DisconnectedException() : base("This conversion cannot be run while disconnected from the server")
        {
        }

        /// <summary>
        /// Creates a new <see cref="DisconnectedException"/>
        /// </summary>
        /// <param name="message"></param>
        public DisconnectedException(string message) : base(message)
        {
        }

        /// <summary>
        /// Creates a new <see cref="DisconnectedException"/>
        /// </summary>
        /// <param name="message"></param>
        /// <param name="innerException"></param>
        public DisconnectedException(string message, Exception innerException) : base(message, innerException)
        {
        }

        protected DisconnectedException(System.Runtime.Serialization.SerializationInfo serializationInfo, System.Runtime.Serialization.StreamingContext streamingContext)
        {
            throw new NotImplementedException();
        }
    }
}
