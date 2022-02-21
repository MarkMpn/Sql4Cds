using System;
using System.Collections.Generic;
using System.Text;

namespace MarkMpn.Sql4Cds.Engine
{
    /// <summary>
    /// Holds information about the progress of a query
    /// </summary>
    public class ProgressEventArgs
    {
        /// <summary>
        /// Creates a new <see cref="ProgressEventArgs"/>
        /// </summary>
        /// <param name="progress">The percentage of the operation that has completed so far</param>
        /// <param name="message">A human-readable status message to display</param>
        internal ProgressEventArgs(double? progress, string message)
        {
            Progress = progress;
            Message = message;
        }

        /// <summary>
        /// The percentage of the operation that has completed so far
        /// </summary>
        /// <remarks>
        /// This is expressed as a number between 0 and 1.
        /// </remarks>
        public double? Progress { get; }

        /// <summary>
        /// A human-readable status message to display
        /// </summary>
        public string Message { get; }
    }
}
