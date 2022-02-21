using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Text;

namespace MarkMpn.Sql4Cds.Engine
{
    /// <summary>
    /// Contains information about a page of data that is about to be retrieved
    /// </summary>
    public class ConfirmRetrieveEventArgs : CancelEventArgs
    {
        /// <summary>
        /// Creates a new <see cref="ConfirmRetrieveEventArgs"/>
        /// </summary>
        /// <param name="count">The number of records retrieved so far</param>
        public ConfirmRetrieveEventArgs(int count)
        {
            Count = count;
        }

        /// <summary>
        /// Returns the number of records retrieved so far
        /// </summary>
        public int Count { get; }
    }
}
