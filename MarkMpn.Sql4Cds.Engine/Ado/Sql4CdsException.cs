using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;

namespace MarkMpn.Sql4Cds.Engine
{
    /// <summary>
    /// Provides information about an error that occurred while executing a query
    /// </summary>
    public class Sql4CdsException : DbException
    {
        internal Sql4CdsException(Sql4CdsError error) : base(error.Message)
        {
            Errors = new List<Sql4CdsError> { error };
        }

        internal Sql4CdsException(string message, Exception innerException) : base(message, innerException)
        {
        }

        public IReadOnlyList<Sql4CdsError> Errors { get; }
    }
}
