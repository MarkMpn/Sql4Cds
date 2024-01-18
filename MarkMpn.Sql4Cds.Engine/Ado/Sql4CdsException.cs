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
            if (innerException is ISql4CdsErrorException ex && ex.Error != null)
                Errors = new List<Sql4CdsError> { ex.Error };
            else
                Errors = new List<Sql4CdsError> { new Sql4CdsError(20, 0, message) };
        }

        public IReadOnlyList<Sql4CdsError> Errors { get; }

        /// <inheritdoc cref="System.Data.SqlClient.SqlException.Class"/>
        public byte Class => Errors[0].Class;

        /// <inheritdoc cref="System.Data.SqlClient.SqlException.LineNumber"/>
        public int LineNumber => Errors[0].LineNumber;

        /// <inheritdoc cref="System.Data.SqlClient.SqlException.Number"/>
        public int Number => Errors[0].Number;

        /// <inheritdoc cref="System.Data.SqlClient.SqlException.Procedure"/>
        public string Procedure => Errors[0].Procedure;

        /// <inheritdoc cref="System.Data.SqlClient.SqlException.Server"/>
        public string Server => Errors[0].Server;

        /// <inheritdoc cref="System.Data.SqlClient.SqlException.State"/>
        public byte State => Errors[0].State;
    }
}
