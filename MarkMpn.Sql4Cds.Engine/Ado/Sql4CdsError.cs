using System;
using System.Collections.Generic;
using System.Text;

namespace MarkMpn.Sql4Cds.Engine
{
    /// <summary>
    /// Collects information relevant to a warning or error returned by SQL 4 CDS
    /// </summary>
    public class Sql4CdsError
    {
        internal Sql4CdsError(byte @class, int lineNumber, int number, string procedure, string server, byte state, string message)
        {
            Class = @class;
            LineNumber = lineNumber;
            Number = number;
            Procedure = procedure;
            Server = server;
            State = state;
            Message = message;
        }

        internal Sql4CdsError(byte @class, int number, string message) : this(@class, -1, number, null, null, 1, message)
        {
        }

        /// <inheritdoc cref="System.Data.SqlClient.SqlError.Class"/>
        public byte Class { get; }

        /// <inheritdoc cref="System.Data.SqlClient.SqlError.LineNumber"/>
        public int LineNumber { get; internal set; }

        /// <inheritdoc cref="System.Data.SqlClient.SqlError.Number"/>
        public int Number { get; }

        /// <inheritdoc cref="System.Data.SqlClient.SqlError.Procedure"/>
        public string Procedure { get; }

        /// <inheritdoc cref="System.Data.SqlClient.SqlError.Class"/>
        public string Server { get; }

        /// <inheritdoc cref="System.Data.SqlClient.SqlError.State"/>
        public byte State { get; }

        /// <inheritdoc cref="System.Data.SqlClient.SqlError.Message"/>
        public string Message { get; }
    }

    /// <summary>
    /// Defines an exception that exposes a <see cref="Sql4CdsError"/>
    /// </summary>
    interface ISql4CdsErrorException
    {
        /// <summary>
        /// The <see cref="Sql4CdsError"/> to report back to the user
        /// </summary>
        Sql4CdsError Error { get; }
    }
}
