using System;
using System.Collections;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Threading;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;

namespace MarkMpn.Sql4Cds.Engine
{
    class SqlDataReaderWrapper : DbDataReader
    {
        private SqlConnection _sqlConnection;
        private SqlCommand _sqlCommand;
        private SqlDataReader _sqlDataReader;
        private readonly SqlNode _node;

        public SqlDataReaderWrapper(SqlConnection sqlConnection, SqlCommand sqlCommand, CommandBehavior behavior, SqlNode node, CancellationToken cancellationToken)
        {
            _sqlConnection = sqlConnection;
            _sqlCommand = sqlCommand;
            cancellationToken.Register(() => _sqlCommand.Cancel());

            _node = node;
            HandleException(() => _sqlDataReader = sqlCommand.ExecuteReader(behavior));

            foreach (SqlParameter parameter in sqlCommand.Parameters)
                _node.Parameters.Add(parameter.ParameterName);
        }

        public IRootExecutionPlanNode CurrentResultQuery => _node;

        public override object this[int ordinal] => HandleException(() => _sqlDataReader[ordinal]);

        public override object this[string name] => HandleException(() => _sqlDataReader[name]);

        public override int Depth => HandleException(() => _sqlDataReader.Depth);

        public override int FieldCount => HandleException(() => _sqlDataReader.FieldCount);

        public override bool HasRows => HandleException(() => _sqlDataReader.HasRows);

        public override bool IsClosed => HandleException(() => _sqlDataReader.IsClosed);

        public override int RecordsAffected => HandleException(() => _sqlDataReader.RecordsAffected);

        public override bool GetBoolean(int ordinal)
        {
            return HandleException(() => _sqlDataReader.GetBoolean(ordinal));
        }

        public override byte GetByte(int ordinal)
        {
            return HandleException(() => _sqlDataReader.GetByte(ordinal));
        }

        public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
        {
            return HandleException(() => _sqlDataReader.GetBytes(ordinal, dataOffset, buffer, bufferOffset, length));
        }

        public override char GetChar(int ordinal)
        {
            return HandleException(() => _sqlDataReader.GetChar(ordinal));
        }

        public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
        {
            return HandleException(() => _sqlDataReader.GetChars(ordinal, dataOffset, buffer, bufferOffset, length));
        }

        public override string GetDataTypeName(int ordinal)
        {
            return HandleException(() => _sqlDataReader.GetDataTypeName(ordinal));
        }

        public override DateTime GetDateTime(int ordinal)
        {
            return HandleException(() => _sqlDataReader.GetDateTime(ordinal));
        }

        public override decimal GetDecimal(int ordinal)
        {
            return HandleException(() => _sqlDataReader.GetDecimal(ordinal));
        }

        public override double GetDouble(int ordinal)
        {
            return HandleException(() => _sqlDataReader.GetDouble(ordinal));
        }

        public override IEnumerator GetEnumerator()
        {
            return HandleException(() => _sqlDataReader.GetEnumerator());
        }

        public override Type GetFieldType(int ordinal)
        {
            return HandleException(() => _sqlDataReader.GetFieldType(ordinal));
        }

        public override float GetFloat(int ordinal)
        {
            return HandleException(() => _sqlDataReader.GetFloat(ordinal));
        }

        public override Guid GetGuid(int ordinal)
        {
            return HandleException(() => _sqlDataReader.GetGuid(ordinal));
        }

        public override short GetInt16(int ordinal)
        {
            return HandleException(() => _sqlDataReader.GetInt16(ordinal));
        }

        public override int GetInt32(int ordinal)
        {
            return HandleException(() => _sqlDataReader.GetInt32(ordinal));
        }

        public override long GetInt64(int ordinal)
        {
            return HandleException(() => _sqlDataReader.GetInt64(ordinal));
        }

        public override string GetName(int ordinal)
        {
            return HandleException(() => _sqlDataReader.GetName(ordinal));
        }

        public override int GetOrdinal(string name)
        {
            return HandleException(() => _sqlDataReader.GetOrdinal(name));
        }

        public override string GetString(int ordinal)
        {
            return HandleException(() => _sqlDataReader.GetString(ordinal));
        }

        public override object GetValue(int ordinal)
        {
            return HandleException(() => _sqlDataReader.GetValue(ordinal)   );
        }

        public override int GetValues(object[] values)
        {
            return HandleException(() => _sqlDataReader.GetValues(values));
        }

        public override bool IsDBNull(int ordinal)
        {
            return HandleException(() => _sqlDataReader.IsDBNull(ordinal));
        }

        public override bool NextResult()
        {
            return HandleException(() => _sqlDataReader.NextResult());
        }

        public override bool Read()
        {
            return HandleException(() => _sqlDataReader.Read());
        }

        public override DataTable GetSchemaTable()
        {
            return HandleException(() => _sqlDataReader.GetSchemaTable());
        }

        public override void Close()
        {
            HandleException<object>(() => { _sqlDataReader.Close(); return null; });
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);

            if (disposing)
            {
                _sqlDataReader.Close();
                _sqlCommand.Dispose();
                _sqlConnection.Dispose();
            }
        }

        private T HandleException<T>(Func<T> action)
        {
            try
            {
                return action();
            }
            catch (SqlException ex)
            {
                var error = new Sql4CdsError(ex.Class, ex.LineNumber + _node.LineNumber - 1, ex.Number, String.IsNullOrEmpty(ex.Procedure) ? null : ex.Procedure, ex.Server, ex.State, ex.Message);
                throw new Sql4CdsException(error, new QueryExecutionException(error, ex) { Node = _node });
            }
        }
    }
}
