using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Text;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;

namespace MarkMpn.Sql4Cds.Engine
{
    class SqlDataReaderWrapper : DbDataReader, ISql4CdsDataReader
    {
        private Sql4CdsConnection _connection;
        private SqlConnection _sqlConnection;
        private SqlCommand _sqlCommand;
        private SqlDataReader _sqlDataReader;
        private readonly SqlNode _node;

        public SqlDataReaderWrapper(Sql4CdsConnection connection, Sql4CdsCommand command, SqlConnection sqlConnection, SqlCommand sqlCommand, string dataSource)
        {
            _connection = connection;
            _sqlConnection = sqlConnection;
            _sqlCommand = sqlCommand;
            _sqlDataReader = sqlCommand.ExecuteReader();
            _node = new SqlNode { Sql = sqlCommand.CommandText, DataSource = dataSource };

            foreach (SqlParameter parameter in sqlCommand.Parameters)
                _node.Parameters.Add(parameter.ParameterName);

            command.OnStatementCompleted(_node, -1);
        }

        public IRootExecutionPlanNode CurrentResultQuery => _node;

        public override object this[int ordinal] => _sqlDataReader[ordinal];

        public override object this[string name] => _sqlDataReader[name];

        public override int Depth => _sqlDataReader.Depth;

        public override int FieldCount => _sqlDataReader.FieldCount;

        public override bool HasRows => _sqlDataReader.HasRows;

        public override bool IsClosed => _sqlDataReader.IsClosed;

        public override int RecordsAffected => _sqlDataReader.RecordsAffected;

        public override bool GetBoolean(int ordinal)
        {
            return _sqlDataReader.GetBoolean(ordinal);
        }

        public override byte GetByte(int ordinal)
        {
            return _sqlDataReader.GetByte(ordinal);
        }

        public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
        {
            return _sqlDataReader.GetBytes(ordinal, dataOffset, buffer, bufferOffset, length);
        }

        public override char GetChar(int ordinal)
        {
            return _sqlDataReader.GetChar(ordinal);
        }

        public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
        {
            return _sqlDataReader.GetChars(ordinal, dataOffset, buffer, bufferOffset, length);
        }

        public override string GetDataTypeName(int ordinal)
        {
            return _sqlDataReader.GetDataTypeName(ordinal);
        }

        public override DateTime GetDateTime(int ordinal)
        {
            return _sqlDataReader.GetDateTime(ordinal);
        }

        public override decimal GetDecimal(int ordinal)
        {
            return _sqlDataReader.GetDecimal(ordinal);
        }

        public override double GetDouble(int ordinal)
        {
            return _sqlDataReader.GetDouble(ordinal);
        }

        public override IEnumerator GetEnumerator()
        {
            return _sqlDataReader.GetEnumerator();
        }

        public override Type GetFieldType(int ordinal)
        {
            return _sqlDataReader.GetFieldType(ordinal);
        }

        public override float GetFloat(int ordinal)
        {
            return _sqlDataReader.GetFloat(ordinal);
        }

        public override Guid GetGuid(int ordinal)
        {
            return _sqlDataReader.GetGuid(ordinal);
        }

        public override short GetInt16(int ordinal)
        {
            return _sqlDataReader.GetInt16(ordinal);
        }

        public override int GetInt32(int ordinal)
        {
            return _sqlDataReader.GetInt32(ordinal);
        }

        public override long GetInt64(int ordinal)
        {
            return _sqlDataReader.GetInt64(ordinal);
        }

        public override string GetName(int ordinal)
        {
            return _sqlDataReader.GetName(ordinal);
        }

        public override int GetOrdinal(string name)
        {
            return _sqlDataReader.GetOrdinal(name);
        }

        public override string GetString(int ordinal)
        {
            return _sqlDataReader.GetString(ordinal);
        }

        public override object GetValue(int ordinal)
        {
            return _sqlDataReader.GetValue(ordinal);
        }

        public override int GetValues(object[] values)
        {
            return _sqlDataReader.GetValues(values);
        }

        public override bool IsDBNull(int ordinal)
        {
            return _sqlDataReader.IsDBNull(ordinal);
        }

        public override bool NextResult()
        {
            return _sqlDataReader.NextResult();
        }

        public override bool Read()
        {
            return _sqlDataReader.Read();
        }

        public override DataTable GetSchemaTable()
        {
            return _sqlDataReader.GetSchemaTable();
        }

        public override void Close()
        {
            _sqlDataReader.Close();
        }

        public DataTable GetCurrentDataTable()
        {
            var table = new DataTable();
            table.Load(this);
            _connection.OnInfoMessage(_node, $"({table.Rows.Count} row{(table.Rows.Count == 1 ? "" : "s")} affected)");
            return table;
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
    }
}
