using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlTypes;
using System.Text;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    class SqlDataReaderWrapper : IDataReader
    {
        private readonly IDataReader _dataReader;
        private readonly IDbCommand _cmd;
        private readonly IDbConnection _con;
        private readonly IDictionary<string, object> _parameterValues;
        private int _rowCount;

        public SqlDataReaderWrapper(IDataReader dataReader, IDbCommand cmd, IDbConnection con, IDictionary<string, object> parameterValues)
        {
            _dataReader = dataReader;
            _cmd = cmd;
            _con = con;
            _parameterValues = parameterValues;
        }

        public bool ConvertToSqlTypes { get; set; }

        public object this[int i] => GetValue(i);

        public object this[string name] => this[GetOrdinal(name)];

        public int Depth => _dataReader.Depth;

        public bool IsClosed => _dataReader.IsClosed;

        public int RecordsAffected => _dataReader.RecordsAffected;

        public int FieldCount => _dataReader.FieldCount;

        public void Close()
        {
            _dataReader.Close();
            _cmd.Dispose();
            _con.Dispose();

            if (_parameterValues != null)
                _parameterValues["@@ROWCOUNT"] = (SqlInt32)_rowCount;
        }

        public void Dispose()
        {
            _dataReader.Dispose();
            _cmd.Dispose();
            _con.Dispose();

            if (_parameterValues != null)
                _parameterValues["@@ROWCOUNT"] = (SqlInt32)_rowCount;
        }

        public bool GetBoolean(int i)
        {
            return _dataReader.GetBoolean(i);
        }

        public byte GetByte(int i)
        {
            return _dataReader.GetByte(i);
        }

        public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
        {
            return _dataReader.GetBytes(i, fieldOffset, buffer, bufferoffset, length);
        }

        public char GetChar(int i)
        {
            return _dataReader.GetChar(i);
        }

        public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            return _dataReader.GetChars(i, fieldoffset, buffer, bufferoffset, length);
        }

        public IDataReader GetData(int i)
        {
            return _dataReader.GetData(i);
        }

        public string GetDataTypeName(int i)
        {
            return _dataReader.GetDataTypeName(i);
        }

        public DateTime GetDateTime(int i)
        {
            return _dataReader.GetDateTime(i);
        }

        public decimal GetDecimal(int i)
        {
            return _dataReader.GetDecimal(i);
        }

        public double GetDouble(int i)
        {
            return _dataReader.GetDouble(i);
        }

        public Type GetFieldType(int i)
        {
            return ConvertType(_dataReader.GetFieldType(i));
        }

        public float GetFloat(int i)
        {
            return _dataReader.GetFloat(i);
        }

        public Guid GetGuid(int i)
        {
            return _dataReader.GetGuid(i);
        }

        public short GetInt16(int i)
        {
            return _dataReader.GetInt16(i);
        }

        public int GetInt32(int i)
        {
            return _dataReader.GetInt32(i);
        }

        public long GetInt64(int i)
        {
            return _dataReader.GetInt64(i);
        }

        public string GetName(int i)
        {
            return _dataReader.GetName(i);
        }

        public int GetOrdinal(string name)
        {
            return _dataReader.GetOrdinal(name);
        }

        public DataTable GetSchemaTable()
        {
            var schemaTable = _dataReader.GetSchemaTable();

            if (!ConvertToSqlTypes)
                return schemaTable;

            var clone = schemaTable.Clone();

            foreach (DataRow row in schemaTable.Rows)
                clone.ImportRow(row);

            var dataTypeCol = clone.Columns.IndexOf("DataType");
            clone.Columns[dataTypeCol].ReadOnly = false;

            foreach (DataRow cloneRow in clone.Rows)
            {
                if (dataTypeCol != -1 && cloneRow[dataTypeCol] is Type t)
                    cloneRow[dataTypeCol] = ConvertType(t);
            }

            return clone;
        }

        public string GetString(int i)
        {
            return _dataReader.GetString(i);
        }

        public object GetValue(int i)
        {
            return ConvertType(_dataReader.GetValue(i), _dataReader.GetFieldType(i));
        }

        public int GetValues(object[] values)
        {
            if (ConvertToSqlTypes)
            {
                for (var i = 0; i < values.Length && i < FieldCount; i++)
                    values[i] = this[i];

                return Math.Min(values.Length, FieldCount);
            }

            return _dataReader.GetValues(values);
        }

        public bool IsDBNull(int i)
        {
            return _dataReader.IsDBNull(i);
        }

        public bool NextResult()
        {
            return _dataReader.NextResult();
        }

        public bool Read()
        {
            var read = _dataReader.Read();

            if (read)
                _rowCount++;

            return read;
        }

        private object ConvertType(object value, Type type)
        {
            if (!ConvertToSqlTypes)
                return value;

            if (DBNull.Value.Equals(value))
                return SqlTypeConverter.GetNullValue(SqlTypeConverter.NetToSqlType(type));

            return SqlTypeConverter.NetToSqlType(null, value);
        }

        private Type ConvertType(Type type)
        {
            if (!ConvertToSqlTypes)
                return type;

            return SqlTypeConverter.NetToSqlType(type);
        }
    }
}
