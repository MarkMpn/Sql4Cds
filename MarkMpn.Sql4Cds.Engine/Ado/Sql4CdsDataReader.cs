using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlTypes;
using System.Linq.Expressions;
using System.Text;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine
{
    public class Sql4CdsDataReader : DbDataReader
    {
        private readonly Sql4CdsConnection _connection;
        private readonly Sql4CdsCommand _command;
        private readonly IQueryExecutionOptions _options;
        private readonly CommandBehavior _behavior;
        private readonly List<DataTable> _results;
        private readonly int _recordsAffected;
        private int _resultIndex;
        private int _rowIndex;
        private bool _closed;

        public Sql4CdsDataReader(Sql4CdsCommand command, IQueryExecutionOptions options, CommandBehavior behavior)
        {
            _connection = (Sql4CdsConnection)command.Connection;
            _command = command;
            _options = options;
            _behavior = behavior;

            _results = new List<DataTable>();
            _recordsAffected = -1;

            var parameterTypes = ((Sql4CdsParameterCollection)command.Parameters).GetParameterTypes();
            var parameterValues = ((Sql4CdsParameterCollection)command.Parameters).GetParameterValues();

            parameterTypes["@@IDENTITY"] = typeof(SqlEntityReference).ToSqlType();
            parameterTypes["@@ROWCOUNT"] = typeof(SqlInt32).ToSqlType();
            parameterValues["@@IDENTITY"] = SqlEntityReference.Null;
            parameterValues["@@ROWCOUNT"] = (SqlInt32)0;

            foreach (var plan in command.Plan)
            {
                if (plan is IDataSetExecutionPlanNode dataSetNode)
                {
                    var table = dataSetNode.Execute(_connection.DataSources, options, parameterTypes, parameterValues);
                    _results.Add(table);
                }
                else if (plan is IDmlQueryExecutionPlanNode dmlNode)
                {
                    var msg = dmlNode.Execute(_connection.DataSources, options, parameterTypes, parameterValues, out var recordsAffected);

                    if (!String.IsNullOrEmpty(msg))
                        _connection.OnInfoMessage(msg);

                    if (recordsAffected != -1)
                    {
                        command.OnStatementCompleted(recordsAffected);

                        if (_recordsAffected == -1)
                            _recordsAffected = 0;

                        _recordsAffected += recordsAffected;
                    }
                }
            }

            _resultIndex = -1;
            NextResult();
        }

        public override object this[int ordinal] => _results[_resultIndex].Rows[_rowIndex][ordinal];

        public override object this[string name] => _results[_resultIndex].Rows[_rowIndex][name];

        public override int Depth => 0;

        public override int FieldCount => _results[_resultIndex].Columns.Count;

        public override bool HasRows => _resultIndex < _results.Count &&  _results[_resultIndex].Rows.Count > 0;

        public override bool IsClosed => _closed;

        public override int RecordsAffected => _recordsAffected;

        public override bool GetBoolean(int ordinal)
        {
            return (bool)(SqlBoolean)this[ordinal];
        }

        public override byte GetByte(int ordinal)
        {
            return (byte)(SqlByte)this[ordinal];
        }

        public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
        {
            var bytes = (byte[])(SqlBinary)this[ordinal];
            length = (int) Math.Min(length, bytes.Length - dataOffset);

            Array.Copy(bytes, dataOffset, buffer, bufferOffset, length);
            return length;
        }

        public override char GetChar(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
        {
            var chars = ((SqlChars)this[ordinal]).Value;
            length = (int)Math.Min(length, chars.Length - dataOffset);

            Array.Copy(chars, dataOffset, buffer, bufferOffset, length);
            return length;
        }

        public override string GetDataTypeName(int ordinal)
        {
            return _results[_resultIndex].Columns[ordinal].DataType.Name;
        }

        public override DateTime GetDateTime(int ordinal)
        {
            return (DateTime)(SqlDateTime)this[ordinal];
        }

        public override decimal GetDecimal(int ordinal)
        {
            var value = this[ordinal];

            if (value is SqlDecimal d)
                return (decimal)d;
            else if (value is SqlMoney m)
                return (decimal)m;
            else
                throw new InvalidCastException();
        }

        public override double GetDouble(int ordinal)
        {
            return (double)(SqlDouble)this[ordinal];
        }

        public override IEnumerator GetEnumerator()
        {
            for (; _rowIndex < _results[_resultIndex].Rows.Count; _rowIndex++)
                yield return this;
        }

        public override Type GetFieldType(int ordinal)
        {
            return _results[_resultIndex].Columns[ordinal].DataType;
        }

        public override float GetFloat(int ordinal)
        {
            return (float)(SqlSingle)this[ordinal];
        }

        public override Guid GetGuid(int ordinal)
        {
            var value = this[ordinal];

            if (value is SqlGuid g)
                return (Guid)g;
            else if (value is SqlEntityReference er)
                return (Guid)er;
            else
                throw new InvalidCastException();
        }

        public override short GetInt16(int ordinal)
        {
            return (short)(SqlInt16)this[ordinal];
        }

        public override int GetInt32(int ordinal)
        {
            return (int)(SqlInt32)this[ordinal];
        }

        public override long GetInt64(int ordinal)
        {
            return (long)(SqlInt64)this[ordinal];
        }

        public override string GetName(int ordinal)
        {
            return _results[_resultIndex].Columns[ordinal].ColumnName;
        }

        public override int GetOrdinal(string name)
        {
            return _results[_resultIndex].Columns[name].Ordinal;
        }

        public override string GetString(int ordinal)
        {
            return (string)(SqlString)this[ordinal];
        }

        public override object GetValue(int ordinal)
        {
            return this[ordinal];
        }

        public override int GetValues(object[] values)
        {
            var items = _results[_resultIndex].Rows[_rowIndex].ItemArray;
            var length = Math.Min(values.Length, items.Length);
            Array.Copy(items, values, length);
            return length;
        }

        public override bool IsDBNull(int ordinal)
        {
            return ((INullable)this[ordinal]).IsNull;
        }

        public override bool NextResult()
        {
            _rowIndex = -1;
            _resultIndex++;

            return _resultIndex < _results.Count;
        }

        public override bool Read()
        {
            if (_resultIndex >= _results.Count)
                return false;

            _rowIndex++;

            if (_rowIndex >= _results[_resultIndex].Rows.Count)
                return false;

            return true;
        }

        public override void Close()
        {
            _closed = true;
            base.Close();
        }
    }
}
