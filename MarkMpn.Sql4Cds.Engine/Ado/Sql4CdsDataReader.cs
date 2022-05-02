using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlTypes;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine
{
    class Sql4CdsDataReader : DbDataReader
    {
        private readonly Sql4CdsConnection _connection;
        private readonly Sql4CdsCommand _command;
        private readonly IQueryExecutionOptions _options;
        private readonly CommandBehavior _behavior;
        private readonly Dictionary<string, DataTypeReference> _parameterTypes;
        private readonly Dictionary<string, object> _parameterValues;
        private readonly Dictionary<string, int> _labelIndexes;
        private int _recordsAffected;
        private int _instructionPointer;
        private IDataReaderExecutionPlanNode _readerQuery;
        private DbDataReader _reader;
        private bool _error;
        private int _rows;
        private int _resultSetsReturned;
        private bool _closed;
        
        public Sql4CdsDataReader(Sql4CdsCommand command, IQueryExecutionOptions options, CommandBehavior behavior)
        {
            _connection = (Sql4CdsConnection)command.Connection;
            _command = command;
            _options = options;
            _behavior = behavior;
            _recordsAffected = -1;

            _parameterTypes = ((Sql4CdsParameterCollection)command.Parameters).GetParameterTypes();
            _parameterValues = ((Sql4CdsParameterCollection)command.Parameters).GetParameterValues();

            foreach (var paramType in _connection.GlobalVariableTypes)
                _parameterTypes[paramType.Key] = paramType.Value;

            foreach (var paramValue in _connection.GlobalVariableValues)
                _parameterValues[paramValue.Key] = paramValue.Value;

            _labelIndexes = command.Plan
                .Select((node, index) => new { node, index })
                .Where(n => n.node is GotoLabelNode)
                .ToDictionary(n => ((GotoLabelNode)n.node).Label, n => n.index);

            if (!NextResult())
                Close();
        }

        private bool Execute(Dictionary<string, DataTypeReference> parameterTypes, Dictionary<string, object> parameterValues)
        {
            try
            {
                while (_instructionPointer < _command.Plan.Length && !_options.CancellationToken.IsCancellationRequested)
                {
                    var node = _command.Plan[_instructionPointer];

                    if (node is IDataReaderExecutionPlanNode dataSetNode)
                    {
                        if (_resultSetsReturned == 0 || (!_behavior.HasFlag(CommandBehavior.SingleResult) && !_behavior.HasFlag(CommandBehavior.SingleRow)))
                        {
                            _readerQuery = (IDataReaderExecutionPlanNode)dataSetNode.Clone();
                            _reader = _readerQuery.Execute(_connection.DataSources, _options, parameterTypes, parameterValues, _behavior);
                            _resultSetsReturned++;
                            _rows = 0;
                            _instructionPointer++;
                            return true;
                        }
                        else
                        {
                            _rows = 0;
                            _instructionPointer++;
                        }
                    }
                    else if (node is IDmlQueryExecutionPlanNode dmlNode)
                    {
                        dmlNode = (IDmlQueryExecutionPlanNode)dmlNode.Clone();
                        var msg = dmlNode.Execute(_connection.DataSources, _options, parameterTypes, parameterValues, out var recordsAffected);

                        if (!String.IsNullOrEmpty(msg))
                            _connection.OnInfoMessage(dmlNode, msg);

                        _command.OnStatementCompleted(dmlNode, recordsAffected);

                        if (recordsAffected != -1)
                        {
                            if (_recordsAffected == -1)
                                _recordsAffected = 0;

                            _recordsAffected += recordsAffected;
                        }
                    }
                    else if (node is IGoToNode cond)
                    {
                        cond = (IGoToNode)cond.Clone();
                        var label = cond.Execute(_connection.DataSources, _options, parameterTypes, parameterValues);

                        if (label != null)
                            _instructionPointer = _labelIndexes[label];
                    }
                    else if (node is GotoLabelNode)
                    {
                        // NOOP
                    }
                    else
                    {
                        throw new NotImplementedException("Unexpected node type " + node.GetType().Name);
                    }

                    if (node is IImpersonateRevertExecutionPlanNode)
                    {
                        // TODO: Update options.UserId
                    }

                    _instructionPointer++;
                }
            }
            catch
            {
                _error = true;
                throw;
            }
            finally
            {
                foreach (var paramName in _connection.GlobalVariableValues.Keys.ToArray())
                    _connection.GlobalVariableValues[paramName] = parameterValues[paramName];
            }

            return false;
        }

        public override object this[int ordinal]
        {
            get
            {
                if (_reader == null)
                    throw new InvalidOperationException();

                var value = _reader[ordinal];

                if (_connection.ReturnEntityReferenceAsGuid && value is SqlEntityReference er)
                    value = er.Id;

                return value;
            }
        }

        public override object this[string name]
        {
            get
            {
                if (_reader == null)
                    throw new InvalidOperationException();

                var value = _reader[name];

                if (_connection.ReturnEntityReferenceAsGuid && value is SqlEntityReference er)
                    value = er.Id;

                return value;
            }
        }

        public override int Depth => 0;

        public override int FieldCount => _reader.FieldCount;

        public override bool HasRows => _reader != null;

        public override bool IsClosed => _closed;

        public override int RecordsAffected => _recordsAffected;

        public override bool GetBoolean(int ordinal)
        {
            return (bool)this[ordinal];
        }

        public override byte GetByte(int ordinal)
        {
            return (byte)this[ordinal];
        }

        public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
        {
            var bytes = (byte[])this[ordinal];

            if (buffer == null)
                return bytes.Length;

            length = (int) Math.Min(length, bytes.Length - dataOffset);
            Array.Copy(bytes, dataOffset, buffer, bufferOffset, length);
            return length;
        }

        public override char GetChar(int ordinal)
        {
            return (char)this[ordinal];
        }

        public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
        {
            var chars = ((string)this[ordinal]).ToCharArray();

            if (buffer == null)
                return chars.Length;

            length = (int)Math.Min(length, chars.Length - dataOffset);
            Array.Copy(chars, dataOffset, buffer, bufferOffset, length);
            return length;
        }

        public override string GetDataTypeName(int ordinal)
        {
            if (_reader == null)
                throw new InvalidOperationException();

            var type = _reader.GetDataTypeName(ordinal);

            if (_connection.ReturnEntityReferenceAsGuid && type == typeof(SqlEntityReference).FullName)
                type = "uniqueidentifier";

            return type;
        }

        public override DateTime GetDateTime(int ordinal)
        {
            return (DateTime)this[ordinal];
        }

        public override decimal GetDecimal(int ordinal)
        {
            return (decimal)this[ordinal];
        }

        public override double GetDouble(int ordinal)
        {
            return (double)this[ordinal];
        }

        public override IEnumerator GetEnumerator()
        {
            while (Read())
                yield return this;
        }

        public override Type GetFieldType(int ordinal)
        {
            if (_reader == null)
                throw new InvalidOperationException();

            var type = _reader.GetFieldType(ordinal);

            if (_connection.ReturnEntityReferenceAsGuid && type == typeof(SqlEntityReference))
                type = typeof(Guid);

            return type;
        }

        public override float GetFloat(int ordinal)
        {
            return (float)this[ordinal];
        }

        public override Guid GetGuid(int ordinal)
        {
            var value = this[ordinal];
            if (value is SqlEntityReference er)
                return (Guid)er;
            else
                return (Guid)value;
        }

        public override short GetInt16(int ordinal)
        {
            return (short)this[ordinal];
        }

        public override int GetInt32(int ordinal)
        {
            return (int)this[ordinal];
        }

        public override long GetInt64(int ordinal)
        {
            return (long)this[ordinal];
        }

        public override string GetName(int ordinal)
        {
            return _reader.GetName(ordinal);
        }

        public override int GetOrdinal(string name)
        {
            return _reader.GetOrdinal(name);
        }

        public override Type GetProviderSpecificFieldType(int ordinal)
        {
            return _reader.GetProviderSpecificFieldType(ordinal);
        }

        public override object GetProviderSpecificValue(int ordinal)
        {
            return _reader.GetProviderSpecificValue(ordinal);
        }

        public override int GetProviderSpecificValues(object[] values)
        {
            return _reader.GetProviderSpecificValues(values);
        }

        public override string GetString(int ordinal)
        {
            return (string)this[ordinal];
        }

        public override object GetValue(int ordinal)
        {
            return this[ordinal];
        }

        public override int GetValues(object[] values)
        {
            var length = Math.Min(values.Length, FieldCount);

            for (var i = 0; i < length; i++)
                values[i] = this[i];

            return length;
        }

        public override bool IsDBNull(int ordinal)
        {
            if (_reader == null)
                throw new InvalidOperationException();

            return _reader.IsDBNull(ordinal);
        }

        public override bool NextResult()
        {
            return Execute(_parameterTypes, _parameterValues);
        }

        public override bool Read()
        {
            if (_reader == null)
                throw new InvalidOperationException();

            bool read;

            try
            {
                read = _reader.Read();
            }
            catch
            {
                _error = true;
                _reader = null;
                _readerQuery = null;
                throw;
            }

            if (read)
            {
                _rows++;
            }
            else
            {
                _connection.OnInfoMessage(_readerQuery, $"({_rows} row{(_rows == 1 ? "" : "s")} affected)");
                _command.OnStatementCompleted(_readerQuery, -1);

                _reader = null;
                _readerQuery = null;
            }

            return read;
        }

        public override DataTable GetSchemaTable()
        {
            var schemaTable = _reader.GetSchemaTable();

            var clone = schemaTable.Clone();

            foreach (DataRow row in schemaTable.Rows)
                clone.ImportRow(row);

            var dataTypeCol = clone.Columns.IndexOf("DataType");
            var dataTypeNameCol = clone.Columns.IndexOf("DataTypeName");
            var providerSpecificDataTypeCol = clone.Columns.IndexOf("ProviderSpecificDataType");

            foreach (DataRow cloneRow in clone.Rows)
            {
                if (dataTypeCol != -1 && cloneRow[dataTypeCol] is Type t)
                {
                    if (t == typeof(SqlEntityReference) && _connection.ReturnEntityReferenceAsGuid)
                    {
                        cloneRow[dataTypeCol] = typeof(Guid);
                    }
                    else if (t == typeof(SqlDateTime2) || t == typeof(SqlDate))
                    {
                        cloneRow[dataTypeCol] = typeof(DateTime);
                        cloneRow[providerSpecificDataTypeCol] = typeof(DateTime);
                    }
                    else if (t == typeof(SqlDateTimeOffset))
                    {
                        cloneRow[dataTypeCol] = typeof(DateTimeOffset);
                        cloneRow[providerSpecificDataTypeCol] = typeof(DateTimeOffset);
                    }
                    else if (t == typeof(SqlTime))
                    {
                        cloneRow[dataTypeCol] = typeof(TimeSpan);
                        cloneRow[providerSpecificDataTypeCol] = typeof(TimeSpan);
                    }
                }

                if (dataTypeNameCol != -1 && cloneRow[dataTypeNameCol] is string s && s == typeof(SqlEntityReference).FullName)
                    cloneRow[dataTypeNameCol] = "uniqueidentifier";
            }

            return clone;
        }

        public override void Close()
        {
            if (!_error)
            {
                while (_reader != null && Read())
                    ;

                while (NextResult())
                {
                    while (Read())
                        ;
                }
            }

            _closed = true;
            base.Close();
        }
    }
}
