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
using System.Threading;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine
{
    class Sql4CdsDataReader : DbDataReader, ISql4CdsDataReader
    {
        private readonly Sql4CdsConnection _connection;
        private readonly Sql4CdsCommand _command;
        private readonly IQueryExecutionOptions _options;
        private readonly CommandBehavior _behavior;
        private readonly List<DataTable> _results;
        private readonly List<IRootExecutionPlanNode> _resultQueries;
        private int _recordsAffected;
        private int _resultIndex;
        private int _rowIndex;
        private bool _closed;

        private static readonly Dictionary<Type, Type> _typeConversions;
        private static readonly Dictionary<Type, Func<object, object>> _typeConversionFuncs;

        static Sql4CdsDataReader()
        {
            _typeConversions = new Dictionary<Type, Type>();
            _typeConversionFuncs = new Dictionary<Type, Func<object, object>>();

            // https://docs.microsoft.com/en-us/dotnet/framework/data/adonet/sql-server-data-type-mappings
            AddTypeConversion<SqlInt64, long>(v => v.Value);
            AddTypeConversion<SqlBinary, byte[]>(v => v.Value);
            AddTypeConversion<SqlBoolean, bool>(v => v.Value);
            AddTypeConversion<SqlString, string>(v => v.Value);
            AddTypeConversion<SqlDateTime, DateTime>(v => v.Value);
            AddTypeConversion<SqlDecimal, decimal>(v => v.Value);
            AddTypeConversion<SqlBytes, byte[]>(v => v.Value);
            AddTypeConversion<SqlDouble, double>(v => v.Value);
            AddTypeConversion<SqlInt32, int>(v => v.Value);
            AddTypeConversion<SqlMoney, decimal>(v => v.Value);
            AddTypeConversion<SqlSingle, float>(v => v.Value);
            AddTypeConversion<SqlInt16, short>(v => v.Value);
            AddTypeConversion<SqlByte, byte>(v => v.Value);
            AddTypeConversion<SqlGuid, Guid>(v => v.Value);
        }

        private static void AddTypeConversion<TSql, TClr>(Func<TSql,TClr> func)
        {
            _typeConversions[typeof(TSql)] = typeof(TClr);
            _typeConversionFuncs[typeof(TSql)] = v => func((TSql)v);
        }

        public Sql4CdsDataReader(Sql4CdsCommand command, IRootExecutionPlanNode[] plan, IQueryExecutionOptions options, CommandBehavior behavior, CancellationToken cancellationToken)
        {
            _connection = (Sql4CdsConnection)command.Connection;
            _command = command;
            _options = options;
            _behavior = behavior;

            _results = new List<DataTable>();
            _resultQueries = new List<IRootExecutionPlanNode>();
            _recordsAffected = -1;

            var parameterTypes = ((Sql4CdsParameterCollection)command.Parameters).GetParameterTypes();
            var parameterValues = ((Sql4CdsParameterCollection)command.Parameters).GetParameterValues();

            parameterTypes["@@IDENTITY"] = typeof(SqlEntityReference).ToSqlType();
            parameterTypes["@@ROWCOUNT"] = typeof(SqlInt32).ToSqlType();
            parameterValues["@@IDENTITY"] = SqlEntityReference.Null;
            parameterValues["@@ROWCOUNT"] = (SqlInt32)0;

            Execute(plan, parameterTypes, parameterValues, cancellationToken);

            _resultIndex = -1;

            if (!NextResult())
                Close();
        }

        private void Execute(IRootExecutionPlanNode[] plan, Dictionary<string, DataTypeReference> parameterTypes, Dictionary<string, object> parameterValues, CancellationToken cancellationToken)
        {
            var instructionPointer = 0;
            var labelIndexes = plan
                .Select((node, index) => new { node, index })
                .Where(n => n.node is GotoLabelNode)
                .ToDictionary(n => ((GotoLabelNode)n.node).Label, n => n.index);

            while (instructionPointer < plan.Length && !cancellationToken.IsCancellationRequested)
            {
                var node = plan[instructionPointer];

                if (node is IDataSetExecutionPlanNode dataSetNode)
                {
                    dataSetNode = (IDataSetExecutionPlanNode)dataSetNode.Clone();
                    var table = dataSetNode.Execute(_connection.DataSources, _options, parameterTypes, parameterValues, cancellationToken);
                    _results.Add(table);
                    _resultQueries.Add(dataSetNode);

                    _connection.OnInfoMessage(dataSetNode, $"({table.Rows.Count} row{(table.Rows.Count == 1 ? "" : "s")} affected)");
                    _command.OnStatementCompleted(dataSetNode, -1);
                }
                else if (node is IDmlQueryExecutionPlanNode dmlNode)
                {
                    dmlNode = (IDmlQueryExecutionPlanNode)dmlNode.Clone();
                    var msg = dmlNode.Execute(_connection.DataSources, _options, parameterTypes, parameterValues, out var recordsAffected, cancellationToken);

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
                    var label = cond.Execute(_connection.DataSources, _options, parameterTypes, parameterValues, cancellationToken);

                    if (label != null)
                        instructionPointer = labelIndexes[label];
                }
                else if (node is GotoLabelNode)
                {
                    // NOOP
                }
                else
                {
                    throw new NotImplementedException("Unexpected node type " + node.GetType().Name);
                }

                instructionPointer++;
            }
        }

        public IRootExecutionPlanNode CurrentResultQuery => _resultQueries[_resultIndex];

        public override object this[int ordinal] => ToClrType(GetRawValue(ordinal));

        public override object this[string name] => ToClrType(GetRawValue(name));

        public override int Depth => 0;

        public override int FieldCount => _results[_resultIndex].Columns.Count;

        public override bool HasRows => _resultIndex < _results.Count &&  _results[_resultIndex].Rows.Count > 0;

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
            length = (int)Math.Min(length, chars.Length - dataOffset);

            Array.Copy(chars, dataOffset, buffer, bufferOffset, length);
            return length;
        }

        public override string GetDataTypeName(int ordinal)
        {
            // Return T-SQL data type name
            var type = _results[_resultIndex].Columns[ordinal].DataType;

            if (type == typeof(SqlEntityReference) && _connection.ReturnEntityReferenceAsGuid)
                type = typeof(SqlGuid);

            var sqlType = type.ToSqlType();

            if (sqlType is SqlDataTypeReference sql)
                return sql.SqlDataTypeOption.ToString().ToLower();

            return String.Join(".", ((UserDataTypeReference)sqlType).Name.Identifiers.Select(id => id.Value));
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
            for (; _rowIndex < _results[_resultIndex].Rows.Count; _rowIndex++)
                yield return this;
        }

        public override Type GetFieldType(int ordinal)
        {
            return ToClrType(_results[_resultIndex].Columns[ordinal].DataType);
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
            return _results[_resultIndex].Columns[ordinal].ColumnName;
        }

        public override int GetOrdinal(string name)
        {
            return _results[_resultIndex].Columns[name].Ordinal;
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
            var items = _results[_resultIndex].Rows[_rowIndex].ItemArray;
            var length = Math.Min(values.Length, items.Length);

            for (var i = 0; i < length; i++)
                values[i] = this[i];

            return length;
        }

        private INullable GetRawValue(int ordinal)
        {
            return (INullable) _results[_resultIndex].Rows[_rowIndex][ordinal];
        }

        private INullable GetRawValue(string name)
        {
            return (INullable) _results[_resultIndex].Rows[_rowIndex][name];
        }

        private Type ToClrType(Type type)
        {
            if (type == typeof(SqlEntityReference) && _connection.ReturnEntityReferenceAsGuid)
                type = typeof(SqlGuid);

            if (_typeConversions.TryGetValue(type, out var clr))
                return clr;

            return type;
        }

        private object ToClrType(INullable value)
        {
            if (value.IsNull)
                return DBNull.Value;

            var type = value.GetType();

            if (type == typeof(SqlEntityReference) && _connection.ReturnEntityReferenceAsGuid)
                return ((SqlEntityReference)value).Id;

            if (_typeConversionFuncs.TryGetValue(type, out var func))
                return func(value);

            return value;
        }

        public override bool IsDBNull(int ordinal)
        {
            return GetRawValue(ordinal).IsNull;
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

        public override DataTable GetSchemaTable()
        {
            if (_resultIndex >= _results.Count)
                return null;

            var schemaTable = new DataTable();
            schemaTable.Columns.Add("ColumnName", typeof(string));
            schemaTable.Columns.Add("ColumnOrdinal", typeof(int));
            schemaTable.Columns.Add("ColumnSize", typeof(int));
            schemaTable.Columns.Add("NumericPrecision", typeof(int));
            schemaTable.Columns.Add("NumericScale", typeof(int));
            schemaTable.Columns.Add("IsUnique", typeof(bool));
            schemaTable.Columns.Add("IsKey", typeof(bool));
            schemaTable.Columns.Add("BaseServerName", typeof(string));
            schemaTable.Columns.Add("BaseCatalogName", typeof(string));
            schemaTable.Columns.Add("BaseColumnName", typeof(string));
            schemaTable.Columns.Add("BaseSchemaName", typeof(string));
            schemaTable.Columns.Add("BaseTableName", typeof(string));
            schemaTable.Columns.Add("DataType", typeof(Type));
            schemaTable.Columns.Add("AllowDBNull", typeof(bool));
            schemaTable.Columns.Add("ProviderType", typeof(Type));
            schemaTable.Columns.Add("IsAliased", typeof(bool));
            schemaTable.Columns.Add("IsExpression", typeof(bool));
            schemaTable.Columns.Add("IsIdentity", typeof(bool));
            schemaTable.Columns.Add("IsAutoIncrement", typeof(bool));
            schemaTable.Columns.Add("IsRowVersion", typeof(bool));
            schemaTable.Columns.Add("IsHidden", typeof(bool));
            schemaTable.Columns.Add("IsLong", typeof(bool));
            schemaTable.Columns.Add("IsReadOnly", typeof(bool));
            schemaTable.Columns.Add("ProviderSpecificDataType", typeof(Type));
            schemaTable.Columns.Add("DataTypeName", typeof(string));
            schemaTable.Columns.Add("XmlSchemaCollectionDatabase", typeof(string));
            schemaTable.Columns.Add("XmlSchemaCollectionOwningSchema", typeof(string));
            schemaTable.Columns.Add("XmlSchemaCollectionName", typeof(string));
            schemaTable.Columns.Add("UdtAssemblyQualifiedName", typeof(string));
            schemaTable.Columns.Add("NonVersionedProviderType", typeof(string));
            schemaTable.Columns.Add("IsColumnSet", typeof(bool));

            foreach (DataColumn column in _results[_resultIndex].Columns)
            {
                schemaTable.Rows.Add(new object[]
                {
                    column.ColumnName,    // ColumnName
                    column.Ordinal,       // ColumnOrdinal
                    4000,                 // ColumnSize
                    255,                  // NumericPrecision
                    255,                  // NumericScale
                    false,                // IsUnique
                    false,                // IsKey
                    DBNull.Value,         // BaseServerName
                    DBNull.Value,         // BaseCatalogName
                    DBNull.Value,         // BaseColumnName
                    DBNull.Value,         // BaseSchemaName
                    DBNull.Value,         // BaseTableName
                    column.DataType,      // DataType
                    false,                // AllowDBNull
                    column.DataType,      // ProviderType
                    false,                // IsAliased
                    false,                // IsExpression
                    false,                // IsIdentity
                    false,                // IsAutoIncrement
                    false,                // IsRowVersion
                    false,                // IsHidden
                    false,                // IsLong
                    true,                 // IsReadOnly
                    column.DataType,      // ProviderSpecificDataType
                    column.DataType.Name, // DataTypeName TODO: Convert to T-SQL data type name
                    DBNull.Value,         // XmlSchemaCollectionDatabase
                    DBNull.Value,         // XmlSchemaCollectionOwningSchema
                    DBNull.Value,         // XmlSchemaCollectionName
                    column.DataType.AssemblyQualifiedName, // UdtAssemblyQualifiedName
                    column.DataType,      // NonVersionedProviderType TODO: Convert to SqlDbType
                    false                 // IsColumnSet
                });
            }

            return schemaTable;
        }

        public DataTable GetCurrentDataTable()
        {
            var table = _results[_resultIndex];

            if (!NextResult())
                Close();

            return table;
        }

        public override void Close()
        {
            _closed = true;
            base.Close();
        }
    }
}
