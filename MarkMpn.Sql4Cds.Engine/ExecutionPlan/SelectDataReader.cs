using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlTypes;
using System.Linq;
using System.Text;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    class SelectDataReader : DbDataReader
    {
        private readonly List<SelectColumn> _columnSet;
        private readonly IDisposable _timer;
        private readonly INodeSchema _schema;
        private readonly IEnumerator<Entity> _source;
        private readonly IDictionary<string, INullable> _parameterValues;
        private Entity _row;
        private bool _closed;
        private int _rowCount;

        public SelectDataReader(List<SelectColumn> columnSet, IDisposable timer, INodeSchema schema, IEnumerable<Entity> source, IDictionary<string, INullable> parameterValues)
        {
            _columnSet = columnSet;
            _timer = timer;
            _schema = schema;
            _source = source.GetEnumerator();
            _parameterValues = parameterValues;
        }

        public override IEnumerator GetEnumerator()
        {
            while (Read())
                yield return this;
        }

        public override bool HasRows => true;

        public override object this[int i]
        {
            get
            {
                var value = GetRawValue(i);

                if (value.IsNull)
                    return DBNull.Value;

                if (value is SqlEntityReference)
                    return value;

                return SqlTypeConverter.SqlToNetType(value);
            }
        }

        public override object this[string name] => this[GetOrdinal(name)];

        public override int Depth => 0;

        public override bool IsClosed => _closed;

        public override int RecordsAffected => -1;

        public override int FieldCount => _columnSet.Count;

        public override void Close()
        {
            if (_closed)
                return;

            base.Close();

            if (_source is IDisposable disposable)
                disposable.Dispose();

            _timer.Dispose();
            _closed = true;
        }

        public override bool GetBoolean(int i)
        {
            return (bool)this[i];
        }

        public override byte GetByte(int i)
        {
            return (byte)this[i];
        }

        public override long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
        {
            var bytes = (byte[])this[i];

            if (buffer == null)
                return bytes.Length;

            length = (int)Math.Min(length, bytes.Length - fieldOffset);
            Array.Copy(bytes, fieldOffset, buffer, bufferoffset, length);
            return length;
        }

        public override char GetChar(int i)
        {
            return (char)this[i];
        }

        public override long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            var chars = ((string)this[i]).ToCharArray();

            if (buffer == null)
                return chars.Length;

            length = (int)Math.Min(length, chars.Length - fieldoffset);
            Array.Copy(chars, fieldoffset, buffer, bufferoffset, length);
            return length;
        }

        public override string GetDataTypeName(int i)
        {
            // Return T-SQL data type name
            var sqlType = _schema.Schema[_columnSet[i].SourceColumn].Type;

            if (sqlType is SqlDataTypeReference sql)
                return sql.SqlDataTypeOption.ToString().ToLower();

            if (sqlType is XmlDataTypeReference)
                return "xml";

            return String.Join(".", ((UserDataTypeReference)sqlType).Name.Identifiers.Select(id => id.Value));
        }

        public override DateTime GetDateTime(int i)
        {
            return (DateTime)this[i];
        }

        public override decimal GetDecimal(int i)
        {
            return (decimal)this[i];
        }

        public override double GetDouble(int i)
        {
            return (double)this[i];
        }

        public override Type GetFieldType(int i)
        {
            var providerType = GetProviderSpecificFieldType(i);

            if (providerType == typeof(SqlEntityReference) || providerType == typeof(SqlXml) || providerType == typeof(object))
                return providerType;

            return SqlTypeConverter.SqlToNetType(providerType);
        }

        public override Type GetProviderSpecificFieldType(int ordinal)
        {
            var type = _schema.Schema[_columnSet[ordinal].SourceColumn].Type.ToNetType(out _);

            if (type == typeof(SqlVariant))
                type = typeof(object);

            return type;
        }

        public override float GetFloat(int i)
        {
            return (float)this[i];
        }

        public override Guid GetGuid(int i)
        {
            var value = this[i];
            if (value is SqlEntityReference er)
                return (Guid)er;
            else
                return (Guid)value;
        }

        public override short GetInt16(int i)
        {
            return (short)this[i];
        }

        public override int GetInt32(int i)
        {
            return (int)this[i];
        }

        public override long GetInt64(int i)
        {
            return (long)this[i];
        }

        public override string GetName(int i)
        {
            return _columnSet[i].OutputColumn;
        }

        public override int GetOrdinal(string name)
        {
            for (var i = 0; i < _columnSet.Count; i++)
            {
                if (_columnSet[i].OutputColumn.Equals(name, StringComparison.OrdinalIgnoreCase))
                    return i;
            }

            return -1;
        }

        public override DataTable GetSchemaTable()
        {
            var schemaTable = new DataTable();
            schemaTable.Columns.Add("ColumnName", typeof(string));
            schemaTable.Columns.Add("ColumnOrdinal", typeof(int));
            schemaTable.Columns.Add("ColumnSize", typeof(int));
            schemaTable.Columns.Add("NumericPrecision", typeof(short));
            schemaTable.Columns.Add("NumericScale", typeof(short));
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

            for (var i = 0; i < _columnSet.Count; i++)
            {
                var column = _columnSet[i];
                var schemaCol = _schema.Schema[_columnSet[i].SourceColumn];
                var sourceServer = (object)schemaCol.SourceServer ?? DBNull.Value;
                var sourceSchema = (object)schemaCol.SourceSchema ?? DBNull.Value;
                var sourceTable = (object)schemaCol.SourceTable ?? DBNull.Value;
                var sourceColumn = (object)schemaCol.SourceColumn ?? DBNull.Value;
                var sqlType = schemaCol.Type;
                var nullable = schemaCol.IsNullable;
                var key = _schema.PrimaryKey == _columnSet[i].SourceColumn;
                var expression = schemaCol.IsCalculated;
                var aliased = _columnSet[i].OutputColumn != null && !_columnSet[i].SourceColumn.SplitMultiPartIdentifier().Last().Equals(_columnSet[i].OutputColumn.SplitMultiPartIdentifier().Last(), StringComparison.OrdinalIgnoreCase);
                var providerType = sqlType.ToNetType(out _);
                var type = providerType == typeof(SqlEntityReference) || providerType == typeof(SqlXml) ? providerType : providerType == typeof(SqlVariant) ? typeof(object) : SqlTypeConverter.SqlToNetType(providerType);
                var size = sqlType.GetSize();
                var precision = sqlType.GetPrecision(255);
                var scale = sqlType.GetScale(255);

                if (providerType == typeof(SqlVariant))
                    providerType = typeof(object);

                schemaTable.Rows.Add(new object[]
                {
                    column.OutputColumn,  // ColumnName
                    i,                    // ColumnOrdinal
                    size,                 // ColumnSize
                    precision,            // NumericPrecision
                    scale,                // NumericScale
                    key,                  // IsUnique
                    key,                  // IsKey
                    sourceServer,         // BaseServerName
                    DBNull.Value,         // BaseCatalogName
                    sourceColumn,         // BaseColumnName
                    sourceSchema,         // BaseSchemaName
                    sourceTable,          // BaseTableName
                    type,                 // DataType
                    nullable,             // AllowDBNull
                    providerType,         // ProviderType
                    aliased,              // IsAliased
                    expression,           // IsExpression
                    false,                // IsIdentity
                    false,                // IsAutoIncrement
                    false,                // IsRowVersion
                    false,                // IsHidden
                    false,                // IsLong
                    true,                 // IsReadOnly
                    providerType,         // ProviderSpecificDataType
                    GetDataTypeName(i),   // DataTypeName
                    DBNull.Value,         // XmlSchemaCollectionDatabase
                    DBNull.Value,         // XmlSchemaCollectionOwningSchema
                    DBNull.Value,         // XmlSchemaCollectionName
                    type.AssemblyQualifiedName, // UdtAssemblyQualifiedName
                    providerType,         // NonVersionedProviderType TODO: Convert to SqlDbType
                    false                 // IsColumnSet
                });
            }

            return schemaTable;
        }

        public override string GetString(int i)
        {
            return (string)this[i];
        }

        public override object GetValue(int i)
        {
            return this[i];
        }

        public override object GetProviderSpecificValue(int ordinal)
        {
            var value = _row[_columnSet[ordinal].SourceColumn];

            if (value is SqlVariant variant)
                value = (object)variant.Value ?? DBNull.Value;

            return value;
        }

        public override int GetProviderSpecificValues(object[] values)
        {
            for (var i = 0; i < values.Length && i < _columnSet.Count; i++)
                values[i] = GetProviderSpecificValue(i);

            return Math.Min(values.Length, _columnSet.Count);
        }

        public override int GetValues(object[] values)
        {
            for (var i = 0; i < values.Length && i < _columnSet.Count; i++)
                values[i] = this[i];

            return Math.Min(values.Length, _columnSet.Count);
        }

        public override bool IsDBNull(int i)
        {
            return GetRawValue(i).IsNull;
        }

        private INullable GetRawValue(int i)
        {
            var value = (INullable) _row[_columnSet[i].SourceColumn];

            if (value is SqlVariant variant)
                value = variant.Value ?? SqlVariant.Null;

            return value;
        }

        public override bool NextResult()
        {
            Close();
            return false;
        }

        public override bool Read()
        {
            if (IsClosed)
                return false;

            if (!_source.MoveNext())
            {
                _parameterValues["@@ROWCOUNT"] = (SqlInt32) _rowCount;
                Close();
                return false;
            }

            _row = _source.Current;
            _rowCount++;
            return true;
        }
    }
}
