using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlTypes;
using System.Globalization;
using System.Text;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine
{
    public class Sql4CdsParameter : DbParameter
    {
        private DbType? _type;
        private DataTypeReference _dataType;
        private object _value;

        public Sql4CdsParameter()
        {
        }

        public Sql4CdsParameter(string name, object value)
        {
            ParameterName = name;
            Value = value;
        }

        public override DbType DbType
        {
            get
            {
                if (_type == null)
                    _type = GetDbType();

                return _type.Value;
            }
            set
            {
                _type = value;
            }
        }

        public override ParameterDirection Direction { get; set; }

        public override bool IsNullable { get; set; }

        public override string ParameterName { get; set; }

        public override int Size { get; set; }

        public override string SourceColumn { get; set; }

        public override bool SourceColumnNullMapping { get; set; }

        public override object Value
        {
            get { return _value; }
            set
            {
                _value = value;
                ResetDbType();
            }
        }

        public override void ResetDbType()
        {
            _type = null;
            _dataType = null;
        }

        private DbType GetDbType()
        {
            if (Value == null || Value == DBNull.Value)
                return DbType.String;

            var type = Value.GetType();

            if (type == typeof(char) || type == typeof(char[]))
                type = typeof(string);

            if (type == typeof(byte[]) || type == typeof(SqlBinary) || type == typeof(SqlBytes))
                return DbType.Binary;

            if (type == typeof(Guid) || type == typeof(SqlGuid))
                return DbType.Guid;

            if (type == typeof(object))
                return DbType.Object;

            if (type == typeof(bool) || type == typeof(SqlBoolean))
                return DbType.Boolean;

            if (type == typeof(byte) || type == typeof(SqlByte))
                return DbType.Byte;

            if (type == typeof(short) || type == typeof(SqlInt16))
                return DbType.Int16;

            if (type == typeof(int) || type == typeof(SqlInt32))
                return DbType.Int32;

            if (type == typeof(long) || type == typeof(SqlInt64))
                return DbType.Int64;

            if (type == typeof(DateTime) || type == typeof(SqlDateTime))
                return DbType.DateTime;

            if (type == typeof(decimal) || type == typeof(SqlDecimal))
                return DbType.Decimal;

            if (type == typeof(double) || type == typeof(SqlDouble))
                return DbType.Double;

            if (type == typeof(float) || type == typeof(SqlSingle))
                return DbType.Single;

            if (type == typeof(string) || type == typeof(SqlString))
                return DbType.String;

            if (type == typeof(DateTimeOffset))
                return DbType.DateTimeOffset;

            if (type == typeof(SqlMoney))
                return DbType.Currency;

            if (type == typeof(EntityReference) || type == typeof(SqlEntityReference))
                return DbType.Object;

            throw new ArgumentOutOfRangeException(nameof(type), "Unsupported parameter type");
        }

        internal DataTypeReference GetDataType()
        {
            if (_dataType == null)
            {
                switch (DbType)
                {
                    case DbType.AnsiString:
                        _dataType = DataTypeHelpers.VarChar(Size);
                        break;

                    case DbType.AnsiStringFixedLength:
                        _dataType = DataTypeHelpers.Char(Size);
                        break;

                    case DbType.Binary:
                        _dataType = DataTypeHelpers.VarBinary(Size);
                        break;

                    case DbType.Boolean:
                        _dataType = DataTypeHelpers.Bit;
                        break;

                    case DbType.Byte:
                        _dataType = DataTypeHelpers.TinyInt;
                        break;

                    case DbType.Currency:
                        _dataType = DataTypeHelpers.Money;
                        break;

                    case DbType.Date:
                        _dataType = DataTypeHelpers.Date;
                        break;

                    case DbType.DateTime:
                        _dataType = DataTypeHelpers.DateTime;
                        break;

                    case DbType.DateTime2:
                        _dataType = DataTypeHelpers.DateTime2;
                        break;

                    case DbType.DateTimeOffset:
                        _dataType = DataTypeHelpers.DateTimeOffset;
                        break;

                    case DbType.Decimal:
                        var value = (SqlDecimal) GetValue();
                        _dataType = DataTypeHelpers.Decimal(value.Precision, value.Scale);
                        break;

                    case DbType.Double:
                        _dataType = DataTypeHelpers.Real;
                        break;

                    case DbType.Guid:
                        _dataType = DataTypeHelpers.UniqueIdentifier;
                        break;

                    case DbType.Int16:
                        _dataType = DataTypeHelpers.SmallInt;
                        break;

                    case DbType.Int32:
                        _dataType = DataTypeHelpers.Int;
                        break;

                    case DbType.Int64:
                        _dataType = DataTypeHelpers.BigInt;
                        break;

                    case DbType.Object:
                        _dataType = DataTypeHelpers.Object(GetValue().GetType());
                        break;

                    case DbType.SByte:
                        _dataType = DataTypeHelpers.TinyInt;
                        break;

                    case DbType.Single:
                        _dataType = DataTypeHelpers.Float;
                        break;

                    case DbType.String:
                        _dataType = DataTypeHelpers.NVarChar(Size);
                        break;

                    case DbType.StringFixedLength:
                        _dataType = DataTypeHelpers.NChar(Size);
                        break;

                    case DbType.Time:
                        _dataType = DataTypeHelpers.Time;
                        break;

                    case DbType.UInt16:
                        _dataType = DataTypeHelpers.Int;
                        break;

                    case DbType.UInt32:
                        _dataType = DataTypeHelpers.BigInt;
                        break;

                    case DbType.UInt64:
                        _dataType = DataTypeHelpers.BigInt;
                        break;

                    case DbType.VarNumeric:
                        _dataType = DataTypeHelpers.NVarChar(Int32.MaxValue);
                        break;

                    case DbType.Xml:
                        _dataType = DataTypeHelpers.NVarChar(Int32.MaxValue);
                        break;
                }
            }

            return _dataType;
        }

        internal INullable GetValue()
        {
            var value = Value;

            if (value is char ch)
                value = new string(ch, 1);
            else if (value is char[] charArray)
                value = new string(charArray);

            if (value is byte[] bys)
                value = (SqlBinary)bys;
            else if (value is Guid g)
                value = (SqlGuid)g;
            else if (value is bool bl)
                value = (SqlBoolean)bl;
            else if (value is byte by)
                value = (SqlByte)by;
            else if (value is short s)
                value = (SqlInt16)s;
            else if (value is int i)
                value = (SqlInt32)i;
            else if (value is long l)
                value = (SqlInt64)l;
            else if (value is DateTime dt)
                value = (SqlDateTime)dt;
            else if (value is decimal d)
                value = (SqlDecimal)d;
            else if (value is double db)
                value = (SqlDouble)db;
            else if (value is float f)
                value = (SqlSingle)f;
            else if (value is string str)
                value = SqlTypeConverter.UseDefaultCollation(str);
            else if (value is DateTimeOffset dto)
                value = (SqlDateTime)dto.DateTime;
            else if (value is EntityReference er)
                value = (SqlEntityReference)er;

            if (value is INullable nl)
                return nl;

            throw new NotSupportedException("Unsupported parameter type");
        }
    }
}
