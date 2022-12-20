using System;
using System.Collections.Generic;
using System.Data;

namespace MarkMpn.Sql4Cds.LanguageServer.QueryExecution.Contracts
{
    /// <summary>
    /// Convert a base data type to another base data type
    /// </summary>
    public sealed class TypeConvertor
    {
        private static Dictionary<SqlDbType, Type> _typeMap = new Dictionary<SqlDbType, Type>();

        static TypeConvertor()
        {
            _typeMap[SqlDbType.BigInt] = typeof(long);
            _typeMap[SqlDbType.Binary] = typeof(byte);
            _typeMap[SqlDbType.Bit] = typeof(bool);
            _typeMap[SqlDbType.Char] = typeof(string);
            _typeMap[SqlDbType.DateTime] = typeof(DateTime);
            _typeMap[SqlDbType.Decimal] = typeof(decimal);
            _typeMap[SqlDbType.Float] = typeof(double);
            _typeMap[SqlDbType.Image] = typeof(byte[]);
            _typeMap[SqlDbType.Int] = typeof(int);
            _typeMap[SqlDbType.Money] = typeof(decimal);
            _typeMap[SqlDbType.NChar] = typeof(string);
            _typeMap[SqlDbType.NChar] = typeof(string);
            _typeMap[SqlDbType.NChar] = typeof(string);
            _typeMap[SqlDbType.NText] = typeof(string);
            _typeMap[SqlDbType.NVarChar] = typeof(string);
            _typeMap[SqlDbType.Real] = typeof(float);
            _typeMap[SqlDbType.UniqueIdentifier] = typeof(Guid);
            _typeMap[SqlDbType.SmallDateTime] = typeof(DateTime);
            _typeMap[SqlDbType.SmallInt] = typeof(short);
            _typeMap[SqlDbType.SmallMoney] = typeof(decimal);
            _typeMap[SqlDbType.Text] = typeof(string);
            _typeMap[SqlDbType.Timestamp] = typeof(byte[]);
            _typeMap[SqlDbType.TinyInt] = typeof(byte);
            _typeMap[SqlDbType.VarBinary] = typeof(byte[]);
            _typeMap[SqlDbType.VarChar] = typeof(string);
            _typeMap[SqlDbType.Variant] = typeof(object);
            // Note: treating as string
            _typeMap[SqlDbType.Xml] = typeof(string);
            _typeMap[SqlDbType.TinyInt] = typeof(byte);
            _typeMap[SqlDbType.TinyInt] = typeof(byte);
            _typeMap[SqlDbType.TinyInt] = typeof(byte);
            _typeMap[SqlDbType.TinyInt] = typeof(byte);
        }

        private TypeConvertor()
        {

        }


        /// <summary>
        /// Convert TSQL type to .Net data type
        /// </summary>
        /// <param name="sqlDbType"></param>
        /// <returns></returns>
        public static Type ToNetType(SqlDbType sqlDbType)
        {
            Type netType;
            if (!_typeMap.TryGetValue(sqlDbType, out netType))
            {
                netType = typeof(string);
            }
            return netType;
        }
    }
}
