using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace MarkMpn.Sql4Cds.Engine
{
    /// <summary>
    /// Helper methods for working with <see cref="DataTypeReference"/> objects
    /// </summary>
    static class DataTypeHelpers
    {
        public static SqlDataTypeReference VarChar(int length)
        {
            return new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.VarChar, Parameters = { length <= 8000 ? (Literal) new IntegerLiteral { Value = length.ToString(CultureInfo.InvariantCulture) } : new MaxLiteral() } };
        }

        public static SqlDataTypeReference Char(int length)
        {
            return new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.Char, Parameters = { length <= 8000 ? (Literal)new IntegerLiteral { Value = length.ToString(CultureInfo.InvariantCulture) } : new MaxLiteral() } };
        }

        public static SqlDataTypeReference VarBinary(int length)
        {
            return new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.VarBinary, Parameters = { length <= 8000 ? (Literal)new IntegerLiteral { Value = length.ToString(CultureInfo.InvariantCulture) } : new MaxLiteral() } };
        }

        public static SqlDataTypeReference Bit { get; } = new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.Bit };

        public static SqlDataTypeReference TinyInt { get; } = new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.TinyInt };

        public static SqlDataTypeReference Money { get; } = new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.Money };

        public static SqlDataTypeReference Date { get; } = new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.Date };

        public static SqlDataTypeReference DateTime { get; } = new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.DateTime };

        public static SqlDataTypeReference DateTime2 { get; } = new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.DateTime2 };

        public static SqlDataTypeReference DateTimeOffset { get; } = new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.DateTimeOffset };

        public static SqlDataTypeReference Decimal(int precision, int scale)
        {
            return new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.Decimal, Parameters = { new IntegerLiteral { Value = precision.ToString(CultureInfo.InvariantCulture) }, new IntegerLiteral { Value = scale.ToString(CultureInfo.InvariantCulture) } } };
        }

        public static SqlDataTypeReference Real { get; } = new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.Real };

        public static SqlDataTypeReference UniqueIdentifier { get; } = new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.UniqueIdentifier };

        public static SqlDataTypeReference SmallInt { get; } = new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.SmallInt };

        public static SqlDataTypeReference Int { get; } = new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.Int };

        public static SqlDataTypeReference BigInt { get; } = new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.BigInt };

        public static UserDataTypeReference Object(Type type)
        {
            return new UserDataTypeReference { Name = new SchemaObjectName { Identifiers = { new Identifier { Value = type.FullName } } } };
        }

        public static SqlDataTypeReference Float { get; } = new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.Float };

        public static SqlDataTypeReference NVarChar(int length)
        {
            return new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.NVarChar, Parameters = { new IntegerLiteral { Value = length.ToString(CultureInfo.InvariantCulture) } } };
        }

        public static SqlDataTypeReference NChar(int length)
        {
            return new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.NChar, Parameters = { new IntegerLiteral { Value = length.ToString(CultureInfo.InvariantCulture) } } };
        }

        public static SqlDataTypeReference Time { get; } = new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.Time };

        public static UserDataTypeReference EntityReference { get; } = Object(typeof(SqlEntityReference));

        /// <summary>
        /// Checks if a type represents an exact numeric type
        /// </summary>
        /// <param name="type">The type to check</param>
        /// <returns><c>true</c> if the <paramref name="type"/> is an exact numeric type, or <c>false</c> otherwise</returns>
        public static bool IsExactNumeric(this SqlDataTypeOption type)
        {
            return type == SqlDataTypeOption.BigInt ||
                type == SqlDataTypeOption.Bit ||
                type == SqlDataTypeOption.Decimal ||
                type == SqlDataTypeOption.Int ||
                type == SqlDataTypeOption.Money ||
                type == SqlDataTypeOption.Numeric ||
                type == SqlDataTypeOption.SmallInt ||
                type == SqlDataTypeOption.SmallMoney ||
                type == SqlDataTypeOption.TinyInt;
        }

        /// <summary>
        /// Checks if a type represents an approximate numeric type
        /// </summary>
        /// <param name="type">The type to check</param>
        /// <returns><c>true</c> if the <paramref name="type"/> is an approximate numeric type, or <c>false</c> otherwise</returns>
        public static bool IsApproximateNumeric(this SqlDataTypeOption type)
        {
            return type == SqlDataTypeOption.Float ||
                type == SqlDataTypeOption.Real;
        }

        /// <summary>
        /// Checks if a type represents an exact or approximate numeric type
        /// </summary>
        /// <param name="type">The type to check</param>
        /// <returns><c>true</c> if the <paramref name="type"/> is an exact or approximate numeric type, or <c>false</c> otherwise</returns>
        public static bool IsNumeric(this SqlDataTypeOption type)
        {
            return IsExactNumeric(type) || IsApproximateNumeric(type);
        }

        /// <summary>
        /// Checks if a type represents a string
        /// </summary>
        /// <param name="type">The type to check</param>
        /// <returns><c>true</c> if the <paramref name="type"/> is a string type, or <c>false</c> otherwise</returns>
        public static bool IsStringType(this SqlDataTypeOption type)
        {
            return type == SqlDataTypeOption.Char ||
                type == SqlDataTypeOption.VarChar ||
                type == SqlDataTypeOption.NChar ||
                type == SqlDataTypeOption.NVarChar ||
                type == SqlDataTypeOption.Text ||
                type == SqlDataTypeOption.NText;
        }

        /// <summary>
        /// Checks if a type represents a date/time
        /// </summary>
        /// <param name="type">The type to check</param>
        /// <returns><c>true</c> if the <paramref name="type"/> is a date/time type, or <c>false</c> otherwise</returns>
        /// <remarks>
        /// This method returns <c>false</c> for the <see cref="SqlDataTypeOption.Date"/> and <see cref="SqlDataTypeOption.Time"/>
        /// types.
        /// </remarks>
        public static bool IsDateTimeType(this SqlDataTypeOption type)
        {
            return type == SqlDataTypeOption.DateTime ||
                type == SqlDataTypeOption.SmallDateTime ||
                type == SqlDataTypeOption.DateTimeOffset ||
                type == SqlDataTypeOption.DateTime2;
        }

        /// <summary>
        /// Gets the size of the data that can be stored in a SQL <see cref="DataTypeReference"/>
        /// </summary>
        /// <param name="type">The data type to get the size of</param>
        /// <returns>The size of the data type to report in <see cref="System.Data.IDataReader.GetSchemaTable"/></returns>
        public static int GetSize(this DataTypeReference type)
        {
            if (!(type is SqlDataTypeReference dataType))
            {
                if (type is UserDataTypeReference udt && udt.Name.BaseIdentifier.Value == typeof(SqlEntityReference).FullName)
                    dataType = new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.UniqueIdentifier };
                else
                    throw new NotSupportedQueryFragmentException("Unsupported data type reference", type);
            }

            switch (dataType.SqlDataTypeOption)
            {
                case SqlDataTypeOption.VarChar:
                case SqlDataTypeOption.NVarChar:
                case SqlDataTypeOption.VarBinary:
                case SqlDataTypeOption.Char:
                case SqlDataTypeOption.NChar:
                case SqlDataTypeOption.Binary:
                    if (dataType.Parameters.Count == 1 && dataType.Parameters[0] is IntegerLiteral length && Int32.TryParse(length.Value, out var lengthValue))
                        return lengthValue;
                    else if (dataType.Parameters.Count == 1 && dataType.Parameters[0] is MaxLiteral)
                        return Int32.MaxValue;
                    else
                        return 1;

                case SqlDataTypeOption.Text:
                case SqlDataTypeOption.NText:
                case SqlDataTypeOption.Image:
                    return Int32.MaxValue;
            }

            var netType = dataType.ToNetType(out _);
            netType = netType.GetProperty("Value")?.PropertyType ?? netType;

            if (netType == typeof(DateTime))
                return 8;

            return Marshal.SizeOf(netType);
        }

        /// <summary>
        /// Gets the precision of the data that can be stored in a SQL <see cref="DataTypeReference"/>
        /// </summary>
        /// <param name="type">The data type to get the size of</param>
        /// <param name="invalidValue">The precision to return for non-precisioned types</param>
        /// <returns>The number of digits that can be stored in the type</returns>
        public static int GetPrecision(this DataTypeReference type, int invalidValue = 0)
        {
            if (!(type is SqlDataTypeReference dataType))
                return invalidValue;

            switch (dataType.SqlDataTypeOption)
            {
                case SqlDataTypeOption.Numeric:
                case SqlDataTypeOption.Decimal:
                    if (dataType.Parameters.Count == 0 ||
                        !(dataType.Parameters[0] is IntegerLiteral) ||
                        !Int32.TryParse(dataType.Parameters[0].Value, out var precision) ||
                        precision < 1 ||
                        precision > 38)
                        return 18;

                    return precision;

                case SqlDataTypeOption.Int:
                    return 10;

                case SqlDataTypeOption.DateTime:
                    return 23;

                case SqlDataTypeOption.SmallInt:
                    return 5;

                case SqlDataTypeOption.Money:
                    return 19;

                case SqlDataTypeOption.TinyInt:
                    return 3;

                case SqlDataTypeOption.BigInt:
                    return 19;

                case SqlDataTypeOption.SmallDateTime:
                    return 16;

                case SqlDataTypeOption.Float:
                    return 15;

                case SqlDataTypeOption.Real:
                    return 7;

                case SqlDataTypeOption.SmallMoney:
                    return 10;
            }

            return invalidValue;
        }

        /// <summary>
        /// Gets the scale of the data that can be stored in a SQL <see cref="DataTypeReference"/>
        /// </summary>
        /// <param name="type">The data type to get the size of</param>
        /// <param name="invalidValue">The scale to return for non-scaled types</param>
        /// <returns>The number of digits that can be stored in the type after the decimal point</returns>
        public static int GetScale(this DataTypeReference type, int invalidValue = 0)
        {
            if (!(type is SqlDataTypeReference dataType))
                return invalidValue;

            switch (dataType.SqlDataTypeOption)
            {
                case SqlDataTypeOption.Numeric:
                case SqlDataTypeOption.Decimal:
                    if (dataType.Parameters.Count < 2 ||
                        !(dataType.Parameters[0] is IntegerLiteral) ||
                        !Int32.TryParse(dataType.Parameters[0].Value, out var precision) ||
                        precision < 1 ||
                        precision > 38 ||
                        !(dataType.Parameters[1] is IntegerLiteral) ||
                        !Int32.TryParse(dataType.Parameters[1].Value, out var scale) ||
                        scale < 0 ||
                        scale > precision)
                        return 0;

                    return scale;

                case SqlDataTypeOption.DateTime:
                    return 3;

                case SqlDataTypeOption.DateTimeOffset:
                    return 7;

                case SqlDataTypeOption.DateTime2:
                    return 7;

                case SqlDataTypeOption.SmallDateTime:
                    return 0;

                case SqlDataTypeOption.Time:
                    return 7;
            }

            return invalidValue;
        }

        /// <summary>
        /// Checks if two data types are the same
        /// </summary>
        /// <param name="x">The first data type to compare</param>
        /// <param name="y">The second data type to compare</param>
        /// <returns><c>true</c> if <paramref name="x"/> and <paramref name="y"/> are equal, or <c>false</c> otherwise</returns>
        public static bool IsSameAs(this DataTypeReference x, DataTypeReference y)
        {
            return DataTypeComparer.Instance.Equals(x, y);
        }
    }

    /// <summary>
    /// Checks if two <see cref="DataTypeReference"/> instances represent the same data type
    /// </summary>
    class DataTypeComparer : IEqualityComparer<DataTypeReference>
    {
        private DataTypeComparer()
        {
        }

        /// <summary>
        /// A reusable instance of the <see cref="DataTypeComparer"/>
        /// </summary>
        public static DataTypeComparer Instance { get; } = new DataTypeComparer();

        public bool Equals(DataTypeReference x, DataTypeReference y)
        {
            if (x == y)
                return true;

            if (x == null || y == null)
                return false;

            var xUser = x as UserDataTypeReference;
            var xSql = x as SqlDataTypeReference;
            var yUser = y as UserDataTypeReference;
            var ySql = y as SqlDataTypeReference;

            if (xUser != null && yUser != null)
                return String.Join(".", xUser.Name.Identifiers.Select(i => i.Value)).Equals(String.Join(".", yUser.Name.Identifiers.Select(i => i.Value)), StringComparison.OrdinalIgnoreCase);

            if (xSql == null || ySql == null)
                return false;

            if (xSql.SqlDataTypeOption != ySql.SqlDataTypeOption)
                return false;

            if (xSql.Parameters.Count != ySql.Parameters.Count)
                return false;

            for (var i = 0; i < xSql.Parameters.Count; i++)
            {
                if (xSql.Parameters[i].LiteralType != ySql.Parameters[i].LiteralType)
                    return false;

                if (xSql.Parameters[i].Value == ySql.Parameters[i].Value)
                    continue;

                if (xSql.Parameters[i].Value == null || ySql.Parameters[i].Value == null)
                    return false;

                if (!xSql.Parameters[i].Value.Equals(ySql.Parameters[i].Value, StringComparison.OrdinalIgnoreCase))
                    return false;
            }

            return true;
        }

        public int GetHashCode(DataTypeReference obj)
        {
            if (obj is SqlDataTypeReference sql)
                return sql.SqlDataTypeOption.GetHashCode();

            if (obj is UserDataTypeReference user)
                return StringComparer.OrdinalIgnoreCase.GetHashCode(String.Join(".", user.Name.Identifiers.Select(i => i.Value)));

            throw new NotSupportedException();
        }
    }
}
