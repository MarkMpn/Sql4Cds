﻿using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Globalization;
using System.Linq;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace MarkMpn.Sql4Cds.Engine
{
    /// <summary>
    /// Helper methods for working with <see cref="DataTypeReference"/> objects
    /// </summary>
    static class DataTypeHelpers
    {
        public static SqlDataTypeReference VarChar(int length, Collation collation, CollationLabel collationLabel)
        {
            return new SqlDataTypeReferenceWithCollation { SqlDataTypeOption = SqlDataTypeOption.VarChar, Parameters = { length <= 8000 ? (Literal) new IntegerLiteral { Value = length.ToString(CultureInfo.InvariantCulture) } : new MaxLiteral() }, Collation = collation, CollationLabel = collationLabel };
        }

        public static SqlDataTypeReference Char(int length, Collation collation, CollationLabel collationLabel)
        {
            return new SqlDataTypeReferenceWithCollation { SqlDataTypeOption = SqlDataTypeOption.Char, Parameters = { length <= 8000 ? (Literal)new IntegerLiteral { Value = length.ToString(CultureInfo.InvariantCulture) } : new MaxLiteral() }, Collation = collation, CollationLabel = collationLabel };
        }

        public static SqlDataTypeReference VarBinary(int length)
        {
            return new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.VarBinary, Parameters = { length <= 8000 ? (Literal)new IntegerLiteral { Value = length.ToString(CultureInfo.InvariantCulture) } : new MaxLiteral() } };
        }

        public static SqlDataTypeReference Binary(int length)
        {
            return new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.Binary, Parameters = { length <= 8000 ? (Literal)new IntegerLiteral { Value = length.ToString(CultureInfo.InvariantCulture) } : new MaxLiteral() } };
        }

        public static SqlDataTypeReference Bit { get; } = new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.Bit };

        public static SqlDataTypeReference TinyInt { get; } = new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.TinyInt };

        public static SqlDataTypeReference Money { get; } = new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.Money };

        public static SqlDataTypeReference SmallMoney { get; } = new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.SmallMoney };

        public static SqlDataTypeReference Date { get; } = new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.Date };

        public static SqlDataTypeReference DateTime { get; } = new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.DateTime };

        public static SqlDataTypeReference SmallDateTime { get; } = new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.SmallDateTime };

        public static SqlDataTypeReference DateTime2(short scale)
        {
            return new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.DateTime2, Parameters = { new IntegerLiteral { Value = scale.ToString(CultureInfo.InvariantCulture) } } };
        }

        public static SqlDataTypeReference DateTimeOffset { get; } = new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.DateTimeOffset };

        public static SqlDataTypeReference Decimal(short precision, short scale)
        {
            return new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.Decimal, Parameters = { new IntegerLiteral { Value = precision.ToString(CultureInfo.InvariantCulture) }, new IntegerLiteral { Value = scale.ToString(CultureInfo.InvariantCulture) } } };
        }

        public static SqlDataTypeReference Real { get; } = new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.Real };

        public static SqlDataTypeReference UniqueIdentifier { get; } = new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.UniqueIdentifier };

        public static SqlDataTypeReference SmallInt { get; } = new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.SmallInt };

        public static SqlDataTypeReference Int { get; } = new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.Int };

        public static SqlDataTypeReference ImplicitIntForNullLiteral { get; } = new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.Int };

        public static SqlDataTypeReference BigInt { get; } = new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.BigInt };

        public static UserDataTypeReference Object(Type type)
        {
            return Object(type.FullName);
        }

        private static UserDataTypeReference Object(string name)
        {
            return new UserDataTypeReference { Name = new SchemaObjectName { Identifiers = { new Identifier { Value = name } } } };
        }

        public static SqlDataTypeReference Float { get; } = new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.Float };

        public static SqlDataTypeReference NVarChar(int length, Collation collation, CollationLabel collationLabel)
        {
            return new SqlDataTypeReferenceWithCollation { SqlDataTypeOption = SqlDataTypeOption.NVarChar, Parameters = { length <= 8000 ? (Literal) new IntegerLiteral { Value = length.ToString(CultureInfo.InvariantCulture) } : new MaxLiteral() }, Collation = collation, CollationLabel = collationLabel };
        }

        public static SqlDataTypeReference NChar(int length, Collation collation, CollationLabel collationLabel)
        {
            return new SqlDataTypeReferenceWithCollation { SqlDataTypeOption = SqlDataTypeOption.NChar, Parameters = { length <= 8000 ? (Literal)new IntegerLiteral { Value = length.ToString(CultureInfo.InvariantCulture) } : new MaxLiteral() }, Collation = collation, CollationLabel = collationLabel };
        }

        public static SqlDataTypeReference Time(short scale)
        {
            return new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.Time, Parameters = { new IntegerLiteral { Value = scale.ToString(CultureInfo.InvariantCulture) } } };
        }

        public static UserDataTypeReference EntityReference { get; } = Object(nameof(EntityReference));

        public static XmlDataTypeReference Xml { get; } = new XmlDataTypeReference();

        public static SqlDataTypeReference Variant { get; } = new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.Sql_Variant };

        /// <summary>
        /// Gets the data type family the type belongs to.
        /// </summary>
        /// <param name="type">The type to get the family for</param>
        /// <returns>The family of the data type</returns>
        /// <remarks>
        /// Uses the definitions from https://learn.microsoft.com/en-us/sql/t-sql/data-types/data-types-transact-sql?view=sql-server-ver16#data-type-categories
        /// </remarks>
        public static DataTypeFamily GetDataTypeFamily(this DataTypeReference type)
        {
            if (type is SqlDataTypeReference sqlType)
            {
                switch (sqlType.SqlDataTypeOption)
                {
                    case SqlDataTypeOption.BigInt:
                    case SqlDataTypeOption.Bit:
                    case SqlDataTypeOption.Decimal:
                    case SqlDataTypeOption.Int:
                    case SqlDataTypeOption.Money:
                    case SqlDataTypeOption.Numeric:
                    case SqlDataTypeOption.SmallInt:
                    case SqlDataTypeOption.SmallMoney:
                    case SqlDataTypeOption.TinyInt:
                        return DataTypeFamily.ExactNumeric;

                    case SqlDataTypeOption.Float:
                    case SqlDataTypeOption.Real:
                        return DataTypeFamily.ApproximateNumeric;

                    case SqlDataTypeOption.Date:
                    case SqlDataTypeOption.DateTime2:
                    case SqlDataTypeOption.DateTime:
                    case SqlDataTypeOption.DateTimeOffset:
                    case SqlDataTypeOption.SmallDateTime:
                    case SqlDataTypeOption.Time:
                        return DataTypeFamily.DateTime;

                    case SqlDataTypeOption.Char:
                    case SqlDataTypeOption.Text:
                    case SqlDataTypeOption.VarChar:
                        return DataTypeFamily.Character;

                    case SqlDataTypeOption.NChar:
                    case SqlDataTypeOption.NText:
                    case SqlDataTypeOption.NVarChar:
                        return DataTypeFamily.UnicodeCharacter;

                    case SqlDataTypeOption.Binary:
                    case SqlDataTypeOption.Image:
                    case SqlDataTypeOption.VarBinary:
                        return DataTypeFamily.Binary;

                    default:
                        return DataTypeFamily.Other;
                }
            }

            if (type is UserDataTypeReference)
                return DataTypeFamily.Custom;

            return DataTypeFamily.Other;
        }

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
                if (type.IsSameAs(EntityReference))
                    dataType = new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.UniqueIdentifier };
                else if (type is XmlDataTypeReference)
                    return Int32.MaxValue;
                else
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidDataType(type));
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

                case SqlDataTypeOption.Int:
                    return 4;

                case SqlDataTypeOption.UniqueIdentifier:
                    return 16;

                case SqlDataTypeOption.DateTime:
                    return 8;

                case SqlDataTypeOption.SmallInt:
                    return 2;

                case SqlDataTypeOption.Decimal:
                    return 17;

                case SqlDataTypeOption.Money:
                    return 8;

                case SqlDataTypeOption.TinyInt:
                    return 1;

                case SqlDataTypeOption.BigInt:
                    return 8;

                case SqlDataTypeOption.DateTimeOffset:
                    return 10;

                case SqlDataTypeOption.DateTime2:
                    return 8;

                case SqlDataTypeOption.SmallDateTime:
                    return 4;

                case SqlDataTypeOption.Date:
                    return 3;

                case SqlDataTypeOption.Time:
                    return 5;

                case SqlDataTypeOption.Float:
                    return 8;

                case SqlDataTypeOption.Real:
                    return 4;

                case SqlDataTypeOption.SmallMoney:
                    return 4;

                case SqlDataTypeOption.Bit:
                    return 1;
            }

            return 0;
        }

        /// <summary>
        /// Gets the precision of the data that can be stored in a SQL <see cref="DataTypeReference"/>
        /// </summary>
        /// <param name="type">The data type to get the size of</param>
        /// <param name="invalidValue">The precision to return for non-precisioned types</param>
        /// <returns>The number of digits that can be stored in the type</returns>
        public static short GetPrecision(this DataTypeReference type, short invalidValue = 0)
        {
            if (!(type is SqlDataTypeReference dataType))
                return invalidValue;

            switch (dataType.SqlDataTypeOption)
            {
                case SqlDataTypeOption.Numeric:
                case SqlDataTypeOption.Decimal:
                    if (dataType.Parameters.Count == 0 ||
                        !(dataType.Parameters[0] is IntegerLiteral) ||
                        !Int16.TryParse(dataType.Parameters[0].Value, out var precision) ||
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
        public static short GetScale(this DataTypeReference type, short invalidValue = 0)
        {
            if (!(type is SqlDataTypeReference dataType))
                return invalidValue;

            switch (dataType.SqlDataTypeOption)
            {
                case SqlDataTypeOption.Numeric:
                case SqlDataTypeOption.Decimal:
                    if (dataType.Parameters.Count < 2 ||
                        !(dataType.Parameters[0] is IntegerLiteral) ||
                        !Int16.TryParse(dataType.Parameters[0].Value, out var precision) ||
                        precision < 1 ||
                        precision > 38 ||
                        !(dataType.Parameters[1] is IntegerLiteral) ||
                        !Int16.TryParse(dataType.Parameters[1].Value, out var scale) ||
                        scale < 0 ||
                        scale > precision)
                        return 0;

                    return scale;

                case SqlDataTypeOption.DateTime:
                    return 3;

                case SqlDataTypeOption.DateTimeOffset:
                    return 7;

                case SqlDataTypeOption.SmallDateTime:
                    return 0;

                case SqlDataTypeOption.DateTime2:
                case SqlDataTypeOption.Time:
                    if (dataType.Parameters.Count == 0 ||
                        !(dataType.Parameters[0] is IntegerLiteral) ||
                        !Int16.TryParse(dataType.Parameters[0].Value, out var timeScale) ||
                        timeScale < 0 ||
                        timeScale > 7)
                        return 7;

                    return timeScale;

                case SqlDataTypeOption.Money:
                case SqlDataTypeOption.SmallMoney:
                    return 4;
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

        /// <summary>
        /// Parses a data type from a string
        /// </summary>
        /// <param name="context">The context in which the type name is being parsed</param>
        /// <param name="value">The string representation of the data type to parse</param>
        /// <param name="parsedType">The data type that has been parsed from the <paramref name="value"/></param>
        /// <returns><c>true</c> if the <paramref name="value"/> could be successfully parsed, or <c>false</c> otherwise</returns>
        public static bool TryParse(ExpressionCompilationContext context, string value, out DataTypeReference parsedType)
        {
            parsedType = null;

            var name = value;
            var parameters = new List<Literal>();
            var parenStart = value.IndexOf('(');
            if (parenStart != -1)
            {
                name = value.Substring(0, parenStart).Trim();

                if (!value.EndsWith(")"))
                    return false;

                var parts = value.Substring(parenStart + 1, value.Length - parenStart - 2).Split(',');

                foreach (var part in parts)
                {
                    if (part.Trim().Equals("max", StringComparison.OrdinalIgnoreCase))
                    {
                        parameters.Add(new MaxLiteral());
                        continue;
                    }

                    if (!Int32.TryParse(part.Trim(), out var paramValue))
                        return false;

                    parameters.Add(new IntegerLiteral { Value = part });
                }
            }

            if (name.Equals("xml", StringComparison.OrdinalIgnoreCase))
            {
                if (parameters.Count > 0)
                    return false;

                parsedType = DataTypeHelpers.Xml;
                return true;
            }

            if (!Enum.TryParse<SqlDataTypeOption>(name, true, out var sqlType))
                return false;

            if (sqlType.IsStringType())
                parsedType = new SqlDataTypeReferenceWithCollation { SqlDataTypeOption = sqlType, Collation = context.PrimaryDataSource.DefaultCollation, CollationLabel = CollationLabel.CoercibleDefault };
            else
                parsedType = new SqlDataTypeReference { SqlDataTypeOption = sqlType };

            foreach (var param in parameters)
                ((SqlDataTypeReference)parsedType).Parameters.Add(param);

            return true;
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
            var xColl = x as SqlDataTypeReferenceWithCollation;
            var xXml = x as XmlDataTypeReference;
            var yUser = y as UserDataTypeReference;
            var ySql = y as SqlDataTypeReference;
            var yColl = y as SqlDataTypeReferenceWithCollation;
            var yXml = y as XmlDataTypeReference;

            if (xUser != null && yUser != null)
                return String.Join(".", xUser.Name.Identifiers.Select(i => i.Value)).Equals(String.Join(".", yUser.Name.Identifiers.Select(i => i.Value)), StringComparison.OrdinalIgnoreCase);

            if (xXml != null && yXml != null)
                return xXml.XmlDataTypeOption == yXml.XmlDataTypeOption && xXml.XmlSchemaCollection == yXml.XmlSchemaCollection;

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

            if (xColl != null && yColl != null && (xColl.Collation == null ^ yColl.Collation == null || xColl.Collation != null && yColl.Collation != null && !xColl.Collation.Equals(yColl.Collation)))
                return false;

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

    /// <summary>
    /// Extends the standard <see cref="SqlDataTypeReference"/> with additional collation information
    /// </summary>
    class SqlDataTypeReferenceWithCollation : SqlDataTypeReference
    {
        /// <summary>
        /// Returns or sets the collation that the data will use
        /// </summary>
        public Collation Collation { get; set; }

        /// <summary>
        /// Indicates how the <see cref="Collation"/> has been spplied
        /// </summary>
        public CollationLabel CollationLabel { get; set; }

        /// <summary>
        /// The error to use if a value of this type is used
        /// </summary>
        public Sql4CdsError CollationConflictError { get; set; }

        /// <summary>
        /// Applies the precedence rules to convert values of different collations to a single collation
        /// </summary>
        /// <param name="lhsSql">The type of the first expression</param>
        /// <param name="rhsSql">The type of the second expression</param>
        /// <param name="fragment">The query fragment to report any errors against</param>
        /// <param name="operation">The operation name to include in the error message</param>
        /// <param name="collation">The final collation to use</param>
        /// <param name="collationLabel">The final collation label to use</param>
        /// <param name="error">The error to use</param>
        /// <returns>The final collation to use</returns>
        internal static bool TryConvertCollation(SqlDataTypeReference lhsSql, SqlDataTypeReference rhsSql, TSqlFragment fragment, string operation, out Collation collation, out CollationLabel collationLabel, out Sql4CdsError error)
        {
            collation = null;
            collationLabel = CollationLabel.NoCollation;
            error = null;

            if (!(lhsSql is SqlDataTypeReferenceWithCollation lhsSqlWithColl))
                return false;

            if (!(rhsSql is SqlDataTypeReferenceWithCollation rhsSqlWithColl))
                return false;

            error = Sql4CdsError.CollationConflict(fragment, lhsSqlWithColl.Collation, rhsSqlWithColl.Collation, operation);

            // Two different explicit collations cannot be converted
            if (lhsSqlWithColl.CollationLabel == CollationLabel.Explicit &&
                rhsSqlWithColl.CollationLabel == CollationLabel.Explicit &&
                !lhsSqlWithColl.Collation.Equals(rhsSqlWithColl.Collation))
                return false;

            // If either collation is explicit, use that
            if (lhsSqlWithColl.CollationLabel == CollationLabel.Explicit)
            {
                collation = lhsSqlWithColl.Collation;
                collationLabel = CollationLabel.Explicit;
                return true;
            }

            if (rhsSqlWithColl.CollationLabel == CollationLabel.Explicit)
            {
                collation = rhsSqlWithColl.Collation;
                collationLabel = CollationLabel.Explicit;
                return true;
            }

            // If either label is no collation, use that
            if (lhsSqlWithColl.CollationLabel == CollationLabel.NoCollation ||
                rhsSqlWithColl.CollationLabel == CollationLabel.NoCollation)
            {
                collationLabel = CollationLabel.NoCollation;
                return true;
            }

            if (lhsSqlWithColl.CollationLabel == CollationLabel.Implicit &&
                rhsSqlWithColl.CollationLabel == CollationLabel.Implicit)
            {
                if (lhsSqlWithColl.Collation.Equals(rhsSqlWithColl.Collation))
                {
                    // Two identical implicit collations remains unchanged
                    // This doesn't appear to be explicitly defined in the docs, but seems reasonable
                    collation = lhsSqlWithColl.Collation;
                    collationLabel = CollationLabel.Implicit;
                    return true;
                }
                else
                {
                    // Two different implicit collations results in no collation
                    collation = null;
                    collationLabel = CollationLabel.NoCollation;
                    return true;
                }
            }

            // Implicit > coercible default
            if (lhsSqlWithColl.CollationLabel == CollationLabel.Implicit)
            {
                collation = lhsSqlWithColl.Collation;
                collationLabel = CollationLabel.Implicit;
                return true;
            }

            if (rhsSqlWithColl.CollationLabel == CollationLabel.Implicit)
            {
                collation = rhsSqlWithColl.Collation;
                collationLabel = CollationLabel.Implicit;
                return true;
            }

            collationLabel = CollationLabel.CoercibleDefault;
            collation = lhsSqlWithColl.Collation;
            return true;
        }
    }

    enum CollationLabel
    {
        CoercibleDefault,
        Implicit,
        Explicit,
        NoCollation
    }

    enum DataTypeFamily
    {
        Variant,
        DateTime,
        ApproximateNumeric,
        ExactNumeric,
        Character,
        UnicodeCharacter,
        Binary,
        Other,
        Custom
    }
}
