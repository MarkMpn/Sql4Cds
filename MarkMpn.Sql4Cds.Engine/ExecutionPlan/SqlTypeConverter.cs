using System;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// Converts values between different types
    /// </summary>
    class SqlTypeConverter
    {
        // Abbreviated version of data type precedence from https://docs.microsoft.com/en-us/sql/t-sql/data-types/data-type-precedence-transact-sql?view=sql-server-ver15
        private static readonly SqlDataTypeOption[] _precendenceOrder = new[]
        {
            SqlDataTypeOption.Sql_Variant,
            SqlDataTypeOption.DateTimeOffset,
            SqlDataTypeOption.DateTime2,
            SqlDataTypeOption.DateTime,
            SqlDataTypeOption.SmallDateTime,
            SqlDataTypeOption.Date,
            SqlDataTypeOption.Time,
            SqlDataTypeOption.Float,
            SqlDataTypeOption.Real,
            SqlDataTypeOption.Decimal,
            SqlDataTypeOption.Money,
            SqlDataTypeOption.SmallMoney,
            SqlDataTypeOption.BigInt,
            SqlDataTypeOption.Int,
            SqlDataTypeOption.SmallInt,
            SqlDataTypeOption.TinyInt,
            SqlDataTypeOption.Bit,
            SqlDataTypeOption.NText,
            SqlDataTypeOption.Text,
            SqlDataTypeOption.Image,
            SqlDataTypeOption.Rowversion,
            SqlDataTypeOption.Timestamp,
            SqlDataTypeOption.UniqueIdentifier,
            SqlDataTypeOption.NVarChar,
            SqlDataTypeOption.NChar,
            SqlDataTypeOption.VarChar,
            SqlDataTypeOption.Char,
            SqlDataTypeOption.VarBinary,
            SqlDataTypeOption.Binary,
        };

        private static readonly IDictionary<Type, INullable> _nullValues;
        private static readonly CultureInfo _hijriCulture;
        private static ConcurrentDictionary<string, Func<object, ExpressionExecutionContext, object>> _conversions;
        private static ConcurrentDictionary<string, Func<INullable, ExpressionExecutionContext, INullable>> _sqlConversions;
        private static Dictionary<Type, Type> _netToSqlTypeConversions;
        private static Dictionary<Type, Func<DataSource, object, DataTypeReference, INullable>> _netToSqlTypeConversionFuncs;
        private static Dictionary<Type, Type> _sqlToNetTypeConversions;
        private static Dictionary<Type, Func<INullable, object>> _sqlToNetTypeConversionFuncs;

        static SqlTypeConverter()
        {
            _nullValues = new Dictionary<Type, INullable>
            {
                [typeof(SqlBinary)] = SqlBinary.Null,
                [typeof(SqlBoolean)] = SqlBoolean.Null,
                [typeof(SqlByte)] = SqlByte.Null,
                [typeof(SqlDateTime)] = SqlDateTime.Null,
                [typeof(SqlDecimal)] = SqlDecimal.Null,
                [typeof(SqlDouble)] = SqlDouble.Null,
                [typeof(SqlGuid)] = SqlGuid.Null,
                [typeof(SqlEntityReference)] = SqlEntityReference.Null,
                [typeof(SqlInt16)] = SqlInt16.Null,
                [typeof(SqlInt32)] = SqlInt32.Null,
                [typeof(SqlInt64)] = SqlInt64.Null,
                [typeof(SqlMoney)] = SqlMoney.Null,
                [typeof(SqlSingle)] = SqlSingle.Null,
                [typeof(SqlString)] = SqlString.Null,
                [typeof(SqlDate)] = SqlDate.Null,
                [typeof(SqlDateTime2)] = SqlDateTime2.Null,
                [typeof(SqlDateTimeOffset)] = SqlDateTimeOffset.Null,
                [typeof(SqlSmallDateTime)] = SqlSmallDateTime.Null,
                [typeof(SqlTime)] = SqlTime.Null,
                [typeof(SqlXml)] = SqlXml.Null,
                [typeof(SqlVariant)] = SqlVariant.Null
            };

            _hijriCulture = (CultureInfo)CultureInfo.GetCultureInfo("ar-JO").Clone();
            _hijriCulture.DateTimeFormat.Calendar = new HijriCalendar();

            _conversions = new ConcurrentDictionary<string, Func<object, ExpressionExecutionContext, object>>();
            _sqlConversions = new ConcurrentDictionary<string, Func<INullable, ExpressionExecutionContext, INullable>>();

            _netToSqlTypeConversions = new Dictionary<Type, Type>();
            _netToSqlTypeConversionFuncs = new Dictionary<Type, Func<DataSource, object, DataTypeReference, INullable>>();
            _sqlToNetTypeConversions = new Dictionary<Type, Type>();
            _sqlToNetTypeConversionFuncs = new Dictionary<Type, Func<INullable, object>>();

            // https://docs.microsoft.com/en-us/dotnet/framework/data/adonet/sql-server-data-type-mappings
            AddNullableTypeConversion<SqlBinary, byte[]>((ds, v, dt) => v, v => v.Value);
            AddTypeConversion<SqlBoolean, bool>((ds, v, dt) => v, v => v.Value);
            AddTypeConversion<SqlByte, byte>((ds, v, dt) => v, v => v.Value);
            AddTypeConversion<SqlDateTime, DateTime>((ds, v, dt) => v, v => v.Value);
            AddTypeConversion<SqlDecimal, decimal>((ds, v, dt) => SqlDecimal.ConvertToPrecScale(v, dt.GetPrecision(), dt.GetScale()), v => v.Value);
            AddTypeConversion<SqlDouble, double>((ds, v, dt) => v, v => v.Value);
            AddTypeConversion<SqlGuid, Guid>((ds, v, dt) => v, v => v.Value);
            AddTypeConversion<SqlInt16, short>((ds, v, dt) => v, v => v.Value);
            AddTypeConversion<SqlInt32, int>((ds, v, dt) => v, v => v.Value);
            AddTypeConversion<SqlInt64, long>((ds, v, dt) => v, v => v.Value);
            AddTypeConversion<SqlSingle, float>((ds, v, dt) => v, v => v.Value);
            AddNullableTypeConversion<SqlString, string>((ds, v, dt) => ((SqlDataTypeReferenceWithCollation)dt).Collation.ToSqlString(v), v => v.Value);
            AddTypeConversion<SqlMoney, decimal>((ds, v, dt) => v, v => v.Value);
            AddTypeConversion<SqlDate, DateTime>((ds, v, dt) => new SqlDate(v), v => v.Value);
            AddTypeConversion<SqlDateTime2, DateTime>((ds, v, dt) => SqlDateTime2.ConvertToScale(new SqlDateTime2(v), dt.GetScale()), v => v.Value);
            AddTypeConversion<SqlDateTimeOffset, DateTimeOffset>((ds, v, dt) => SqlDateTimeOffset.ConvertToScale(new SqlDateTimeOffset(v), dt.GetScale()), v => v.Value);
            AddTypeConversion<SqlSmallDateTime, DateTime>((ds, v, dt) => new SqlSmallDateTime(v), v => v.Value);
            AddTypeConversion<SqlTime, TimeSpan>((ds, v, dt) => SqlTime.ConvertToScale(new SqlTime(v), dt.GetScale()), v => v.Value);
            AddNullableTypeConversion<SqlXml, string>((ds, v, dt) => new SqlXml(new MemoryStream(Encoding.GetEncoding("utf-16").GetBytes(v))), v => v.Value);

            AddNullableTypeConversion<SqlMoney, Money>((ds, v, dt) => v.Value, null);
            AddNullableTypeConversion<SqlInt32, OptionSetValue>((ds, v, dt) => v.Value, null);
            AddNullableTypeConversion<SqlString, OptionSetValueCollection>((ds, v, dt) => ds.DefaultCollation.ToSqlString(String.Join(",", v.Select(osv => osv.Value))), null);
            AddNullableTypeConversion<SqlString, EntityCollection>((ds, v, dt) => ds.DefaultCollation.ToSqlString(String.Join(",", v.Entities.Select(e => FormatEntityCollectionEntry(e)))), null);
            AddNullableTypeConversion<SqlEntityReference, EntityReference>((ds, v, dt) => new SqlEntityReference(ds.Name, v), v => v);
        }

        private static void AddTypeConversion<TSql, TNet>(Func<DataSource, TNet, DataTypeReference, TSql> netToSql, Func<TSql, TNet> sqlToNet)
            where TSql: INullable
            where TNet: struct
        {
            if (netToSql != null)
            {
                if (!_netToSqlTypeConversions.ContainsKey(typeof(TNet)))
                {
                    _netToSqlTypeConversions[typeof(TNet)] = typeof(TSql);
                    _netToSqlTypeConversionFuncs[typeof(TNet)] = (ds, v, dt) => netToSql(ds, (TNet)v, dt);
                }

                var nullValue = _nullValues[typeof(TSql)];

                if (!_netToSqlTypeConversions.ContainsKey(typeof(TNet?)))
                {
                    _netToSqlTypeConversions[typeof(TNet?)] = typeof(TSql);
                    _netToSqlTypeConversionFuncs[typeof(TNet?)] = (ds, v, dt) => v == null ? nullValue : netToSql(ds, (TNet)v, dt);
                }

                if (!_netToSqlTypeConversions.ContainsKey(typeof(ManagedProperty<TNet>)))
                {
                    _netToSqlTypeConversions[typeof(ManagedProperty<TNet>)] = typeof(TSql);
                    _netToSqlTypeConversionFuncs[typeof(ManagedProperty<TNet>)] = (ds, v, dt) => v == null ? nullValue : netToSql(ds, ((ManagedProperty<TNet>)v).Value, dt);
                }
            }

            if (sqlToNet != null && !_sqlToNetTypeConversions.ContainsKey(typeof(TSql)))
            {
                _sqlToNetTypeConversions[typeof(TSql)] = typeof(TNet);
                _sqlToNetTypeConversionFuncs[typeof(TSql)] = v => sqlToNet((TSql)v);
            }
        }

        private static void AddNullableTypeConversion<TSql, TNet>(Func<DataSource, TNet, DataTypeReference, TSql> netToSql, Func<TSql, TNet> sqlToNet)
            where TSql : INullable
            where TNet : class
        {
            if (netToSql != null)
            {
                if (!_netToSqlTypeConversions.ContainsKey(typeof(TNet)))
                {
                    _netToSqlTypeConversions[typeof(TNet)] = typeof(TSql);
                    _netToSqlTypeConversionFuncs[typeof(TNet)] = (ds, v, dt) => netToSql(ds, (TNet)v, dt);
                }

                var nullValue = _nullValues[typeof(TSql)];

                if (!_netToSqlTypeConversions.ContainsKey(typeof(ManagedProperty<TNet>)))
                {
                    _netToSqlTypeConversions[typeof(ManagedProperty<TNet>)] = typeof(TSql);
                    _netToSqlTypeConversionFuncs[typeof(ManagedProperty<TNet>)] = (ds, v, dt) => v == null ? nullValue : netToSql(ds, ((ManagedProperty<TNet>)v).Value, dt);
                }
            }

            if (sqlToNet != null && !_sqlToNetTypeConversions.ContainsKey(typeof(TSql)))
            {
                _sqlToNetTypeConversions[typeof(TSql)] = typeof(TNet);
                _sqlToNetTypeConversionFuncs[typeof(TSql)] = v => ((TSql)v).IsNull ? null : sqlToNet((TSql)v);
            }
        }

        /// <summary>
        /// Checks if values of two different types can be converted to a consistent type
        /// </summary>
        /// <param name="lhs">The type of the first value</param>
        /// <param name="rhs">The type of the second value</param>
        /// <param name="primaryDataSource">The details of the primary data source being used for the connection</param>
        /// <param name="consistent">The type that both values can be converted to</param>
        /// <returns><c>true</c> if the two values can be converted to a consistent type, or <c>false</c> otherwise</returns>
        public static bool CanMakeConsistentTypes(DataTypeReference lhs, DataTypeReference rhs, DataSource primaryDataSource, TSqlFragment fragment, string operation, out DataTypeReference consistent)
        {
            if (lhs == DataTypeHelpers.ImplicitIntForNullLiteral)
            {
                consistent = rhs;
                return true;
            }

            if (rhs == DataTypeHelpers.ImplicitIntForNullLiteral)
            {
                consistent = lhs;
                return true;
            }

            if (lhs.IsSameAs(rhs))
            {
                consistent = lhs;
                return true;
            }

            var lhsUser = lhs as UserDataTypeReference;
            var lhsSql = lhs as SqlDataTypeReference;
            var rhsUser = rhs as UserDataTypeReference;
            var rhsSql = rhs as SqlDataTypeReference;

            // Check user-defined types are identical
            if (lhsUser != null && rhsUser != null)
            {
                // We can convert a typed entity reference to an untyped one
                if (lhsUser.Name.BaseIdentifier.Value.Equals(nameof(DataTypeHelpers.EntityReference), StringComparison.OrdinalIgnoreCase) && rhsUser.Name.BaseIdentifier.Value.Equals(nameof(DataTypeHelpers.EntityReference), StringComparison.OrdinalIgnoreCase))
                {
                    if (lhsUser.Parameters.Count == 0)
                    {
                        consistent = lhs;
                        return true;
                    }

                    if (rhsUser.Parameters.Count == 0)
                    {
                        consistent = rhs;
                        return true;
                    }

                    if (lhsUser.Parameters.Count == 1 && rhsUser.Parameters.Count == 1 && lhsUser.Parameters[0].Value.Equals(rhsUser.Parameters[0].Value, StringComparison.OrdinalIgnoreCase))
                    {
                        consistent = lhs;
                        return true;
                    }

                    // We can also make two different typed entity references consistent by down-casting them
                    // to untyped entity references
                    consistent = DataTypeHelpers.EntityReference;
                    return true;
                }

                if (String.Join(".", lhsUser.Name.Identifiers.Select(i => i.Value)).Equals(String.Join(".", rhsUser.Name.Identifiers.Select(i => i.Value)), StringComparison.OrdinalIgnoreCase))
                {
                    consistent = lhs;
                    return true;
                }

                consistent = null;
                return false;
            }

            // If one or other type is a user-defined type, check it is a known type (SqlEntityReference)
            if (lhsUser != null && !lhsUser.IsEntityReference())
            {
                consistent = null;
                return false;
            }

            if (rhsUser != null && !rhsUser.IsEntityReference())
            {
                consistent = null;
                return false;
            }

            // Get the basic type. Substitute SqlEntityReference with uniqueidentifier
            var lhsType = lhsSql?.SqlDataTypeOption ?? SqlDataTypeOption.UniqueIdentifier;
            var rhsType = rhsSql?.SqlDataTypeOption ?? SqlDataTypeOption.UniqueIdentifier;

            var lhsPrecedence = Array.IndexOf(_precendenceOrder, lhsType);
            var rhsPrecedence = Array.IndexOf(_precendenceOrder, rhsType);

            if (lhsPrecedence == -1 || rhsPrecedence == -1)
            {
                consistent = null;
                return false;
            }

            var targetType = _precendenceOrder[Math.Min(lhsPrecedence, rhsPrecedence)];
            SqlDataTypeReference fullTargetType;

            if (targetType.IsStringType())
            {
                var lhsColl = lhs as SqlDataTypeReferenceWithCollation;
                var rhsColl = rhs as SqlDataTypeReferenceWithCollation;

                if (lhsColl != null && rhsColl != null)
                {
                    if (!SqlDataTypeReferenceWithCollation.TryConvertCollation(lhsSql, rhsSql, fragment, operation, out var coll, out var collLabel, out var collError))
                    {
                        consistent = null;
                        return false;
                    }

                    fullTargetType = new SqlDataTypeReferenceWithCollation
                    {
                        SqlDataTypeOption = targetType,
                        Collation = coll,
                        CollationLabel = collLabel,
                        CollationConflictError = collError
                    };
                }
                else
                {
                    fullTargetType = new SqlDataTypeReferenceWithCollation
                    {
                        SqlDataTypeOption = targetType,
                        Collation = primaryDataSource?.DefaultCollation ?? Collation.USEnglish,
                        CollationLabel = CollationLabel.CoercibleDefault
                    };
                }
            }
            else
            {
                fullTargetType = new SqlDataTypeReference { SqlDataTypeOption = targetType };
            }

            // If we're converting to a type that uses a length, choose the longest length
            if (targetType == SqlDataTypeOption.Binary || targetType == SqlDataTypeOption.VarBinary ||
                targetType == SqlDataTypeOption.Image ||
                targetType == SqlDataTypeOption.Char || targetType == SqlDataTypeOption.VarChar ||
                targetType == SqlDataTypeOption.NChar || targetType == SqlDataTypeOption.NVarChar)
            {
                Literal length = null;

                if (lhsSql != null && lhsSql.Parameters.Count == 1)
                    length = lhsSql.Parameters[0];

                if (rhsSql != null && rhsSql.Parameters.Count == 1 && (length == null || rhsSql.Parameters[0].LiteralType == LiteralType.Max || (length.LiteralType == LiteralType.Integer && Int32.TryParse(length.Value, out var lhsLength) && Int32.TryParse(rhsSql.Parameters[0].Value, out var rhsLength) && rhsLength > lhsLength)))
                    length = rhsSql.Parameters[0];

                if (length != null)
                    fullTargetType.Parameters.Add(length);
            }

            // If we're converting to a decimal, check the precision and length
            if (targetType == SqlDataTypeOption.Decimal || targetType == SqlDataTypeOption.Numeric)
            {
                var p1 = lhs.GetPrecision();
                var s1 = lhs.GetScale();
                var p2 = rhs.GetPrecision();
                var s2 = rhs.GetScale();

                fullTargetType.Parameters.Add(new IntegerLiteral { Value = (Math.Max(s1, s2) + Math.Max(p1 - s1, p2 - s2)).ToString(CultureInfo.InvariantCulture) });
                fullTargetType.Parameters.Add(new IntegerLiteral { Value = Math.Max(s1, s2).ToString(CultureInfo.InvariantCulture) });
            }

            if (CanChangeTypeImplicit(lhs, fullTargetType) && CanChangeTypeImplicit(rhs, fullTargetType))
            {
                consistent = fullTargetType;
                return true;
            }

            consistent = null;
            return false;
        }

        /// <summary>
        /// Checks if values of one type can be converted to another type implicitly
        /// </summary>
        /// <param name="from">The type to convert from</param>
        /// <param name="to">The type to convert to</param>
        /// <returns><c>true</c> if the types can be converted implicitly, or <c>false</c> otherwise</returns>
        public static bool CanChangeTypeImplicit(DataTypeReference from, DataTypeReference to)
        {
            if (from.IsSameAs(to))
                return true;

            if (from == DataTypeHelpers.ImplicitIntForNullLiteral)
                return true;

            var fromUser = from as UserDataTypeReference;
            var fromSql = from as SqlDataTypeReference;
            var fromXml = from as XmlDataTypeReference;
            var toUser = to as UserDataTypeReference;
            var toSql = to as SqlDataTypeReference;
            var toXml = to as XmlDataTypeReference;

            // Check user-defined types are identical
            if (fromUser != null && toUser != null)
            {
                // We can convert a typed entity reference to an untyped one and vice versa,
                // but not between different types.
                if (fromUser.Name.BaseIdentifier.Value.Equals(nameof(DataTypeHelpers.EntityReference), StringComparison.OrdinalIgnoreCase) && toUser.Name.BaseIdentifier.Value.Equals(nameof(DataTypeHelpers.EntityReference), StringComparison.OrdinalIgnoreCase))
                {
                    if (fromUser.Parameters.Count == 0 || toUser.Parameters.Count == 0)
                        return true;

                    if (fromUser.Parameters.Count == 1 && toUser.Parameters.Count == 1)
                        return fromUser.Parameters[0].Value.Equals(toUser.Parameters[0].Value, StringComparison.OrdinalIgnoreCase);

                    return false;
                }

                return String.Join(".", fromUser.Name.Identifiers.Select(i => i.Value)).Equals(String.Join(".", toUser.Name.Identifiers.Select(i => i.Value)), StringComparison.OrdinalIgnoreCase);
            }

            // If one or other type is a user-defined type, check it is a known type (SqlEntityReference)
            if (fromUser != null && !fromUser.IsEntityReference())
                return false;

            // Guids and strings can be converted to typed entity references
            if (toUser != null && toUser.IsEntityReference() && toUser.Parameters.Count == 1)
                return fromSql?.SqlDataTypeOption == SqlDataTypeOption.UniqueIdentifier || fromSql?.SqlDataTypeOption.IsStringType() == true;

            // Nothing else can be converted to SqlEntityReference
            if (toUser != null)
                return false;

            // Xml can't be implicitly converted to anything
            if (fromXml != null)
                return false;

            // Get the basic type. Substitute SqlEntityReference with uniqueidentifier
            var fromType = fromSql?.SqlDataTypeOption ?? SqlDataTypeOption.UniqueIdentifier;

            // Only strings and binary types can be converted to Xml
            if (toXml != null)
                return fromType.IsStringType() || fromType == SqlDataTypeOption.Binary || fromType == SqlDataTypeOption.VarBinary;

            var toType = toSql.SqlDataTypeOption;

            // Any numeric type can be implicitly converted to any other.
            if (fromType.IsNumeric() && toType.IsNumeric())
                return true;

            if (Array.IndexOf(_precendenceOrder, fromType) == -1 ||
                Array.IndexOf(_precendenceOrder, toType) == -1)
                return false;

            // Anything can be converted to/from strings (except converting string -> entity reference)
            if (fromType.IsStringType() || toType.IsStringType())
                return true;

            // [var]binary can be converted to anything except a few selected types
            if ((fromType == SqlDataTypeOption.Binary || fromType == SqlDataTypeOption.VarBinary) &&
                (toType != SqlDataTypeOption.Date &&
                toType != SqlDataTypeOption.Time &&
                toType != SqlDataTypeOption.DateTimeOffset &&
                toType != SqlDataTypeOption.DateTime2 &&
                toType != SqlDataTypeOption.Float &&
                toType != SqlDataTypeOption.Real &&
                toType != SqlDataTypeOption.NText &&
                toType != SqlDataTypeOption.Text))
                return true;

            // Any numeric type can be implicitly converted to datetime
            if (fromType.IsNumeric() && (toType == SqlDataTypeOption.DateTime || toType == SqlDataTypeOption.SmallDateTime))
                return true;

            // datetime can be converted implicitly to other datetime and date/time types
            if (fromType.IsDateTimeType() && (toType.IsDateTimeType() || toType == SqlDataTypeOption.Date || toType == SqlDataTypeOption.Time))
                return true;

            // date/time can be converted to datetime
            if ((fromType == SqlDataTypeOption.Date || fromType == SqlDataTypeOption.Time) && toType.IsDateTimeType())
                return true;

            // Entity reference can be converted to guid
            if (fromType == SqlDataTypeOption.UniqueIdentifier && fromUser != null && toType == SqlDataTypeOption.UniqueIdentifier)
                return true;

            // Anything can be converted to sql_variant except timestamp, image, text, ntext, xml and hierarchyid
            if (toType == SqlDataTypeOption.Sql_Variant &&
                !fromType.IsRowVersion() &&
                fromType != SqlDataTypeOption.Image &&
                fromType != SqlDataTypeOption.Text &&
                fromType != SqlDataTypeOption.NText)
                return true;

            // binary, decimal and integer types can be converted to timestamp/rowversion
            if (toType.IsRowVersion() &&
                (fromType == SqlDataTypeOption.Binary ||
                fromType == SqlDataTypeOption.VarBinary ||
                fromType.IsExactNumeric() ||
                fromType == SqlDataTypeOption.Bit))
                return true;

            // [var]binary, numeric, bit, timestamp and uniqueidentifier can be converted to [var]binary
            if ((toType == SqlDataTypeOption.Binary || toType == SqlDataTypeOption.VarBinary) &&
                (fromType == SqlDataTypeOption.Binary ||
                fromType == SqlDataTypeOption.VarBinary ||
                fromType.IsNumeric() ||
                fromType == SqlDataTypeOption.Bit ||
                fromType.IsRowVersion() ||
                fromType == SqlDataTypeOption.UniqueIdentifier))
                return true;

            // timestamp can be converted to [var]binary, [var]char, [small]datetime, exact numerics, bit and image types
            if (fromType.IsRowVersion() &&
                (toType == SqlDataTypeOption.Binary ||
                toType == SqlDataTypeOption.VarBinary ||
                toType == SqlDataTypeOption.Char ||
                toType == SqlDataTypeOption.VarChar ||
                toType == SqlDataTypeOption.SmallDateTime ||
                toType == SqlDataTypeOption.DateTime ||
                toType.IsExactNumeric() ||
                toType == SqlDataTypeOption.Bit ||
                toType == SqlDataTypeOption.Image))
                return true;

            return false;
        }

        /// <summary>
        /// Checks if values of one type can be converted to another type explicitly
        /// </summary>
        /// <param name="from">The type to convert from</param>
        /// <param name="to">The type to convert to</param>
        /// <returns><c>true</c> if the types can be converted explicitly, or <c>false</c> otherwise</returns>
        public static bool CanChangeTypeExplicit(DataTypeReference from, DataTypeReference to)
        {
            if (CanChangeTypeImplicit(from, to))
                return true;

            var fromSql = from as SqlDataTypeReference;
            var fromXml = from as XmlDataTypeReference;
            var toSql = to as SqlDataTypeReference;

            // Require explicit conversion from datetime to numeric types
            if ((fromSql?.SqlDataTypeOption == SqlDataTypeOption.DateTime || fromSql?.SqlDataTypeOption == SqlDataTypeOption.SmallDateTime) &&
                toSql?.SqlDataTypeOption.IsNumeric() == true)
                return true;

            // Require explicit conversion from xml to string/binary types
            if (fromXml != null && toSql != null && (toSql.SqlDataTypeOption.IsStringType() || toSql.SqlDataTypeOption == SqlDataTypeOption.Binary || toSql.SqlDataTypeOption == SqlDataTypeOption.VarBinary))
                return true;

            // Require explicit conversion from sql_variant to everything other than timestamp, image, text, ntext, xml and hierarchyid
            if (fromSql?.SqlDataTypeOption == SqlDataTypeOption.Sql_Variant &&
                toSql != null &&
                !toSql.SqlDataTypeOption.IsRowVersion() &&
                toSql.SqlDataTypeOption != SqlDataTypeOption.Image &&
                toSql.SqlDataTypeOption != SqlDataTypeOption.Text &&
                toSql.SqlDataTypeOption != SqlDataTypeOption.NText)
                return true;

            // Require explicit version from string and datetime types to [var]binary
            if ((fromSql?.SqlDataTypeOption.IsStringType() == true || fromSql?.SqlDataTypeOption.IsDateTimeType() == true) &&
                (toSql?.SqlDataTypeOption == SqlDataTypeOption.Binary || toSql?.SqlDataTypeOption == SqlDataTypeOption.VarBinary))
                return true;

            // Require explicit conversion from [var]binary to modern datetime types
            if ((fromSql?.SqlDataTypeOption == SqlDataTypeOption.Binary || fromSql?.SqlDataTypeOption == SqlDataTypeOption.VarBinary) &&
                (toSql?.SqlDataTypeOption == SqlDataTypeOption.Date || toSql?.SqlDataTypeOption == SqlDataTypeOption.Time || toSql?.SqlDataTypeOption == SqlDataTypeOption.DateTimeOffset || toSql?.SqlDataTypeOption == SqlDataTypeOption.DateTime2))
                return true;

            return false;
        }

        /// <summary>
        /// Produces the required expression to convert values to a specific type
        /// </summary>
        /// <param name="expr">The expression that generates the values to convert</param>
        /// <param name="context">The expression which contains the <see cref="ExpressionExecutionContext"/> the expression will be evaluated in</param>
        /// <param name="to">The type to convert to</param>
        /// <returns>An expression to generate values of the required type</returns>
        public static Expression Convert(Expression expr, Expression context, Type to)
        {
            if ((expr.Type == typeof(SqlDateTime) || expr.Type == typeof(SqlSmallDateTime)) && (to == typeof(SqlBoolean) || to == typeof(SqlByte) || to == typeof(SqlInt16) || to == typeof(SqlInt32) || to == typeof(SqlInt64) || to == typeof(SqlDecimal) || to == typeof(SqlSingle) || to == typeof(SqlDouble)))
            {
                // Conversion from datetime types to numeric types uses the number of days since 1900-01-01
                Expression converted;

                if (expr.Type == typeof(SqlDateTime))
                    converted = Expression.Convert(expr, typeof(DateTime));
                else
                    converted = Expression.PropertyOrField(expr, nameof(SqlSmallDateTime.Value));

                converted = Expression.PropertyOrField(
                    Expression.Subtract(
                        converted,
                        Expression.Constant(new DateTime(1900, 1, 1))
                    ),
                    nameof(TimeSpan.TotalDays)
                );

                // Round the value when converting from datetime to an integer type, rather than
                // using the standard double -> int truncation
                if (to == typeof(SqlBoolean) || to == typeof(SqlByte) || to == typeof(SqlInt16) || to == typeof(SqlInt32) || to == typeof(SqlInt64))
                {
                    converted = Expr.Call(() => Math.Round(Expr.Arg<double>()), converted);
                }

                // Convert the result to a SqlDouble and handle null values
                expr = Expression.Condition(
                    Expression.PropertyOrField(expr, nameof(SqlDateTime.IsNull)),
                    Expression.Constant(SqlDouble.Null),
                    Expression.Convert(converted, typeof(SqlDouble)
                    )
                );
            }

            if ((expr.Type == typeof(SqlBoolean) || expr.Type == typeof(SqlByte) || expr.Type == typeof(SqlInt16) || expr.Type == typeof(SqlInt32) || expr.Type == typeof(SqlInt64) || expr.Type == typeof(SqlDecimal) || expr.Type == typeof(SqlSingle) || expr.Type == typeof(SqlDouble)) && (to == typeof(SqlDateTime) || to == typeof(SqlSmallDateTime)))
            {
                expr = Expression.Condition(
                    NullCheck(expr),
                    Expression.Constant(SqlDateTime.Null),
                    Expression.Convert(
                        Expression.Call(
                            Expression.New(
                                typeof(DateTime).GetConstructor(new[] { typeof(int), typeof(int), typeof(int) }),
                                Expression.Constant(1900),
                                Expression.Constant(1),
                                Expression.Constant(1)
                            ),
                            typeof(DateTime).GetMethod(nameof(DateTime.MinValue.AddDays)),
                            Expression.Convert(
                                Expression.Convert(expr, typeof(SqlDouble)),
                                typeof(double)
                            )
                        ),
                        typeof(SqlDateTime)
                    )
                );
            }

            if (expr.Type == typeof(SqlEntityReference) && to == typeof(EntityCollection))
                expr = Expr.Call(() => CreateEntityCollection(Expr.Arg<SqlEntityReference>()), expr);

            if (expr.Type == typeof(SqlString) && to == typeof(EntityCollection))
                expr = Expr.Call(() => ParseEntityCollection(Expr.Arg<SqlString>()), expr);

            if (expr.Type == typeof(SqlString) && to == typeof(OptionSetValueCollection))
                expr = Expr.Call(() => ParseOptionSetValueCollection(Expr.Arg<SqlString>()), expr);

            if (expr.Type == typeof(SqlString) && to == typeof(SqlXml))
                expr = Expr.Call(() => ParseXml(Expr.Arg<SqlString>()), expr);

            if (expr.Type == typeof(SqlString) && to == typeof(SqlTime))
                expr = Expr.Call(() => ParseTime(Expr.Arg<SqlString>(), Expr.Arg<ExpressionExecutionContext>()), expr, context);

            if (expr.Type == typeof(SqlString) && to == typeof(SqlDate))
                expr = Expr.Call(() => ParseDate(Expr.Arg<SqlString>(), Expr.Arg<ExpressionExecutionContext>()), expr, context);

            if (expr.Type == typeof(SqlString) && (to == typeof(SqlDateTime) || to == typeof(SqlSmallDateTime)))
                expr = Expr.Call(() => ParseDateTime(Expr.Arg<SqlString>(), Expr.Arg<ExpressionExecutionContext>()), expr, context);

            if (expr.Type == typeof(SqlString) && to == typeof(SqlDateTime2))
                expr = Expr.Call(() => ParseDateTime2(Expr.Arg<SqlString>(), Expr.Arg<ExpressionExecutionContext>()), expr, context);

            if (expr.Type == typeof(SqlString) && to == typeof(SqlDateTimeOffset))
                expr = Expr.Call(() => ParseDateTimeOffset(Expr.Arg<SqlString>(), Expr.Arg<ExpressionExecutionContext>()), expr, context);

            if (expr.Type == typeof(SqlBinary) && to == typeof(SqlInt64))
                expr = Expr.Call(() => ParseBinaryInt64(Expr.Arg<SqlBinary>()), expr);
            else if (expr.Type == typeof(SqlInt64) && to == typeof(SqlBinary))
                expr = Expr.Call(() => SerializeBinaryInt64(Expr.Arg<SqlInt64>()), expr);

            if (expr.Type == typeof(SqlBinary) && to == typeof(SqlInt32))
                expr = Expr.Call(() => ParseBinaryInt32(Expr.Arg<SqlBinary>()), expr);
            else if (expr.Type == typeof(SqlInt32) && to == typeof(SqlBinary))
                expr = Expr.Call(() => SerializeBinaryInt32(Expr.Arg<SqlInt32>()), expr);

            if (expr.Type == typeof(SqlBinary) && to == typeof(SqlInt16))
                expr = Expr.Call(() => ParseBinaryInt16(Expr.Arg<SqlBinary>()), expr);
            else if (expr.Type == typeof(SqlInt16) && to == typeof(SqlBinary))
                expr = Expr.Call(() => SerializeBinaryInt16(Expr.Arg<SqlInt16>()), expr);

            if (expr.Type == typeof(SqlBinary) && to == typeof(SqlByte))
                expr = Expr.Call(() => ParseBinaryByte(Expr.Arg<SqlBinary>()), expr);
            else if (expr.Type == typeof(SqlInt16) && to == typeof(SqlBinary))
                expr = Expr.Call(() => SerializeBinaryByte(Expr.Arg<SqlByte>()), expr);

            if (expr.Type == typeof(SqlBinary) && to == typeof(SqlMoney))
                expr = Expr.Call(() => ParseBinaryMoney(Expr.Arg<SqlBinary>()), expr);
            else if (expr.Type == typeof(SqlMoney) && to == typeof(SqlBinary))
                expr = Expr.Call(() => SerializeBinaryMoney(Expr.Arg<SqlMoney>()), expr);

            if (expr.Type == typeof(SqlBinary) && to == typeof(SqlBoolean))
                expr = Expr.Call(() => ParseBinaryBit(Expr.Arg<SqlBinary>()), expr);
            else if (expr.Type == typeof(SqlBoolean) && to == typeof(SqlBinary))
                expr = Expr.Call(() => SerializeBinaryBit(Expr.Arg<SqlBoolean>()), expr);

            if (expr.Type == typeof(SqlBinary) && to == typeof(SqlDecimal))
                expr = Expr.Call(() => ParseBinaryDecimal(Expr.Arg<SqlBinary>()), expr);
            else if (expr.Type == typeof(SqlDecimal) && to == typeof(SqlBinary))
                expr = Expr.Call(() => SerializeBinaryDecimal(Expr.Arg<SqlDecimal>()), expr);

            if (expr.Type == typeof(SqlBinary) && to == typeof(SqlDateTime))
                expr = Expr.Call(() => ParseBinaryDateTime(Expr.Arg<SqlBinary>()), expr);
            else if (expr.Type == typeof(SqlDateTime) && to == typeof(SqlBinary))
                expr = Expr.Call(() => SerializeBinaryDateTime(Expr.Arg<SqlDateTime>()), expr);

            if (expr.Type == typeof(SqlBinary) && to == typeof(SqlSmallDateTime))
                expr = Expr.Call(() => ParseBinarySmallDateTime(Expr.Arg<SqlBinary>()), expr);
            else if (expr.Type == typeof(SqlSmallDateTime) && to == typeof(SqlBinary))
                expr = Expr.Call(() => SerializeBinarySmallDateTime(Expr.Arg<SqlSmallDateTime>()), expr);

            if (expr.Type == typeof(SqlBinary) && to == typeof(SqlDate))
                expr = Expr.Call(() => ParseBinaryDate(Expr.Arg<SqlBinary>()), expr);
            else if (expr.Type == typeof(SqlDate) && to == typeof(SqlBinary))
                expr = Expr.Call(() => SerializeBinaryDate(Expr.Arg<SqlDate>()), expr);

            if (expr.Type == typeof(SqlBinary) && to == typeof(SqlTime))
                expr = Expr.Call(() => ParseBinaryTime(Expr.Arg<SqlBinary>()), expr);
            else if (expr.Type == typeof(SqlTime) && to == typeof(SqlBinary))
                expr = Expr.Call(() => SerializeBinaryTime(Expr.Arg<SqlTime>()), expr);

            if (expr.Type == typeof(SqlBinary) && to == typeof(SqlDateTime2))
                expr = Expr.Call(() => ParseBinaryDateTime2(Expr.Arg<SqlBinary>()), expr);
            else if (expr.Type == typeof(SqlDateTime2) && to == typeof(SqlBinary))
                expr = Expr.Call(() => SerializeBinaryDateTime2(Expr.Arg<SqlDateTime2>()), expr);

            if (expr.Type == typeof(SqlBinary) && to == typeof(SqlDateTimeOffset))
                expr = Expr.Call(() => ParseBinaryDateTimeOffset(Expr.Arg<SqlBinary>()), expr);
            else if (expr.Type == typeof(SqlDateTimeOffset) && to == typeof(SqlBinary))
                expr = Expr.Call(() => SerializeBinaryDateTimeOffset(Expr.Arg<SqlDateTimeOffset>()), expr);

            if (expr.Type == typeof(SqlDecimal) && (to == typeof(SqlInt64) || to == typeof(SqlInt32) || to == typeof(SqlInt16) || to == typeof(SqlByte)))
            {
                // Built-in conversion uses rounding, should use truncation
                // https://learn.microsoft.com/en-us/sql/t-sql/functions/cast-and-convert-transact-sql?view=sql-server-ver16#truncating-and-rounding-results
                expr = Expr.Call(() => SqlDecimal.Truncate(Expr.Arg<SqlDecimal>(), Expr.Arg<int>()), expr, Expression.Constant(0));
            }

            if (expr.Type != to)
            {
                if (expr.Type == typeof(object) && expr is ConstantExpression constant && constant.Value == null && typeof(INullable).IsAssignableFrom(to))
                    return Expression.Constant(GetNullValue(to));

                if (_sqlToNetTypeConversions.TryGetValue(expr.Type, out var sqlToNetType) &&
                    sqlToNetType == to)
                {
                    var func = _sqlToNetTypeConversionFuncs[expr.Type];
                    expr = Expression.Invoke(Expression.Constant(func), Expression.Convert(expr, typeof(INullable)));
                }
                
                expr = Expression.Convert(expr, to);

                //if (to == typeof(SqlString))
                //    expr = Expr.Call(() => ApplyCollation(Expr.Arg<Collation>(), Expr.Arg<SqlString>()), Expression.Constant(collation), expr);
            }

            return expr;
        }

        private static SqlInt64 ParseBinaryInt64(SqlBinary value)
        {
            if (value.IsNull)
                return SqlInt64.Null;

            var bytes = value.Value;

            if (bytes.Length < 8)
            {
                var enlarged = new byte[8];
                bytes.CopyTo(enlarged, 8 - bytes.Length);
                bytes = enlarged;
            }

            return BinaryPrimitives.ReadInt64BigEndian(bytes.AsSpan());
        }

        private static SqlBinary SerializeBinaryInt64(SqlInt64 value)
        {
            if (value.IsNull)
                return SqlBinary.Null;

            var bytes = new byte[8];
            BinaryPrimitives.WriteInt64BigEndian(bytes.AsSpan(), value.Value);

            return bytes;
        }

        private static SqlInt32 ParseBinaryInt32(SqlBinary value)
        {
            if (value.IsNull)
                return SqlInt32.Null;

            var bytes = value.Value;

            if (bytes.Length < 4)
            {
                var enlarged = new byte[4];
                bytes.CopyTo(enlarged, 4 - bytes.Length);
                bytes = enlarged;
            }

            return BinaryPrimitives.ReadInt32BigEndian(bytes.AsSpan());
        }

        private static SqlBinary SerializeBinaryInt32(SqlInt32 value)
        {
            if (value.IsNull)
                return SqlBinary.Null;

            var bytes = new byte[4];
            BinaryPrimitives.WriteInt32BigEndian(bytes.AsSpan(), value.Value);

            return bytes;
        }

        private static SqlInt32 ParseBinaryInt16(SqlBinary value)
        {
            if (value.IsNull)
                return SqlInt16.Null;

            var bytes = value.Value;

            if (bytes.Length < 2)
            {
                var enlarged = new byte[2];
                bytes.CopyTo(enlarged, 2 - bytes.Length);
                bytes = enlarged;
            }

            return BinaryPrimitives.ReadInt16BigEndian(bytes.AsSpan());
        }

        private static SqlBinary SerializeBinaryInt16(SqlInt16 value)
        {
            if (value.IsNull)
                return SqlBinary.Null;

            var bytes = new byte[2];
            BinaryPrimitives.WriteInt16BigEndian(bytes.AsSpan(), value.Value);

            return bytes;
        }

        private static SqlByte ParseBinaryByte(SqlBinary value)
        {
            if (value.IsNull)
                return SqlByte.Null;

            var bytes = value.Value;

            if (bytes.Length < 1)
                return SqlByte.Zero;

            return bytes[0];
        }

        private static SqlBinary SerializeBinaryByte(SqlByte value)
        {
            if (value.IsNull)
                return SqlBinary.Null;

            var bytes = new byte[] { value.Value };

            return bytes;
        }

        private static SqlMoney ParseBinaryMoney(SqlBinary value)
        {
            if (value.IsNull)
                return SqlMoney.Null;

            var bytes = value.Value;

            if (bytes.Length < 8)
            {
                var enlarged = new byte[8];
                bytes.CopyTo(enlarged, 8 - bytes.Length);
                bytes = enlarged;
            }

            // SQL Server stores money8 as ticks of 1/10000.
            // Could use SqlMoney.FromTdsValue but not supported in Net462
            return BinaryPrimitives.ReadInt64BigEndian(bytes.AsSpan()) / 10000M;
        }

        private static SqlBinary SerializeBinaryMoney(SqlMoney value)
        {
            if (value.IsNull)
                return SqlBinary.Null;

            var bytes = new byte[8];
            BinaryPrimitives.WriteInt64BigEndian(bytes.AsSpan(), (long)(value.Value * 10000M));

            return bytes;
        }

        private static SqlMoney ParseBinarySmallMoney(SqlBinary sqlBinary)
        {
            if (sqlBinary.IsNull)
                return SqlMoney.Null;

            var bytes = sqlBinary.Value;

            if (bytes.Length < 4)
            {
                var enlarged = new byte[4];
                bytes.CopyTo(enlarged, 4 - bytes.Length);
                bytes = enlarged;
            }

            // https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/1266679d-cd6e-492a-b2b2-3a9ba004196d
            return BinaryPrimitives.ReadInt32BigEndian(bytes.AsSpan()) / 10000M;
        }

        private static SqlBinary SerializeBinarySmallMoney(SqlMoney value)
        {
            if (value.IsNull)
                return SqlBinary.Null;

            var bytes = new byte[4];
            BinaryPrimitives.WriteInt32BigEndian(bytes.AsSpan(), (int)(value.Value * 10000M));

            return bytes;
        }

        private static SqlBoolean ParseBinaryBit(SqlBinary value)
        {
            if (value.IsNull)
                return SqlBoolean.Null;

            var bytes = value.Value;

            if (bytes.Length < 1)
                return SqlBoolean.False;

            return bytes[0] != 0;
        }

        private static SqlBinary SerializeBinaryBit(SqlBoolean value)
        {
            if (value.IsNull)
                return SqlBinary.Null;

            var bytes = new byte[] { (byte)(value.Value ? 1 : 0) };

            return bytes;
        }

        private static SqlDecimal ParseBinaryDecimal(SqlBinary value)
        {
            if (value.IsNull)
                return SqlDecimal.Null;

            var bytes = value.Value;

            if (bytes.Length < 20)
            {
                var enlarged = new byte[20];
                bytes.CopyTo(enlarged, 0);
                bytes = enlarged;
            }

            // precision
            // scale
            // unused?
            // ispositive
            // int[4]
            var offset = 0;
            var precision = bytes[offset++];
            var scale = bytes[offset++];
            _ = bytes[offset++];
            var positive = bytes[offset++];
            var data1 = BitConverter.ToInt32(bytes, offset); offset += 4;
            var data2 = BitConverter.ToInt32(bytes, offset); offset += 4;
            var data3 = BitConverter.ToInt32(bytes, offset); offset += 4;
            var data4 = BitConverter.ToInt32(bytes, offset);
            return new SqlDecimal(precision, scale, positive != 0, data1, data2, data3, data4);
        }

        private static SqlBinary SerializeBinaryDecimal(SqlDecimal value)
        {
            if (value.IsNull)
                return SqlBinary.Null;

            var bytes = new byte[20];
            var offset = 0;
            bytes[offset++] = value.Precision;
            bytes[offset++] = value.Scale;
            bytes[offset++] = 0;
            bytes[offset++] = (byte)(value.IsPositive ? 1 : 0);
            Array.Copy(value.BinData, 0, bytes, offset, 16);

            return bytes;
        }

        private static SqlDateTime ParseBinaryDateTime(SqlBinary value)
        {
            if (value.IsNull)
                return SqlDateTime.Null;

            var bytes = value.Value;

            if (bytes.Length < 8)
            {
                var enlarged = new byte[8];
                bytes.CopyTo(enlarged, 8 - bytes.Length);
                bytes = enlarged;
            }

            // days (int)
            // time (uint)
            // https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/786f5b8a-f87d-4980-9070-b9b7274c681d
            var span = bytes.AsSpan();
            var days = BinaryPrimitives.ReadInt32BigEndian(span); span = span.Slice(4);
            var time = BinaryPrimitives.ReadInt32BigEndian(span);
            return new SqlDateTime(days, time);
        }

        private static SqlBinary SerializeBinaryDateTime(SqlDateTime value)
        {
            if (value.IsNull)
                return SqlBinary.Null;

            var bytes = new byte[8];
            var span = bytes.AsSpan();
            BinaryPrimitives.WriteInt32BigEndian(span, value.DayTicks); span = span.Slice(4);
            BinaryPrimitives.WriteInt32BigEndian(span, value.TimeTicks);

            return bytes;
        }

        private static SqlDateTime ParseBinarySmallDateTime(SqlBinary value)
        {
            if (value.IsNull)
                return SqlDateTime.Null;

            var bytes = value.Value;

            if (bytes.Length < 4)
            {
                var enlarged = new byte[4];
                bytes.CopyTo(enlarged, 4 - bytes.Length);
                bytes = enlarged;
            }

            // days (short)
            // time (ushort)
            // https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/786f5b8a-f87d-4980-9070-b9b7274c681d
            var span = bytes.AsSpan();
            var days = BinaryPrimitives.ReadInt16BigEndian(span); span = span.Slice(2);
            var time = BinaryPrimitives.ReadInt16BigEndian(span);
            return new SqlDateTime(days, time);
        }

        private static SqlBinary SerializeBinarySmallDateTime(SqlSmallDateTime value)
        {
            if (value.IsNull)
                return SqlBinary.Null;

            var bytes = new byte[4];
            var span = bytes.AsSpan();
            BinaryPrimitives.WriteInt16BigEndian(span, value.DayTicks); span = span.Slice(4);
            BinaryPrimitives.WriteInt16BigEndian(span, value.TimeTicks);

            return bytes;
        }

        private static SqlDate ParseBinaryDate(SqlBinary value)
        {
            if (value.IsNull)
                return SqlDate.Null;

            var bytes = value.Value;
            return ParseBinaryDate(bytes.AsSpan());
        }

        private static SqlDate ParseBinaryDate(Span<byte> span)
        {
            // date uses a 3-byte integer - expand it to 4 bytes so we can reuse the standard ReadInt32 method
            if (span.Length < 4)
            {
                var enlarged = new byte[4];
                span.CopyTo(enlarged.AsSpan());
                span = enlarged.AsSpan();
            }

            var days = BinaryPrimitives.ReadInt32LittleEndian(span);
            return DateTime.MinValue.AddDays(days);
        }

        private static SqlBinary SerializeBinaryDate(SqlDate value)
        {
            if (value.IsNull)
                return SqlBinary.Null;

            var bytes = new byte[3];
            SerializeBinaryDate(bytes.AsSpan(), value.Value);

            return bytes;
        }

        private static void SerializeBinaryDate(Span<byte> span, DateTime value)
        {
            var days = (int)(value - DateTime.MinValue).TotalDays;
            span[0] = (byte)(days & 0xff);
            days >>= 8;
            span[1] = (byte)(days & 0xff);
            days >>= 8;
            span[2] = (byte)(days & 0xff);
        }

        private static SqlTime ParseBinaryTime(SqlBinary value)
        {
            if (value.IsNull)
                return SqlTime.Null;

            var bytes = value.Value;
            return ParseBinaryTime(bytes.AsSpan(), out _);
        }

        private static SqlTime ParseBinaryTime(Span<byte> span, out int read)
        {
            // first byte is for scale
            var scale = span[0];

            if (scale <= 2)
                read = 4;
            else if (scale <= 4)
                read = 5;
            else if (scale <= 7)
                read = 6;
            else
                throw new OverflowException();

            if (span.Length < read)
                throw new OverflowException();

            span = span.Slice(1, read - 1);

            // Expand it to 8 bytes so we can reuse the standard ReadInt64 method
            var enlarged = new byte[8];
            span.CopyTo(enlarged.AsSpan());
            span = enlarged.AsSpan();

            var ticks = BinaryPrimitives.ReadInt64LittleEndian(span);
            ticks = ticks * (long)Math.Pow(10, 7 - scale);
            return TimeSpan.FromTicks(ticks);
        }

        private static SqlBinary SerializeBinaryTime(SqlTime value)
        {
            if (value.IsNull)
                return SqlBinary.Null;

            // Variable length depending on scale
            var length = GetTimeBinarySize(value.Scale);

            var bytes = new byte[length];
            SerializeBinaryTime(bytes.AsSpan(), value.Scale, value.Value);

            return bytes;
        }

        private static int GetTimeBinarySize(short scale)
        {
            var length = 4;

            if (scale >= 3)
                length++;

            if (scale >= 4)
                length++;

            return length;
        }

        private static void SerializeBinaryTime(Span<byte> span, short scale, TimeSpan value)
        {
            span[0] = (byte)scale;

            var ticks = value.Ticks / (long)Math.Pow(10, 7 - scale);

            for (var offset = 1; offset < span.Length; offset++)
            {
                span[offset] = (byte)(ticks & 0xff);
                ticks >>= 8;
            }
        }

        private static SqlDateTime2 ParseBinaryDateTime2(SqlBinary value)
        {
            if (value.IsNull)
                return SqlDateTime2.Null;

            var bytes = value.Value;
            var span = bytes.AsSpan();
            var time = ParseBinaryTime(span, out var read);
            span = span.Slice(read);
            var date = ParseBinaryDate(span);

            return date.Value.Add(time.Value);
        }

        private static SqlBinary SerializeBinaryDateTime2(SqlDateTime2 value)
        {
            if (value.IsNull)
                return SqlBinary.Null;

            var timeLength = GetTimeBinarySize(value.Scale);
            var dateLength = 3;
            var bytes = new byte[timeLength + dateLength];
            var span = bytes.AsSpan();
            SerializeBinaryTime(span.Slice(0, timeLength), value.Scale, value.Value.TimeOfDay);
            SerializeBinaryDate(span.Slice(timeLength, dateLength), value.Value);

            return bytes;
        }

        private static SqlDateTimeOffset ParseBinaryDateTimeOffset(SqlBinary value)
        {
            if (value.IsNull)
                return SqlDateTimeOffset.Null;

            var bytes = value.Value;
            var span = bytes.AsSpan();
            var time = ParseBinaryTime(span, out var read);
            span = span.Slice(read);
            var date = ParseBinaryDate(span);
            span = span.Slice(3);
            var offset = BinaryPrimitives.ReadInt16BigEndian(span);

            return new DateTimeOffset(date.Value.Add(time.Value), TimeSpan.FromMinutes(offset));
        }

        private static SqlBinary SerializeBinaryDateTimeOffset(SqlDateTimeOffset value)
        {
            if (value.IsNull)
                return SqlBinary.Null;

            var timeLength = GetTimeBinarySize(value.Scale);
            var dateLength = 3;
            var offsetLength = 2;
            var bytes = new byte[timeLength + dateLength + offsetLength];
            var span = bytes.AsSpan();
            SerializeBinaryTime(span.Slice(0, timeLength), value.Scale, value.Value.TimeOfDay);
            SerializeBinaryDate(span.Slice(timeLength, dateLength), value.Value.Date);
            BinaryPrimitives.WriteInt16BigEndian(span.Slice(timeLength + dateLength, offsetLength), (short)value.Value.Offset.TotalMinutes);

            return bytes;
        }

        private static SqlTime ParseTime(SqlString value, ExpressionExecutionContext context)
        {
            if (!SqlDateParsing.TryParse(value.Value, context.Session.DateFormat, out SqlTime time))
                throw new QueryExecutionException(Sql4CdsError.DateTimeParseError(null));

            return time;
        }

        private static SqlDate ParseDate(SqlString value, ExpressionExecutionContext context)
        {
            if (!SqlDateParsing.TryParse(value.Value, context.Session.DateFormat, out SqlDate date))
                throw new QueryExecutionException(Sql4CdsError.DateTimeParseError(null));

            return date;
        }

        private static SqlDateTime ParseDateTime(SqlString value, ExpressionExecutionContext context)
        {
            if (!SqlDateParsing.TryParse(value.Value, context.Session.DateFormat, out SqlDateTime dateTime))
                throw new QueryExecutionException(Sql4CdsError.DateTimeParseError(null));

            return dateTime;
        }

        private static SqlDateTime2 ParseDateTime2(SqlString value, ExpressionExecutionContext context)
        {
            if (!SqlDateParsing.TryParse(value.Value, context.Session.DateFormat, out SqlDateTime2 dateTime2))
                throw new QueryExecutionException(Sql4CdsError.DateTimeParseError(null));

            return dateTime2;
        }

        private static SqlDateTimeOffset ParseDateTimeOffset(SqlString value, ExpressionExecutionContext context)
        {
            if (!SqlDateParsing.TryParse(value.Value, context.Session.DateFormat, out SqlDateTimeOffset dateTimeOffset))
                throw new QueryExecutionException(Sql4CdsError.DateTimeParseError(null));

            return dateTimeOffset;
        }

        private static SqlString ApplyCollation(Collation collation, SqlString sqlString)
        {
            if (sqlString.IsNull)
                return sqlString;

            return collation.ToSqlString(sqlString.Value);
        }

        /// <summary>
        /// Produces the required expression to convert values to a specific type
        /// </summary>
        /// <param name="expr">The expression that generates the values to convert</param>
        /// <param name="context">The expression that provides the current <see cref="ExpressionExecutionContext"/> when the expression is evaluated</param>
        /// <param name="from">The type to convert from</param>
        /// <param name="to">The type to convert to</param>
        /// <param name="style">An optional parameter defining the style of the conversion</param>
        /// <param name="styleType">An optional parameter defining the type of the <paramref name="style"/> expression</param>
        /// <param name="convert">An optional parameter containing the SQL CONVERT() function call to report any errors against</param>
        /// <returns>An expression to generate values of the required type</returns>
        public static Expression Convert(Expression expr, Expression context, DataTypeReference from, DataTypeReference to, Expression style = null, DataTypeReference styleType = null, TSqlFragment convert = null, bool throwOnTruncate = false, string table = null, string column = null)
        {
            if (from.IsSameAs(to))
                return expr;

            if (from.IsEntityReference(out var fromEr) && to.IsEntityReference(out var toEr))
            {
                // If we're converting from untyped to typed entity reference, we need to check the type
                if (fromEr == null && toEr != null)
                    expr = Expr.Call(() => CheckEntityReferenceType(Expr.Arg<SqlEntityReference>(), Expr.Arg<string>()), expr, Expression.Constant(toEr));

                return expr;
            }

            from.ToNetType(out var fromSqlType);
            var targetType = to.ToNetType(out var toSqlType);

            if (from == DataTypeHelpers.ImplicitIntForNullLiteral)
                return Expression.Constant(_nullValues[targetType]);

            var sourceType = expr.Type;
            var originalExpr = expr;

            if (!CanChangeTypeExplicit(from, to))
                throw new NotSupportedQueryFragmentException(Sql4CdsError.ExplicitConversionNotAllowed(convert, from, to));

            // Special cases for styles
            if (style == null)
            {
                if (fromSqlType != null && (fromSqlType.SqlDataTypeOption == SqlDataTypeOption.Date || fromSqlType.SqlDataTypeOption == SqlDataTypeOption.Time || fromSqlType.SqlDataTypeOption == SqlDataTypeOption.DateTime2 || fromSqlType.SqlDataTypeOption == SqlDataTypeOption.DateTimeOffset))
                    style = Expression.Constant((SqlInt32)21);
                else
                    style = Expression.Constant((SqlInt32)0);

                styleType = DataTypeHelpers.Int;
            }

            if (!CanChangeTypeImplicit(styleType, DataTypeHelpers.Int))
                throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidArgumentType(((ConvertCall)convert).Style, styleType, 3, "convert"));

            // Special case for conversion to sql_variant
            if (to.IsSameAs(DataTypeHelpers.Variant))
            {
                var ctor = typeof(SqlVariant).GetConstructor(new[] { typeof(DataTypeReference), typeof(INullable), typeof(ExpressionExecutionContext) });
                return Expression.New(ctor, Expression.Constant(from), Expression.Convert(expr, typeof(INullable)), context);
            }

            // Special case for conversion from sql_variant
            if (from.IsSameAs(DataTypeHelpers.Variant))
            {
                return Expression.Convert(Expr.Call(() => Convert(Expr.Arg<SqlVariant>(), Expr.Arg<DataTypeReference>(), Expr.Arg<SqlInt32>(), Expr.Arg<ExpressionExecutionContext>()), expr, Expression.Constant(to), style, context), targetType);
            }

            var targetCollation = (to as SqlDataTypeReferenceWithCollation)?.Collation;

            if (fromSqlType != null && (fromSqlType.SqlDataTypeOption.IsDateTimeType() || fromSqlType.SqlDataTypeOption == SqlDataTypeOption.Date || fromSqlType.SqlDataTypeOption == SqlDataTypeOption.Time) && targetType == typeof(SqlString))
                expr = Expr.Call(() => Convert(Expr.Arg<SqlDateTimeOffset>(), Expr.Arg<bool>(), Expr.Arg<bool>(), Expr.Arg<int>(), Expr.Arg<DataTypeReference>(), Expr.Arg<DataTypeReference>(), Expr.Arg<SqlInt32>(), Expr.Arg<Collation>()), Convert(expr, context, typeof(SqlDateTimeOffset)), Expression.Constant(fromSqlType.SqlDataTypeOption != SqlDataTypeOption.Time), Expression.Constant(fromSqlType.SqlDataTypeOption != SqlDataTypeOption.Date), Expression.Constant(from.GetScale()), Expression.Constant(from), Expression.Constant(to), style, Expression.Constant(targetCollation));
            else if ((expr.Type == typeof(SqlDouble) || expr.Type == typeof(SqlSingle)) && targetType == typeof(SqlString))
                expr = Expr.Call(() => Convert(Expr.Arg<SqlDouble>(), Expr.Arg<SqlInt32>(), Expr.Arg<Collation>()), expr, style, Expression.Constant(targetCollation));
            else if (expr.Type == typeof(SqlMoney) && targetType == typeof(SqlString))
                expr = Expr.Call(() => Convert(Expr.Arg<SqlMoney>(), Expr.Arg<SqlInt32>(), Expr.Arg<Collation>()), expr, style, Expression.Constant(targetCollation));
            else if (expr.Type == typeof(SqlBinary) && targetType == typeof(SqlString))
                expr = Expr.Call(() => Convert(Expr.Arg<SqlBinary>(), Expr.Arg<Collation>(), Expr.Arg<bool>()), expr, Expression.Constant(targetCollation), Expression.Constant(toSqlType.SqlDataTypeOption == SqlDataTypeOption.NChar || toSqlType.SqlDataTypeOption == SqlDataTypeOption.NVarChar));
            else if (expr.Type == typeof(SqlString) && targetType == typeof(SqlBinary))
                expr = Expr.Call(() => Convert(Expr.Arg<SqlString>(), Expr.Arg<bool>()), expr, Expression.Constant(fromSqlType.SqlDataTypeOption == SqlDataTypeOption.NChar || fromSqlType.SqlDataTypeOption == SqlDataTypeOption.NVarChar));
            else if (expr.Type == typeof(SqlGuid) && to.IsEntityReference(out toEr) && toEr != null)
                expr = Expr.Call(() => ExpressionFunctions.CreateLookup(Expr.Arg<SqlString>(), Expr.Arg<SqlGuid>(), Expr.Arg<ExpressionExecutionContext>()), Expression.Constant((SqlString)toEr), expr, context);
            else if (expr.Type == typeof(SqlBinary) && targetType == typeof(SqlMoney) && toSqlType.SqlDataTypeOption == SqlDataTypeOption.SmallMoney)
                expr = Expr.Call(() => ParseBinarySmallMoney(Expr.Arg<SqlBinary>()), expr);
            else if (expr.Type == typeof(SqlMoney) && targetType == typeof(SqlBinary) && fromSqlType.SqlDataTypeOption == SqlDataTypeOption.SmallMoney)
                expr = Expr.Call(() => SerializeBinarySmallMoney(Expr.Arg<SqlMoney>()), expr);
            else if (expr.Type == typeof(SqlString) && to.IsEntityReference(out toEr) && toEr != null)
            {
                expr = Convert(expr, context, from, DataTypeHelpers.UniqueIdentifier, style, styleType, convert, throwOnTruncate, table, column);
                expr = Expr.Call(() => ExpressionFunctions.CreateLookup(Expr.Arg<SqlString>(), Expr.Arg<SqlGuid>(), Expr.Arg<ExpressionExecutionContext>()), Expression.Constant((SqlString)toEr), expr, context);
            }

            if (expr.Type != targetType)
            {
                expr = Convert(expr, context, targetType);

                // Handle errors during type conversion
                var conversionError = Expr.Call(() => Sql4CdsError.ConversionError(Expr.Arg<TSqlFragment>(), Expr.Arg<DataTypeReference>(), Expr.Arg<DataTypeReference>()), Expression.Constant(convert, typeof(TSqlFragment)), Expression.Constant(from), Expression.Constant(to));
                
                if (fromSqlType?.SqlDataTypeOption.IsStringType() == true)
                {
                    if (to.IsSameAs(DataTypeHelpers.UniqueIdentifier) || to.IsEntityReference())
                        conversionError = Expr.Call(() => Sql4CdsError.GuidParseError(Expr.Arg<TSqlFragment>()), Expression.Constant(convert, typeof(TSqlFragment)));
                    else if (toSqlType?.SqlDataTypeOption.IsDateTimeType() == true)
                        conversionError = Expr.Call(() => Sql4CdsError.DateTimeParseError(Expr.Arg<TSqlFragment>()), Expression.Constant(convert, typeof(TSqlFragment)));
                    else if (toSqlType?.SqlDataTypeOption == SqlDataTypeOption.Money)
                        conversionError = Expr.Call(() => Sql4CdsError.MoneyParseError(Expr.Arg<TSqlFragment>()), Expression.Constant(convert, typeof(TSqlFragment)));
                    else if (toSqlType?.SqlDataTypeOption == SqlDataTypeOption.SmallMoney)
                        conversionError = Expr.Call(() => Sql4CdsError.SmallMoneyParseError(Expr.Arg<TSqlFragment>()), Expression.Constant(convert, typeof(TSqlFragment)));
                    else if (toSqlType?.SqlDataTypeOption == SqlDataTypeOption.Int || toSqlType?.SqlDataTypeOption == SqlDataTypeOption.SmallInt || toSqlType?.SqlDataTypeOption == SqlDataTypeOption.TinyInt || toSqlType?.SqlDataTypeOption == SqlDataTypeOption.Bit)
                        conversionError = Expr.Call(() => Sql4CdsError.ConversionErrorWithValue(Expr.Arg<TSqlFragment>(), Expr.Arg<DataTypeReference>(), Expr.Arg<DataTypeReference>(), Expr.Arg<SqlString>()), Expression.Constant(convert, typeof(TSqlFragment)), Expression.Constant(from), Expression.Constant(to), originalExpr);
                }

                var catchFormatExceptionBlock = Expression.Catch(typeof(FormatException), Expression.Throw(Expression.New(typeof(QueryExecutionException).GetConstructor(new[] { typeof(Sql4CdsError) }), conversionError), targetType));

                var overflowError = Expr.Call(() => Sql4CdsError.ArithmeticOverflow(Expr.Arg<DataTypeReference>(), Expr.Arg<DataTypeReference>(), Expr.Arg<TSqlFragment>()), Expression.Constant(from), Expression.Constant(to), Expression.Constant(convert, typeof(TSqlFragment)));

                if (from.IsSameAs(DataTypeHelpers.Int))
                    overflowError = Expr.Call(() => Sql4CdsError.ArithmeticOverflow(Expr.Arg<DataTypeReference>(), Expr.Arg<SqlInt32>()), Expression.Constant(to), originalExpr);

                var catchOverflowExceptionBlock = Expression.Catch(typeof(OverflowException), Expression.Throw(Expression.New(typeof(QueryExecutionException).GetConstructor(new[] { typeof(Sql4CdsError) }), overflowError), targetType));
                
                expr = Expression.TryCatch(expr, catchFormatExceptionBlock, catchOverflowExceptionBlock);
                //expr = Expression.Invoke(Expression.Lambda(expr));
            }

            if (toSqlType == null)
                return expr;

            // Truncate results for [n][var]char
            // https://docs.microsoft.com/en-us/sql/t-sql/functions/cast-and-convert-transact-sql?view=sql-server-ver15#truncating-and-rounding-results
            if (toSqlType.SqlDataTypeOption == SqlDataTypeOption.Char ||
                toSqlType.SqlDataTypeOption == SqlDataTypeOption.NChar ||
                toSqlType.SqlDataTypeOption == SqlDataTypeOption.VarChar ||
                toSqlType.SqlDataTypeOption == SqlDataTypeOption.NVarChar)
            {
                if (toSqlType.Parameters.Count == 1)
                {
                    if (toSqlType.Parameters[0].LiteralType == LiteralType.Integer && Int32.TryParse(toSqlType.Parameters[0].Value, out var maxLength))
                    {
                        if ((sourceType != typeof(SqlString) || fromSqlType.Parameters[0] is IntegerLiteral fromLengthParam && Int32.TryParse(fromLengthParam.Value, out var fromLength) && fromLength > maxLength))
                        {
                            if (maxLength < 1)
                                throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidLengthOrPrecision(toSqlType));

                            // Truncate the value to the specified length, but some special cases
                            string valueOnTruncate = null;
                            Func<string, Sql4CdsError> errorOnTruncate = null;

                            if (sourceType == typeof(SqlInt32) || sourceType == typeof(SqlInt16) || sourceType == typeof(SqlByte))
                            {
                                if (toSqlType.SqlDataTypeOption == SqlDataTypeOption.Char || toSqlType.SqlDataTypeOption == SqlDataTypeOption.VarChar)
                                    valueOnTruncate = "*";
                                else if (toSqlType.SqlDataTypeOption == SqlDataTypeOption.NChar || toSqlType.SqlDataTypeOption == SqlDataTypeOption.NVarChar)
                                    errorOnTruncate = _ => Sql4CdsError.ArithmeticOverflow(from, toSqlType, convert);
                            }
                            else if ((sourceType == typeof(SqlMoney) || sourceType == typeof(SqlDecimal) || sourceType == typeof(SqlSingle)) &&
                                (toSqlType.SqlDataTypeOption == SqlDataTypeOption.Char || toSqlType.SqlDataTypeOption == SqlDataTypeOption.VarChar || toSqlType.SqlDataTypeOption == SqlDataTypeOption.NChar || toSqlType.SqlDataTypeOption == SqlDataTypeOption.NVarChar))
                            {
                                errorOnTruncate = _ => Sql4CdsError.ArithmeticOverflow(from, toSqlType, convert);
                            }
                            else if (sourceType == typeof(SqlGuid) || sourceType == typeof(SqlEntityReference))
                            {
                                errorOnTruncate = _ => Sql4CdsError.GuidTruncationError(convert);
                            }
                            else if (throwOnTruncate)
                            {
                                errorOnTruncate = truncated => Sql4CdsError.StringTruncation(convert, table, column, truncated);
                            }

                            expr = Expr.Call(() => Truncate(Expr.Arg<SqlString>(), Expr.Arg<int>(), Expr.Arg<string>(), Expr.Arg<Func<string, Sql4CdsError>>()),
                                expr,
                                Expression.Constant(maxLength),
                                Expression.Constant(valueOnTruncate, typeof(string)),
                                Expression.Constant(errorOnTruncate, typeof(Func<string, Sql4CdsError>)));
                        }
                    }
                    else if (toSqlType.Parameters[0].LiteralType != LiteralType.Max)
                    {
                        throw new NotSupportedQueryFragmentException(Sql4CdsError.SyntaxError(toSqlType)) { Suggestion = "Invalid attributes specified for type " + toSqlType.SqlDataTypeOption };
                    }
                }
                else if (toSqlType.Parameters.Count > 1)
                {
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.SyntaxError(toSqlType)) { Suggestion = "Invalid attributes specified for type " + toSqlType.SqlDataTypeOption };
                }

                if (targetCollation != null && !targetCollation.Equals((fromSqlType as SqlDataTypeReferenceWithCollation)?.Collation))
                    expr = Expr.Call(() => ConvertCollation(Expr.Arg<SqlString>(), Expr.Arg<Collation>()), expr, Expression.Constant(targetCollation));
            }

            // Truncate results for [var]binary
            if (toSqlType.SqlDataTypeOption == SqlDataTypeOption.Binary ||
                toSqlType.SqlDataTypeOption == SqlDataTypeOption.VarBinary ||
                toSqlType.SqlDataTypeOption.IsRowVersion())
            {
                if (toSqlType.Parameters.Count == 1)
                {
                    if (toSqlType.Parameters[0].LiteralType == LiteralType.Integer && Int32.TryParse(toSqlType.Parameters[0].Value, out var maxLength))
                    {
                        if ((sourceType != typeof(SqlBinary) || fromSqlType.Parameters[0] is IntegerLiteral fromLengthParam && Int32.TryParse(fromLengthParam.Value, out var fromLength) && fromLength > maxLength))
                        {
                            if (maxLength < 1)
                                throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidLengthOrPrecision(toSqlType));

                            // Truncate the value to the specified length
                            expr = Expr.Call(() => Truncate(Expr.Arg<SqlBinary>(), Expr.Arg<int>()),
                                expr,
                                Expression.Constant(maxLength));
                        }
                    }
                    else if (toSqlType.Parameters[0].LiteralType != LiteralType.Max)
                    {
                        throw new NotSupportedQueryFragmentException(Sql4CdsError.SyntaxError(toSqlType)) { Suggestion = "Invalid attributes specified for type " + toSqlType.SqlDataTypeOption };
                    }
                }
                else if (toSqlType.Parameters.Count > 1)
                {
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.SyntaxError(toSqlType)) { Suggestion = "Invalid attributes specified for type " + toSqlType.SqlDataTypeOption };
                }
            }

            // Apply changes to precision & scale
            if (expr.Type == typeof(SqlDecimal))
            {
                // Default precision and scale are (18, 0)
                // https://learn.microsoft.com/en-us/sql/t-sql/data-types/decimal-and-numeric-transact-sql?view=sql-server-ver16#p-precision
                var precision = 18;
                var scale = 0;

                if (toSqlType.Parameters.Count > 0)
                {
                    if (!Int32.TryParse(toSqlType.Parameters[0].Value, out precision) || precision < 0)
                        throw new NotSupportedQueryFragmentException(Sql4CdsError.SyntaxError(toSqlType)) { Suggestion = "Invalid attributes specified for type " + toSqlType.SqlDataTypeOption };

                    if (precision < 1)
                        throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidLengthOrPrecision(toSqlType));

                    if (toSqlType.Parameters.Count > 1)
                    {
                        if (!Int32.TryParse(toSqlType.Parameters[1].Value, out scale) || scale < 0)
                            throw new NotSupportedQueryFragmentException(Sql4CdsError.SyntaxError(toSqlType)) { Suggestion = "Invalid attributes specified for type " + toSqlType.SqlDataTypeOption };

                        if (scale > precision)
                            throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidScale(toSqlType, 0)) { Suggestion = "Scale cannot be greater than precision" };
                    }

                    if (toSqlType.Parameters.Count > 2)
                        throw new NotSupportedQueryFragmentException(Sql4CdsError.SyntaxError(toSqlType)) { Suggestion = "Invalid attributes specified for type " + toSqlType.SqlDataTypeOption };
                }

                expr = Expr.Call(() => ApplyPrecisionScale(Expr.Arg<SqlDecimal>(), Expr.Arg<int>(), Expr.Arg<int>(), Expr.Arg<DataTypeReference>(), Expr.Arg<DataTypeReference>(), Expr.Arg<TSqlFragment>()),
                    expr,
                    Expression.Constant(precision),
                    Expression.Constant(scale),
                    Expression.Constant(from),
                    Expression.Constant(to),
                    Expression.Constant(convert, typeof(TSqlFragment)));
            }
            else if (expr.Type == typeof(SqlTime) || expr.Type == typeof(SqlDateTime2) || expr.Type == typeof(SqlDateTimeOffset))
            {
                // Default scale is 7
                short scale = 7;

                if (toSqlType.Parameters.Count > 0)
                {
                    if (!Int16.TryParse(toSqlType.Parameters[0].Value, out scale) || scale < 0)
                        throw new NotSupportedQueryFragmentException(Sql4CdsError.SyntaxError(toSqlType)) { Suggestion = "Invalid attributes specified for type " + toSqlType.SqlDataTypeOption };

                    if (scale > 7)
                        throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidScale(toSqlType, 1)) { Suggestion = "Scale cannot be greater than 7" };
                }

                if (expr.Type == typeof(SqlTime))
                    expr = Expr.Call(() => SqlTime.ConvertToScale(Expr.Arg<SqlTime>(), Expr.Arg<short>()), expr, Expression.Constant(scale));
                else if (expr.Type == typeof(SqlDateTime2))
                    expr = Expr.Call(() => SqlDateTime2.ConvertToScale(Expr.Arg<SqlDateTime2>(), Expr.Arg<short>()), expr, Expression.Constant(scale));
                else
                    expr = Expr.Call(() => SqlDateTimeOffset.ConvertToScale(Expr.Arg<SqlDateTimeOffset>(), Expr.Arg<short>()), expr, Expression.Constant(scale));
            }

            return expr;
        }

        private static SqlEntityReference CheckEntityReferenceType(SqlEntityReference er, string requiredType)
        {
            if (er.IsNull)
                return er;

            if (er.LogicalName.Equals(requiredType, StringComparison.OrdinalIgnoreCase))
                return er;

            throw new QueryExecutionException(Sql4CdsError.InvalidEntityReferenceType(null, er.LogicalName, requiredType));
        }

        private static SqlDecimal ApplyPrecisionScale(SqlDecimal value, int precision, int scale, DataTypeReference from, DataTypeReference to, TSqlFragment fragment)
        {
            try
            {
                return SqlDecimal.ConvertToPrecScale(value, precision, scale);
            }
            catch (SqlTruncateException)
            {
                throw new QueryExecutionException(Sql4CdsError.ArithmeticOverflow(from, to, fragment));
            }
        }

        /// <summary>
        /// Converts a <see cref="SqlString"/> value from one collation to another
        /// </summary>
        /// <param name="value">The value to convert</param>
        /// <param name="collation">The collation to convert the <paramref name="value"/> to</param>
        /// <returns>A new <see cref="SqlString"/> value with the requested collation</returns>
        public static SqlString ConvertCollation(SqlString value, Collation collation)
        {
            if (value.IsNull)
                return value;

            return collation.ToSqlString(value.Value);
        }

        private static SqlString Truncate(SqlString value, int maxLength, string valueOnTruncate, Func<string, Sql4CdsError> errorOnTruncate)
        {
            if (value.IsNull)
                return value;

            if (value.Value.Length <= maxLength)
                return value;

            if (valueOnTruncate != null)
                return new SqlString(valueOnTruncate, value.LCID, value.SqlCompareOptions);

            var truncated = value.Value.Substring(0, maxLength);

            if (errorOnTruncate != null)
                throw new QueryExecutionException(errorOnTruncate(truncated));

            return new SqlString(truncated, value.LCID, value.SqlCompareOptions);
        }

        private static SqlBinary Truncate(SqlBinary value, int maxLength)
        {
            if (value.IsNull)
                return value;

            if (value.Value.Length <= maxLength)
                return value;

            var truncated = new byte[maxLength];
            Array.Copy(value.Value, truncated, maxLength);

            return truncated;
        }

        private static EntityCollection CreateEntityCollection(SqlEntityReference value)
        {
            if (value.IsNull)
                return null;

            return new EntityCollection(new[]
            {
                new Entity("activityparty")
                {
                    ["partyid"] = (EntityReference)value
                }
            })
            {
                EntityName = "activityparty"
            };
        }

        private static EntityCollection ParseEntityCollection(SqlString value)
        {
            if (value.IsNull)
                return null;

            // Convert the string from the same format used by FormatEntityCollectionEntry
            // Could be logicalname:guid or email address

            var parts = value.Value.Split(',');
            var entities = parts
                .Where(p => !String.IsNullOrEmpty(p))
                .Select(p =>
                {
                    var party = new Entity("activityparty");
                    var subParts = p.Split(':');

                    if (subParts.Length == 2 && Guid.TryParse(subParts[1], out var id))
                        party["partyid"] = new EntityReference(subParts[0], id);
                    else
                        party["addressused"] = p;

                    return party;
                })
                .ToList();

            return new EntityCollection(entities) { EntityName = "activityparty" };
        }

        private static OptionSetValueCollection ParseOptionSetValueCollection(SqlString value)
        {
            if (value.IsNull)
                return null;

            var parts = value.Value.Split(',');
            var osvs = parts
                .Where(p => !String.IsNullOrEmpty(p))
                .Select(p =>
                {
                    if (!Int32.TryParse(p, out var v))
                        throw new QueryExecutionException($"'{p}' is not a valid Choice value. Only integer values are supported");

                    return new OptionSetValue(v);
                })
                .ToList();

            return new OptionSetValueCollection(osvs);
        }

        private static SqlXml ParseXml(SqlString value)
        {
            if (value.IsNull)
                return SqlXml.Null;

            var stream = new MemoryStream(Encoding.GetEncoding("utf-16").GetBytes(value.Value));
            return new SqlXml(stream);
        }

        /// <summary>
        /// Specialized type conversion from DateTime to String using a style
        /// </summary>
        /// <param name="value">The value to convert</param>
        /// <param name="date">Indicates if the date part should be included</param>
        /// <param name="time">Indicates if the time part should be included</param>
        /// <param name="timeScale">The scale of the fractional seconds part</param>
        /// <param name="fromType">The original SQL type that is being converted from</param>
        /// <param name="toType">The original SQL type that is being converted to</param>
        /// <param name="style">The style to apply</param>
        /// <param name="collation">The collation to use for the returned result</param>
        /// <returns>The converted string</returns>
        private static SqlString Convert(SqlDateTimeOffset value, bool date, bool time, int timeScale, DataTypeReference fromType, DataTypeReference toType, SqlInt32 style, Collation collation)
        {
            if (value.IsNull || style.IsNull)
                return SqlString.Null;

            var dateFormatString = "";
            var timeFormatString = "";
            var dateTimeSeparator = " ";
            var cultureInfo = CultureInfo.InvariantCulture;

            switch (style.Value)
            {
                case 0:
                case 100:
                    dateFormatString = "MMM dd yyyy";
                    timeFormatString = "hh:mmtt";
                    break;

                case 1:
                    dateFormatString = "MM/dd/yy";
                    break;

                case 101:
                    dateFormatString = "MM/dd/yyyy";
                    break;

                case 2:
                    dateFormatString = "yy.MM.dd";
                    break;

                case 102:
                    dateFormatString = "yyyy.MM.dd";
                    break;

                case 3:
                    dateFormatString = "dd/MM/yy";
                    break;

                case 103:
                    dateFormatString = "dd/MM/yyyy";
                    break;

                case 4:
                    dateFormatString = "dd.MM.yy";
                    break;

                case 104:
                    dateFormatString = "dd.MM.yyyy";
                    break;

                case 5:
                    dateFormatString = "dd-MM-yy";
                    break;

                case 105:
                    dateFormatString = "dd-MM-yyyy";
                    break;

                case 6:
                    dateFormatString = "dd MMM yy";
                    break;

                case 106:
                    dateFormatString = "dd MMM yyyy";
                    break;

                case 7:
                    dateFormatString = "MMM dd, yy";
                    break;

                case 107:
                    dateFormatString = "MMM dd, yyyy";
                    break;

                case 8:
                case 24:
                case 108:
                    timeFormatString = "HH:mm:ss";
                    break;

                case 9:
                case 109:
                    dateFormatString = "MMM dd yyyy";
                    timeFormatString = "hh:mm:ss";

                    if (timeScale > 0)
                        timeFormatString += ":" + new string('f', timeScale);

                    timeFormatString += "tt";
                    break;

                case 10:
                    dateFormatString = "MM-dd-yy";
                    break;

                case 110:
                    dateFormatString = "MM-dd-yyyy";
                    break;

                case 11:
                    dateFormatString = "yy/MM/dd";
                    break;

                case 111:
                    dateFormatString = "yyyy/MM/dd";
                    break;

                case 12:
                    dateFormatString = "yyMMdd";
                    break;

                case 112:
                    dateFormatString = "yyyyMMdd";
                    break;

                case 13:
                case 113:
                    dateFormatString = "dd MMM yyyy";
                    timeFormatString = "HH:mm:ss";

                    if (timeScale > 0)
                        timeFormatString += ":" + new string('f', timeScale);
                    break;

                case 14:
                case 114:
                    timeFormatString = "HH:mm:ss";

                    if (timeScale > 0)
                        timeFormatString += ":" + new string('f', timeScale);
                    break;

                case 20:
                case 120:
                    dateFormatString = "yyyy-MM-dd";
                    timeFormatString = "HH:mm:ss";
                    break;

                case 21:
                case 25:
                case 121:
                    dateFormatString = "yyyy-MM-dd";
                    timeFormatString = "HH:mm:ss";

                    if (timeScale > 0)
                        timeFormatString += "." + new string('f', timeScale);
                    break;

                case 22:
                    dateFormatString = "MM/dd/yy";
                    timeFormatString = "hh:mm:ss tt";
                    break;

                case 23:
                    dateFormatString = "yyyy-MM-dd";
                    break;

                case 126:
                    dateFormatString = "yyyy-MM-dd";
                    dateTimeSeparator = "T";
                    timeFormatString = "HH:mm:ss";

                    if (timeScale > 0)
                        timeFormatString += "." + new string('F', timeScale);
                    break;

                case 127:
                    dateFormatString = "yyyy-MM-dd";
                    dateTimeSeparator = "T";
                    timeFormatString = "HH:mm:ss";

                    if (timeScale > 0)
                        timeFormatString += "." + new string('F', timeScale);

                    timeFormatString += "\\Z";
                    break;

                case 130:
                    dateFormatString = "dd MMMM yyyy";
                    timeFormatString = "hh:mm:ss";

                    if (timeScale > 0)
                        timeFormatString += ":" + new string('f', timeScale);

                    timeFormatString += "tt";

                    cultureInfo = _hijriCulture;
                    break;

                case 131:
                    dateFormatString = "dd/MM/yyyy";
                    timeFormatString = "hh:mm:ss";

                    if (timeScale > 0)
                        timeFormatString += ":" + new string('f', timeScale);

                    timeFormatString += "tt";

                    cultureInfo = _hijriCulture;
                    break;

                default:
                    throw new QueryExecutionException(Sql4CdsError.InvalidConversionStyle(null, style.Value, fromType));
            }

            if (!date && String.IsNullOrEmpty(timeFormatString) ||
                !time && String.IsNullOrEmpty(dateFormatString))
                    throw new QueryExecutionException($"Error converting data type {fromType.ToSql()} to {toType.ToSql()}");

            var formatString = "";
            if (date && !String.IsNullOrEmpty(dateFormatString))
            {
                formatString += dateFormatString;

                if (time && !String.IsNullOrEmpty(timeFormatString))
                    formatString += dateTimeSeparator;
            }

            if (time && !String.IsNullOrEmpty(timeFormatString))
            {
                formatString += timeFormatString;

                if (fromType is SqlDataTypeReference sqlFromType && sqlFromType.SqlDataTypeOption == SqlDataTypeOption.DateTimeOffset)
                    formatString += " zzz";
            }

            var formatted = value.Value.ToString(formatString, cultureInfo);
            return collation.ToSqlString(formatted);
        }

        /// <summary>
        /// Specialized type conversion from Double to String using a style
        /// </summary>
        /// <param name="value">The value to convert</param>
        /// <param name="style">The style to apply</param>
        /// <param name="collation">The collation to use for the returned result</param>
        /// <returns>The converted string</returns>
        private static SqlString Convert(SqlDouble value, SqlInt32 style, Collation collation)
        {
            if (value.IsNull || style.IsNull)
                return SqlString.Null;

            string formatString;

            switch (style.Value)
            {
                case 1:
                    formatString = "E8";
                    break;

                case 2:
                    formatString = "E16";
                    break;

                case 3:
                    formatString = "G17";
                    break;

                default:
                    formatString = "G6";
                    break;
            }

            var formatted = value.Value.ToString(formatString);
            return collation.ToSqlString(formatted);
        }

        /// <summary>
        /// Specialized type conversion from Money to String using a style
        /// </summary>
        /// <param name="value">The value to convert</param>
        /// <param name="style">The style to apply</param>
        /// <param name="collation">The collation to use for the returned result</param>
        /// <returns>The converted string</returns>
        private static SqlString Convert(SqlMoney value, SqlInt32 style, Collation collation)
        {
            if (value.IsNull || style.IsNull)
                return SqlString.Null;

            string formatString;

            switch (style.Value)
            {
                case 1:
                    formatString = "N2";
                    break;

                case 2:
                case 126:
                    formatString = "F4";
                    break;

                default:
                    formatString = "F2";
                    break;
            }

            var formatted = value.Value.ToString(formatString);
            return collation.ToSqlString(formatted);
        }

        /// <summary>
        /// Specialized type conversion from Decimal to String using a collation
        /// </summary>
        /// <param name="value">The value to convert</param>
        /// <param name="collation">The collation to use for the returned result</param>
        /// <param name="unicode">Indicates if the target string is Unicode</param>
        /// <returns>The converted string</returns>
        private static SqlString Convert(SqlBinary value, Collation collation, bool unicode)
        {
            if (value.IsNull)
                return SqlString.Null;

            return new SqlString(collation.LCID, collation.CompareOptions, value.Value, unicode);
        }

        private static SqlBinary Convert(SqlString value, bool unicode)
        {
            if (value.IsNull)
                return SqlBinary.Null;

            if (unicode)
                return value.GetUnicodeBytes();
            else
                return value.GetNonUnicodeBytes();
        }

        /// <summary>
        /// Specialized type conversion from sql_variant to a target type
        /// </summary>
        /// <param name="variant">The variant to convert</param>
        /// <param name="targetType">The type to convert to</param>
        /// <param name="style">The style of the conversion</param>
        /// <returns>The underlying value of the variant converted to the target type</returns>
        private static INullable Convert(SqlVariant variant, DataTypeReference targetType, SqlInt32 style, ExpressionExecutionContext context)
        {
            if (variant.BaseType == null)
                return GetNullValue(targetType.ToNetType(out _));

            if (variant.BaseType.IsSameAs(targetType))
                return variant.Value;

            if (!CanChangeTypeExplicit(variant.BaseType, targetType))
                throw new QueryExecutionException(Sql4CdsError.ExplicitConversionNotAllowed(null, variant.BaseType, targetType));

            var conversion = GetConversion(variant.BaseType, targetType, style.IsNull ? (int?)null : style.Value);
            return conversion(variant.Value, context);
        }

        /// <summary>
        /// Converts a standard CLR type to the equivalent SQL type
        /// </summary>
        /// <param name="type">The CLR type to convert from</param>
        /// <returns>The equivalent SQL type</returns>
        public static Type NetToSqlType(Type type)
        {
            if (_netToSqlTypeConversions.TryGetValue(type, out var sqlType))
                return sqlType;

            var originalType = type;
            type = type.BaseType;

            while (type != null && type != typeof(object))
            {
                if (_netToSqlTypeConversions.TryGetValue(type, out sqlType))
                {
                    _netToSqlTypeConversions[originalType] = sqlType;
                    return sqlType;
                }

                type = type.BaseType;
            }

            // Convert any other complex types (e.g. from metadata queries) to strings
            sqlType = typeof(SqlString);
            _netToSqlTypeConversions[originalType] = sqlType;
            return sqlType;
        }

        /// <summary>
        /// Converts a value from a CLR type to the equivalent SQL type. 
        /// </summary>
        /// <param name="dataSource">The data source the <paramref name="value"/> was obtained from</param>
        /// <param name="value">The value in a standard CLR type</param>
        /// <param name="dataType">The expected data type</param>
        /// <returns>The value converted to a SQL type</returns>
        public static INullable NetToSqlType(DataSource dataSource, object value, DataTypeReference dataType)
        {
            var type = value.GetType();

            if (_netToSqlTypeConversionFuncs.TryGetValue(type, out var func))
                return func(dataSource, value, dataType);

            var originalType = type;
            type = type.BaseType;

            while (type != null)
            {
                if (_netToSqlTypeConversionFuncs.TryGetValue(type, out func))
                {
                    _netToSqlTypeConversionFuncs[originalType] = func;
                    return func(dataSource, value, dataType);
                }

                type = type.BaseType;
            }

            // Convert any other complex types (e.g. from metadata queries) to strings
            func = (ds, v, __) => ds.DefaultCollation.ToSqlString(v.ToString());
            _netToSqlTypeConversionFuncs[originalType] = func;
            return func(dataSource, value, dataType);
        }

        /// <summary>
        /// Converts a SQL type to the equivalent standard CLR type
        /// </summary>
        /// <param name="type">The SQL type to convert from</param>
        /// <returns>The equivalent CLR type</returns>
        public static Type SqlToNetType(Type type)
        {
            if (_sqlToNetTypeConversions.TryGetValue(type, out var netType))
                return netType;

            throw new ArgumentOutOfRangeException("Unsupported type " + type.FullName);
        }

        /// <summary>
        /// Converts a value from a SQL type to the equivalent CLR type
        /// </summary>
        /// <param name="value">The value in a SQL type</param>
        /// <returns>The value converted to a CLR type</returns>
        public static object SqlToNetType(INullable value)
        {
            if (_sqlToNetTypeConversionFuncs.TryGetValue(value.GetType(), out var func))
                return func(value);

            throw new ArgumentOutOfRangeException("Unsupported type " + value.GetType().FullName);
        }

        private static string FormatEntityCollectionEntry(Entity e)
        {
            if (e.LogicalName == "activityparty")
            {
                // Show the details of the party
                var partyId = e.GetAttributeValue<EntityReference>("partyid");

                if (partyId != null)
                    return $"{partyId.LogicalName}:{partyId.Id}";

                return e.GetAttributeValue<string>("addressused");
            }

            return e.Id.ToString();
        }

        /// <summary>
        /// Gets the null value for a given SQL type
        /// </summary>
        /// <param name="sqlType">The SQL type to get the null value for</param>
        /// <returns>The null value for the requested SQL type</returns>
        public static INullable GetNullValue(Type sqlType)
        {
            return _nullValues[sqlType];
        }

        /// <summary>
        /// Converts a value from one type to another
        /// </summary>
        /// <typeparam name="T">The type to convert the value to</typeparam>
        /// <param name="value">The value to convert</param>
        /// <returns>The value converted to the requested type</returns>
        public static T ChangeType<T>(object value, ExpressionExecutionContext context)
        {
            return (T)ChangeType(value, typeof(T), context);
        }

        /// <summary>
        /// Converts a value from one type to another
        /// </summary>
        /// <param name="context">The context in which the conversion is being performed</param>
        /// <param name="value">The value to convert</param>
        /// <param name="type">The type to convert the value to</param>
        /// <returns>The value converted to the requested type</returns>
        public static object ChangeType(object value, Type type, ExpressionExecutionContext context)
        {
            if (value != null && value.GetType() == type)
                return value;

            var conversion = GetConversion(value.GetType(), type);
            return conversion(value, context);
        }

        /// <summary>
        /// Gets a function to convert from one type to another
        /// </summary>
        /// <param name="sourceType">The type to convert from</param>
        /// <param name="destType">The type to convert to</param>
        /// <returns>A function that converts between the requested types</returns>
        public static Func<object, ExpressionExecutionContext, object> GetConversion(Type sourceType, Type destType)
        {
            var key = sourceType.FullName + " -> " + destType.FullName;
            return _conversions.GetOrAdd(key, _ => CompileConversion(sourceType, destType));
        }

        private static Func<object, ExpressionExecutionContext, object> CompileConversion(Type sourceType, Type destType)
        {
            if (sourceType == destType)
                return (object value, ExpressionExecutionContext _) => value;

            var param = Expression.Parameter(typeof(object));
            var contextParam = Expression.Parameter(typeof(ExpressionExecutionContext));
            var expression = (Expression) Expression.Convert(param, sourceType);

            // Special case for converting from string to enum for metadata filters
            if (expression.Type == typeof(SqlString) && destType.IsGenericType && destType.GetGenericTypeDefinition() == typeof(Nullable<>) && destType.GetGenericArguments()[0].IsEnum)
            {
                var nullCheck = NullCheck(expression);
                var nullValue = (Expression)Expression.Constant(null);
                nullValue = Expression.Convert(nullValue, destType);
                var parsedValue = (Expression)Expression.Convert(expression, typeof(string));
                parsedValue = Expr.Call(() => Enum.Parse(Expr.Arg<Type>(), Expr.Arg<string>(), Expr.Arg<bool>()), Expression.Constant(destType.GetGenericArguments()[0]), parsedValue, Expression.Constant(true));
                parsedValue = Expression.Convert(parsedValue, destType);

                // Convert invalid string values to invalid enum values
                var variables = new List<ParameterExpression>();
                var typedParam = Expression.Variable(destType);
                variables.Add(typedParam);
                var body = new List<Expression>();
                var returnTarget = Expression.Label(destType);
                body.Add(Expression.Assign(typedParam, Expression.Convert(Expression.Constant(-1), destType)));
                body.Add(Expression.Return(returnTarget, typedParam));
                body.Add(Expression.Label(returnTarget, typedParam));
                var block = Expression.Block(destType, variables, body);
                var catchBlock = Expression.Catch(typeof(ArgumentException), block);
                parsedValue = Expression.TryCatch(parsedValue, catchBlock);
                //parsedValue = Expression.Invoke(Expression.Lambda(parsedValue));

                expression = Expression.Condition(nullCheck, nullValue, parsedValue);
            }
            else if (expression.Type == typeof(SqlString) && destType.IsEnum)
            {
                expression = (Expression)Expression.Convert(expression, typeof(string));
                expression = Expr.Call(() => Enum.Parse(Expr.Arg<Type>(), Expr.Arg<string>(), Expr.Arg<bool>()), Expression.Constant(destType), expression, Expression.Constant(true));
                expression = Expression.Convert(expression, destType);

                // Convert invalid string values to invalid enum values
                var variables = new List<ParameterExpression>();
                var typedParam = Expression.Variable(destType);
                variables.Add(typedParam);
                var body = new List<Expression>();
                var returnTarget = Expression.Label(destType);
                body.Add(Expression.Assign(typedParam, Expression.Convert(Expression.Constant(-1), destType)));
                body.Add(Expression.Return(returnTarget, typedParam));
                body.Add(Expression.Label(returnTarget, typedParam));
                var block = Expression.Block(destType, variables, body);
                var catchBlock = Expression.Catch(typeof(ArgumentException), block);
                expression = Expression.TryCatch(expression, catchBlock);
                //expression = Expression.Invoke(Expression.Lambda(expression));
            }
            else if (expression.Type == typeof(SqlInt32) && destType == typeof(OptionSetValue))
            {
                var nullCheck = NullCheck(expression);
                var nullValue = (Expression)Expression.Constant(null, destType);
                var parsedValue = (Expression)Expression.Convert(expression, typeof(int));
                parsedValue = Expression.New(typeof(OptionSetValue).GetConstructor(new[] { typeof(int) }), parsedValue);
                expression = Expression.Condition(nullCheck, nullValue, parsedValue);
            }
            else
            {
                expression = Convert(expression, contextParam, destType);
            }

            //if (destType == typeof(SqlString))
            //    expression = Expr.Call(() => ApplyCollation(Expr.Arg<ExpressionExecutionContext>(), Expr.Arg<SqlString>()), contextParam, expression);

            expression = Expression.Convert(expression, typeof(object));
            return Expression.Lambda<Func<object,ExpressionExecutionContext,object>>(expression, param, contextParam).Compile();
        }

        /// <summary>
        /// Gets a function to convert from one type to another
        /// </summary>
        /// <param name="sourceType">The type to convert from</param>
        /// <param name="destType">The type to convert to</param>
        /// <param name="style">The style of the converesion</param>
        /// <returns>A function that converts between the requested types</returns>
        public static Func<INullable, ExpressionExecutionContext, INullable> GetConversion(DataTypeReference sourceType, DataTypeReference destType, int? style = null)
        {
            var key = sourceType.ToSql() + " -> " + destType.ToSql();

            if (destType is SqlDataTypeReferenceWithCollation collation)
            {
                if (!String.IsNullOrEmpty(collation.Collation.Name))
                    key += " COLLATE " + collation.Collation.Name;
                else
                    key += " COLLATE " + collation.Collation.LCID + ":" + collation.Collation.CompareOptions;
            }

            if (style != null)
                key += " STYLE " + style;
            
            return _sqlConversions.GetOrAdd(key, _ => CompileConversion(sourceType, destType, style));
        }

        private static Func<INullable, ExpressionExecutionContext, INullable> CompileConversion(DataTypeReference sourceType, DataTypeReference destType, int? style = null)
        {
            if (sourceType.IsSameAs(destType))
                return (INullable value, ExpressionExecutionContext _) => value;

            var sourceNetType = sourceType.ToNetType(out _);
            var destNetType = destType.ToNetType(out _);

            var param = Expression.Parameter(typeof(INullable));
            var contextParam = Expression.Parameter(typeof(ExpressionExecutionContext));
            var expression = (Expression)Expression.Convert(param, sourceNetType);
            var styleExpr = (Expression)null;
            var styleType = (DataTypeReference)null;

            if (style != null)
            {
                styleExpr = Expression.Constant(new SqlInt32(style.Value));
                styleType = DataTypeHelpers.Int;
            }

            expression = Convert(expression, contextParam, sourceType, destType, styleExpr, styleType);
            expression = Expression.Convert(expression, typeof(INullable));
            return Expression.Lambda<Func<INullable, ExpressionExecutionContext, INullable>>(expression, param, contextParam).Compile();
        }

        /// <summary>
        /// Creates an expression to check if a value is null
        /// </summary>
        /// <param name="expr">The expression to check for null</param>
        /// <returns>An expression which returns <c>true</c> if the <paramref name="expr"/> is <c>null</c></returns>
        public static Expression NullCheck(Expression expr)
        {
            if (typeof(INullable).IsAssignableFrom(expr.Type))
                return Expression.Property(expr, nameof(INullable.IsNull));

            if (expr.Type.IsGenericType && expr.Type.GetGenericTypeDefinition() == typeof(Nullable<>))
                return Expression.Not(Expression.Property(expr, nameof(Nullable<int>.HasValue)));

            if (expr.Type.IsValueType)
                return Expression.Constant(false);

            return Expression.Equal(expr, Expression.Constant(null));
        }
    }
}