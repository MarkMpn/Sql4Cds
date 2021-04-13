using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// Converts values between different types
    /// </summary>
    class SqlTypeConverter
    {
        // Abbreviated version of data type precedence from https://docs.microsoft.com/en-us/sql/t-sql/data-types/data-type-precedence-transact-sql?view=sql-server-ver15
        private static readonly Type[] _precendenceOrder = new[]
        {
            typeof(SqlDateTime),
            typeof(SqlDouble),
            typeof(SqlSingle),
            typeof(SqlDecimal),
            typeof(SqlMoney),
            typeof(SqlInt64),
            typeof(SqlInt32),
            typeof(SqlInt16),
            typeof(SqlByte),
            typeof(SqlBoolean),
            typeof(SqlGuid),
            typeof(SqlString),
        };

        private static readonly IDictionary<Type, object> _nullValues;
        private static readonly CultureInfo _hijriCulture;

        static SqlTypeConverter()
        {
            _nullValues = new Dictionary<Type, object>
            {
                [typeof(SqlBinary)] = SqlBinary.Null,
                [typeof(SqlBoolean)] = SqlBoolean.Null,
                [typeof(SqlByte)] = SqlByte.Null,
                [typeof(SqlDateTime)] = SqlDateTime.Null,
                [typeof(SqlDecimal)] = SqlDecimal.Null,
                [typeof(SqlDouble)] = SqlDouble.Null,
                [typeof(SqlGuid)] = SqlGuid.Null,
                [typeof(SqlInt16)] = SqlInt16.Null,
                [typeof(SqlInt32)] = SqlInt32.Null,
                [typeof(SqlInt64)] = SqlInt64.Null,
                [typeof(SqlMoney)] = SqlMoney.Null,
                [typeof(SqlSingle)] = SqlSingle.Null,
                [typeof(SqlString)] = SqlString.Null
            };

            _hijriCulture = (CultureInfo)CultureInfo.GetCultureInfo("ar-JO").Clone();
            _hijriCulture.DateTimeFormat.Calendar = new HijriCalendar();
        }

        /// <summary>
        /// Checks if values of two different types can be converted to a consistent type
        /// </summary>
        /// <param name="lhs">The type of the first value</param>
        /// <param name="rhs">The type of the second value</param>
        /// <param name="consistent">The type that both values can be converted to</param>
        /// <returns><c>true</c> if the two values can be converted to a consistent type, or <c>false</c> otherwise</returns>
        public static bool CanMakeConsistentTypes(Type lhs, Type rhs, out Type consistent)
        {
            if (lhs == rhs)
            {
                consistent = lhs;
                return true;
            }

            // Special case for null -> anything
            if (lhs == null || lhs == typeof(object))
            {
                consistent = rhs;
                return true;
            }

            if (rhs == null || rhs == typeof(object))
            {
                consistent = lhs;
                return true;
            }

            var lhsPrecedence = Array.IndexOf(_precendenceOrder, lhs);
            var rhsPrecedence = Array.IndexOf(_precendenceOrder, rhs);

            if (lhsPrecedence == -1 || rhsPrecedence == -1)
            {
                consistent = null;
                return false;
            }

            var targetType = _precendenceOrder[Math.Min(lhsPrecedence, rhsPrecedence)];

            if (CanChangeTypeImplicit(lhs, targetType) && CanChangeTypeImplicit(rhs, targetType))
            {
                consistent = targetType;
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
        public static bool CanChangeTypeImplicit(Type from, Type to)
        {
            if (from == to)
                return true;

            // Special case for null -> anything
            if (from == null || from == typeof(object))
                return true;

            if (Array.IndexOf(_precendenceOrder, from) == -1 ||
                Array.IndexOf(_precendenceOrder, to) == -1)
                return false;

            // Anything can be converted to/from strings
            if (from == typeof(SqlString) || to == typeof(SqlString))
                return true;

            // Any numeric type can be implicitly converted to any other. SQL requires a cast between decimal/numeric when precision/scale changes
            // but we don't have precision/scale as part of the data types
            if ((from == typeof(SqlBoolean) || from == typeof(SqlByte) || from == typeof(SqlInt16) || from == typeof(SqlInt32) || from == typeof(SqlInt64) || from == typeof(SqlMoney) || from == typeof(SqlDecimal) || from == typeof(SqlSingle) || from == typeof(SqlDouble)) &&
                (to == typeof(SqlBoolean) || to == typeof(SqlByte) || to == typeof(SqlInt16) || to == typeof(SqlInt32) || to == typeof(SqlInt64)|| to == typeof(SqlMoney) || to == typeof(SqlDecimal) || to == typeof(SqlSingle) || to == typeof(SqlDouble)))
                return true;

            // Any numeric type can be implicitly converted to datetime
            if ((from == typeof(SqlInt32) || from == typeof(SqlInt64) || from == typeof(SqlMoney) || from == typeof(SqlDecimal) || from == typeof(SqlSingle) || from == typeof(SqlDouble)) &&
                to == typeof(SqlDateTime))
                return true;

            // datetime can only be converted implicitly to string
            if (from == typeof(DateTime) && to == typeof(SqlString))
                return true;

            return false;
        }

        /// <summary>
        /// Checks if values of one type can be converted to another type explicitly
        /// </summary>
        /// <param name="from">The type to convert from</param>
        /// <param name="to">The type to convert to</param>
        /// <returns><c>true</c> if the types can be converted explicitly, or <c>false</c> otherwise</returns>
        public static bool CanChangeTypeExplicit(Type from, Type to)
        {
            if (CanChangeTypeImplicit(from, to))
                return true;

            // Require explicit conversion from datetime to numeric types
            if (from == typeof(SqlDateTime) && (to == typeof(SqlBoolean) || to == typeof(SqlByte) || to == typeof(SqlInt16) || to == typeof(SqlInt32) || to == typeof(SqlInt64) || to == typeof(SqlDecimal) || to == typeof(SqlSingle) || to == typeof(SqlDouble)))
                return true;

            return false;
        }

        /// <summary>
        /// Produces the required expression to convert values to a specific type
        /// </summary>
        /// <param name="expr">The expression that generates the values to convert</param>
        /// <param name="to">The type to convert to</param>
        /// <returns>An expression to generate values of the required type</returns>
        public static Expression Convert(Expression expr, Type to)
        {
            if (expr.Type == typeof(SqlDateTime) && (to == typeof(SqlBoolean) || to == typeof(SqlByte) || to == typeof(SqlInt16) || to == typeof(SqlInt32) || to == typeof(SqlInt64) || to == typeof(SqlDecimal) || to == typeof(SqlSingle) || to == typeof(SqlDouble)))
            {
                expr = Expression.Condition(
                    Expression.PropertyOrField(expr, nameof(SqlDateTime.IsNull)),
                    Expression.Constant(SqlDouble.Null),
                    Expression.Convert(
                        Expression.PropertyOrField(
                            Expression.Subtract(
                                Expression.Convert(expr, typeof(DateTime)),
                                Expression.Constant(SqlDateTime.MinValue)
                            ),
                            nameof(TimeSpan.TotalDays)
                        ),
                        typeof(SqlDouble)
                    )
                );
            }

            if (expr.Type != to)
            {
                if (expr.Type == typeof(object) && typeof(INullable).IsAssignableFrom(to))
                    return Expression.Constant(GetNullValue(to));

                expr = Expression.Convert(expr, to);

                if (to == typeof(SqlString))
                    expr = Expr.Call(() => UseDefaultCollation(Expr.Arg<SqlString>()), expr);
            }

            return expr;
        }

        /// <summary>
        /// Specialized type conversion from DateTime to String using a style
        /// </summary>
        /// <param name="value">The value to convert</param>
        /// <param name="style">The style to apply</param>
        /// <returns>The converted string</returns>
        public static SqlString Convert(SqlDateTime value, SqlInt32 style)
        {
            if (value.IsNull || style.IsNull)
                return SqlString.Null;

            string formatString;
            var cultureInfo = CultureInfo.InvariantCulture;

            switch (style.Value)
            {
                case 0:
                case 100:
                    formatString = "MMM dd yyyy hh:mmtt";
                    break;

                case 1:
                    formatString = "MM/dd/yy";
                    break;

                case 101:
                    formatString = "MM/dd/yyyy";
                    break;

                case 2:
                    formatString = "yy.MM.dd";
                    break;

                case 102:
                    formatString = "yyyy.MM.dd";
                    break;

                case 3:
                    formatString = "dd/MM/yy";
                    break;

                case 103:
                    formatString = "dd/MM/yyyy";
                    break;

                case 4:
                    formatString = "dd.MM.yy";
                    break;

                case 104:
                    formatString = "dd.MM.yyyy";
                    break;

                case 5:
                    formatString = "dd-MM-yy";
                    break;

                case 105:
                    formatString = "dd-MM-yyyy";
                    break;

                case 6:
                    formatString = "dd MMM yy";
                    break;

                case 106:
                    formatString = "dd MMM yyyy";
                    break;

                case 7:
                    formatString = "MMM dd, yy";
                    break;

                case 107:
                    formatString = "MMM dd, yyyy";
                    break;

                case 8:
                case 24:
                case 108:
                    formatString = "HH:mm:ss";
                    break;

                case 9:
                case 109:
                    formatString = "MMM dd yyyy hh:mm:ss:ffftt";
                    break;

                case 10:
                    formatString = "MM-dd-yy";
                    break;

                case 110:
                    formatString = "MM-dd-yyyy";
                    break;

                case 11:
                    formatString = "yy/MM/dd";
                    break;

                case 111:
                    formatString = "yyyy/MM/dd";
                    break;

                case 12:
                    formatString = "yyMMdd";
                    break;

                case 112:
                    formatString = "yyyyMMdd";
                    break;

                case 13:
                case 113:
                    formatString = "dd MMM yyyy HH:mm:ss:fff";
                    break;

                case 14:
                case 114:
                    formatString = "HH:mm:ss:fff";
                    break;

                case 20:
                case 120:
                    formatString = "yyyy-MM-dd HH:mm:ss";
                    break;

                case 21:
                case 25:
                case 121:
                    formatString = "yyyy-MM-dd HH:mm:ss.fff";
                    break;

                case 22:
                    formatString = "MM/dd/yy hh:mm:ss tt";
                    break;

                case 23:
                    formatString = "yyyy-MM-dd";
                    break;

                case 126:
                    formatString = "yyyy-MM-ddTHH:mm:ss.FFF";
                    break;

                case 127:
                    formatString = "yyyy-MM-ddTHH:mm:ss.FFF\\Z";
                    break;

                case 130:
                    formatString = "dd MMMM yyyy hh:mm:ss:ffftt";
                    cultureInfo = _hijriCulture;
                    break;

                case 131:
                    formatString = "dd/MM/yyyy HH:mm:ss:ffftt";
                    cultureInfo = _hijriCulture;
                    break;

                default:
                    throw new QueryExecutionException($"{style.Value} is not a valid style number when converting from datetime to a character string");
            }

            var formatted = value.Value.ToString(formatString, cultureInfo);
            return UseDefaultCollation(formatted);
        }

        /// <summary>
        /// Specialized type conversion from Double to String using a style
        /// </summary>
        /// <param name="value">The value to convert</param>
        /// <param name="style">The style to apply</param>
        /// <returns>The converted string</returns>
        public static SqlString Convert(SqlDouble value, SqlInt32 style)
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
            return UseDefaultCollation(formatted);
        }

        /// <summary>
        /// Specialized type conversion from Money to String using a style
        /// </summary>
        /// <param name="value">The value to convert</param>
        /// <param name="style">The style to apply</param>
        /// <returns>The converted string</returns>
        public static SqlString Convert(SqlMoney value, SqlInt32 style)
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
            return UseDefaultCollation(formatted);
        }

        /// <summary>
        /// Converts a <see cref="SqlString"/> value to the default collation
        /// </summary>
        /// <param name="value">The <see cref="SqlString"/> value to convert</param>
        /// <returns>A <see cref="SqlString"/> value using the default collation</returns>
        public static SqlString UseDefaultCollation(SqlString value)
        {
            if (value.IsNull)
                return value;

            if (value.LCID == CultureInfo.CurrentCulture.LCID && value.SqlCompareOptions == (SqlCompareOptions.IgnoreCase | SqlCompareOptions.IgnoreWidth))
                return value;

            return new SqlString((string)value, CultureInfo.CurrentCulture.LCID, SqlCompareOptions.IgnoreCase | SqlCompareOptions.IgnoreNonSpace);
        }

        /// <summary>
        /// Converts a standard CLR type to the equivalent SQL type
        /// </summary>
        /// <param name="type">The CLR type to convert from</param>
        /// <returns>The equivalent SQL type</returns>
        public static Type NetToSqlType(Type type)
        {
            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>))
                type = type.GetGenericArguments()[0];

            if (type.BaseType != null && type.BaseType.IsGenericType && type.BaseType.GetGenericTypeDefinition() == typeof(ManagedProperty<>))
                type = type.BaseType.GetGenericArguments()[0];

            if (type == typeof(byte[]))
                return typeof(SqlBinary);

            if (type == typeof(bool))
                return typeof(SqlBoolean);

            if (type == typeof(byte))
                return typeof(SqlByte);

            if (type == typeof(DateTime))
                return typeof(SqlDateTime);

            if (type == typeof(decimal))
                return typeof(SqlDecimal);

            if (type == typeof(double))
                return typeof(SqlDouble);

            if (type == typeof(Guid))
                return typeof(SqlGuid);

            if (type == typeof(short))
                return typeof(SqlInt16);

            if (type == typeof(int))
                return typeof(SqlInt32);

            if (type == typeof(long))
                return typeof(SqlInt64);

            if (type == typeof(float))
                return typeof(SqlSingle);

            if (type == typeof(string))
                return typeof(SqlString);

            // Convert any other complex types (e.g. from metadata queries) to strings
            return typeof(SqlString);
        }

        /// <summary>
        /// Converts a value from a CLR type to the equivalent SQL type. 
        /// </summary>
        /// <param name="value">The value in a standard CLR type</param>
        /// <returns>The value converted to a SQL type</returns>
        public static object NetToSqlType(object value)
        {
            if (value != null)
            {
                var type = value.GetType();
                if (type.BaseType != null && type.BaseType.IsGenericType && type.BaseType.GetGenericTypeDefinition() == typeof(ManagedProperty<>))
                    value = type.GetProperty("Value").GetValue(value);
            }

            if (value is byte[] bin)
                return new SqlBinary(bin);

            if (value is bool b)
                return new SqlBoolean(b);

            if (value is byte by)
                return new SqlByte(by);

            if (value is DateTime dt)
                return new SqlDateTime(dt);

            if (value is decimal dec)
                return new SqlDecimal(dec);

            if (value is double dbl)
                return new SqlDouble(dbl);

            if (value is Guid g)
                return new SqlGuid(g);

            if (value is short s)
                return new SqlInt16(s);

            if (value is int i)
                return new SqlInt32(i);

            if (value is long l)
                return new SqlInt64(l);

            if (value is float f)
                return new SqlSingle(f);

            if (value is string str)
                return new SqlString(str, CultureInfo.CurrentCulture.LCID, SqlCompareOptions.IgnoreCase | SqlCompareOptions.IgnoreNonSpace);

            if (value is Money m)
                return new SqlMoney(m.Value);

            if (value is OptionSetValue osv)
                return new SqlInt32(osv.Value);

            if (value is OptionSetValueCollection osvc)
                return new SqlString(String.Join(",", osvc.Select(v => v.Value)), CultureInfo.CurrentCulture.LCID, SqlCompareOptions.IgnoreCase | SqlCompareOptions.IgnoreNonSpace);

            // Convert any other complex types (e.g. from metadata queries) to strings
            return new SqlString(value.ToString(), CultureInfo.CurrentCulture.LCID, SqlCompareOptions.IgnoreCase | SqlCompareOptions.IgnoreNonSpace);
        }

        /// <summary>
        /// Gets the null value for a given SQL type
        /// </summary>
        /// <param name="sqlType">The SQL type to get the null value for</param>
        /// <returns>The null value for the requested SQL type</returns>
        public static object GetNullValue(Type sqlType)
        {
            return _nullValues[sqlType];
        }

        /// <summary>
        /// Converts a value from one type to another
        /// </summary>
        /// <typeparam name="T">The type to convert the value to</typeparam>
        /// <param name="value">The value to convert</param>
        /// <returns>The value converted to the requested type</returns>
        public static T ChangeType<T>(object value)
        {
            return (T)ChangeType(value, typeof(T));
        }

        /// <summary>
        /// Converts a value from one type to another
        /// </summary>
        /// <param name="value">The value to convert</param>
        /// <param name="type">The type to convert the value to</param>
        /// <returns>The value converted to the requested type</returns>
        public static object ChangeType(object value, Type type)
        {
            var expression = (Expression)Expression.Constant(value);

            // Special case for converting from string to enum for metadata filters
            if (expression.Type == typeof(SqlString) && type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>) && type.GetGenericArguments()[0].IsEnum)
            {
                var nullCheck = Expression.PropertyOrField(expression, nameof(INullable.IsNull));
                var nullValue = (Expression)Expression.Constant(null);
                nullValue = Expression.Convert(nullValue, type);
                var parsedValue = (Expression)Expression.Convert(expression, typeof(string));
                parsedValue = Expr.Call(() => Enum.Parse(Expr.Arg<Type>(), Expr.Arg<string>()), Expression.Constant(type.GetGenericArguments()[0]), parsedValue);
                parsedValue = Expression.Convert(parsedValue, type);
                expression = Expression.Condition(nullCheck, nullValue, parsedValue);
            }
            else
            {
                expression = Expression.Convert(expression, type);
            }

            expression = Expression.Convert(expression, typeof(object));
            return Expression.Lambda<Func<object>>(expression).Compile()();
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