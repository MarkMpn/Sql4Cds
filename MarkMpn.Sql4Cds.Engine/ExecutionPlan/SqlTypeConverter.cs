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
            typeof(SqlInt64),
            typeof(SqlInt32),
            typeof(SqlInt16),
            typeof(SqlByte),
            typeof(SqlBoolean),
            typeof(SqlGuid),
            typeof(SqlString),
        };

        private static readonly IDictionary<Type, object> _nullValues;

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
                [typeof(SqlSingle)] = SqlSingle.Null,
                [typeof(SqlString)] = SqlString.Null
            };
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
            if ((from == typeof(SqlBoolean) || from == typeof(SqlByte) || from == typeof(SqlInt16) || from == typeof(SqlInt32) || from == typeof(SqlInt64) || from == typeof(SqlDecimal) || from == typeof(SqlSingle) || from == typeof(SqlDouble)) &&
                (to == typeof(SqlBoolean) || to == typeof(SqlByte) || to == typeof(SqlInt16) || to == typeof(SqlInt32) || to == typeof(SqlInt64) || to == typeof(SqlDecimal) || to == typeof(SqlSingle) || to == typeof(SqlDouble)))
                return true;

            // Any numeric type can be implicitly converted to datetime
            if ((from == typeof(SqlInt32) || from == typeof(SqlInt64) || from == typeof(SqlDecimal) || from == typeof(SqlSingle) || from == typeof(SqlDouble)) &&
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
                return new SqlDecimal(m.Value);

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