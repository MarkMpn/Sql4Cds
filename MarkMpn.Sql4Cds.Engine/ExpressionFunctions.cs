using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.VisualBasic;
using Microsoft.Xrm.Sdk;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;

namespace MarkMpn.Sql4Cds.Engine
{
    /// <summary>
    /// Implements functions that can be called from SQL expressions
    /// </summary>
    class ExpressionFunctions
    {
        /// <summary>
        /// Creates a SqlEntityReference value
        /// </summary>
        /// <param name="logicalName">The logical name of the entity type being referred to</param>
        /// <param name="id">The unique identifier of the record</param>
        /// <returns>A <see cref="SqlEntityReference"/> value combining the <paramref name="logicalName"/> and <paramref name="id"/></returns>
        public static SqlEntityReference CreateLookup(SqlString logicalName, SqlGuid id)
        {
            if (logicalName.IsNull || id.IsNull)
                return SqlEntityReference.Null;

            return new SqlEntityReference(null, logicalName.Value, id);
        }

        /// <summary>
        /// Extracts a scalar value from a JSON string
        /// </summary>
        /// <param name="json">An expression containing the JSON document to parse</param>
        /// <param name="jpath">A JSON path that specifies the property to extract</param>
        /// <returns>Returns a single text value of type nvarchar(4000)</returns>
        public static SqlString Json_Value(SqlString json, SqlString jpath)
        {
            if (json.IsNull || jpath.IsNull)
                return SqlString.Null;

            var path = jpath.Value;
            var lax = !path.StartsWith("strict ", StringComparison.OrdinalIgnoreCase);

            if (path.StartsWith("strict ", StringComparison.OrdinalIgnoreCase))
                path = path.Substring(7);
            else if (path.StartsWith("lax ", StringComparison.OrdinalIgnoreCase))
                path = path.Substring(4);

            try
            {
                var jsonDoc = JToken.Parse(json.Value);
                var jtoken = jsonDoc.SelectToken(path);

                if (jtoken == null)
                {
                    if (lax)
                        return SqlString.Null;
                    else
                        throw new QueryExecutionException("Property does not exist");
                }

                if (jtoken.Type == JTokenType.Object || jtoken.Type == JTokenType.Array)
                {
                    if (lax)
                        return SqlString.Null;
                    else
                        throw new QueryExecutionException("Not a scalar value");
                }

                var value = jtoken.Value<string>();

                if (value.Length > 4000)
                {
                    if (lax)
                        return SqlString.Null;
                    else
                        throw new QueryExecutionException("Value too long");
                }

                return new SqlString(value, json.LCID, json.SqlCompareOptions);
            }
            catch (Newtonsoft.Json.JsonException ex)
            {
                throw new QueryExecutionException(ex.Message, ex);
            }
        }

        /// <summary>
        /// Tests whether a specified SQL/JSON path exists in the input JSON string.
        /// </summary>
        /// <param name="json">An expression containing the JSON document to parse</param>
        /// <param name="jpath">A JSON path that specifies the property to extract</param>
        /// <returns>A value indicating if the path exists in the JSON document</returns>
        public static SqlBoolean Json_Path_Exists(SqlString json, SqlString jpath)
        {
            if (json.IsNull || jpath.IsNull)
                return SqlBoolean.Null;

            var path = jpath.Value;

            if (path.StartsWith("strict ", StringComparison.OrdinalIgnoreCase))
                path = path.Substring(7);
            else if (path.StartsWith("lax ", StringComparison.OrdinalIgnoreCase))
                path = path.Substring(4);

            try
            {

                var jsonDoc = JToken.Parse(json.Value);
                var jtoken = jsonDoc.SelectToken(path);

                return jtoken != null;
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// Implements the DATEADD function
        /// </summary>
        /// <param name="datepart">The part of <paramref name="date"/> to which DATEADD adds an integer <paramref name="number"/></param>
        /// <param name="number">The value to add to the <paramref name="datepart"/> of the <paramref name="date"/></param>
        /// <param name="date">The date value to add to</param>
        /// <returns>The modified date</returns>
        /// <see href="https://docs.microsoft.com/en-us/sql/t-sql/functions/dateadd-transact-sql?view=sql-server-ver15"/>
        public static SqlDateTime DateAdd(SqlString datepart, SqlDouble number, SqlDateTime date)
        {
            if (number.IsNull || date.IsNull)
                return SqlDateTime.Null;

            var interval = DatePartToInterval(datepart.Value);
            var value = DateAndTime.DateAdd(interval, number.Value, date.Value);

            // DateAdd loses the Kind property for some interval types - add it back in again
            if (value.Kind == DateTimeKind.Unspecified)
                value = DateTime.SpecifyKind(value, date.Value.Kind);

            return value;
        }

        /// <summary>
        /// Implements the DATEDIFF function
        /// </summary>
        /// <param name="datepart">The units in which DATEDIFF reports the difference between <paramref name="startdate"/> and <paramref name="enddate"/></param>
        /// <param name="startdate">The first date to compare from</param>
        /// <param name="enddate">The second date to compare to</param>
        /// <returns>The number of whole <paramref name="datepart"/> units between <paramref name="startdate"/> and <paramref name="enddate"/></returns>
        /// <see href="https://docs.microsoft.com/en-us/sql/t-sql/functions/datediff-transact-sql?view=sql-server-ver15"/>
        public static SqlInt32 DateDiff(SqlString datepart, SqlDateTime startdate, SqlDateTime enddate)
        {
            if (startdate.IsNull || enddate.IsNull)
                return SqlInt32.Null;

            var interval = DatePartToInterval(datepart.Value);
            return (int) DateAndTime.DateDiff(interval, startdate.Value, enddate.Value);
        }

        /// <summary>
        /// Implements the DATEPART function
        /// </summary>
        /// <param name="datepart">The specific part of the <paramref name="date"/> argument for which DATEPART will return an integer</param>
        /// <param name="date">The date to extract the <paramref name="datepart"/> from</param>
        /// <returns>The <paramref name="datepart"/> of the <paramref name="date"/></returns>
        public static SqlInt32 DatePart(SqlString datepart, SqlDateTime date)
        {
            if (date.IsNull)
                return SqlInt32.Null;

            var interval = DatePartToInterval(datepart.Value);
            return DateAndTime.DatePart(interval, date.Value);
        }

        /// <summary>
        /// Converts the SQL datepart argument names to the equivalent enum values used by VisualBasic
        /// </summary>
        /// <param name="datepart">The SQL name for the datepart argument</param>
        /// <returns>The equivalent <see cref="DateInterval"/> value</returns>
        internal static DateInterval DatePartToInterval(string datepart)
        {
            switch (datepart.ToLower())
            {
                case "year":
                case "yy":
                case "yyyy":
                    return DateInterval.Year;

                case "quarter":
                case "qq":
                case "q":
                    return DateInterval.Quarter;

                case "month":
                case "mm":
                case "m":
                    return DateInterval.Month;

                case "dayofyear":
                case "dy":
                case "y":
                    return DateInterval.DayOfYear;

                case "day":
                case "dd":
                case "d":
                    return DateInterval.Day;

                case "weekday":
                case "dw":
                case "w":
                    return DateInterval.Weekday;

                case "week":
                case "wk":
                case "ww":
                    return DateInterval.WeekOfYear;

                case "hour":
                case "hh":
                    return DateInterval.Hour;

                case "minute":
                case "mi":
                case "n":
                    return DateInterval.Minute;

                case "second":
                case "ss":
                case "s":
                    return DateInterval.Second;

                default:
                    throw new ArgumentOutOfRangeException(nameof(datepart), $"Unsupported DATEPART value {datepart}");
            }
        }

        /// <summary>
        /// Gets the current date/time in user-local timezone
        /// </summary>
        /// <returns>The current date/time in user-local timezone</returns>
        public static SqlDateTime GetDate()
        {
            return DateTime.Now;
        }

        /// <summary>
        /// Gets the current date/time in user-local timezone
        /// </summary>
        /// <returns>The current date/time in user-local timezone</returns>
        public static SqlDateTime SysDateTime()
        {
            return DateTime.Now;
        }

        /// <summary>
        /// Gets the current date/time in user-local timezone
        /// </summary>
        /// <returns>The current date/time in user-local timezone</returns>
        public static SqlDateTime SysDateTimeOffset()
        {
            return DateTime.Now;
        }

        /// <summary>
        /// Gets the current date/time in UTC timezone
        /// </summary>
        /// <returns>The current date/time in UTC timezone</returns>
        public static SqlDateTime GetUtcDate()
        {
            return DateTime.UtcNow;
        }

        /// <summary>
        /// Gets the current date/time in UTC timezone
        /// </summary>
        /// <returns>The current date/time in UTC timezone</returns>
        public static SqlDateTime SysUtcDateTime()
        {
            return DateTime.UtcNow;
        }

        /// <summary>
        /// Gets the day of the month from the specified date
        /// </summary>
        /// <param name="date">The date to get the day number from</param>
        /// <returns>The day of the month</returns>
        public static SqlInt32 Day(SqlDateTime date)
        {
            if (date.IsNull)
                return SqlInt32.Null;

            return date.Value.Day;
        }

        /// <summary>
        /// Gets the month number from the specified date
        /// </summary>
        /// <param name="date">The date to get the month number from</param>
        /// <returns>The month number</returns>
        public static SqlInt32 Month(SqlDateTime date)
        {
            if (date.IsNull)
                return SqlInt32.Null;

            return date.Value.Month;
        }

        /// <summary>
        /// Gets the year from the specified date
        /// </summary>
        /// <param name="date">The date to get the year number from</param>
        /// <returns>The year number</returns>
        public static SqlInt32 Year(SqlDateTime date)
        {
            if (date.IsNull)
                return SqlInt32.Null;

            return date.Value.Year;
        }

        /// <summary>
        /// Returns the prefix of a string
        /// </summary>
        /// <param name="s">The string to get the prefix of</param>
        /// <param name="length">The number of characters to return</param>
        /// <returns>The first <paramref name="length"/> characters of the string <paramref name="s"/></returns>
        public static SqlString Left(SqlString s, [MaxLength] SqlInt32 length)
        {
            if (s.IsNull || length.IsNull)
                return SqlString.Null;

            if (s.Value.Length <= length)
                return s;

            return SqlTypeConverter.UseDefaultCollation(s.Value.Substring(0, length.Value));
        }

        /// <summary>
        /// Returns the suffix of a string
        /// </summary>
        /// <param name="s">The string to get the suffix of</param>
        /// <param name="length">The number of characters to return</param>
        /// <returns>The last <paramref name="length"/> characters of the string <paramref name="s"/></returns>
        public static SqlString Right(SqlString s, [MaxLength] SqlInt32 length)
        {
            if (s.IsNull || length.IsNull)
                return SqlString.Null;

            if (s.Value.Length <= length)
                return s;

            return SqlTypeConverter.UseDefaultCollation(s.Value.Substring(s.Value.Length - length.Value, length.Value));
        }

        /// <summary>
        /// Replaces all occurrences of a specified string value with another string value.
        /// </summary>
        /// <param name="input">The string expression to be searched</param>
        /// <param name="find">The substring to be found</param>
        /// <param name="replace">The replacement string</param>
        /// <returns>Replaces any instances of <paramref name="find"/> with <paramref name="replace"/> in the <paramref name="input"/></returns>
        public static SqlString Replace(SqlString input, SqlString find, SqlString replace)
        {
            if (input.IsNull || find.IsNull || replace.IsNull)
                return SqlString.Null;

            return SqlTypeConverter.UseDefaultCollation(Regex.Replace(input.Value, Regex.Escape(find.Value), replace.Value.Replace("$", "$$"), RegexOptions.IgnoreCase));
        }

        /// <summary>
        /// Returns the number of characters of the specified string expression, excluding trailing spaces
        /// </summary>
        /// <param name="s">The string expression to be evaluated</param>
        /// <returns></returns>
        public static SqlInt32 Len(SqlString s)
        {
            if (s.IsNull)
                return SqlInt32.Null;

            return s.Value.TrimEnd().Length;
        }

        /// <summary>
        /// Returns the number of bytes used to represent any expression
        /// </summary>
        /// <param name="value">Any expression</param>
        /// <returns></returns>
        public static SqlInt32 DataLength<T>(T value, [SourceType] DataTypeReference type)
            where T:INullable
        {
            if (value.IsNull)
                return SqlInt32.Null;

            var sqlType = type as SqlDataTypeReference;

            if (sqlType != null && value is SqlString s && 
                (sqlType.SqlDataTypeOption == SqlDataTypeOption.VarChar || sqlType.SqlDataTypeOption == SqlDataTypeOption.NVarChar))
            {
                var length = s.Value.Length;

                if (sqlType.SqlDataTypeOption == SqlDataTypeOption.NVarChar)
                    length *= 2;

                return length;
            }

            var size = type.GetSize();

            if (sqlType != null && sqlType.SqlDataTypeOption == SqlDataTypeOption.NChar)
                size *= 2;

            return size;
        }

        /// <summary>
        /// Returns part of a character expression
        /// </summary>
        /// <param name="expression">A character expression</param>
        /// <param name="start">An integer that specifies where the returned characters start (the numbering is 1 based, meaning that the first character in the expression is 1)</param>
        /// <param name="length">A positive integer that specifies how many characters of the expression will be returned</param>
        /// <returns></returns>
        public static SqlString Substring(SqlString expression, SqlInt32 start, [MaxLength] SqlInt32 length)
        {
            if (expression.IsNull || start.IsNull || length.IsNull)
                return SqlString.Null;

            if (length < 0)
                throw new ArgumentOutOfRangeException(nameof(length));

            if (start < 1)
                start = 1;

            if (start > expression.Value.Length)
                return SqlTypeConverter.UseDefaultCollation(String.Empty);

            start -= 1;

            if (start + length > expression.Value.Length)
                length = expression.Value.Length - start;

            return SqlTypeConverter.UseDefaultCollation(expression.Value.Substring(start.Value, length.Value));
        }

        /// <summary>
        /// Removes the space character from the start and end of a string
        /// </summary>
        /// <param name="expression">A character expression where characters should be removed</param>
        /// <returns></returns>
        public static SqlString Trim([MaxLength] SqlString expression)
        {
            if (expression.IsNull)
                return expression;

            return SqlTypeConverter.UseDefaultCollation(expression.Value.Trim(' '));
        }

        /// <summary>
        /// Removes the space character from the start of a string
        /// </summary>
        /// <param name="expression">A character expression where characters should be removed</param>
        /// <returns></returns>
        public static SqlString LTrim([MaxLength] SqlString expression)
        {
            if (expression.IsNull)
                return expression;

            return SqlTypeConverter.UseDefaultCollation(expression.Value.TrimStart(' '));
        }

        /// <summary>
        /// Removes the space character from the end of a string
        /// </summary>
        /// <param name="expression">A character expression where characters should be removed</param>
        /// <returns></returns>
        public static SqlString RTrim([MaxLength] SqlString expression)
        {
            if (expression.IsNull)
                return expression;

            return SqlTypeConverter.UseDefaultCollation(expression.Value.TrimEnd(' '));
        }

        /// <summary>
        /// Searches for one character expression inside a second character expression, returning the starting position of the first expression if found
        /// </summary>
        /// <param name="find">A character expression containing the sequence to find</param>
        /// <param name="search">A character expression to search.</param>
        /// <returns></returns>
        public static SqlInt32 CharIndex(SqlString find, SqlString search)
        {
            return CharIndex(find, search, 0);
        }

        /// <summary>
        /// Searches for one character expression inside a second character expression, returning the starting position of the first expression if found
        /// </summary>
        /// <param name="find">A character expression containing the sequence to find</param>
        /// <param name="search">A character expression to search.</param>
        /// <param name="startLocation">An integer or bigint expression at which the search starts. If start_location is not specified, has a negative value, or has a zero (0) value, the search starts at the beginning of expressionToSearch.</param>
        /// <returns></returns>
        public static SqlInt32 CharIndex(SqlString find, SqlString search, SqlInt32 startLocation)
        {
            if (find.IsNull || search.IsNull || startLocation.IsNull)
                return SqlInt32.Null;

            if (startLocation <= 0)
                startLocation = 1;

            if (startLocation > search.Value.Length)
                return 0;

            return search.Value.IndexOf(find.Value, startLocation.Value - 1, StringComparison.OrdinalIgnoreCase) + 1;
        }

        /// <summary>
        /// Returns the single-byte character with the specified integer code
        /// </summary>
        /// <param name="value">An integer from 0 through 255</param>
        /// <returns></returns>
        public static SqlString Char(SqlInt32 value)
        {
            if (value.IsNull || value.Value < 0 || value.Value > 255)
                return SqlString.Null;

            return SqlTypeConverter.UseDefaultCollation(new string((char)value.Value, 1));
        }

        /// <summary>
        /// Returns the Unicode character with the specified integer code
        /// </summary>
        /// <param name="value">An integer from 0 through 255</param>
        /// <returns></returns>
        public static SqlString NChar(SqlInt32 value)
        {
            if (value.IsNull || value.Value < 0 || value.Value > 0x10FFFF)
                return SqlString.Null;

            return SqlTypeConverter.UseDefaultCollation(new string((char)value.Value, 1));
        }

        /// <summary>
        /// Returns the ASCII code value of the leftmost character of a character expression.
        /// </summary>
        /// <param name="value">A string to convert</param>
        /// <returns></returns>
        public static SqlInt32 Ascii(SqlString value)
        {
            if (value.IsNull || value.Value.Length == 0)
                return SqlInt32.Null;

            var b = Encoding.ASCII.GetBytes(value.Value);
            return b[0];
        }

        /// <summary>
        /// Returns the integer value, as defined by the Unicode standard, for the first character of the input expression.
        /// </summary>
        /// <param name="value">A string to convert</param>
        /// <returns></returns>
        public static SqlInt32 Unicode(SqlString value)
        {
            if (value.IsNull || value.Value.Length == 0)
                return SqlInt32.Null;

            return value.Value[0];
        }

        /// <summary>
        /// Returns the identifier of the user
        /// </summary>
        /// <param name="options">The options that provide access to the user details</param>
        /// <returns></returns>
        public static SqlEntityReference User_Name(IQueryExecutionOptions options)
        {
            return new SqlEntityReference(options.PrimaryDataSource, "systemuser", options.UserId);
        }

        /// <summary>
        /// The value of <paramref name="check"/> is returned if it is not NULL; otherwise, <paramref name="replacement"/> is returned
        /// </summary>
        /// <typeparam name="T">The type of values being compared</typeparam>
        /// <param name="check">The expression to be checked for NULL</param>
        /// <param name="replacement">The value to be returned if <paramref name="check"/> is NULL</param>
        /// <returns></returns>
        public static T IsNull<T>(T check, T replacement)
            where T:INullable
        {
            if (!check.IsNull)
                return check;

            return replacement;
        }
    }

    /// <summary>
    /// Helper methods for generating function call expressions
    /// </summary>
    class Expr
    {
        /// <summary>
        /// Create a method call expression
        /// </summary>
        /// <typeparam name="T">The type of value returned by the method</typeparam>
        /// <param name="expression">The expression containing the method call to</param>
        /// <param name="args">The expressions to supply as arguments to the method call</param>
        /// <returns>The method call expression</returns>
        /// <see href="http://blog.functionalfun.net/2009/10/getting-methodinfo-of-generic-method.html"/>
        public static MethodCallExpression Call<T>(Expression<Func<T>> expression, params Expression[] args)
        {
            var method = GetMethodInfo((LambdaExpression)expression);
            return Call(method, args);
        }

        /// <summary>
        /// Creates a method call expression
        /// </summary>
        /// <param name="method">The details of the method to call</param>
        /// <param name="args">The expressions to supply as arguments to the method call</param>
        /// <returns>The method call expression</returns>
        public static MethodCallExpression Call(MethodInfo method, params Expression[] args)
        {
            var parameters = method.GetParameters();
            var converted = new Expression[parameters.Length];

            for (var i = 0; i < parameters.Length; i++)
            {
                if (parameters[i].ParameterType == args[i].Type)
                {
                    converted[i] = args[i];
                }
                else
                {
                    if (i == parameters.Length - 1 && parameters[i].ParameterType.IsArray && !args[i].Type.IsArray)
                    {
                        var elementType = parameters[i].ParameterType.GetElementType();
                        var elements = new List<Expression>();

                        for (var j = i; j < args.Length; j++)
                            elements.Add(Convert(args[j], elementType));

                        converted[i] = Expression.NewArrayInit(elementType, elements.ToArray());
                    }
                    else
                    {
                        converted[i] = Convert(args[i], parameters[i].ParameterType);
                    }
                }
            }

            return Expression.Call(method, converted);
        }

        /// <summary>
        /// Given a lambda expression that calls a method, returns the method info.
        /// </summary>
        /// <param name="expression">The expression.</param>
        public static MethodInfo GetMethodInfo<T>(Expression<Func<T>> expression)
        {
            var method = GetMethodInfo((LambdaExpression)expression);
            return method;
        }

        /// <summary>
        /// Given a lambda expression that calls a method, returns the method info.
        /// </summary>
        /// <param name="expression">The expression.</param>
        /// <returns></returns>
        /// <see href="http://blog.functionalfun.net/2009/10/getting-methodinfo-of-generic-method.html"/>
        private static MethodInfo GetMethodInfo(LambdaExpression expression)
        {
            if (!(expression.Body is MethodCallExpression outermostExpression))
                throw new ArgumentException("Invalid Expression. Expression should consist of a Method call only.");

            return outermostExpression.Method;
        }

        /// <summary>
        /// Placeholder for a function argument when using <see cref="Call{T}(Expression{Func{T}}, Expression[])"/>
        /// </summary>
        /// <typeparam name="T">The type of value expected by the function argument</typeparam>
        /// <returns></returns>
        public static T Arg<T>()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Box an expression
        /// </summary>
        /// <param name="expr"></param>
        /// <returns></returns>
        public static Expression Box(Expression expr)
        {
            if (!expr.Type.IsValueType)
                return expr;

            return Expression.Convert(expr, typeof(object));
        }

        /// <summary>
        /// Applies the required conversion process to return a value of an expected type from an expression
        /// </summary>
        /// <param name="expr">The expression that is generating a value</param>
        /// <param name="type">The type of value required from an expression</param>
        /// <returns>An expression that converts the generated value to the required type</returns>
        public static Expression Convert(Expression expr, Type type)
        {
            // Simple case - expression already generates the required type
            if (expr.Type == type)
                return expr;

            // If the generated type directly implements the required interface
            if (type.IsInterface && expr.Type.GetInterfaces().Contains(type))
                return expr;

            // Box value types
            if (expr.Type.IsValueType && type == typeof(object))
                return Expression.Convert(expr, type);

            // WIP: Implicit type conversions to match SQL
            if (type == typeof(bool?))
            {
                // https://docs.microsoft.com/en-us/sql/t-sql/data-types/bit-transact-sql
                if (expr.Type == typeof(string))
                    return Expr.Call(() => StringToBool(Arg<string>()), expr);

                if (expr.Type == typeof(int))
                    expr = Expression.Convert(expr, typeof(int?));

                if (expr.Type == typeof(int?))
                    return Expr.Call(() => IntToBool(Arg<int?>()), expr);
            }

            // In-built conversions between value types
            if (expr.Type.IsValueType && type.IsValueType)
                return Expression.Convert(expr, type);

            // Parse string literals to DateTime values
            if (expr.Type == typeof(string) && type == typeof(DateTime?))
                return Expr.Call(() => ParseDateTime(Arg<string>()), expr);

            // Convert integers to optionset values
            if (expr.Type == typeof(int) && type == typeof(OptionSetValue))
                return Expr.Call(() => CreateOptionSetValue(Arg<int>()), expr);
            if (expr.Type == typeof(int?) && type == typeof(OptionSetValue))
                return Expr.Call(() => CreateOptionSetValue(Arg<int?>()), expr);

            // Extract IDs from EntityReferences
            if (expr.Type == typeof(EntityReference) && type == typeof(Guid?))
                return Expr.Call(() => GetEntityReferenceId(Arg<EntityReference>()), expr);

            // Check for compatible class types
            if (expr.Type.IsClass && type.IsClass)
            {
                var baseType = expr.Type.BaseType;

                while (baseType != null)
                {
                    if (baseType == type)
                        return expr;

                    baseType = baseType.BaseType;
                }
            }

            // Check for compatible array types
            if (expr.Type.IsArray && type.IsArray)
            {
                if (type.GetElementType().IsAssignableFrom(expr.Type.GetElementType()))
                    return expr;
            }

            throw new NotSupportedException($"Cannot convert from {expr.Type} to {type}");
        }

        private static Guid? GetEntityReferenceId(EntityReference entityReference)
        {
            return entityReference.Id;
        }

        private static bool? StringToBool(string str)
        {
            if (str == null)
                return null;

            if (str.Equals("TRUE", StringComparison.OrdinalIgnoreCase))
                return true;

            if (str.Equals("FALSE", StringComparison.OrdinalIgnoreCase))
                return false;

            throw new ArgumentOutOfRangeException($"Cannot convert string value '{str}' to boolean");
        }

        private static bool? IntToBool(int? value)
        {
            if (value == null)
                return null;

            if (value == 0)
                return false;

            return true;
        }

        private static DateTime? ParseDateTime(string str)
        {
            if (str == null)
                return null;

            return DateTime.Parse(str);
        }

        private static OptionSetValue CreateOptionSetValue(int? i)
        {
            if (i == null)
                return null;

            return new OptionSetValue(i.Value);
        }
    }

    /// <summary>
    /// Indicates that the parameter gives the maximum length of the return value
    /// </summary>
    [AttributeUsage(AttributeTargets.Parameter)]
    class MaxLengthAttribute : Attribute
    {
    }

    /// <summary>
    /// Indicates that the parameter gives the type of the returned value
    /// </summary>
    [AttributeUsage(AttributeTargets.Parameter)]
    class TargetTypeAttribute : Attribute
    {
    }

    /// <summary>
    /// Indicates that the parameter gives the type of the preceding parameter
    /// </summary>
    [AttributeUsage(AttributeTargets.Parameter)]
    class SourceTypeAttribute : Attribute
    {
    }
}
