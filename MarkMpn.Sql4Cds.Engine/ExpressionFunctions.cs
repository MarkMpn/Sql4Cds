using Microsoft.VisualBasic;
using Microsoft.Xrm.Sdk;
using System;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.RegularExpressions;

namespace MarkMpn.Sql4Cds.Engine
{
    /// <summary>
    /// Implements functions that can be called from SQL expressions
    /// </summary>
    class ExpressionFunctions
    {
        /// <summary>
        /// Implements the LIKE operator
        /// </summary>
        /// <param name="value">The value to match from</param>
        /// <param name="pattern">The pattern to match against</param>
        /// <returns><c>true</c> if the <paramref name="value"/> matches the <paramref name="pattern"/>, or <c>false</c> otherwise</returns>
        public static bool Like(string value, string pattern)
        {
            if (value == null || pattern == null)
                return false;

            var regex = "^" + Regex.Escape(pattern).Replace("%", ".*").Replace("_", ".") + "$";
            return Regex.IsMatch(value, regex, RegexOptions.IgnoreCase);
        }

        /// <summary>
        /// Implements the DATEADD function
        /// </summary>
        /// <param name="datepart">The part of <paramref name="date"/> to which DATEADD adds an integer <paramref name="number"/></param>
        /// <param name="number">The value to add to the <paramref name="datepart"/> of the <paramref name="date"/></param>
        /// <param name="date">The date value to add to</param>
        /// <returns>The modified date</returns>
        /// <see href="https://docs.microsoft.com/en-us/sql/t-sql/functions/dateadd-transact-sql?view=sql-server-ver15"/>
        public static DateTime? DateAdd(string datepart, double? number, DateTime? date)
        {
            if (number == null || date == null)
                return null;

            var interval = DatePartToInterval(datepart);
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
        public static int? DateDiff(string datepart, DateTime? startdate, DateTime? enddate)
        {
            if (startdate == null || enddate == null)
                return null;

            var interval = DatePartToInterval(datepart);
            return (int) DateAndTime.DateDiff(interval, startdate.Value, enddate.Value);
        }

        /// <summary>
        /// Implements the DATEPART function
        /// </summary>
        /// <param name="datepart">The specific part of the <paramref name="date"/> argument for which DATEPART will return an integer</param>
        /// <param name="date">The date to extract the <paramref name="datepart"/> from</param>
        /// <returns>The <paramref name="datepart"/> of the <paramref name="date"/></returns>
        public static int? DatePart(string datepart, DateTime? date)
        {
            if (date == null)
                return null;

            var interval = DatePartToInterval(datepart);
            return DateAndTime.DatePart(interval, date.Value);
        }

        /// <summary>
        /// Converts the SQL datepart argument names to the equivalent enum values used by VisualBasic
        /// </summary>
        /// <param name="datepart">The SQL name for the datepart argument</param>
        /// <returns>The equivalent <see cref="DateInterval"/> value</returns>
        private static DateInterval DatePartToInterval(string datepart)
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
        public static DateTime GetDate()
        {
            return DateTime.Now;
        }

        /// <summary>
        /// Gets the current date/time in user-local timezone
        /// </summary>
        /// <returns>The current date/time in user-local timezone</returns>
        public static DateTime SysDateTime()
        {
            return DateTime.Now;
        }

        /// <summary>
        /// Gets the current date/time in user-local timezone
        /// </summary>
        /// <returns>The current date/time in user-local timezone</returns>
        public static DateTime SysDateTimeOffset()
        {
            return DateTime.Now;
        }

        /// <summary>
        /// Gets the current date/time in UTC timezone
        /// </summary>
        /// <returns>The current date/time in UTC timezone</returns>
        public static DateTime GetUtcDate()
        {
            return DateTime.UtcNow;
        }

        /// <summary>
        /// Gets the current date/time in UTC timezone
        /// </summary>
        /// <returns>The current date/time in UTC timezone</returns>
        public static DateTime SysUtcDateTime()
        {
            return DateTime.UtcNow;
        }

        /// <summary>
        /// Gets the day of the month from the specified date
        /// </summary>
        /// <param name="date">The date to get the day number from</param>
        /// <returns>The day of the month</returns>
        public static int? Day(DateTime? date)
        {
            return date?.Day;
        }

        /// <summary>
        /// Gets the month number from the specified date
        /// </summary>
        /// <param name="date">The date to get the month number from</param>
        /// <returns>The month number</returns>
        public static int? Month(DateTime? date)
        {
            return date?.Month;
        }

        /// <summary>
        /// Gets the year from the specified date
        /// </summary>
        /// <param name="date">The date to get the year number from</param>
        /// <returns>The year number</returns>
        public static int? Year(DateTime? date)
        {
            return date?.Year;
        }

        /// <summary>
        /// Returns the prefix of a string
        /// </summary>
        /// <param name="s">The string to get the prefix of</param>
        /// <param name="length">The number of characters to return</param>
        /// <returns>The first <paramref name="length"/> characters of the string <paramref name="s"/></returns>
        public static string Left(string s, int length)
        {
            if (s == null)
                return s;

            if (s.Length <= length)
                return s;

            return s.Substring(0, length);
        }

        /// <summary>
        /// Returns the suffix of a string
        /// </summary>
        /// <param name="s">The string to get the suffix of</param>
        /// <param name="length">The number of characters to return</param>
        /// <returns>The last <paramref name="length"/> characters of the string <paramref name="s"/></returns>
        public static string Right(string s, int length)
        {
            if (s == null)
                return s;

            if (s.Length <= length)
                return s;

            return s.Substring(s.Length - length, length);
        }

        /// <summary>
        /// Implements a case-insensitive string equality comparison to match the standard SQL collation
        /// </summary>
        /// <param name="x">The first string to compare</param>
        /// <param name="y">The second string to compare</param>
        /// <returns><c>true</c> if the two strings are equal, or <c>false</c> otherwise</returns>
        public static bool CaseInsensitiveEquals(string x, string y)
        {
            return StringComparer.OrdinalIgnoreCase.Equals(x, y);
        }

        /// <summary>
        /// Implements a case-insensitive string inequality comparison to match the standard SQL collation
        /// </summary>
        /// <param name="x">The first string to compare</param>
        /// <param name="y">The second string to compare</param>
        /// <returns><c>true</c> if the two strings are not equal, or <c>false</c> otherwise</returns>
        public static bool CaseInsensitiveNotEquals(string x, string y)
        {
            return !StringComparer.OrdinalIgnoreCase.Equals(x, y);
        }

        /// <summary>
        /// Implements equality between <see cref="EntityReference"/> and <see cref="Guid"/>
        /// </summary>
        /// <param name="x">The <see cref="EntityReference"/> to compare</param>
        /// <param name="y">The <see cref="Guid"/> to compare</param>
        /// <returns><c>true</c> if the ID in the <see cref="EntityReference"/> matches the supplied <see cref="Guid"/>, or <c>false</c> otherwise</returns>
        public static bool Equal(EntityReference x, Guid y)
        {
            return x.Id == y;
        }

        /// <summary>
        /// Implements equality between <see cref="EntityReference"/> and <see cref="Guid"/>
        /// </summary>
        /// <param name="x">The <see cref="Guid"/> to compare</param>
        /// <param name="y">The <see cref="EntityReference"/> to compare</param>
        /// <returns><c>true</c> if the ID in the <see cref="EntityReference"/> matches the supplied <see cref="Guid"/>, or <c>false</c> otherwise</returns>
        public static bool Equal(Guid x, EntityReference y)
        {
            return x == y.Id;
        }

        /// <summary>
        /// Implements equality between two <see cref="EntityReference"/>s
        /// </summary>
        /// <param name="x">The first <see cref="EntityReference"/> to compare</param>
        /// <param name="y">The second <see cref="EntityReference"/> to compare</param>
        /// <returns><c>true</c> if the first <see cref="EntityReference"/> matches the second <see cref="EntityReference"/>, or <c>false</c> otherwise</returns>
        public static bool Equal(EntityReference x, EntityReference y)
        {
            return x.Equals(y);
        }

        /// <summary>
        /// Implements inequality between <see cref="EntityReference"/> and <see cref="Guid"/>
        /// </summary>
        /// <param name="x">The <see cref="EntityReference"/> to compare</param>
        /// <param name="y">The <see cref="Guid"/> to compare</param>
        /// <returns><c>false</c> if the ID in the <see cref="EntityReference"/> matches the supplied <see cref="Guid"/>, or <c>true</c> otherwise</returns>
        public static bool NotEqual(EntityReference x, Guid y)
        {
            return x.Id != y;
        }

        /// <summary>
        /// Implements inequality between <see cref="EntityReference"/> and <see cref="Guid"/>
        /// </summary>
        /// <param name="x">The <see cref="Guid"/> to compare</param>
        /// <param name="y">The <see cref="EntityReference"/> to compare</param>
        /// <returns><c>false</c> if the ID in the <see cref="EntityReference"/> matches the supplied <see cref="Guid"/>, or <c>true</c> otherwise</returns>
        public static bool NotEqual(Guid x, EntityReference y)
        {
            return x != y.Id;
        }

        /// <summary>
        /// Implements inequality between two <see cref="EntityReference"/>s
        /// </summary>
        /// <param name="x">The first <see cref="EntityReference"/> to compare</param>
        /// <param name="y">The second <see cref="EntityReference"/> to compare</param>
        /// <returns><c>false</c> if the first <see cref="EntityReference"/> matches the second <see cref="EntityReference"/>, or <c>true</c> otherwise</returns>
        public static bool NotEqual(EntityReference x, EntityReference y)
        {
            return !x.Equals(y);
        }

        /// <summary>
        /// Creates an <see cref="EntityReference"/> value
        /// </summary>
        /// <param name="entityType">The logical name of the entity to reference</param>
        /// <param name="id">The unique identifier of the entity to reference</param>
        /// <returns>An <see cref="EntityReference"/> with the requested values</returns>
        public static EntityReference CreateLookup(string entityType, string id)
        {
            if (id == null)
                return null;

            return new EntityReference(entityType, new Guid(id));
        }

        /// <summary>
        /// Creates an <see cref="EntityReference"/> value
        /// </summary>
        /// <param name="entityType">The logical name of the entity to reference</param>
        /// <param name="id">The unique identifier of the entity to reference</param>
        /// <returns>An <see cref="EntityReference"/> with the requested values</returns>
        public static EntityReference CreatePrimaryKeyLookup(string entityType, Guid? id)
        {
            if (id == null)
                return null;

            return new EntityReference(entityType, id.Value);
        }

        /// <summary>
        /// Replaces all occurrences of a specified string value with another string value.
        /// </summary>
        /// <param name="input">The string expression to be searched</param>
        /// <param name="find">The substring to be found</param>
        /// <param name="replace">The replacement string</param>
        /// <returns>Replaces any instances of <paramref name="find"/> with <paramref name="replace"/> in the <paramref name="input"/></returns>
        public static string Replace(string input, string find, string replace)
        {
            if (input == null || find == null || replace == null)
                return null;

            return Regex.Replace(input, Regex.Escape(find), replace.Replace("$", "$$"), RegexOptions.IgnoreCase);
        }

        /// <summary>
        /// Checks if a multi-select picklist field contains specific values
        /// </summary>
        /// <param name="selected">The selected values</param>
        /// <param name="values">The values to check for</param>
        /// <returns><c>true</c> if the <paramref name="selected"/> values contain any of the requested <paramref name="values"/></returns>
        public static bool Contains(OptionSetValueCollection selected, int[] values)
        {
            if (selected == null)
                return false;

            return selected.Any(osv => values.Contains(osv.Value));
        }

        /// <summary>
        /// Returns the number of characters of the specified string expression, excluding trailing spaces
        /// </summary>
        /// <param name="s">The string expression to be evaluated</param>
        /// <returns></returns>
        public static int? Len(string s)
        {
            if (s == null)
                return null;

            return s.TrimEnd().Length;
        }

        /// <summary>
        /// Returns part of a character expression
        /// </summary>
        /// <param name="expression">A character expression</param>
        /// <param name="start">An integer that specifies where the returned characters start (the numbering is 1 based, meaning that the first character in the expression is 1)</param>
        /// <param name="length">A positive integer that specifies how many characters of the expression will be returned</param>
        /// <returns></returns>
        public static string Substring(string expression, int? start, int? length)
        {
            if (length < 0)
                throw new ArgumentOutOfRangeException(nameof(length));

            if (expression == null || start == null || length == null)
                return null;

            if (start < 1)
                start = 1;

            if (start > expression.Length)
                start = expression.Length;

            start--;

            if (start + length > expression.Length)
                length = expression.Length - start;

            return expression.Substring(start.Value, length.Value);
        }

        /// <summary>
        /// Removes the space character from the start and end of a string
        /// </summary>
        /// <param name="expression">A character expression where characters should be removed</param>
        /// <returns></returns>
        public static string Trim(string expression)
        {
            if (expression == null)
                return expression;

            return expression.Trim(' ');
        }

        /// <summary>
        /// Removes the space character from the start of a string
        /// </summary>
        /// <param name="expression">A character expression where characters should be removed</param>
        /// <returns></returns>
        public static string LTrim(string expression)
        {
            if (expression == null)
                return expression;

            return expression.TrimStart(' ');
        }

        /// <summary>
        /// Removes the space character from the end of a string
        /// </summary>
        /// <param name="expression">A character expression where characters should be removed</param>
        /// <returns></returns>
        public static string RTrim(string expression)
        {
            if (expression == null)
                return expression;

            return expression.TrimEnd(' ');
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

            for (var i = 0; i < parameters.Length; i++)
            {
                if (parameters[i].ParameterType != args[i].Type)
                    args[i] = Convert(args[i], parameters[i].ParameterType);
            }

            return Expression.Call(method, args);
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
        /// Applies the required conversion process to return a value of an expected type from an expression
        /// </summary>
        /// <typeparam name="T">The type of value required from an expression</typeparam>
        /// <param name="expr">The expression that is generating a value</param>
        /// <returns>An expression that converts the generated value to the required type</returns>
        public static Expression Convert<T>(Expression expr)
        {
            return Convert(expr, typeof(T));
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
}
