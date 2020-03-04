using Microsoft.VisualBasic;
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
            return DateAndTime.DateAdd(interval, number.Value, date.Value);
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

            // In-built conversions between value types
            if (expr.Type.IsValueType && type.IsValueType)
                return Expression.Convert(expr, type);

            // Parse string literals to DateTime values
            if (expr.Type == typeof(string) && type == typeof(DateTime?))
                return Expr.Call(() => ParseDateTime(Arg<string>()), expr);

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

        private static DateTime? ParseDateTime(string str)
        {
            if (str == null)
                return null;

            return DateTime.Parse(str);
        }
    }
}
