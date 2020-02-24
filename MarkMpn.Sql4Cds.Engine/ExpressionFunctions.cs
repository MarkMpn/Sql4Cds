using Microsoft.VisualBasic;
using System;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.RegularExpressions;

namespace MarkMpn.Sql4Cds.Engine
{
    class ExpressionFunctions
    {
        public static bool LikeFunction(string value, string pattern)
        {
            var regex = "^" + Regex.Escape(pattern).Replace("%", ".*").Replace("_", ".") + "$";
            return Regex.IsMatch(value, regex);
        }

        public static DateTime DateAdd(string datepart, double number, DateTime date)
        {
            var interval = DatePartToInterval(datepart);
            return DateAndTime.DateAdd(interval, number, date);
        }

        public static int DateDiff(string datepart, DateTime startdate, DateTime enddate)
        {
            var interval = DatePartToInterval(datepart);
            return (int) DateAndTime.DateDiff(interval, startdate, enddate);
        }

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
                    throw new ArgumentOutOfRangeException(nameof(datepart));
            }
        }
    }

    class Expr
    {
        /// <summary>
        /// Given a lambda expression that calls a method, returns the method info.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="expression">The expression.</param>
        /// <returns></returns>
        /// <see href="http://blog.functionalfun.net/2009/10/getting-methodinfo-of-generic-method.html"/>
        public static MethodCallExpression Call<T>(Expression<Func<T>> expression, params Expression[] args)
        {
            var method = GetMethodInfo((LambdaExpression)expression);

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

        public static T Arg<T>()
        {
            throw new NotImplementedException();
        }

        public static Expression Convert<T>(Expression expr)
        {
            return Convert(expr, typeof(T));
        }

        public static Expression Convert(Expression expr, Type type)
        {
            if (expr.Type == type)
                return expr;

            if (type.IsInterface && expr.Type.GetInterfaces().Contains(type))
                return expr;

            if (expr.Type.IsValueType && type == typeof(object))
                return Expression.Convert(expr, type);

            if (expr.Type.IsValueType && type.IsValueType)
                return Expression.Convert(expr, type);

            if (expr.Type == typeof(string) && type == typeof(DateTime))
                return Expr.Call(() => DateTime.Parse(Arg<string>()), expr);

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
    }
}
