using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.Engine.Visitors;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    static class ExpressionExtensions
    {
        public static Type GetType(this TSqlFragment expr, NodeSchema schema, IDictionary<string,Type> parameterTypes)
        {
            if (expr is ColumnReferenceExpression col)
                return GetType(col, schema, parameterTypes);
            else if (expr is IdentifierLiteral guid)
                return GetType(guid, schema, parameterTypes);
            else if (expr is IntegerLiteral i)
                return GetType(i, schema, parameterTypes);
            else if (expr is MoneyLiteral money)
                return GetType(money, schema, parameterTypes);
            else if (expr is NullLiteral n)
                return GetType(n, schema, parameterTypes);
            else if (expr is NumericLiteral num)
                return GetType(num, schema, parameterTypes);
            else if (expr is RealLiteral real)
                return GetType(real, schema, parameterTypes);
            else if (expr is StringLiteral str)
                return GetType(str, schema, parameterTypes);
            else if (expr is OdbcLiteral odbc)
                return GetType(odbc, schema, parameterTypes);
            else if (expr is BooleanExpression b)
                return GetType(b, schema, parameterTypes);
            else if (expr is Microsoft.SqlServer.TransactSql.ScriptDom.BinaryExpression bin)
                return GetType(bin, schema, parameterTypes);
            else if (expr is FunctionCall func)
                return GetType(func, schema, parameterTypes);
            else if (expr is ParenthesisExpression paren)
                return GetType(paren, schema, parameterTypes);
            else if (expr is Microsoft.SqlServer.TransactSql.ScriptDom.UnaryExpression unary)
                return GetType(unary, schema, parameterTypes);
            else if (expr is VariableReference var)
                return GetType(var, schema, parameterTypes);
            else if (expr is SimpleCaseExpression simpleCase)
                return GetType(simpleCase, schema, parameterTypes);
            else if (expr is SearchedCaseExpression searchedCase)
                return GetType(searchedCase, schema, parameterTypes);
            else if (expr is ConvertCall convert)
                return GetType(convert, schema, parameterTypes);
            else if (expr is CastCall cast)
                return GetType(cast, schema, parameterTypes);
            else
                throw new NotSupportedQueryFragmentException("Unhandled expression type", expr);
        }

        public static Func<Entity, IDictionary<string, object>, object> Compile(this TSqlFragment expr, NodeSchema schema, IDictionary<string, Type> parameterTypes)
        {
            var entityParam = Expression.Parameter(typeof(Entity));
            var parameterParam = Expression.Parameter(typeof(IDictionary<string, object>));

            var expression = ToExpression(expr, schema, parameterTypes, entityParam, parameterParam);
            expression = Expr.Box(expression);

            return Expression.Lambda<Func<Entity, IDictionary<string, object>, object>>(expression, entityParam, parameterParam).Compile();
        }

        private static Expression ToExpression(this TSqlFragment expr, NodeSchema schema, IDictionary<string, Type> parameterTypes, ParameterExpression entityParam, ParameterExpression parameterParam)
        {
            if (expr is ColumnReferenceExpression col)
                return ToExpression(col, schema, parameterTypes, entityParam, parameterParam);
            else if (expr is IdentifierLiteral guid)
                return ToExpression(guid, schema, parameterTypes, entityParam, parameterParam);
            else if (expr is IntegerLiteral i)
                return ToExpression(i, schema, parameterTypes, entityParam, parameterParam);
            else if (expr is MoneyLiteral money)
                return ToExpression(money, schema, parameterTypes, entityParam, parameterParam);
            else if (expr is NullLiteral n)
                return ToExpression(n, schema, parameterTypes, entityParam, parameterParam);
            else if (expr is NumericLiteral num)
                return ToExpression(num, schema, parameterTypes, entityParam, parameterParam);
            else if (expr is RealLiteral real)
                return ToExpression(real, schema, parameterTypes, entityParam, parameterParam);
            else if (expr is StringLiteral str)
                return ToExpression(str, schema, parameterTypes, entityParam, parameterParam);
            else if (expr is OdbcLiteral odbc)
                return ToExpression(odbc, schema, parameterTypes, entityParam, parameterParam);
            else if (expr is BooleanExpression b)
                return ToExpression(b, schema, parameterTypes, entityParam, parameterParam);
            else if (expr is Microsoft.SqlServer.TransactSql.ScriptDom.BinaryExpression bin)
                return ToExpression(bin, schema, parameterTypes, entityParam, parameterParam);
            else if (expr is FunctionCall func)
                return ToExpression(func, schema, parameterTypes, entityParam, parameterParam);
            else if (expr is ParenthesisExpression paren)
                return ToExpression(paren, schema, parameterTypes, entityParam, parameterParam);
            else if (expr is Microsoft.SqlServer.TransactSql.ScriptDom.UnaryExpression unary)
                return ToExpression(unary, schema, parameterTypes, entityParam, parameterParam);
            else if (expr is VariableReference var)
                return ToExpression(var, schema, parameterTypes, entityParam, parameterParam);
            else if (expr is SimpleCaseExpression simpleCase)
                return ToExpression(simpleCase, schema, parameterTypes, entityParam, parameterParam);
            else if (expr is SearchedCaseExpression searchedCase)
                return ToExpression(searchedCase, schema, parameterTypes, entityParam, parameterParam);
            else if (expr is ConvertCall convert)
                return ToExpression(convert, schema, parameterTypes, entityParam, parameterParam);
            else if (expr is CastCall cast)
                return ToExpression(cast, schema, parameterTypes, entityParam, parameterParam);
            else
                throw new NotSupportedQueryFragmentException("Unhandled expression type", expr);
        }

        public static Type GetType(this BooleanExpression b, NodeSchema schema, IDictionary<string, Type> parameterTypes)
        {
            if (b is BooleanBinaryExpression bin)
                return GetType(bin, schema, parameterTypes);
            else if (b is BooleanComparisonExpression cmp)
                return GetType(cmp, schema, parameterTypes);
            else if (b is BooleanParenthesisExpression paren)
                return GetType(paren, schema, parameterTypes);
            else if (b is InPredicate inPred)
                return GetType(inPred, schema, parameterTypes);
            else if (b is BooleanIsNullExpression isNull)
                return GetType(isNull, schema, parameterTypes);
            else if (b is LikePredicate like)
                return GetType(like, schema, parameterTypes);
            else if (b is BooleanNotExpression not)
                return GetType(not, schema, parameterTypes);
            else
                throw new NotSupportedQueryFragmentException("Unhandled expression type", b);
        }

        public static Func<Entity, IDictionary<string, object>, bool> Compile(this BooleanExpression b, NodeSchema schema, IDictionary<string, Type> parameterTypes)
        {
            var entityParam = Expression.Parameter(typeof(Entity));
            var parameterParam = Expression.Parameter(typeof(IDictionary<string, object>));

            var expression = ToExpression(b, schema, parameterTypes, entityParam, parameterParam);
            return Expression.Lambda<Func<Entity, IDictionary<string, object>, bool>>(expression, entityParam, parameterParam).Compile();
        }

        private static Expression ToExpression(BooleanExpression b, NodeSchema schema, IDictionary<string, Type> parameterTypes, ParameterExpression entityParam, ParameterExpression parameterParam)
        {
            if (b is BooleanBinaryExpression bin)
                return ToExpression(bin, schema, parameterTypes, entityParam, parameterParam);
            else if (b is BooleanComparisonExpression cmp)
                return ToExpression(cmp, schema, parameterTypes, entityParam, parameterParam);
            else if (b is BooleanParenthesisExpression paren)
                return ToExpression(paren, schema, parameterTypes, entityParam, parameterParam);
            else if (b is InPredicate inPred)
                return ToExpression(inPred, schema, parameterTypes, entityParam, parameterParam);
            else if (b is BooleanIsNullExpression isNull)
                return ToExpression(isNull, schema, parameterTypes, entityParam, parameterParam);
            else if (b is LikePredicate like)
                return ToExpression(like, schema, parameterTypes, entityParam, parameterParam);
            else if (b is BooleanNotExpression not)
                return ToExpression(not, schema, parameterTypes, entityParam, parameterParam);
            else
                throw new NotSupportedQueryFragmentException("Unhandled expression type", b);
        }

        private static Type GetType(ColumnReferenceExpression col, NodeSchema schema, IDictionary<string, Type> parameterTypes)
        {
            var name = col.GetColumnName();

            if (!schema.ContainsColumn(name, out var normalizedName))
            {
                if (!schema.Aliases.TryGetValue(name, out var normalized))
                    throw new NotSupportedQueryFragmentException("Unknown column", col);

                throw new NotSupportedQueryFragmentException("Ambiguous column reference", col)
                {
                    Suggestion = $"Did you mean:\r\n{String.Join("\r\n", normalized.Select(c => $"* {c}"))}"
                };
            }

            return schema.Schema[normalizedName];
        }

        private static Expression ToExpression(ColumnReferenceExpression col, NodeSchema schema, IDictionary<string, Type> parameterTypes, ParameterExpression entityParam, ParameterExpression parameterParam)
        {
            var name = col.GetColumnName();

            if (!schema.ContainsColumn(name, out name))
                throw new QueryExecutionException("Unknown column");

            var type = GetType(col, schema, parameterTypes);
            var expr = Expr.Call(() => GetColumnValue(Expr.Arg<Entity>(), Expr.Arg<string>()), entityParam, Expression.Constant(name));
            return Expression.Convert(expr, type);
        }

        private static object GetColumnValue(Entity entity, string attribute)
        {
            entity.Attributes.TryGetValue(attribute, out var value);
            return value;
        }

        private static Type GetType(IdentifierLiteral guid, NodeSchema schema, IDictionary<string, Type> parameterTypes)
        {
            return typeof(SqlGuid);
        }

        private static Expression ToExpression(IdentifierLiteral guid, NodeSchema schema, IDictionary<string, Type> parameterTypes, ParameterExpression entityParam, ParameterExpression parameterParam)
        {
            return Expression.Constant(new SqlGuid(guid.Value));
        }

        private static Type GetType(IntegerLiteral i, NodeSchema schema, IDictionary<string, Type> parameterTypes)
        {
            return typeof(int);
        }

        private static Expression ToExpression(IntegerLiteral i, NodeSchema schema, IDictionary<string, Type> parameterTypes, ParameterExpression entityParam, ParameterExpression parameterParam)
        {
            return Expression.Constant(Int32.Parse(i.Value));
        }

        private static Type GetType(MoneyLiteral money, NodeSchema schema, IDictionary<string, Type> parameterTypes)
        {
            return typeof(decimal);
        }

        private static Expression ToExpression(MoneyLiteral money, NodeSchema schema, IDictionary<string, Type> parameterTypes, ParameterExpression entityParam, ParameterExpression parameterParam)
        {
            return Expression.Constant(Decimal.Parse(money.Value));
        }

        private static Type GetType(NullLiteral n, NodeSchema schema, IDictionary<string, Type> parameterTypes)
        {
            return typeof(object);
        }

        private static Expression ToExpression(NullLiteral n, NodeSchema schema, IDictionary<string, Type> parameterTypes, ParameterExpression entityParam, ParameterExpression parameterParam)
        {
            return Expression.Constant(null);
        }

        private static Type GetType(NumericLiteral num, NodeSchema schema, IDictionary<string, Type> parameterTypes)
        {
            return typeof(decimal);
        }

        private static Expression ToExpression(NumericLiteral num, NodeSchema schema, IDictionary<string, Type> parameterTypes, ParameterExpression entityParam, ParameterExpression parameterParam)
        {
            return Expression.Constant(Decimal.Parse(num.Value));
        }

        private static Type GetType(RealLiteral real, NodeSchema schema, IDictionary<string, Type> parameterTypes)
        {
            return typeof(float);
        }

        private static Expression ToExpression(RealLiteral real, NodeSchema schema, IDictionary<string, Type> parameterTypes, ParameterExpression entityParam, ParameterExpression parameterParam)
        {
            return Expression.Constant(Single.Parse(real.Value));
        }

        private static Type GetType(StringLiteral str, NodeSchema schema, IDictionary<string, Type> parameterTypes)
        {
            return typeof(string);
        }

        private static Expression ToExpression(StringLiteral str, NodeSchema schema, IDictionary<string, Type> parameterTypes, ParameterExpression entityParam, ParameterExpression parameterParam)
        {
            return Expression.Constant(str.Value);
        }

        private static Type GetType(OdbcLiteral odbc, NodeSchema schema, IDictionary<string, Type> parameterTypes)
        {
            switch (odbc.OdbcLiteralType)
            {
                case OdbcLiteralType.Date:
                case OdbcLiteralType.Timestamp:
                    return typeof(DateTime);

                case OdbcLiteralType.Guid:
                    return typeof(SqlGuid);

                default:
                    throw new NotSupportedQueryFragmentException("Unknown literal type", odbc);
            }
        }

        private static Expression ToExpression(OdbcLiteral odbc, NodeSchema schema, IDictionary<string, Type> parameterTypes, ParameterExpression entityParam, ParameterExpression parameterParam)
        {
            switch (odbc.OdbcLiteralType)
            {
                case OdbcLiteralType.Date:
                    return Expression.Constant(DateTime.ParseExact(odbc.Value, "yyyy'-'MM'-'dd", CultureInfo.CurrentCulture, DateTimeStyles.None));

                case OdbcLiteralType.Timestamp:
                    return Expression.Constant(DateTime.ParseExact(odbc.Value, "yyyy'-'MM'-'dd HH':'mm':'ss", CultureInfo.CurrentCulture, DateTimeStyles.None));

                case OdbcLiteralType.Guid:
                    return Expression.Constant(new SqlGuid(Guid.Parse(odbc.Value)));

                default:
                    throw new QueryExecutionException("Unknown literal type");
            }
        }

        private static Type GetType(BooleanComparisonExpression cmp, NodeSchema schema, IDictionary<string, Type> parameterTypes)
        {
            // Special case for field = func() where func is defined in FetchXmlConditionMethods
            if (cmp.FirstExpression is ColumnReferenceExpression col &&
                cmp.ComparisonType == BooleanComparisonType.Equals &&
                cmp.SecondExpression is FunctionCall func
                )
            {
                var paramTypes = func.Parameters.Select(p => p.GetType(schema, parameterTypes)).ToList();
                paramTypes.Insert(0, col.GetType(schema, parameterTypes));

                var fetchXmlComparison = GetMethod(typeof(FetchXmlConditionMethods), func, paramTypes.ToArray(), false);

                if (fetchXmlComparison != null)
                    return typeof(bool);
            }

            var lhs = cmp.FirstExpression.GetType(schema, parameterTypes);
            var rhs = cmp.SecondExpression.GetType(schema, parameterTypes);

            if (!SqlTypeConverter.CanMakeConsistentTypes(lhs, rhs, out var type))
                throw new NotSupportedQueryFragmentException($"No implicit conversion exists for types {lhs} and {rhs}", cmp);

            if (!typeof(IComparable).IsAssignableFrom(type))
                throw new NotSupportedQueryFragmentException($"Values of type {type} cannot be compared", cmp);

            return typeof(bool);
        }

        private static Expression ToExpression(BooleanComparisonExpression cmp, NodeSchema schema, IDictionary<string, Type> parameterTypes, ParameterExpression entityParam, ParameterExpression parameterParam)
        {
            // Special case for field = func() where func is defined in FetchXmlConditionMethods
            if (cmp.FirstExpression is ColumnReferenceExpression col &&
                cmp.ComparisonType == BooleanComparisonType.Equals &&
                cmp.SecondExpression is FunctionCall func
                )
            {
                var paramTypes = func.Parameters.Select(p => p.GetType(schema, parameterTypes)).ToList();
                paramTypes.Insert(0, col.GetType(schema, parameterTypes));

                var fetchXmlComparison = GetMethod(typeof(FetchXmlConditionMethods), func, paramTypes.ToArray(), false);

                if (fetchXmlComparison != null)
                    throw new QueryExecutionException("Custom FetchXML filter conditions must only be used where they can be folded into a FetchXML Scan operator");
            }

            var lhs = cmp.FirstExpression.ToExpression(schema, parameterTypes, entityParam, parameterParam);
            var rhs = cmp.SecondExpression.ToExpression(schema, parameterTypes, entityParam, parameterParam);

            if (!SqlTypeConverter.CanMakeConsistentTypes(lhs.Type, rhs.Type, out var type))
                throw new NotSupportedQueryFragmentException($"No implicit conversion exists for types {lhs} and {rhs}", cmp);

            if (lhs.Type != type)
            {
                lhs = Expr.Box(lhs);
                lhs = Expr.Call(() => SqlTypeConverter.ChangeType(Expr.Arg<object>(), Expr.Arg<Type>()), lhs, Expression.Constant(type));
            }

            if (rhs.Type != type)
            {
                rhs = Expr.Box(rhs);
                rhs = Expr.Call(() => SqlTypeConverter.ChangeType(Expr.Arg<object>(), Expr.Arg<Type>()), rhs, Expression.Constant(type));
            }

            lhs = Expr.Box(lhs);
            rhs = Expr.Box(rhs);

            return Expr.Call(() => BooleanComparison(Expr.Arg<object>(), Expr.Arg<object>(), Expr.Arg<BooleanComparisonType>()), lhs, rhs, Expression.Constant(cmp.ComparisonType));
        }

        private static bool BooleanComparison(object lhs, object rhs, BooleanComparisonType comparisonType)
        {
            if (lhs == null || rhs == null)
                return false;

            var comparison = StringComparer.CurrentCultureIgnoreCase.Compare(lhs, rhs);

            switch (comparisonType)
            {
                case BooleanComparisonType.Equals:
                    return comparison == 0;

                case BooleanComparisonType.GreaterThan:
                    return comparison > 0;

                case BooleanComparisonType.GreaterThanOrEqualTo:
                case BooleanComparisonType.NotLessThan:
                    return comparison >= 0;

                case BooleanComparisonType.LessThan:
                    return comparison < 0;

                case BooleanComparisonType.LessThanOrEqualTo:
                case BooleanComparisonType.NotGreaterThan:
                    return comparison <= 0;

                case BooleanComparisonType.NotEqualToBrackets:
                case BooleanComparisonType.NotEqualToExclamation:
                    return comparison != 0;

                default:
                    throw new QueryExecutionException("Unknown comparison type");
            }
        }

        private static Type GetType(BooleanBinaryExpression bin, NodeSchema schema, IDictionary<string, Type> parameterTypes)
        {
            bin.FirstExpression.GetType(schema, parameterTypes);
            bin.SecondExpression.GetType(schema, parameterTypes);

            return typeof(bool);
        }

        private static Expression ToExpression(BooleanBinaryExpression bin, NodeSchema schema, IDictionary<string, Type> parameterTypes, ParameterExpression entityParam, ParameterExpression parameterParam)
        {
            var lhs = bin.FirstExpression.ToExpression(schema, parameterTypes, entityParam, parameterParam);
            var rhs = bin.SecondExpression.ToExpression(schema, parameterTypes, entityParam, parameterParam);

            if (bin.BinaryExpressionType == BooleanBinaryExpressionType.And)
                return Expression.AndAlso(lhs, rhs);

            return Expression.OrElse(lhs, rhs);
        }

        private static Type GetType(BooleanParenthesisExpression paren, NodeSchema schema, IDictionary<string, Type> parameterTypes)
        {
            paren.Expression.GetType(schema, parameterTypes);

            return typeof(bool);
        }

        private static Expression ToExpression(BooleanParenthesisExpression paren, NodeSchema schema, IDictionary<string, Type> parameterTypes, ParameterExpression entityParam, ParameterExpression parameterParam)
        {
            return paren.Expression.ToExpression(schema, parameterTypes, entityParam, parameterParam);
        }

        private static Type GetType(Microsoft.SqlServer.TransactSql.ScriptDom.BinaryExpression bin, NodeSchema schema, IDictionary<string, Type> parameterTypes)
        {
            var lhs = bin.FirstExpression.GetType(schema, parameterTypes);
            var rhs = bin.SecondExpression.GetType(schema, parameterTypes);

            if (!SqlTypeConverter.CanMakeConsistentTypes(lhs, rhs, out var type))
                throw new NotSupportedQueryFragmentException($"No implicit conversion exists for types {lhs} and {rhs}", bin);

            var typeCategory = SqlTypeConverter.GetCategory(type);

            switch (bin.BinaryExpressionType)
            {
                case BinaryExpressionType.Add:
                    // Can be used on any numeric type except bit
                    if ((typeCategory == SqlTypeCategory.ExactNumerics || typeCategory == SqlTypeCategory.ApproximateNumerics) && type != typeof(bool))
                        return type;

                    // Addition can also be used on strings
                    if (type == typeof(string))
                        return type;
                    break;

                case BinaryExpressionType.Subtract:
                    // Can be used on any numeric type except bit
                    if ((typeCategory == SqlTypeCategory.ExactNumerics || typeCategory == SqlTypeCategory.ApproximateNumerics) && type != typeof(bool))
                        return type;
                    break;

                case BinaryExpressionType.Multiply:
                case BinaryExpressionType.Divide:
                    // Can be used on any numeric type
                    if (typeCategory == SqlTypeCategory.ExactNumerics || typeCategory == SqlTypeCategory.ApproximateNumerics)
                        return type;
                    break;

                case BinaryExpressionType.Modulo:
                    // Can be used on any exact numeric type
                    if (typeCategory == SqlTypeCategory.ExactNumerics)
                        return type;
                    break;

                case BinaryExpressionType.BitwiseAnd:
                case BinaryExpressionType.BitwiseOr:
                case BinaryExpressionType.BitwiseXor:
                    // Can be used on any integer, bit or binary type
                    if (type == typeof(int) || type == typeof(byte))
                        return type;

                    if (type == typeof(bool))
                        return typeof(byte);
                    break;
            }

            throw new NotSupportedQueryFragmentException($"Operator {bin.BinaryExpressionType} is not defined for expressions of type {type}", bin);
        }

        private static Expression ToExpression(Microsoft.SqlServer.TransactSql.ScriptDom.BinaryExpression bin, NodeSchema schema, IDictionary<string, Type> parameterTypes, ParameterExpression entityParam, ParameterExpression parameterParam)
        {
            var lhs = bin.FirstExpression.ToExpression(schema, parameterTypes, entityParam, parameterParam);
            var rhs = bin.SecondExpression.ToExpression(schema, parameterTypes, entityParam, parameterParam);

            if (!SqlTypeConverter.CanMakeConsistentTypes(lhs.Type, rhs.Type, out var type))
                throw new NotSupportedQueryFragmentException($"No implicit conversion exists for types {lhs} and {rhs}", bin);

            if (lhs.Type != type)
            {
                lhs = Expr.Box(lhs);
                lhs = Expr.Call(() => SqlTypeConverter.ChangeType(Expr.Arg<object>(), Expr.Arg<Type>()), lhs, Expression.Constant(type));
            }

            if (rhs.Type != type)
            {
                rhs = Expr.Box(rhs);
                rhs = Expr.Call(() => SqlTypeConverter.ChangeType(Expr.Arg<object>(), Expr.Arg<Type>()), rhs, Expression.Constant(type));
            }

            lhs = Expr.Box(lhs);
            rhs = Expr.Box(rhs);

            var expr = Expr.Call(() => BinaryOperation(Expr.Arg<object>(), Expr.Arg<object>(), Expr.Arg<BinaryExpressionType>(), Expr.Arg<Type>()), lhs, rhs, Expression.Constant(bin.BinaryExpressionType), Expression.Constant(type));
            return Expression.Convert(expr, type);
        }

        private static object BinaryOperation(object lhs, object rhs, BinaryExpressionType binaryExpressionType, Type type)
        {
            if (lhs == null || rhs == null)
                return null;

            switch (binaryExpressionType)
            {
                case BinaryExpressionType.Add:
                    if (type == typeof(long))
                        return (long)lhs + (long)rhs;

                    if (type == typeof(int))
                        return (int)lhs + (int)rhs;

                    if (type == typeof(decimal))
                        return (decimal)lhs + (decimal)rhs;

                    if (type == typeof(double))
                        return (double)lhs + (double)rhs;

                    if (type == typeof(float))
                        return (float)lhs + (float)rhs;

                    if (type == typeof(string))
                        return (string)lhs + (string)rhs;
                    break;

                case BinaryExpressionType.Subtract:
                    if (type == typeof(long))
                        return (long)lhs - (long)rhs;

                    if (type == typeof(int))
                        return (int)lhs - (int)rhs;

                    if (type == typeof(decimal))
                        return (decimal)lhs - (decimal)rhs;

                    if (type == typeof(double))
                        return (double)lhs - (double)rhs;

                    if (type == typeof(float))
                        return (float)lhs - (float)rhs;
                    break;

                case BinaryExpressionType.Multiply:
                    if (type == typeof(long))
                        return (long)lhs * (long)rhs;

                    if (type == typeof(int))
                        return (int)lhs * (int)rhs;

                    if (type == typeof(decimal))
                        return (decimal)lhs * (decimal)rhs;

                    if (type == typeof(bool))
                        return (bool)lhs && (bool)rhs;

                    if (type == typeof(double))
                        return (double)lhs * (double)rhs;

                    if (type == typeof(float))
                        return (float)lhs * (float)rhs;
                    break;

                case BinaryExpressionType.Divide:
                    if (type == typeof(long))
                        return (long)lhs / (long)rhs;

                    if (type == typeof(int))
                        return (int)lhs / (int)rhs;

                    if (type == typeof(decimal))
                        return (decimal)lhs / (decimal)rhs;

                    // TODO:
                    //if (type == typeof(bool))
                    //    return (bool)lhs && (bool)rhs;

                    if (type == typeof(double))
                        return (double)lhs / (double)rhs;

                    if (type == typeof(float))
                        return (float)lhs / (float)rhs;
                    break;

                case BinaryExpressionType.BitwiseAnd:
                    if (type == typeof(bool))
                    {
                        lhs = SqlTypeConverter.ChangeType<byte>(lhs);
                        rhs = SqlTypeConverter.ChangeType<byte>(rhs);
                        type = typeof(byte);
                    }

                    if (type == typeof(int))
                        return (int)lhs & (int)rhs;

                    if (type == typeof(byte))
                        return (byte)lhs & (byte)rhs;

                    break;

                case BinaryExpressionType.BitwiseOr:
                    if (type == typeof(bool))
                    {
                        lhs = SqlTypeConverter.ChangeType<byte>(lhs);
                        rhs = SqlTypeConverter.ChangeType<byte>(rhs);
                        type = typeof(byte);
                    }

                    if (type == typeof(int))
                        return (int)lhs | (int)rhs;

                    if (type == typeof(byte))
                        return (byte)lhs | (byte)rhs;

                    break;

                case BinaryExpressionType.BitwiseXor:
                    if (type == typeof(bool))
                    {
                        lhs = SqlTypeConverter.ChangeType<byte>(lhs);
                        rhs = SqlTypeConverter.ChangeType<byte>(rhs);
                        type = typeof(byte);
                    }

                    if (type == typeof(int))
                        return (int)lhs ^ (int)rhs;

                    if (type == typeof(byte))
                        return (byte)lhs ^ (byte)rhs;

                    break;

                default:
                    throw new QueryExecutionException("Unsupported operator");
            }

            throw new QueryExecutionException($"Operator {binaryExpressionType} is not defined for expressions of type {type}");
        }

        private static MethodInfo GetMethod(FunctionCall func, NodeSchema schema, IDictionary<string, Type> parameterTypes)
        {
            Type[] paramTypes;

            // Special case for DATEPART / DATEDIFF / DATEADD - first parameter looks like a field but is actually an identifier
            if (func.FunctionName.Value.Equals("DATEPART", StringComparison.OrdinalIgnoreCase) ||
                func.FunctionName.Value.Equals("DATEDIFF", StringComparison.OrdinalIgnoreCase) ||
                func.FunctionName.Value.Equals("DATEADD", StringComparison.OrdinalIgnoreCase))
            {
                paramTypes = func.Parameters
                    .Select((param, index) =>
                    {
                        if (index == 0)
                        {
                            // Check parameter is an expected datepart value
                            if (!(param is ColumnReferenceExpression col))
                                throw new NotSupportedQueryFragmentException("Expected a datepart name", param);

                            try
                            {
                                ExpressionFunctions.DatePartToInterval(col.MultiPartIdentifier.Identifiers.Single().Value);
                            }
                            catch
                            {
                                throw new NotSupportedQueryFragmentException("Expected a datepart name", param);
                            }

                            return typeof(string);
                        }

                        return param.GetType(schema, parameterTypes);
                    })
                    .ToArray();
            }
            else
            {
                paramTypes = func.Parameters
                    .Select(param => param.GetType(schema, parameterTypes))
                    .ToArray();
            }

            return GetMethod(typeof(ExpressionFunctions), func, paramTypes, true);
        }

        private static MethodInfo GetMethod(Type targetType, FunctionCall func, Type[] paramTypes, bool throwOnMissing)
        {
            // Find a method that implements this function
            var methods = targetType
                .GetMethods(BindingFlags.InvokeMethod | BindingFlags.Public | BindingFlags.Static)
                .Where(m => m.Name.Equals(func.FunctionName.Value, StringComparison.OrdinalIgnoreCase))
                .ToList();

            if (methods.Count == 0)
            {
                if (throwOnMissing)
                    throw new NotSupportedQueryFragmentException("Unknown function", func);

                return null;
            }

            // Check parameter count is correct
            var correctParameterCount = methods
                .Select(m => new { Method = m, Parameters = m.GetParameters() })
                .Where(m => m.Parameters.Length == paramTypes.Length || (m.Parameters.Length < paramTypes.Length && m.Parameters.Length > 0 && m.Parameters.Last().ParameterType.IsArray))
                .ToList();

            if (correctParameterCount.Count == 0)
                throw new NotSupportedQueryFragmentException($"Method expects {methods[0].GetParameters().Length} parameters", func);

            if (correctParameterCount.Count > 1)
                throw new NotSupportedQueryFragmentException("Ambiguous method", func);

            // Check parameter types can be converted
            var parameters = correctParameterCount[0].Parameters;
            var paramOffset = targetType == typeof(FetchXmlConditionMethods) ? 1 : 0;

            for (var i = 0; i < parameters.Length; i++)
            {
                var paramType = parameters[i].ParameterType;

                if (i == parameters.Length - 1 && paramTypes.Length > parameters.Length && paramType.IsArray)
                    paramType = paramType.GetElementType();

                if (!SqlTypeConverter.CanChangeTypeImplicit(paramTypes[i], paramType))
                    throw new NotSupportedQueryFragmentException($"Cannot convert {paramTypes[i]} to {paramType}", func.Parameters[i - paramOffset]);
            }

            for (var i = parameters.Length; i < paramTypes.Length; i++)
            {
                var paramType = parameters.Last().ParameterType.GetElementType();

                if (!SqlTypeConverter.CanChangeTypeImplicit(paramTypes[i], paramType))
                    throw new NotSupportedQueryFragmentException($"Cannot convert {paramTypes[i]} to {paramType}", func.Parameters[i - paramOffset]);
            }

            return correctParameterCount[0].Method;
        }

        private static Type GetType(this FunctionCall func, NodeSchema schema, IDictionary<string, Type> parameterTypes)
        {
            var method = GetMethod(func, schema, parameterTypes);
            return method.ReturnType;
        }

        private static Expression ToExpression(this FunctionCall func, NodeSchema schema, IDictionary<string, Type> parameterTypes, ParameterExpression entityParam, ParameterExpression parameterParam)
        {
            var method = GetMethod(func, schema, parameterTypes);

            // Get the parameter values
            Expression[] paramValues;

            // Special case for DATEPART / DATEDIFF / DATEADD - first parameter looks like a field but is actually an identifier
            if (func.FunctionName.Value.Equals("DATEPART", StringComparison.OrdinalIgnoreCase) ||
                func.FunctionName.Value.Equals("DATEDIFF", StringComparison.OrdinalIgnoreCase) ||
                func.FunctionName.Value.Equals("DATEADD", StringComparison.OrdinalIgnoreCase))
            {
                paramValues = func.Parameters
                    .Select((param, index) =>
                    {
                        if (index == 0)
                            return Expression.Constant(((ColumnReferenceExpression)param).MultiPartIdentifier.Identifiers.Single().Value);

                        return param.ToExpression(schema, parameterTypes, entityParam, parameterParam);
                    })
                    .ToArray();
            }
            else
            {
                paramValues = func.Parameters
                    .Select(param => param.ToExpression(schema, parameterTypes, entityParam, parameterParam))
                    .ToArray();
            }

            // Convert the parameters to the expected types
            var parameters = method.GetParameters();

            for (var i = 0; i < parameters.Length; i++)
            {
                if (paramValues[i].Type != parameters[i].ParameterType)
                {
                    paramValues[i] = Expr.Box(paramValues[i]);
                    paramValues[i] = Expr.Call(() => SqlTypeConverter.ChangeType(Expr.Arg<object>(), Expr.Arg<Type>()), paramValues[i], Expression.Constant(parameters[i].ParameterType));
                    paramValues[i] = Expression.Convert(paramValues[i], parameters[i].ParameterType);
                }
            }

            return Expression.Call(method, paramValues);
        }

        private static Type GetType(this ParenthesisExpression paren, NodeSchema schema, IDictionary<string, Type> parameterTypes)
        {
            return paren.Expression.GetType(schema, parameterTypes);
        }

        private static Expression ToExpression(this ParenthesisExpression paren, NodeSchema schema, IDictionary<string, Type> parameterTypes, ParameterExpression entityParam, ParameterExpression parameterParam)
        {
            return paren.Expression.ToExpression(schema, parameterTypes, entityParam, parameterParam);
        }

        private static Type GetType(this Microsoft.SqlServer.TransactSql.ScriptDom.UnaryExpression unary, NodeSchema schema, IDictionary<string, Type> parameterTypes)
        {
            var type = unary.Expression.GetType(schema, parameterTypes);
            var typeCategory = SqlTypeConverter.GetCategory(type);

            switch (unary.UnaryExpressionType)
            {
                case UnaryExpressionType.Positive:
                case UnaryExpressionType.Negative:
                    if (typeCategory != SqlTypeCategory.ExactNumerics && typeCategory != SqlTypeCategory.ApproximateNumerics)
                        throw new NotSupportedQueryFragmentException("Invalid operator for data type", unary);

                    return type;

                case UnaryExpressionType.BitwiseNot:
                    if (typeCategory != SqlTypeCategory.ExactNumerics)
                        throw new NotSupportedQueryFragmentException("Invalid operator for data type", unary);

                    if (type == typeof(decimal))
                        throw new NotSupportedQueryFragmentException("Invalid operator for data type", unary);

                    return type;

                default:
                    throw new NotSupportedQueryFragmentException("Unknown unary operator", unary);
            }
        }

        private static Expression ToExpression(this Microsoft.SqlServer.TransactSql.ScriptDom.UnaryExpression unary, NodeSchema schema, IDictionary<string, Type> parameterTypes, ParameterExpression entityParam, ParameterExpression parameterParam)
        {
            var value = unary.Expression.ToExpression(schema, parameterTypes, entityParam, parameterParam);
            var type = unary.GetType(schema, parameterTypes);

            value = Expr.Box(value);
            value = Expr.Call(() => UnaryOperation(Expr.Arg<object>(), Expr.Arg<UnaryExpressionType>()), value, Expression.Constant(unary.UnaryExpressionType));
            value = Expression.Convert(value, type);
            return value;
        }

        private static object UnaryOperation(object value, UnaryExpressionType unaryExpressionType)
        {
            if (value == null)
                return null;

            if (unaryExpressionType == UnaryExpressionType.Positive)
                return value;

            if (value is long l)
                return unaryExpressionType == UnaryExpressionType.Negative ? -l : ~l;

            if (value is int i)
                return unaryExpressionType == UnaryExpressionType.Negative ? -i : ~i;

            if (value is byte b)
                return unaryExpressionType == UnaryExpressionType.Negative ? -b : ~b;

            if (value is decimal d && unaryExpressionType == UnaryExpressionType.Negative)
                return -d;

            if (value is double m && unaryExpressionType == UnaryExpressionType.Negative)
                return -m;

            if (value is float f && unaryExpressionType == UnaryExpressionType.Negative)
                return -f;

            if (value is bool tf && unaryExpressionType == UnaryExpressionType.BitwiseNot)
                return !tf;

            throw new QueryExecutionException("Invalid operator for data type");
        }

        private static Type GetType(this InPredicate inPred, NodeSchema schema, IDictionary<string, Type> parameterTypes)
        {
            if (inPred.Subquery != null)
                throw new NotSupportedQueryFragmentException("Subquery should have been eliminated by query plan", inPred);

            var exprType = inPred.Expression.GetType(schema, parameterTypes);

            foreach (var value in inPred.Values)
            {
                var valueType = value.GetType(schema, parameterTypes);

                if (!SqlTypeConverter.CanMakeConsistentTypes(exprType, valueType, out var type))
                    throw new NotSupportedQueryFragmentException($"No implicit conversion exists for types {exprType} and {valueType}", inPred);

                if (!typeof(IComparable).IsAssignableFrom(type))
                    throw new NotSupportedQueryFragmentException($"Values of type {type} cannot be compared", inPred);
            }

            return typeof(bool);
        }

        private static Expression ToExpression(this InPredicate inPred, NodeSchema schema, IDictionary<string, Type> parameterTypes, ParameterExpression entityParam, ParameterExpression parameterParam)
        {
            if (inPred.Subquery != null)
                throw new NotSupportedQueryFragmentException("Subquery should have been eliminated by query plan", inPred);

            var exprValue = inPred.Expression.ToExpression(schema, parameterTypes, entityParam, parameterParam);

            Expression result = null;

            foreach (var value in inPred.Values)
            {
                var comparisonValue = value.ToExpression(schema, parameterTypes, entityParam, parameterParam);

                var comparison = Expression.NotEqual(comparisonValue, Expression.Constant(null));

                if (!SqlTypeConverter.CanMakeConsistentTypes(exprValue.Type, comparisonValue.Type, out var type))
                    throw new NotSupportedQueryFragmentException($"No implicit conversion exists for types {exprValue.Type} and {comparison.Type}", inPred);

                var convertedExprValue = exprValue;

                if (exprValue.Type != type)
                {
                    convertedExprValue = Expr.Box(convertedExprValue);
                    convertedExprValue = Expr.Call(() => SqlTypeConverter.ChangeType(Expr.Arg<object>(), Expr.Arg<Type>()), convertedExprValue, Expression.Constant(type));
                }

                if (comparisonValue.Type != type)
                {
                    comparisonValue = Expr.Box(comparisonValue);
                    comparisonValue = Expr.Call(() => SqlTypeConverter.ChangeType(Expr.Arg<object>(), Expr.Arg<Type>()), comparisonValue, Expression.Constant(type));
                }

                convertedExprValue = Expr.Box(convertedExprValue);
                comparisonValue = Expr.Box(comparisonValue);
                comparison = Expression.AndAlso(comparison, Expr.Call(() => StringComparer.CurrentCultureIgnoreCase.Equals(Expr.Arg<object>(), Expr.Arg<object>()), convertedExprValue, comparisonValue));

                if (result == null)
                    result = comparison;
                else
                    result = Expression.OrElse(result, comparison);
            }

            if (inPred.NotDefined)
                result = Expression.Not(result);

            result = Expression.AndAlso(Expression.NotEqual(exprValue, Expression.Constant(null)), result);

            return result;
        }

        private static Type GetType(this VariableReference var, NodeSchema schema, IDictionary<string, Type> parameterTypes)
        {
            if (parameterTypes == null || !parameterTypes.TryGetValue(var.Name, out var type))
                throw new NotSupportedQueryFragmentException("Undefined variable", var);

            return type;
        }

        private static Expression ToExpression(this VariableReference var, NodeSchema schema, IDictionary<string, Type> parameterTypes, ParameterExpression entityParam, ParameterExpression parameterParam)
        {
            var type = var.GetType(schema, parameterTypes);
            var value = Expr.Call(() => GetParameterValue(Expr.Arg<IDictionary<string, object>>(), Expr.Arg<string>()), parameterParam, Expression.Constant(var.Name));
            return Expression.Convert(value, type);
        }

        private static object GetParameterValue(IDictionary<string, object> parameterValues, string name)
        {
            if (parameterValues == null || !parameterValues.TryGetValue(name, out var value))
                throw new QueryExecutionException("Undefined variable");

            return value;
        }

        private static Type GetType(this BooleanIsNullExpression isNull, NodeSchema schema, IDictionary<string, Type> parameterTypes)
        {
            isNull.Expression.GetType(schema, parameterTypes);
            return typeof(bool);
        }

        private static Expression ToExpression(this BooleanIsNullExpression isNull, NodeSchema schema, IDictionary<string, Type> parameterTypes, ParameterExpression entityParam, ParameterExpression parameterParam)
        {
            var value = isNull.Expression.ToExpression(schema, parameterTypes, entityParam, parameterParam);

            if (isNull.IsNot)
                return Expression.NotEqual(value, Expression.Constant(null));

            return Expression.Equal(value, Expression.Constant(null));
        }

        private static Type GetType(this LikePredicate like, NodeSchema schema, IDictionary<string, Type> parameterTypes)
        {
            var valueType = like.FirstExpression.GetType(schema, parameterTypes);
            var patternType = like.SecondExpression.GetType(schema, parameterTypes);

            if (!SqlTypeConverter.CanChangeTypeImplicit(valueType, typeof(string)))
                throw new NotSupportedQueryFragmentException("Cannot convert value to string", like.FirstExpression);

            if (!SqlTypeConverter.CanChangeTypeImplicit(patternType, typeof(string)))
                throw new NotSupportedQueryFragmentException("Cannot convert pattern to string", like.SecondExpression);

            if (like.EscapeExpression != null)
            {
                var escapeType = like.EscapeExpression.GetType(schema, parameterTypes);
                if (!SqlTypeConverter.CanChangeTypeImplicit(escapeType, typeof(string)))
                    throw new NotSupportedQueryFragmentException("Cannot convert escape sequence to string", like.EscapeExpression);
            }

            return typeof(bool);
        }

        private static Expression ToExpression(this LikePredicate like, NodeSchema schema, IDictionary<string, Type> parameterTypes, ParameterExpression entityParam, ParameterExpression parameterParam)
        {
            var value = like.FirstExpression.ToExpression(schema, parameterTypes, entityParam, parameterParam);
            var pattern = like.SecondExpression.ToExpression(schema, parameterTypes, entityParam, parameterParam);
            var escape = like.EscapeExpression?.ToExpression(schema, parameterTypes, entityParam, parameterParam);

            if (value.Type != typeof(string))
            {
                value = Expr.Box(value);
                value = Expr.Call(() => SqlTypeConverter.ChangeType<string>(Expr.Arg<object>()), value);
            }

            if (pattern.Type != typeof(string))
            {
                pattern = Expr.Box(pattern);
                pattern = Expr.Call(() => SqlTypeConverter.ChangeType<string>(Expr.Arg<object>()), pattern);
            }

            if (escape != null && escape.Type != typeof(string))
            {
                escape = Expr.Box(escape);
                escape = Expr.Call(() => SqlTypeConverter.ChangeType<string>(Expr.Arg<object>()), escape);
            }

            if (pattern.NodeType == ExpressionType.Constant && (escape == null || escape.NodeType == ExpressionType.Constant))
            {
                // Do a one-off conversion to regex
                var regex = LikeToRegex((string)((ConstantExpression)pattern).Value, (string)((ConstantExpression)escape)?.Value);
                return Expr.Call(() => Like(Expr.Arg<string>(), Expr.Arg<Regex>(), Expr.Arg<bool>()), value, Expression.Constant(regex), Expression.Constant(like.NotDefined));
            }

            return Expr.Call(() => Like(Expr.Arg<string>(), Expr.Arg<string>(), Expr.Arg<string>(), Expr.Arg<bool>()), value, pattern, escape, Expression.Constant(like.NotDefined));
        }

        private static Regex LikeToRegex(string pattern, string escape)
        {
            var regexBuilder = new StringBuilder();
            regexBuilder.Append("^");

            var escaped = false;
            var inRange = false;

            foreach (var ch in pattern)
            {
                if (escape != null && ch == escape[0])
                {
                    escaped = true;
                    continue;
                }

                if (escaped)
                {
                    regexBuilder.Append(Regex.Escape(ch.ToString()));
                    escaped = false;
                    continue;
                }

                if (ch == '[' && !inRange)
                {
                    regexBuilder.Append("[");
                    inRange = true;
                    continue;
                }

                if (ch == ']' && inRange)
                {
                    regexBuilder.Append("]");
                    inRange = false;
                    continue;
                }

                if ((ch == '^' || ch == '-') && inRange)
                {
                    regexBuilder.Append(ch);
                    continue;
                }

                if (inRange)
                {
                    regexBuilder.Append(Regex.Escape(ch.ToString()));
                    continue;
                }

                if (ch == '%')
                {
                    regexBuilder.Append(".*");
                    continue;
                }

                if (ch == '_')
                {
                    regexBuilder.Append('.');
                    continue;
                }

                regexBuilder.Append(Regex.Escape(ch.ToString()));
            }

            if (escaped || inRange)
                throw new QueryExecutionException("Invalid LIKE pattern");

            regexBuilder.Append("$");

            return new Regex(regexBuilder.ToString(), RegexOptions.IgnoreCase);
        }

        private static bool Like(string value, string pattern, string escape, bool not)
        {
            if (value == null || pattern == null)
                return false;

            // Convert the LIKE pattern to a regex
            var regex = LikeToRegex(pattern, escape);
            var result = regex.IsMatch(value);

            if (not)
                result = !result;

            return result;
        }

        private static bool Like(string value, Regex pattern, bool not)
        {
            if (value == null)
                return false;

            var result = pattern.IsMatch(value);

            if (not)
                result = !result;

            return result;
        }

        private static Type GetType(SimpleCaseExpression simpleCase, NodeSchema schema, IDictionary<string, Type> parameterTypes)
        {
            var exprType = simpleCase.InputExpression.GetType(schema, parameterTypes);
            Type returnType = null;

            foreach (var when in simpleCase.WhenClauses)
            {
                var whenType = when.WhenExpression.GetType(schema, parameterTypes);

                if (!SqlTypeConverter.CanMakeConsistentTypes(exprType, whenType, out _))
                    throw new NotSupportedQueryFragmentException($"Cannot compare values of type {exprType} and {whenType}", when);

                var thenType = when.ThenExpression.GetType(schema, parameterTypes);

                if (returnType == null)
                    returnType = thenType;
                else if (!SqlTypeConverter.CanMakeConsistentTypes(returnType, thenType, out returnType))
                    throw new NotSupportedQueryFragmentException($"Cannot determine return type", simpleCase);
            }

            if (simpleCase.ElseExpression != null)
            {
                var elseType = simpleCase.ElseExpression.GetType(schema, parameterTypes);

                if (returnType == null)
                    returnType = elseType;
                else if (!SqlTypeConverter.CanMakeConsistentTypes(returnType, elseType, out returnType))
                    throw new NotSupportedQueryFragmentException($"Cannot determine return type", simpleCase);
            }

            return returnType;
        }

        private static Expression ToExpression(this SimpleCaseExpression simpleCase, NodeSchema schema, IDictionary<string, Type> parameterTypes, ParameterExpression entityParam, ParameterExpression parameterParam)
        {
            var value = simpleCase.InputExpression.ToExpression(schema, parameterTypes, entityParam, parameterParam);
            var type = simpleCase.GetType(schema, parameterTypes);

            Expression result = null;

            var elseValue = simpleCase.ElseExpression?.ToExpression(schema, parameterTypes, entityParam, parameterParam);
            if (elseValue != null)
            {
                if (elseValue.Type != type)
                {
                    elseValue = Expr.Box(elseValue);
                    elseValue = Expr.Call(() => SqlTypeConverter.ChangeType(Expr.Arg<object>(), Expr.Arg<Type>()), elseValue, Expression.Constant(type));
                }

                result = elseValue;
            }
            else
            {
                result = Expression.Constant(null);
            }

            foreach (var when in simpleCase.WhenClauses.Reverse())
            {
                var valueCopy = value;
                var whenValue = when.WhenExpression.ToExpression(schema, parameterTypes, entityParam, parameterParam);

                if (!SqlTypeConverter.CanMakeConsistentTypes(value.Type, whenValue.Type, out var caseType))
                    throw new NotSupportedQueryFragmentException($"Cannot compare values of type {value.Type} and {whenValue.Type}", when);

                if (value.Type != caseType)
                {
                    valueCopy = Expr.Box(valueCopy);
                    valueCopy = Expr.Call(() => SqlTypeConverter.ChangeType(Expr.Arg<object>(), Expr.Arg<Type>()), valueCopy, Expression.Constant(caseType));
                }

                if (whenValue.Type != caseType)
                {
                    whenValue = Expr.Box(whenValue);
                    whenValue = Expr.Call(() => SqlTypeConverter.ChangeType(Expr.Arg<object>(), Expr.Arg<Type>()), valueCopy, Expression.Constant(whenValue));
                }

                var comparison = Expression.NotEqual(value, Expression.Constant(null));
                comparison = Expression.AndAlso(comparison, Expression.NotEqual(whenValue, Expression.Constant(null)));
                comparison = Expression.AndAlso(comparison, Expr.Call(() => StringComparer.CurrentCultureIgnoreCase.Equals(Expr.Arg<object>(), Expr.Arg<object>()), valueCopy, whenValue));

                var returnValue = when.ThenExpression.ToExpression(schema, parameterTypes, entityParam, parameterParam);

                if (returnValue.Type != type)
                {
                    returnValue = Expr.Box(returnValue);
                    returnValue = Expr.Call(() => SqlTypeConverter.ChangeType(Expr.Arg<object>(), Expr.Arg<Type>()), returnValue, Expression.Constant(type));
                }

                result = Expression.Condition(comparison, returnValue, result);
            }

            return Expression.Convert(result, type);
        }

        private static Type GetType(SearchedCaseExpression searchedCase, NodeSchema schema, IDictionary<string, Type> parameterTypes)
        {
            Type returnType = null;

            foreach (var when in searchedCase.WhenClauses)
            {
                when.WhenExpression.GetType(schema, parameterTypes);

                var thenType = when.ThenExpression.GetType(schema, parameterTypes);

                if (returnType == null)
                    returnType = thenType;
                else if (!SqlTypeConverter.CanMakeConsistentTypes(returnType, thenType, out returnType))
                    throw new NotSupportedQueryFragmentException($"Cannot determine return type", searchedCase);
            }

            if (searchedCase.ElseExpression != null)
            {
                var elseType = searchedCase.ElseExpression.GetType(schema, parameterTypes);

                if (returnType == null)
                    returnType = elseType;
                else if (!SqlTypeConverter.CanMakeConsistentTypes(returnType, elseType, out returnType))
                    throw new NotSupportedQueryFragmentException($"Cannot determine return type", searchedCase);
            }

            return returnType;
        }

        private static Expression ToExpression(this SearchedCaseExpression searchedCase, NodeSchema schema, IDictionary<string, Type> parameterTypes, ParameterExpression entityParam, ParameterExpression parameterParam)
        {
            var type = searchedCase.GetType(schema, parameterTypes);

            Expression result = null;

            var elseValue = searchedCase.ElseExpression?.ToExpression(schema, parameterTypes, entityParam, parameterParam);
            if (elseValue != null)
            {
                if (elseValue.Type != type)
                {
                    elseValue = Expr.Box(elseValue);
                    elseValue = Expr.Call(() => SqlTypeConverter.ChangeType(Expr.Arg<object>(), Expr.Arg<Type>()), elseValue, Expression.Constant(type));
                }

                result = elseValue;
            }
            else
            {
                result = Expression.Constant(null);
            }

            foreach (var when in searchedCase.WhenClauses.Reverse())
            {
                var whenValue = when.WhenExpression.ToExpression(schema, parameterTypes, entityParam, parameterParam);

                var returnValue = when.ThenExpression.ToExpression(schema, parameterTypes, entityParam, parameterParam);

                if (returnValue.Type != type)
                {
                    returnValue = Expr.Box(returnValue);
                    returnValue = Expr.Call(() => SqlTypeConverter.ChangeType(Expr.Arg<object>(), Expr.Arg<Type>()), returnValue, Expression.Constant(type));
                }

                result = Expression.Condition(whenValue, returnValue, result);
            }

            return Expression.Convert(result, type);
        }

        private static Type GetType(this BooleanNotExpression not, NodeSchema schema, IDictionary<string,Type> parameterTypes)
        {
            not.Expression.GetType(schema, parameterTypes);
            return typeof(bool);
        }

        private static Expression ToExpression(this BooleanNotExpression not, NodeSchema schema, IDictionary<string, Type> parameterTypes, ParameterExpression entityParam, ParameterExpression parameterParam)
        {
            var value = not.Expression.ToExpression(schema, parameterTypes, entityParam, parameterParam);
            return Expression.Not(value);
        }

        private static readonly IDictionary<string, Type> _typeMapping = new Dictionary<string, Type>(StringComparer.OrdinalIgnoreCase)
        {
            ["bit"] = typeof(bool?),
            ["tinyint"] = typeof(byte?),
            ["smallint"] = typeof(short?),
            ["int"] = typeof(int?),
            ["bigint"] = typeof(long?),
            ["real"] = typeof(float?),
            ["float"] = typeof(double?),
            ["decimal"] = typeof(decimal?),
            ["numeric"] = typeof(decimal?),
            ["smallmoney"] = typeof(decimal?),
            ["money"] = typeof(decimal?),
            ["char"] = typeof(string),
            ["nchar"] = typeof(string),
            ["varchar"] = typeof(string),
            ["nvarchar"] = typeof(string),
            ["text"] = typeof(string),
            ["ntext"] = typeof(string),
            ["binary"] = typeof(byte[]),
            ["varbinary"] = typeof(byte[]),
            ["image"] = typeof(byte[]),
            ["rowversion"] = typeof(byte[]),
            ["date"] = typeof(DateTime?),
            ["smalldatetime"] = typeof(DateTime?),
            ["datetime"] = typeof(DateTime?),
            ["datetime2"] = typeof(DateTime?),
            ["datetimeoffset"] = typeof(DateTimeOffset?),
            ["time"] = typeof(TimeSpan?),
            ["uniqueidentifer"] = typeof(SqlGuid?)
        };

        private static Type GetType(this ConvertCall convert, NodeSchema schema, IDictionary<string, Type> parameterTypes)
        {
            var sourceType = convert.Parameter.GetType(schema, parameterTypes);
            var targetTypeName = convert.DataType.Name.BaseIdentifier.Value;

            if (!_typeMapping.TryGetValue(targetTypeName, out var targetType))
                throw new NotSupportedQueryFragmentException("Unknown type name", convert.DataType);

            if (!SqlTypeConverter.CanChangeTypeExplicit(sourceType, targetType))
                throw new NotSupportedQueryFragmentException($"No type conversion available from {sourceType} to {targetType}", convert);

            return targetType;
        }

        private static Expression ToExpression(this ConvertCall convert, NodeSchema schema, IDictionary<string, Type> parameterTypes, ParameterExpression entityParam, ParameterExpression parameterParam)
        {
            var value = convert.Parameter.ToExpression(schema, parameterTypes, entityParam, parameterParam);
            var targetTypeName = convert.DataType.Name.BaseIdentifier.Value;

            if (!_typeMapping.TryGetValue(targetTypeName, out var targetType))
                throw new NotSupportedQueryFragmentException("Unknown type name", convert.DataType);

            if (value.Type != targetType)
            {
                value = Expr.Box(value);
                value = Expr.Call(() => SqlTypeConverter.ChangeType(Expr.Arg<object>(), Expr.Arg<Type>()), value, Expression.Constant(targetType));
            }

            if (targetTypeName.Equals("date", StringComparison.OrdinalIgnoreCase))
            {
                // Remove the time part of the DateTime value
                value = Expression.Condition(Expression.Equal(value, Expression.Constant(null)), Expression.Constant(null), Expression.Convert(Expression.Property(Expression.Convert(value, typeof(DateTime)), nameof(DateTime.Date)), typeof(object)));
            }

            return Expression.Convert(value, targetType);
        }

        private static Type GetType(this CastCall cast, NodeSchema schema, IDictionary<string, Type> parameterTypes)
        {
            return GetType(new ConvertCall { Parameter = cast.Parameter, DataType = cast.DataType }, schema, parameterTypes);
        }

        private static Expression ToExpression(this CastCall cast, NodeSchema schema, IDictionary<string, Type> parameterTypes, ParameterExpression entityParam, ParameterExpression parameterParam)
        {
            return ToExpression(new ConvertCall { Parameter = cast.Parameter, DataType = cast.DataType }, schema, parameterTypes, entityParam, parameterParam);
        }

        public static BooleanExpression RemoveCondition(this BooleanExpression expr, BooleanExpression remove)
        {
            if (expr == remove)
                return null;

            if (expr is BooleanBinaryExpression binary)
            {
                if (binary.FirstExpression == remove)
                    return binary.SecondExpression;

                if (binary.SecondExpression == remove)
                    return binary.FirstExpression;

                var clone = new BooleanBinaryExpression
                {
                    BinaryExpressionType = binary.BinaryExpressionType,
                    FirstExpression = binary.FirstExpression.RemoveCondition(remove),
                    SecondExpression = binary.SecondExpression.RemoveCondition(remove)
                };

                return clone;
            }

            if (expr is BooleanParenthesisExpression paren)
            {
                if (paren.Expression == remove)
                    return null;

                return new BooleanParenthesisExpression { Expression = paren.Expression.RemoveCondition(remove) };
            }

            return expr;
        }

        public static string GetColumnName(this ColumnReferenceExpression col)
        {
            return String.Join(".", col.MultiPartIdentifier.Identifiers.Select(id => id.Value));
        }

        public static IEnumerable<string> GetColumns(this TSqlFragment fragment)
        {
            var visitor = new ColumnCollectingVisitor();
            fragment.Accept(visitor);

            return visitor.Columns
                .Select(col => col.GetColumnName())
                .Distinct();
        }

        public static IEnumerable<string> GetVariables(this TSqlFragment fragment)
        {
            var visitor = new VariableCollectingVisitor();
            fragment.Accept(visitor);

            return visitor.Variables
                .Select(var => var.Name)
                .Distinct();
        }

        public static ColumnReferenceExpression ToColumnReference(this string colName)
        {
            var col = new ColumnReferenceExpression { MultiPartIdentifier = new MultiPartIdentifier() };

            foreach (var part in colName.Split('.'))
                col.MultiPartIdentifier.Identifiers.Add(new Identifier { Value = part });

            return col;
        }

        public static bool IsConstantValueExpression(this ScalarExpression expr, NodeSchema schema, out Literal literal)
        {
            literal = expr as Literal;

            if (literal != null)
                return true;

            var columnVisitor = new ColumnCollectingVisitor();
            expr.Accept(columnVisitor);

            if (columnVisitor.Columns.Count > 0)
                return false;

            var variableVisitor = new VariableCollectingVisitor();
            expr.Accept(variableVisitor);

            if (variableVisitor.Variables.Count > 0)
                return false;

            var value = expr.Compile(schema, null)(null, null);

            if (value is int i)
                literal = new IntegerLiteral { Value = i.ToString() };
            else if (value == null)
                literal = new NullLiteral();
            else if (value is decimal dec)
                literal = new NumericLiteral { Value = dec.ToString() };
            else if (value is double dbl)
                literal = new NumericLiteral { Value = dbl.ToString() };
            else if (value is float flt)
                literal = new RealLiteral { Value = flt.ToString() };
            else if (value is string str)
                literal = new StringLiteral { Value = str };
            else if (value is DateTime dt)
                literal = new StringLiteral { Value = dt.ToString("o") };
            else if (value is EntityReference er)
                literal = new StringLiteral { Value = er.Id.ToString() };
            else if (value is SqlGuid g)
                literal = new StringLiteral { Value = g.ToString() };
            else
                return false;

            return true;
        }
    }
}
