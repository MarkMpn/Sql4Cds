using System;
using System.Collections.Generic;
using System.Linq;
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
            else if (expr is BooleanExpression b)
                return GetType(b, schema, parameterTypes);
            else if (expr is BinaryExpression bin)
                return GetType(bin, schema, parameterTypes);
            else if (expr is FunctionCall func)
                return GetType(func, schema, parameterTypes);
            else if (expr is ParenthesisExpression paren)
                return GetType(paren, schema, parameterTypes);
            else if (expr is UnaryExpression unary)
                return GetType(unary, schema, parameterTypes);
            else if (expr is VariableReference var)
                return GetType(var, schema, parameterTypes);
            else
                throw new NotSupportedQueryFragmentException("Unhandled expression type", expr);
        }

        public static object GetValue(this TSqlFragment expr, Entity entity, NodeSchema schema, IDictionary<string, Type> parameterTypes, IDictionary<string,object> parameterValues)
        {
            if (expr is ColumnReferenceExpression col)
                return GetValue(col, entity, schema, parameterTypes, parameterValues);
            else if (expr is IdentifierLiteral guid)
                return GetValue(guid, entity, schema, parameterTypes, parameterValues);
            else if (expr is IntegerLiteral i)
                return GetValue(i, entity, schema, parameterTypes, parameterValues);
            else if (expr is MoneyLiteral money)
                return GetValue(money, entity, schema, parameterTypes, parameterValues);
            else if (expr is NullLiteral n)
                return GetValue(n, entity, schema, parameterTypes, parameterValues);
            else if (expr is NumericLiteral num)
                return GetValue(num, entity, schema, parameterTypes, parameterValues);
            else if (expr is RealLiteral real)
                return GetValue(real, entity, schema, parameterTypes, parameterValues);
            else if (expr is StringLiteral str)
                return GetValue(str, entity, schema, parameterTypes, parameterValues);
            else if (expr is BooleanExpression b)
                return GetValue(b, entity, schema, parameterTypes, parameterValues);
            else if (expr is BinaryExpression bin)
                return GetValue(bin, entity, schema, parameterTypes, parameterValues);
            else if (expr is FunctionCall func)
                return GetValue(func, entity, schema, parameterTypes, parameterValues);
            else if (expr is ParenthesisExpression paren)
                return GetValue(paren, entity, schema, parameterTypes, parameterValues);
            else if (expr is UnaryExpression unary)
                return GetValue(unary, entity, schema, parameterTypes, parameterValues);
            else if (expr is VariableReference var)
                return GetValue(var, entity, schema, parameterTypes, parameterValues);
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
            else
                throw new NotSupportedQueryFragmentException("Unhandled expression type", b);
        }

        public static bool GetValue(this BooleanExpression b, Entity entity, NodeSchema schema, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues)
        {
            if (b is BooleanBinaryExpression bin)
                return GetValue(bin, entity, schema, parameterTypes, parameterValues);
            else if (b is BooleanComparisonExpression cmp)
                return GetValue(cmp, entity, schema, parameterTypes, parameterValues);
            else if (b is BooleanParenthesisExpression paren)
                return GetValue(paren, entity, schema, parameterTypes, parameterValues);
            else if (b is InPredicate inPred)
                return GetValue(inPred, entity, schema, parameterTypes, parameterValues);
            else if (b is BooleanIsNullExpression isNull)
                return GetValue(isNull, entity, schema, parameterTypes, parameterValues);
            else if (b is LikePredicate like)
                return GetValue(like, entity, schema, parameterTypes, parameterValues);
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

        private static object GetValue(ColumnReferenceExpression col, Entity entity, NodeSchema schema, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues)
        {
            var name = col.GetColumnName();

            if (!schema.ContainsColumn(name, out name))
                throw new QueryExecutionException("Unknown column");

            entity.Attributes.TryGetValue(name, out var value);
            return value;
        }

        private static Type GetType(IdentifierLiteral guid, NodeSchema schema, IDictionary<string, Type> parameterTypes)
        {
            return typeof(Guid);
        }

        private static Guid GetValue(IdentifierLiteral guid, Entity entity, NodeSchema schema, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues)
        {
            return new Guid(guid.Value);
        }

        private static Type GetType(IntegerLiteral i, NodeSchema schema, IDictionary<string, Type> parameterTypes)
        {
            return typeof(int);
        }

        private static int GetValue(IntegerLiteral i, Entity entity, NodeSchema schema, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues)
        {
            return Int32.Parse(i.Value);
        }

        private static Type GetType(MoneyLiteral money, NodeSchema schema, IDictionary<string, Type> parameterTypes)
        {
            return typeof(decimal);
        }

        private static decimal GetValue(MoneyLiteral money, Entity entity, NodeSchema schema, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues)
        {
            return Decimal.Parse(money.Value);
        }

        private static Type GetType(NullLiteral n, NodeSchema schema, IDictionary<string, Type> parameterTypes)
        {
            return typeof(object);
        }

        private static object GetValue(NullLiteral n, Entity entity, NodeSchema schema, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues)
        {
            return null;
        }

        private static Type GetType(NumericLiteral num, NodeSchema schema, IDictionary<string, Type> parameterTypes)
        {
            return typeof(decimal);
        }

        private static decimal GetValue(NumericLiteral num, Entity entity, NodeSchema schema, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues)
        {
            return Decimal.Parse(num.Value);
        }

        private static Type GetType(RealLiteral real, NodeSchema schema, IDictionary<string, Type> parameterTypes)
        {
            return typeof(float);
        }

        private static float GetValue(RealLiteral real, Entity entity, NodeSchema schema, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues)
        {
            return Single.Parse(real.Value);
        }

        private static Type GetType(StringLiteral str, NodeSchema schema, IDictionary<string, Type> parameterTypes)
        {
            return typeof(string);
        }

        private static string GetValue(StringLiteral str, Entity entity, NodeSchema schema, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues)
        {
            return str.Value;
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

        private static bool GetValue(BooleanComparisonExpression cmp, Entity entity, NodeSchema schema, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues)
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
                {
                    // Get the parameter values
                    var paramValues = func.Parameters.Select(p => p.GetValue(entity, schema, parameterTypes, parameterValues)).ToList();
                    paramValues.Insert(0, col.GetValue(entity, schema, parameterTypes, parameterValues));

                    // Convert the parameters to the expected types
                    var parameters = fetchXmlComparison.GetParameters();

                    for (var i = 0; i < parameters.Length; i++)
                        paramValues[i] = SqlTypeConverter.ChangeType(paramValues[i], parameters[i].ParameterType);

                    return (bool) fetchXmlComparison.Invoke(null, paramValues.ToArray());
                }
            }

            var lhs = cmp.FirstExpression.GetValue(entity, schema, parameterTypes, parameterValues);
            var rhs = cmp.SecondExpression.GetValue(entity, schema, parameterTypes, parameterValues);

            if (lhs == null || rhs == null)
                return false;

            SqlTypeConverter.MakeConsistentTypes(ref lhs, ref rhs);

            var comparison = StringComparer.CurrentCultureIgnoreCase.Compare(lhs, rhs);

            switch (cmp.ComparisonType)
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

        private static bool GetValue(BooleanBinaryExpression bin, Entity entity, NodeSchema schema, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues)
        {
            var lhs = bin.FirstExpression.GetValue(entity, schema, parameterTypes, parameterValues);

            if (bin.BinaryExpressionType == BooleanBinaryExpressionType.And && !lhs)
                return false;

            if (bin.BinaryExpressionType == BooleanBinaryExpressionType.Or && lhs)
                return true;

            var rhs = bin.SecondExpression.GetValue(entity, schema, parameterTypes, parameterValues);
            return rhs;
        }

        private static Type GetType(BooleanParenthesisExpression paren, NodeSchema schema, IDictionary<string, Type> parameterTypes)
        {
            paren.Expression.GetType(schema, parameterTypes);

            return typeof(bool);
        }

        private static bool GetValue(BooleanParenthesisExpression paren, Entity entity, NodeSchema schema, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues)
        {
            return paren.Expression.GetValue(entity, schema, parameterTypes, parameterValues);
        }

        private static Type GetType(BinaryExpression bin, NodeSchema schema, IDictionary<string, Type> parameterTypes)
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

        private static object GetValue(BinaryExpression bin, Entity entity, NodeSchema schema, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues)
        {
            var lhs = bin.FirstExpression.GetValue(entity, schema, parameterTypes, parameterValues);
            var rhs = bin.SecondExpression.GetValue(entity, schema, parameterTypes, parameterValues);

            if (lhs == null || rhs == null)
                return null;

            var type = SqlTypeConverter.MakeConsistentTypes(ref lhs, ref rhs);

            switch (bin.BinaryExpressionType)
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

            throw new QueryExecutionException($"Operator {bin.BinaryExpressionType} is not defined for expressions of type {type}");
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
            var correctParameterCount = methods.Where(m => m.GetParameters().Length == paramTypes.Length).ToList();

            if (correctParameterCount.Count == 0)
                throw new NotSupportedQueryFragmentException($"Method expects {methods[0].GetParameters().Length} parameters", func);

            if (correctParameterCount.Count > 1)
                throw new NotSupportedQueryFragmentException("Ambiguous method", func);

            // Check parameter types can be converted
            var parameters = correctParameterCount[0].GetParameters();

            for (var i = 0; i < parameters.Length; i++)
            {
                if (!SqlTypeConverter.CanChangeType(paramTypes[i], parameters[i].ParameterType))
                    throw new NotSupportedQueryFragmentException($"Cannot convert {paramTypes[i]} to {parameters[i].ParameterType}", func.Parameters[i]);
            }

            return correctParameterCount[0];
        }

        private static Type GetType(this FunctionCall func, NodeSchema schema, IDictionary<string, Type> parameterTypes)
        {
            var method = GetMethod(func, schema, parameterTypes);
            return method.ReturnType;
        }

        private static object GetValue(this FunctionCall func, Entity entity, NodeSchema schema, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues)
        {
            var method = GetMethod(func, schema, parameterTypes);

            // Get the parameter values
            object[] paramValues;

            // Special case for DATEPART / DATEDIFF / DATEADD - first parameter looks like a field but is actually an identifier
            if (func.FunctionName.Value.Equals("DATEPART", StringComparison.OrdinalIgnoreCase) ||
                func.FunctionName.Value.Equals("DATEDIFF", StringComparison.OrdinalIgnoreCase) ||
                func.FunctionName.Value.Equals("DATEADD", StringComparison.OrdinalIgnoreCase))
            {
                paramValues = func.Parameters
                    .Select((param, index) =>
                    {
                        if (index == 0)
                            return ((ColumnReferenceExpression)param).MultiPartIdentifier.Identifiers.Single().Value;

                        return param.GetValue(entity, schema, parameterTypes, parameterValues);
                    })
                    .ToArray();
            }
            else
            {
                paramValues = func.Parameters
                    .Select(param => param.GetValue(entity, schema, parameterTypes, parameterValues))
                    .ToArray();
            }

            // Convert the parameters to the expected types
            var parameters = method.GetParameters();

            for (var i = 0; i < parameters.Length; i++)
                paramValues[i] = SqlTypeConverter.ChangeType(paramValues[i], parameters[i].ParameterType);

            return method.Invoke(null, paramValues);
        }

        private static Type GetType(this ParenthesisExpression paren, NodeSchema schema, IDictionary<string, Type> parameterTypes)
        {
            return paren.Expression.GetType(schema, parameterTypes);
        }

        private static object GetValue(this ParenthesisExpression paren, Entity entity, NodeSchema schema, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues)
        {
            return paren.Expression.GetValue(entity, schema, parameterTypes, parameterValues);
        }

        private static Type GetType(this UnaryExpression unary, NodeSchema schema, IDictionary<string, Type> parameterTypes)
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

        private static object GetValue(this UnaryExpression unary, Entity entity, NodeSchema schema, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues)
        {
            var value = unary.Expression.GetValue(entity, schema, parameterTypes, parameterValues);

            if (value == null)
                return null;

            if (unary.UnaryExpressionType == UnaryExpressionType.Positive)
                return value;

            if (value is long l)
                return unary.UnaryExpressionType == UnaryExpressionType.Negative ? -l : ~l;

            if (value is int i)
                return unary.UnaryExpressionType == UnaryExpressionType.Negative ? -i : ~i;

            if (value is byte b)
                return unary.UnaryExpressionType == UnaryExpressionType.Negative ? -b : ~b;

            if (value is decimal d && unary.UnaryExpressionType == UnaryExpressionType.Negative)
                return -d;

            if (value is double m && unary.UnaryExpressionType == UnaryExpressionType.Negative)
                return -m;

            if (value is float f && unary.UnaryExpressionType == UnaryExpressionType.Negative)
                return -f;

            if (value is bool tf && unary.UnaryExpressionType == UnaryExpressionType.BitwiseNot)
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

        private static bool GetValue(this InPredicate inPred, Entity entity, NodeSchema schema, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues)
        {
            if (inPred.Subquery != null)
                throw new NotSupportedQueryFragmentException("Subquery should have been eliminated by query plan", inPred);

            var exprValue = inPred.Expression.GetValue(entity, schema, parameterTypes, parameterValues);
            var match = false;

            if (exprValue != null)
            {
                foreach (var value in inPred.Values)
                {
                    var comparisonValue = value.GetValue(entity, schema, parameterTypes, parameterValues);

                    if (comparisonValue == null)
                        continue;

                    var convertedExprValue = exprValue;
                    SqlTypeConverter.MakeConsistentTypes(ref convertedExprValue, ref comparisonValue);

                    if (StringComparer.CurrentCultureIgnoreCase.Equals(convertedExprValue, comparisonValue))
                    {
                        match = true;
                        break;
                    }
                }
            }

            if (inPred.NotDefined)
                match = !match;

            return match;
        }

        private static Type GetType(this VariableReference var, NodeSchema schema, IDictionary<string, Type> parameterTypes)
        {
            if (parameterTypes == null || !parameterTypes.TryGetValue(var.Name, out var type))
                throw new NotSupportedQueryFragmentException("Undefined variable", var);

            return type;
        }

        private static object GetValue(this VariableReference var, Entity entity, NodeSchema schema, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues)
        {
            if (parameterValues == null || !parameterValues.TryGetValue(var.Name, out var value))
                throw new QueryExecutionException("Undefined variable");

            return value;
        }

        private static Type GetType(this BooleanIsNullExpression isNull, NodeSchema schema, IDictionary<string, Type> parameterTypes)
        {
            isNull.Expression.GetType(schema, parameterTypes);
            return typeof(bool);
        }

        private static bool GetValue(this BooleanIsNullExpression isNull, Entity entity, NodeSchema schema, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues)
        {
            var value = isNull.Expression.GetValue(entity, schema, parameterTypes, parameterValues);
            var result = value == null;

            if (isNull.IsNot)
                result = !result;

            return result;
        }

        private static Type GetType(this LikePredicate like, NodeSchema schema, IDictionary<string, Type> parameterTypes)
        {
            var valueType = like.FirstExpression.GetType(schema, parameterTypes);
            var patternType = like.SecondExpression.GetType(schema, parameterTypes);

            if (!SqlTypeConverter.CanChangeType(valueType, typeof(string)))
                throw new NotSupportedQueryFragmentException("Cannot convert value to string", like.FirstExpression);

            if (!SqlTypeConverter.CanChangeType(patternType, typeof(string)))
                throw new NotSupportedQueryFragmentException("Cannot convert pattern to string", like.SecondExpression);

            if (like.EscapeExpression != null)
            {
                var escapeType = like.EscapeExpression.GetType(schema, parameterTypes);
                if (!SqlTypeConverter.CanChangeType(escapeType, typeof(string)))
                    throw new NotSupportedQueryFragmentException("Cannot convert escape sequence to string", like.EscapeExpression);
            }

            return typeof(bool);
        }

        private static bool GetValue(this LikePredicate like, Entity entity, NodeSchema schema, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues)
        {
            var value = SqlTypeConverter.ChangeType<string>(like.FirstExpression.GetValue(entity, schema, parameterTypes, parameterValues));
            var pattern = SqlTypeConverter.ChangeType<string>(like.SecondExpression.GetValue(entity, schema, parameterTypes, parameterValues));
            var escape = SqlTypeConverter.ChangeType<string>(like.EscapeExpression?.GetValue(entity, schema, parameterTypes, parameterValues));

            if (value == null || pattern == null)
                return false;

            // Convert the LIKE pattern to a regex
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

            var result = Regex.IsMatch(value, regexBuilder.ToString(), RegexOptions.IgnoreCase);

            if (like.NotDefined)
                result = !result;

            return result;
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
    }
}
