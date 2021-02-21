using System;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    internal class SqlTypeConverter
    {
        // Abbreviated version of data type precedence from https://docs.microsoft.com/en-us/sql/t-sql/data-types/data-type-precedence-transact-sql?view=sql-server-ver15
        private static Type[] _precendenceOrder = new[]
        {
            typeof(DateTime),
            typeof(double),
            typeof(float),
            typeof(decimal),
            typeof(long),
            typeof(int),
            typeof(bool),
            typeof(Guid),
            typeof(string),
            typeof(byte[]),
        };

        public static Type MakeConsistentTypes(ref object lhs, ref object rhs)
        {
            if (lhs == null || rhs == null)
                return typeof(object);

            var lhsType = lhs.GetType();
            var rhsType = rhs.GetType();

            if (!CanMakeConsistentTypes(lhsType, rhsType, out var type))
                throw new InvalidCastException($"Cannot implicity convert objects of type {lhsType} and {rhsType} to a consistent type");

            if (lhsType != type)
                lhs = ChangeType(lhs, type);

            if (rhsType != type)
                rhs = ChangeType(rhs, type);

            return type;
        }

        public static bool CanMakeConsistentTypes(Type lhs, Type rhs, out Type consistent)
        {
            if (lhs == rhs)
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

            if (CanChangeType(lhs, targetType) && CanChangeType(rhs, targetType))
            {
                consistent = targetType;
                return true;
            }

            consistent = null;
            return false;
        }

        public static bool CanChangeType(Type from, Type to)
        {
            if (from == to)
                return true;

            return false;
        }

        public static T ChangeType<T>(object value)
        {
            return (T)ChangeType(value, typeof(T));
        }

        public static object ChangeType(object value, Type type)
        {
            return Convert.ChangeType(value, type);
        }

        public static SqlTypeCategory GetCategory(Type type)
        {
            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>))
                type = type.GetGenericArguments()[0];

            if (type == typeof(long))
                return SqlTypeCategory.ExactNumerics;

            if (type == typeof(int))
                return SqlTypeCategory.ExactNumerics;

            if (type == typeof(decimal))
                return SqlTypeCategory.ExactNumerics;

            if (type == typeof(bool))
                return SqlTypeCategory.ExactNumerics;

            if (type == typeof(double))
                return SqlTypeCategory.ApproximateNumerics;

            if (type == typeof(float))
                return SqlTypeCategory.ApproximateNumerics;

            if (type == typeof(DateTime))
                return SqlTypeCategory.DateTime;

            if (type == typeof(string))
                return SqlTypeCategory.UnicodeStrings;

            if (type == typeof(byte[]))
                return SqlTypeCategory.BinaryStrings;

            return SqlTypeCategory.Other;
        }
    }
}