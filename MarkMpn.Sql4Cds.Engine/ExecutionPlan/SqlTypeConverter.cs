using System;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    internal class SqlTypeConverter
    {
        internal static void MakeConsistentTypes(ref object lhs, ref object rhs)
        {
            if (lhs == null || rhs == null)
                return;

            if (lhs.GetType() == rhs.GetType())
                return;

            throw new NotImplementedException();
        }

        internal static T ChangeType<T>(object value)
        {
            return (T)ChangeType(value, typeof(T));
        }

        internal static object ChangeType(object value, Type type)
        {
            return Convert.ChangeType(value, type);
        }
    }
}