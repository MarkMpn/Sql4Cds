using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Globalization;
using System.Linq;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    internal class SqlTypeConverter
    {
        // Abbreviated version of data type precedence from https://docs.microsoft.com/en-us/sql/t-sql/data-types/data-type-precedence-transact-sql?view=sql-server-ver15
        private static readonly Type[] _precendenceOrder = new[]
        {
            typeof(DateTime),
            typeof(double),
            typeof(float),
            typeof(decimal),
            typeof(long),
            typeof(int),
            typeof(byte),
            typeof(bool),
            typeof(SqlGuid),
            typeof(string),
        };

        private static readonly DateTime MinDateTime = new DateTime(1900, 1, 1);
        private static readonly List<string> _datetimeFormats;

        static SqlTypeConverter()
        {
            // Handle different formats as per https://docs.microsoft.com/en-us/sql/t-sql/data-types/datetime-transact-sql?view=sql-server-ver15#supported-string-literal-formats-for-datetime
            // Note regional settings from https://docs.microsoft.com/en-us/sql/t-sql/statements/set-dateformat-transact-sql?view=sql-server-ver15
            // Extracted from sp_helplanguage
            var dateformat = "dmy";

            if (CultureInfo.CurrentCulture.Name == "en-US")
                dateformat = "mdy";
            else if (CultureInfo.CurrentCulture.Name.StartsWith("jp"))
                dateformat = "ymd";
            else if (CultureInfo.CurrentCulture.Name.StartsWith("se"))
                dateformat = "ymd";
            else if (CultureInfo.CurrentCulture.Name.StartsWith("hu"))
                dateformat = "ymd";
            else if (CultureInfo.CurrentCulture.Name.StartsWith("hr"))
                dateformat = "ymd";
            else if (CultureInfo.CurrentCulture.Name.StartsWith("lv"))
                dateformat = "ymd";
            else if (CultureInfo.CurrentCulture.Name.StartsWith("lt"))
                dateformat = "ymd";
            else if (CultureInfo.CurrentCulture.Name.StartsWith("cn"))
                dateformat = "ymd";
            else if (CultureInfo.CurrentCulture.Name.StartsWith("kr"))
                dateformat = "ymd";

            var baseFormats = new List<string>();

            // Numeric formats
            switch (dateformat)
            {
                case "mdy":
                    baseFormats.AddRange(ExpandDateTimeFormat("M'/'d'/'[yy]yy"));
                    baseFormats.AddRange(ExpandDateTimeFormat("M-d-[yy]yy"));
                    baseFormats.AddRange(ExpandDateTimeFormat("M.d.[yy]yy"));
                    break;

                case "myd":
                    baseFormats.AddRange(ExpandDateTimeFormat("M'/'d'/'[yy]yy"));
                    break;

                case "dmy":
                    baseFormats.AddRange(ExpandDateTimeFormat("d'/'M'/'[yy]yy"));
                    break;

                case "dym":
                    baseFormats.AddRange(ExpandDateTimeFormat("d'/'[yy]yy'/'M"));
                    break;

                case "ydm":
                    baseFormats.AddRange(ExpandDateTimeFormat("[yy]yy'/'d'/'M"));
                    break;

                case "ymd":
                    baseFormats.AddRange(ExpandDateTimeFormat("[yy]yy'/'M'/'d"));
                    break;
            }

            var timeFormats = new[]
            {
                "H':'m",
                "H':'m':'s",
                "H':'m':'s':'fff",
                "H':'m':'s'.'f",
                "Htt",
                "H tt"
            };

            _datetimeFormats = baseFormats
                .Concat(baseFormats.SelectMany(d => timeFormats.Select(t => d + " " + t)))
                .ToList();

            // Alphabetical
            _datetimeFormats.AddRange(ExpandDateTimeFormat("MMM[M] [d][,] yyyy"));
            _datetimeFormats.AddRange(ExpandDateTimeFormat("MMM[M] d[,] [yy]yy"));
            _datetimeFormats.AddRange(ExpandDateTimeFormat("MMM[M] yyyy [d]"));
            _datetimeFormats.AddRange(ExpandDateTimeFormat("[d] MMM[M][,] yyyy"));
            _datetimeFormats.AddRange(ExpandDateTimeFormat("d MMM[M][,] [yy]yy"));
            _datetimeFormats.AddRange(ExpandDateTimeFormat("d [yy]yy MMM[M]"));
            _datetimeFormats.AddRange(ExpandDateTimeFormat("[d] yyyy MMM[M]"));
            _datetimeFormats.AddRange(ExpandDateTimeFormat("yyyy MMM[M] [d]"));
            _datetimeFormats.AddRange(ExpandDateTimeFormat("yyyy [d] MMM[M]"));

            // ISO 8601
            _datetimeFormats.AddRange(ExpandDateTimeFormat("yyyy'-'MM'-'dd'T'HH':'mm':'ss['.'fff]"));

            // Unseparated
            _datetimeFormats.AddRange(ExpandDateTimeFormat("yyyyMMdd [HH':'mm':'ss]"));
            _datetimeFormats.Add("yyyyMMddHH HH':'mm':'ss'.'fff");
        }

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

        private static Type MakeNonNullable(Type type)
        {
            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>))
                return type.GetGenericArguments()[0];

            return type;
        }

        public static bool CanMakeConsistentTypes(Type lhs, Type rhs, out Type consistent)
        {
            lhs = MakeNonNullable(lhs);
            rhs = MakeNonNullable(rhs);

            if (lhs == rhs)
            {
                consistent = lhs;
                return true;
            }

            // Special case for null -> anything
            if (lhs == typeof(object))
            {
                consistent = rhs;
                return true;
            }

            if (rhs == typeof(object))
            {
                consistent = lhs;
                return true;
            }

            // Special case for string -> enum
            if (lhs == typeof(string) && rhs.IsEnum)
            {
                consistent = rhs;
                return true;
            }

            if (lhs.IsEnum && rhs == typeof(string))
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

        public static bool CanChangeTypeImplicit(Type from, Type to)
        {
            from = MakeNonNullable(from);
            to = MakeNonNullable(to);

            if (from == to)
                return true;

            // Special case for null -> anything
            if (from == typeof(object))
                return true;

            // Special case for string -> enum (from metadata types)
            if (from == typeof(string) && to.IsEnum)
                return true;

            if (Array.IndexOf(_precendenceOrder, from) == -1 ||
                Array.IndexOf(_precendenceOrder, to) == -1)
                return false;

            // Anything can be converted to/from strings
            if (from == typeof(string) || to == typeof(string))
                return true;

            // Any numeric type can be implicitly converted to any other. SQL requires a cast between decimal/numeric when precision/scale changes
            // but we don't have precision/scale as part of the data types
            if ((from == typeof(bool) || from == typeof(byte) || from == typeof(int) || from == typeof(long) || from == typeof(decimal) || from == typeof(float) || from == typeof(double)) &&
                (to == typeof(bool) || to == typeof(byte) || to == typeof(int) || to == typeof(long) || to == typeof(decimal) || to == typeof(float) || to == typeof(double)))
                return true;

            // Any numeric type can be implicitly converted to datetime
            if ((from == typeof(int) || from == typeof(long) || from == typeof(decimal) || from == typeof(float) || from == typeof(double)) &&
                to == typeof(DateTime))
                return true;

            // datetime can only be converted implicitly to string
            if (from == typeof(DateTime) && to == typeof(string))
                return true;

            return false;
        }

        public static bool CanChangeTypeExplicit(Type from, Type to)
        {
            if (CanChangeTypeImplicit(from, to))
                return true;

            // Require explicit conversion from datetime to numeric types
            if (from == typeof(DateTime) && (to == typeof(bool) || to == typeof(byte) || to == typeof(int) || to == typeof(long) || to == typeof(decimal) || to == typeof(float) || to == typeof(double)))
                return true;

            return false;
        }

        public static T ChangeType<T>(object value)
        {
            return (T)ChangeType(value, typeof(T));
        }

        public static object ChangeType(object value, Type type)
        {
            if (value == null)
                return null;

            type = MakeNonNullable(type);

            if (type == typeof(string))
            {
                if (value is DateTime date)
                    return date.ToString("yyyy-MM-dd HH:mm:ss.fff");

                return value.ToString();
            }

            if (value is string str)
            {
                if (type == typeof(SqlGuid))
                    return new SqlGuid(str);

                if (type == typeof(bool))
                    return str.Equals("true", StringComparison.OrdinalIgnoreCase) || str == "1";

                if (type == typeof(byte))
                    return Byte.Parse(str);

                if (type == typeof(int))
                    return Int32.Parse(str);

                if (type == typeof(long))
                    return Int64.Parse(str);

                if (type == typeof(decimal))
                    return Decimal.Parse(str);

                if (type == typeof(float))
                    return Single.Parse(str);

                if (type == typeof(double))
                    return Double.Parse(str);

                if (type == typeof(DateTime))
                {
                    foreach (var format in _datetimeFormats)
                    {
                        if (DateTime.TryParseExact(str, format, CultureInfo.CurrentCulture, DateTimeStyles.None, out var parsedDate))
                            return parsedDate;
                    }

                    throw new QueryExecutionException("Cannot convert string value to datetime");
                }

                if (type.IsEnum)
                    return Enum.Parse(type, str);
            }

            if (value is bool b)
            {
                if (type == typeof(byte))
                    return b ? (byte) 1 : (byte) 0;

                if (type == typeof(int))
                    return b ? 1 : 0;

                if (type == typeof(long))
                    return b ? 1L : 0L;

                if (type == typeof(decimal))
                    return b ? 1M : 0M;

                if (type == typeof(float))
                    return b ? 1F : 0F;

                if (type == typeof(double))
                    return b ? 1D : 0D;

                if (type == typeof(DateTime))
                    return MinDateTime.AddDays(b ? 1 : 0);
            }

            if (value is byte by)
            {
                if (type == typeof(bool))
                    return by != 0;

                if (type == typeof(int))
                    return (int)by;

                if (type == typeof(long))
                    return (long)by;

                if (type == typeof(decimal))
                    return (decimal)by;

                if (type == typeof(float))
                    return (float)by;

                if (type == typeof(double))
                    return (double)by;

                if (type == typeof(DateTime))
                    return MinDateTime.AddDays(by);
            }

            if (value is int i)
            {
                if (type == typeof(bool))
                    return i != 0;

                if (type == typeof(byte))
                    return (byte)i;

                if (type == typeof(long))
                    return (long)i;

                if (type == typeof(decimal))
                    return (decimal)i;

                if (type == typeof(float))
                    return (float)i;

                if (type == typeof(double))
                    return (double)i;

                if (type == typeof(DateTime))
                    return MinDateTime.AddDays(i);
            }

            if (value is long l)
            {
                if (type == typeof(bool))
                    return l != 0;

                if (type == typeof(byte))
                    return (byte)l;

                if (type == typeof(int))
                    return (int)l;

                if (type == typeof(decimal))
                    return (decimal)l;

                if (type == typeof(float))
                    return (float)l;

                if (type == typeof(double))
                    return (double)l;

                if (type == typeof(DateTime))
                    return MinDateTime.AddDays(l);
            }

            if (value is decimal dec)
            {
                if (type == typeof(bool))
                    return dec != 0;

                if (type == typeof(byte))
                    return (byte)dec;

                if (type == typeof(int))
                    return (int)dec;

                if (type == typeof(long))
                    return (long)dec;

                if (type == typeof(float))
                    return (float)dec;

                if (type == typeof(double))
                    return (double)dec;

                if (type == typeof(DateTime))
                    return MinDateTime.AddDays((double)dec);
            }

            if (value is float f)
            {
                if (type == typeof(bool))
                    return f != 0;

                if (type == typeof(byte))
                    return (byte)f;

                if (type == typeof(int))
                    return (int)f;

                if (type == typeof(long))
                    return (long)f;

                if (type == typeof(decimal))
                    return (decimal)f;

                if (type == typeof(double))
                    return (double)f;

                if (type == typeof(DateTime))
                    return MinDateTime.AddDays(f);
            }

            if (value is DateTime dt)
                value = (dt - MinDateTime).TotalDays;

            if (value is double dbl)
            {
                if (type == typeof(bool))
                    return dbl != 0;

                if (type == typeof(byte))
                    return (byte)dbl;

                if (type == typeof(int))
                    return (int)dbl;

                if (type == typeof(long))
                    return (long)dbl;

                if (type == typeof(decimal))
                    return (decimal)dbl;

                if (type == typeof(float))
                    return (float)dbl;

                if (type == typeof(DateTime))
                    return MinDateTime.AddDays(dbl);
            }

            return Convert.ChangeType(value, type);
        }

        private static IEnumerable<string> ExpandDateTimeFormat(string format)
        {
            var parts = new List<string>();
            var options = new List<bool>();

            var partStart = -1;
            var optional = false;
            for (var i = 0; i < format.Length; i++)
            {
                if (format[i] == '[' || format[i] == ']')
                {
                    if (partStart != -1)
                    {
                        parts.Add(format.Substring(partStart, i - partStart));
                        options.Add(optional);
                    }

                    partStart = i + 1;
                    optional = format[i] == '[';
                }

                if (partStart == -1)
                    partStart = 0;
            }

            if (partStart < format.Length)
            {
                parts.Add(format.Substring(partStart));
                options.Add(false);
            }

            var combinations = new List<int>();
            combinations.AddRange(parts.Select(p => 0));

            var count = Math.Pow(2, options.Count(o => o));

            for (var combination = 0; combination < count; combination++)
            {
                yield return String.Join("", parts.Select((p, i) =>
                {
                    if (combinations[i] == 0)
                        return p;

                    return String.Empty;
                })).Trim();

                for (var i = 0; i < parts.Count; i++)
                {
                    if (combinations[i] == 0 && options[i])
                    {
                        combinations[i] = 1;
                        break;
                    }

                    if (combinations[i] == 1)
                    {
                        combinations[i] = 0;
                    }
                }
            }
        }

        public static SqlTypeCategory GetCategory(Type type)
        {
            type = MakeNonNullable(type);

            if (type == typeof(long))
                return SqlTypeCategory.ExactNumerics;

            if (type == typeof(int))
                return SqlTypeCategory.ExactNumerics;

            if (type == typeof(byte))
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