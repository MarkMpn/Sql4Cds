using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using Microsoft.Crm.Sdk.Messages;

namespace MarkMpn.Sql4Cds.Engine
{
    public struct SqlSmallDateTime : INullable, IComparable
    {
        private readonly DateTime? _dt;

        public SqlSmallDateTime(DateTime? dt)
        {
            if (dt == null)
            {
                _dt = dt;
            }
            else
            {
                // Value is rounded to the nearest minute
                // https://learn.microsoft.com/en-us/sql/t-sql/functions/dateadd-transact-sql?view=sql-server-ver16#return-values-for-a-smalldatetime-date-and-a-second-or-fractional-seconds-datepart
                _dt = new DateTime(dt.Value.Year, dt.Value.Month, dt.Value.Day, dt.Value.Hour, dt.Value.Minute, 0);

                if (dt.Value.TimeOfDay.Seconds >= 30)
                    _dt = _dt.Value.AddMinutes(1);
            }
        }

        public static readonly SqlSmallDateTime Null = new SqlSmallDateTime(null);

        public static readonly SqlSmallDateTime MinValue = new SqlSmallDateTime(new DateTime(1900, 1, 1));

        public static readonly SqlSmallDateTime MaxValue = new SqlSmallDateTime(new DateTime(2079, 6, 6, 23, 59, 0));

        public bool IsNull => _dt == null;

        public int CompareTo(object obj)
        {
            var value = (SqlSmallDateTime)obj;

            if (IsNull)
            {
                if (!value.IsNull)
                {
                    return -1;
                }
                return 0;
            }

            if (value.IsNull)
            {
                return 1;
            }

            if (this < value)
            {
                return -1;
            }

            if (this > value)
            {
                return 1;
            }

            return 0;
        }

        public override int GetHashCode()
        {
            return _dt?.GetHashCode() ?? 0;
        }

        public override bool Equals(object obj)
        {
            if (!(obj is SqlSmallDateTime dt))
                return false;

            return _dt == dt._dt;
        }

        public DateTime Value => _dt.Value;

        public static SqlBoolean operator ==(SqlSmallDateTime x, SqlSmallDateTime y) => x.IsNull || y.IsNull ? SqlBoolean.Null : x._dt == y._dt;

        public static SqlBoolean operator !=(SqlSmallDateTime x, SqlSmallDateTime y) => !(x == y);

        public static SqlBoolean operator <(SqlSmallDateTime x, SqlSmallDateTime y) => x.IsNull || y.IsNull ? SqlBoolean.Null : x._dt < y._dt;

        public static SqlBoolean operator >(SqlSmallDateTime x, SqlSmallDateTime y) => x.IsNull || y.IsNull ? SqlBoolean.Null : x._dt > y._dt;

        public static SqlBoolean operator <=(SqlSmallDateTime x, SqlSmallDateTime y) => x.IsNull || y.IsNull ? SqlBoolean.Null : x._dt <= y._dt;

        public static SqlBoolean operator >=(SqlSmallDateTime x, SqlSmallDateTime y) => x.IsNull || y.IsNull ? SqlBoolean.Null : x._dt >= y._dt;

        public static implicit operator SqlDateTime(SqlSmallDateTime dt)
        {
            if (dt.IsNull)
                return SqlDateTime.Null;

            return dt._dt.Value;
        }

        public static implicit operator SqlSmallDateTime(SqlDateTime dt)
        {
            if (dt.IsNull)
                return Null;

            return new SqlSmallDateTime(dt.Value);
        }

        public static implicit operator SqlDateTime2(SqlSmallDateTime dt)
        {
            if (dt.IsNull)
                return SqlDateTime2.Null;

            return new SqlDateTime2(dt.Value);
        }

        public static implicit operator SqlSmallDateTime(SqlDateTime2 dt)
        {
            if (dt.IsNull)
                return Null;

            return new SqlSmallDateTime(dt.Value);
        }

        public static implicit operator SqlDateTimeOffset(SqlSmallDateTime dt)
        {
            if (dt.IsNull)
                return SqlDateTimeOffset.Null;

            return new SqlDateTimeOffset(dt.Value);
        }

        public static implicit operator SqlSmallDateTime(SqlDateTimeOffset dt)
        {
            if (dt.IsNull)
                return Null;

            return new SqlSmallDateTime(dt.Value.DateTime);
        }

        public static implicit operator SqlDate(SqlSmallDateTime dt)
        {
            if (dt.IsNull)
                return SqlDate.Null;

            return new SqlDate(dt.Value);
        }

        public static implicit operator SqlSmallDateTime(SqlDate dt)
        {
            if (dt.IsNull)
                return Null;

            return new SqlSmallDateTime(dt.Value);
        }

        public static implicit operator SqlTime(SqlSmallDateTime dt)
        {
            if (dt.IsNull)
                return SqlTime.Null;

            return new SqlTime(dt.Value.TimeOfDay);
        }

        public static implicit operator SqlSmallDateTime(SqlTime dt)
        {
            if (dt.IsNull)
                return Null;

            return new SqlSmallDateTime(new DateTime(1900, 1, 1) + dt.Value);
        }
    }

    public struct SqlDate : INullable, IComparable
    {
        private readonly DateTime? _dt;

        public SqlDate(DateTime? dt)
        {
            _dt = dt;
        }

        public static readonly SqlDate Null = new SqlDate(null);

        public bool IsNull => _dt == null;

        public int CompareTo(object obj)
        {
            var value = (SqlDate)obj;

            if (IsNull)
            {
                if (!value.IsNull)
                {
                    return -1;
                }
                return 0;
            }

            if (value.IsNull)
            {
                return 1;
            }

            if (this < value)
            {
                return -1;
            }

            if (this > value)
            {
                return 1;
            }

            return 0;
        }

        public override int GetHashCode()
        {
            return _dt?.GetHashCode() ?? 0;
        }

        public override bool Equals(object obj)
        {
            if (!(obj is SqlDate dt))
                return false;

            return _dt == dt._dt;
        }

        public DateTime Value => _dt.Value;

        public static SqlBoolean operator ==(SqlDate x, SqlDate y) => x.IsNull || y.IsNull ? SqlBoolean.Null : x._dt == y._dt;

        public static SqlBoolean operator !=(SqlDate x, SqlDate y) => !(x == y);

        public static SqlBoolean operator <(SqlDate x, SqlDate y) => x.IsNull || y.IsNull ? SqlBoolean.Null : x._dt < y._dt;

        public static SqlBoolean operator >(SqlDate x, SqlDate y) => x.IsNull || y.IsNull ? SqlBoolean.Null : x._dt > y._dt;

        public static SqlBoolean operator <=(SqlDate x, SqlDate y) => x.IsNull || y.IsNull ? SqlBoolean.Null : x._dt <= y._dt;

        public static SqlBoolean operator >=(SqlDate x, SqlDate y) => x.IsNull || y.IsNull ? SqlBoolean.Null : x._dt >= y._dt;

        public static implicit operator SqlDateTime(SqlDate dt)
        {
            if (dt.IsNull)
                return SqlDateTime.Null;

            return dt._dt.Value;
        }

        public static implicit operator SqlDate(SqlDateTime dt)
        {
            if (dt.IsNull)
                return Null;

            return new SqlDate(dt.Value.Date);
        }

        public static implicit operator SqlDate(DateTime? dt)
        {
            return new SqlDate(dt);
        }

        public override string ToString()
        {
            return _dt?.ToString() ?? "Null";
        }

        /// <summary>
        /// Converts a string literal to the equivalent <see cref="SqlDate"/> value
        /// </summary>
        /// <see href="https://learn.microsoft.com/en-us/sql/t-sql/data-types/date-transact-sql?view=sql-server-ver16#supported-string-literal-formats-for-date"/>
        /// <param name="value">The string literal to parse</param>
        /// <param name="dateFormat">The current DATEFORMAT setting to control the expected formats</param>
        /// <param name="date">The parsed version of the date</param>
        /// <returns><see langword="true"/> if the <paramref name="value"/> was parsed successfully, or <see langword="false"/> otherwise</returns>
        internal static bool TryParse(SqlString value, DateFormat dateFormat, out SqlDate date)
        {
            return SqlDateParsing.TryParse(value, dateFormat, out date);
        }
    }

    public struct SqlTime : INullable, IComparable
    {
        private readonly TimeSpan? _ts;
        private static readonly DateTime _defaultDate = new DateTime(1900, 1, 1);

        public SqlTime(TimeSpan? ts)
        {
            _ts = ts;
        }

        public static readonly SqlTime Null = new SqlTime(null);

        public bool IsNull => _ts == null;

        public int CompareTo(object obj)
        {
            var value = (SqlTime)obj;

            if (IsNull)
            {
                if (!value.IsNull)
                {
                    return -1;
                }
                return 0;
            }

            if (value.IsNull)
            {
                return 1;
            }

            if (this < value)
            {
                return -1;
            }

            if (this > value)
            {
                return 1;
            }

            return 0;
        }

        public override int GetHashCode()
        {
            return _ts?.GetHashCode() ?? 0;
        }

        public override bool Equals(object obj)
        {
            if (!(obj is SqlTime dt))
                return false;

            return _ts == dt._ts;
        }

        public TimeSpan Value => _ts.Value;

        public static SqlBoolean operator ==(SqlTime x, SqlTime y) => x.IsNull || y.IsNull ? SqlBoolean.Null : x._ts == y._ts;

        public static SqlBoolean operator !=(SqlTime x, SqlTime y) => !(x == y);

        public static SqlBoolean operator <(SqlTime x, SqlTime y) => x.IsNull || y.IsNull ? SqlBoolean.Null : x._ts < y._ts;

        public static SqlBoolean operator >(SqlTime x, SqlTime y) => x.IsNull || y.IsNull ? SqlBoolean.Null : x._ts > y._ts;

        public static SqlBoolean operator <=(SqlTime x, SqlTime y) => x.IsNull || y.IsNull ? SqlBoolean.Null : x._ts <= y._ts;

        public static SqlBoolean operator >=(SqlTime x, SqlTime y) => x.IsNull || y.IsNull ? SqlBoolean.Null : x._ts >= y._ts;

        public static implicit operator SqlDateTime(SqlTime dt)
        {
            if (dt.IsNull)
                return SqlDateTime.Null;

            return _defaultDate + dt._ts.Value;
        }
        /*
        public static implicit operator SqlDateTime2(SqlTime dt)
        {
            if (dt.IsNull)
                return SqlDateTime2.Null;

            return _defaultDate + dt._ts.Value;
        }

        public static implicit operator SqlDateTimeOffset(SqlTime dt)
        {
            if (dt.IsNull)
                return SqlDateTimeOffset.Null;

            return (DateTimeOffset)(_defaultDate + dt._ts.Value);
        }
        */
        public static implicit operator SqlTime(SqlDateTime dt)
        {
            if (dt.IsNull)
                return Null;

            return new SqlTime(dt.Value.TimeOfDay);
        }
        /*
        public static implicit operator SqlTime(SqlDateTime2 dt)
        {
            if (dt.IsNull)
                return Null;

            return new SqlTime(dt.Value.TimeOfDay);
        }

        public static implicit operator SqlTime(SqlDateTimeOffset dt)
        {
            if (dt.IsNull)
                return Null;

            return new SqlTime(dt.Value.TimeOfDay);
        }
        */
        public static implicit operator SqlTime(SqlString str)
        {
            return (SqlTime)(SqlDateTime)str;
        }

        public static implicit operator SqlTime(TimeSpan? ts)
        {
            return new SqlTime(ts);
        }

        public override string ToString()
        {
            return _ts?.ToString() ?? "Null";
        }
    }

    public struct SqlDateTime2 : INullable, IComparable
    {
        private readonly DateTime? _dt;
        private static readonly string[] x_DateTimeFormats = new string[8] { "MMM d yyyy hh:mm:ss:ffftt", "MMM d yyyy hh:mm:ss:fff", "d MMM yyyy hh:mm:ss:ffftt", "d MMM yyyy hh:mm:ss:fff", "hh:mm:ss:ffftt", "hh:mm:ss:fff", "yyMMdd", "yyyyMMdd" };
        private static readonly DateTime _defaultDate = new DateTime(1900, 1, 1);

        public SqlDateTime2(DateTime? dt)
        {
            _dt = dt;
        }

        public static readonly SqlDateTime2 Null = new SqlDateTime2(null);

        public bool IsNull => _dt == null;

        public int CompareTo(object obj)
        {
            var value = (SqlDateTime2)obj;

            if (IsNull)
            {
                if (!value.IsNull)
                {
                    return -1;
                }
                return 0;
            }

            if (value.IsNull)
            {
                return 1;
            }

            if (this < value)
            {
                return -1;
            }

            if (this > value)
            {
                return 1;
            }

            return 0;
        }

        public override int GetHashCode()
        {
            return _dt?.GetHashCode() ?? 0;
        }

        public override bool Equals(object obj)
        {
            if (!(obj is SqlDateTime2 value))
                return false;

            return _dt == value._dt;
        }

        public DateTime Value => _dt.Value;

        public static SqlBoolean operator ==(SqlDateTime2 x, SqlDateTime2 y) => x.IsNull || y.IsNull ? SqlBoolean.Null : x._dt == y._dt;

        public static SqlBoolean operator !=(SqlDateTime2 x, SqlDateTime2 y) => !(x == y);

        public static SqlBoolean operator <(SqlDateTime2 x, SqlDateTime2 y) => x.IsNull || y.IsNull ? SqlBoolean.Null : x._dt < y._dt;

        public static SqlBoolean operator >(SqlDateTime2 x, SqlDateTime2 y) => x.IsNull || y.IsNull ? SqlBoolean.Null : x._dt > y._dt;

        public static SqlBoolean operator <=(SqlDateTime2 x, SqlDateTime2 y) => x.IsNull || y.IsNull ? SqlBoolean.Null : x._dt <= y._dt;

        public static SqlBoolean operator >=(SqlDateTime2 x, SqlDateTime2 y) => x.IsNull || y.IsNull ? SqlBoolean.Null : x._dt >= y._dt;

        public static implicit operator SqlDateTime(SqlDateTime2 dt)
        {
            if (dt.IsNull)
                return SqlDateTime.Null;

            return (SqlDateTime)dt._dt.Value;
        }

        public static implicit operator SqlDate(SqlDateTime2 dt)
        {
            if (dt.IsNull)
                return SqlDate.Null;

            return (SqlDate)dt._dt.Value.Date;
        }

        public static implicit operator SqlTime(SqlDateTime2 dt)
        {
            if (dt.IsNull)
                return SqlTime.Null;

            return (SqlTime)dt._dt.Value.TimeOfDay;
        }
        /*
        public static implicit operator SqlDateTimeOffset(SqlDateTime2 dt)
        {
            if (dt.IsNull)
                return SqlDateTimeOffset.Null;

            return (DateTimeOffset)dt._dt.Value;
        }
        */
        public static implicit operator SqlDateTime2(SqlDateTime dt)
        {
            if (dt.IsNull)
                return Null;

            return new SqlDateTime2(dt.Value);
        }

        public static implicit operator SqlDateTime2(SqlDate dt)
        {
            if (dt.IsNull)
                return Null;

            return new SqlDateTime2(dt.Value);
        }

        public static implicit operator SqlDateTime2(SqlTime dt)
        {
            if (dt.IsNull)
                return Null;

            return new SqlDateTime2(_defaultDate + dt.Value);
        }
        /*
        public static implicit operator SqlDateTime2(SqlDateTimeOffset dt)
        {
            if (dt.IsNull)
                return Null;

            return new SqlDateTime2(dt.Value.DateTime);
        }
        */
        public static implicit operator SqlDateTime2(DateTime? dt)
        {
            return new SqlDateTime2(dt);
        }

        public static implicit operator SqlDateTime2(SqlString str)
        {
            if (str.IsNull)
                return Null;

            DateTime value;
            try
            {
                value = DateTime.Parse(str.Value, CultureInfo.InvariantCulture);
            }
            catch (FormatException)
            {
                DateTimeFormatInfo provider = (DateTimeFormatInfo)Thread.CurrentThread.CurrentCulture.GetFormat(typeof(DateTimeFormatInfo));
                value = DateTime.ParseExact(str.Value, x_DateTimeFormats, provider, DateTimeStyles.AllowWhiteSpaces);
            }

            return new SqlDateTime2((DateTime?)value);
        }

        public override string ToString()
        {
            return _dt?.ToString() ?? "Null";
        }
    }

    public struct SqlDateTimeOffset : INullable, IComparable
    {
        private readonly DateTimeOffset? _dt;
        private static readonly DateTime _defaultDate = new DateTime(1900, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        public SqlDateTimeOffset(DateTimeOffset? dt)
        {
            _dt = dt;
        }

        public static readonly SqlDateTimeOffset Null = new SqlDateTimeOffset(null);

        public bool IsNull => _dt == null;

        public int CompareTo(object obj)
        {
            var value = (SqlDateTimeOffset)obj;

            if (IsNull)
            {
                if (!value.IsNull)
                {
                    return -1;
                }
                return 0;
            }

            if (value.IsNull)
            {
                return 1;
            }

            if (this < value)
            {
                return -1;
            }

            if (this > value)
            {
                return 1;
            }

            return 0;
        }

        public override int GetHashCode()
        {
            return _dt?.GetHashCode() ?? 0;
        }

        public override bool Equals(object obj)
        {
            if (!(obj is SqlDateTimeOffset dt))
                return false;

            return _dt == dt._dt;
        }

        public DateTimeOffset Value => _dt.Value;

        public static SqlBoolean operator ==(SqlDateTimeOffset x, SqlDateTimeOffset y) => x.IsNull || y.IsNull ? SqlBoolean.Null : x._dt == y._dt;

        public static SqlBoolean operator !=(SqlDateTimeOffset x, SqlDateTimeOffset y) => !(x._dt != y._dt);

        public static SqlBoolean operator <(SqlDateTimeOffset x, SqlDateTimeOffset y) => x.IsNull || y.IsNull ? SqlBoolean.Null : x._dt < y._dt;

        public static SqlBoolean operator >(SqlDateTimeOffset x, SqlDateTimeOffset y) => x.IsNull || y.IsNull ? SqlBoolean.Null : x._dt > y._dt;

        public static SqlBoolean operator <=(SqlDateTimeOffset x, SqlDateTimeOffset y) => x.IsNull || y.IsNull ? SqlBoolean.Null : x._dt <= y._dt;

        public static SqlBoolean operator >=(SqlDateTimeOffset x, SqlDateTimeOffset y) => x.IsNull || y.IsNull ? SqlBoolean.Null : x._dt >= y._dt;

        public static implicit operator SqlDateTime(SqlDateTimeOffset dt)
        {
            if (dt.IsNull)
                return SqlDateTime.Null;

            return (SqlDateTime)dt._dt.Value.DateTime;
        }

        public static implicit operator SqlDate(SqlDateTimeOffset dt)
        {
            if (dt.IsNull)
                return SqlDateTime.Null;

            return (SqlDate)dt._dt.Value.Date;
        }

        public static implicit operator SqlTime(SqlDateTimeOffset dt)
        {
            if (dt.IsNull)
                return SqlTime.Null;

            return (SqlTime)dt._dt.Value.TimeOfDay;
        }

        public static implicit operator SqlDateTime2(SqlDateTimeOffset dt)
        {
            if (dt.IsNull)
                return SqlDateTime.Null;

            return (SqlDateTime2)dt._dt.Value.DateTime;
        }

        public static implicit operator SqlDateTimeOffset(SqlDateTime dt)
        {
            if (dt.IsNull)
                return Null;

            return new SqlDateTimeOffset(DateTime.SpecifyKind(dt.Value, DateTimeKind.Utc));
        }

        public static implicit operator SqlDateTimeOffset(SqlDate dt)
        {
            if (dt.IsNull)
                return Null;

            return new SqlDateTimeOffset(DateTime.SpecifyKind(dt.Value, DateTimeKind.Utc));
        }

        public static implicit operator SqlDateTimeOffset(SqlTime dt)
        {
            if (dt.IsNull)
                return Null;

            return new SqlDateTimeOffset(_defaultDate + dt.Value);
        }

        public static implicit operator SqlDateTimeOffset(SqlDateTime2 dt)
        {
            if (dt.IsNull)
                return Null;

            return new SqlDateTimeOffset(DateTime.SpecifyKind(dt.Value, DateTimeKind.Utc));
        }

        public static implicit operator SqlDateTimeOffset(SqlString str)
        {
            if (str.IsNull)
                return SqlDateTimeOffset.Null;

            var dto = DateTimeOffset.Parse(str.Value);
            return new SqlDateTimeOffset(dto);
        }

        public static implicit operator SqlDateTimeOffset(DateTimeOffset? dt)
        {
            return new SqlDateTimeOffset(dt);
        }

        public override string ToString()
        {
            return _dt?.ToString() ?? "Null";
        }
    }

    /// <summary>
    /// Sets the DATEFORMAT that is used to parse string literals into dates
    /// </summary>
    /// <see href="https://learn.microsoft.com/en-us/sql/t-sql/statements/set-dateformat-transact-sql?view=sql-server-ver16"/>
    enum DateFormat
    {
        mdy,
        dmy,
        ymd,
        ydm,
        myd,
        dym
    }

    /// <summary>
    /// Provides methods to parse string literals into various date/time types
    /// </summary>
    /// <remarks>
    /// The built-in conversion for strings to SqlDateTime does not respect the DATEFORMAT setting. The
    /// conversion from strings to the other date/time/datetime2/datetimeoffset types also support a different
    /// set of formats, so we need to implement them here. Those types use a consistent set of formats between
    /// them, so we implement them here in the most precise format (datetimeoffset) and then down-cast the result
    /// to the other types as necessary.
    /// </remarks>
    static class SqlDateParsing
    {
        /// <summary>
        /// Converts a string literal to the equivalent <see cref="SqlDate"/> value
        /// </summary>
        /// <see href="https://learn.microsoft.com/en-us/sql/t-sql/data-types/date-transact-sql?view=sql-server-ver16#supported-string-literal-formats-for-date"/>
        /// <param name="value">The string literal to parse</param>
        /// <param name="dateFormat">The current DATEFORMAT setting to control the expected formats</param>
        /// <param name="date">The parsed version of the date</param>
        /// <returns><see langword="true"/> if the <paramref name="value"/> was parsed successfully, or <see langword="false"/> otherwise</returns>
        public static bool TryParse(SqlString value, DateFormat dateFormat, out SqlDate date)
        {
            var ret = TryParse(value, dateFormat, out SqlDateTimeOffset dto);
            date = dto;
            return ret;
        }

        /// <summary>
        /// Converts a string literal to the equivalent <see cref="SqlDateTime2"/> value
        /// </summary>
        /// <see href="https://learn.microsoft.com/en-us/sql/t-sql/data-types/datetime2-transact-sql?view=sql-server-ver16#supported-string-literal-formats-for-datetime2"/>
        /// <param name="value">The string literal to parse</param>
        /// <param name="dateFormat">The current DATEFORMAT setting to control the expected formats</param>
        /// <param name="dateTime2">The parsed version of the datetime</param>
        /// <returns><see langword="true"/> if the <paramref name="value"/> was parsed successfully, or <see langword="false"/> otherwise</returns>
        public static bool TryParse(SqlString value, DateFormat dateFormat, out SqlDateTime2 dateTime2)
        {
            var ret = TryParse(value, dateFormat, out SqlDateTimeOffset dto);
            dateTime2 = dto;
            return ret;
        }

        /// <summary>
        /// Converts a string literal to the equivalent <see cref="SqlTime"/> value
        /// </summary>
        /// <see href="https://learn.microsoft.com/en-us/sql/t-sql/data-types/time-transact-sql?view=sql-server-ver16#supported-string-literal-formats-for-time"/>
        /// <param name="value">The string literal to parse</param>
        /// <param name="dateFormat">The current DATEFORMAT setting to control the expected formats</param>
        /// <param name="time">The parsed version of the time</param>
        /// <returns><see langword="true"/> if the <paramref name="value"/> was parsed successfully, or <see langword="false"/> otherwise</returns>
        public static bool TryParse(SqlString value, DateFormat dateFormat, out SqlTime time)
        {
            var ret = TryParse(value, dateFormat, out SqlDateTimeOffset dto);
            time = dto;
            return ret;
        }

        /// <summary>
        /// Converts a string literal to the equivalent <see cref="SqlDateTime2"/> value
        /// </summary>
        /// <see href="https://learn.microsoft.com/en-us/sql/t-sql/data-types/datetimeoffset-transact-sql?view=sql-server-ver16#supported-string-literal-formats-for-datetimeoffset"/>
        /// <param name="value">The string literal to parse</param>
        /// <param name="dateFormat">The current DATEFORMAT setting to control the expected formats</param>
        /// <param name="dateTimeOffset">The parsed version of the datetimeoffset</param>
        /// <returns><see langword="true"/> if the <paramref name="value"/> was parsed successfully, or <see langword="false"/> otherwise</returns>
        public static bool TryParse(SqlString value, DateFormat dateFormat, out SqlDateTimeOffset dateTimeOffset)
        {
            if (value.IsNull)
            {
                dateTimeOffset = SqlDateTimeOffset.Null;
                return true;
            }

            // Allowed formats vary depending on format, but all formats are consistent combinations of
            // year, month and day with different separators
            string formatStringPart1;
            string formatStringPart2;
            string formatStringPart3;
            string[] separators = new[] { "/", "-", "." };

            switch (dateFormat)
            {
                case DateFormat.mdy:
                    formatStringPart1 = "[M]M";
                    formatStringPart2 = "[d]d";
                    formatStringPart3 = "[yy]yy";
                    break;

                case DateFormat.myd:
                    formatStringPart1 = "[M]M";
                    formatStringPart2 = "[yy]yy";
                    formatStringPart3 = "[d]d";
                    break;

                case DateFormat.dmy:
                    formatStringPart1 = "[d]d";
                    formatStringPart2 = "[M]M";
                    formatStringPart3 = "[yy]yy";
                    break;

                case DateFormat.dym:
                    formatStringPart1 = "[d]d";
                    formatStringPart2 = "[yy]yy";
                    formatStringPart3 = "[M]M";
                    break;

                case DateFormat.ymd:
                    formatStringPart1 = "[yy]yy";
                    formatStringPart2 = "[M]M";
                    formatStringPart3 = "[d]d";
                    break;

                default:
                    // ydm format is not supported for datetimeoffset/datetime2/date/time
                    dateTimeOffset = SqlDateTimeOffset.Null;
                    return false;
            }

            var numericFormatStrings = separators
                .Select(s => $"{formatStringPart1}{s}{formatStringPart2}{s}{formatStringPart3}");

            var alphaFormatStrings = new[]
            {
                "mon [dd][,] yyyy",
                "mon dd[,] [yy]",
                "mon yyyy [dd]",
                "[dd] mon[,] yyyy",
                "dd mon[,][yy]yy",
                "dd [yy]yy mon",
                "[dd] yyyy mon",
                "yyyy mon [dd]",
                "yyyy [dd] mon"
            };

            var isoFormatStrings = new[]
            {
                "yyyy-MM-dd",
                "yyyyMMdd"
            };

            var unseparatedFormatStrings = new[]
            {
                "yyMMdd",
                "yyyy"
            };

            var w3cFormatString = new[]
            {
                "yyyy-MM-ddK"
            };

            var timeFormatStrings = new[]
            {
                "HH:mm",
                "HH:mm:ss",
                "HH:mm:ss:fffffff",
                "HH:mm:ss.fffffff",
                "hh:mmtt",
                "hh:mm:sstt",
                "hh:mm:ss:ffffffftt",
                "hh:mm:ss.ffffffftt",
                "hhtt",
                "hh tt"
            };

            var allDateFormats = numericFormatStrings
                .Concat(alphaFormatStrings)
                .Concat(isoFormatStrings)
                .Concat(unseparatedFormatStrings)
                .Concat(w3cFormatString)
                .SelectMany(f => SqlToNetFormatString(f))
                .ToArray();

            var allTimeFormats = timeFormatStrings
                .SelectMany(f => new[] { f, f + "K", f + " K" }) // Allow optional timezone with all time formats, with optional space
                .SelectMany(f => SqlToNetFormatString(f))
                .ToArray();

            var allDateTimeFormats = allDateFormats
                .Concat(allDateFormats.SelectMany(d => allTimeFormats.Select(t => d + " " + t)))
                .ToArray();

            if (!DateTimeOffset.TryParseExact(value.Value.Trim(), allDateTimeFormats, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var parsed))
            {
                if (DateTimeOffset.TryParseExact(value.Value, allTimeFormats, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out parsed))
                {
                    dateTimeOffset = new DateTimeOffset(new DateTime(1900, 1, 1), parsed.Offset) + parsed.TimeOfDay;
                    return true;
                }

                dateTimeOffset = SqlDateTimeOffset.Null;
                return false;
            }

            dateTimeOffset = parsed;
            return true;
        }
        /// <summary>
        /// Converts a string literal to the equivalent <see cref="SqlDateTime"/> value
        /// </summary>
        /// <see href="https://learn.microsoft.com/en-us/sql/t-sql/data-types/datetime-transact-sql?view=sql-server-ver16#supported-string-literal-formats-for-datetime"/>
        /// <param name="value">The string literal to parse</param>
        /// <param name="dateFormat">The current DATEFORMAT setting to control the expected formats</param>
        /// <param name="date">The parsed version of the date</param>
        /// <returns><see langword="true"/> if the <paramref name="value"/> was parsed successfully, or <see langword="false"/> otherwise</returns>
        public static bool TryParse(SqlString value, DateFormat dateFormat, out SqlDateTime date)
        {
            if (value.IsNull)
            {
                date = SqlDateTime.Null;
                return true;
            }

            // Allowed formats vary depending on format, but all formats are consistent combinations of
            // year, month and day with different separators
            string formatStringPart1;
            string formatStringPart2;
            string formatStringPart3;
            string formatString4DigitYearPart1;
            string formatString4DigitYearPart2;
            string[] separators = new[] { "/", "-", "." };

            switch (dateFormat)
            {
                case DateFormat.mdy:
                    formatStringPart1 = "[M]M";
                    formatStringPart2 = "[d]d";
                    formatStringPart3 = "[yy]yy";

                    formatString4DigitYearPart1 = "[M]M";
                    formatString4DigitYearPart2 = "[d]d";
                    break;

                case DateFormat.myd:
                    formatStringPart1 = "[M]M";
                    formatStringPart2 = "[yy]yy";
                    formatStringPart3 = "[d]d";

                    formatString4DigitYearPart1 = "[M]M";
                    formatString4DigitYearPart2 = "[d]d";
                    break;

                case DateFormat.dmy:
                    formatStringPart1 = "[d]d";
                    formatStringPart2 = "[M]M";
                    formatStringPart3 = "[yy]yy";

                    formatString4DigitYearPart1 = "[d]d";
                    formatString4DigitYearPart2 = "[M]M";
                    break;

                case DateFormat.dym:
                    formatStringPart1 = "[d]d";
                    formatStringPart2 = "[yy]yy";
                    formatStringPart3 = "[M]M";

                    formatString4DigitYearPart1 = "[d]d";
                    formatString4DigitYearPart2 = "[M]M";
                    break;

                case DateFormat.ymd:
                    formatStringPart1 = "[yy]yy";
                    formatStringPart2 = "[M]M";
                    formatStringPart3 = "[d]d";

                    formatString4DigitYearPart1 = "[M]M";
                    formatString4DigitYearPart2 = "[d]d";
                    break;

                case DateFormat.ydm:
                    formatStringPart1 = "[yy]yy";
                    formatStringPart2 = "[d]d";
                    formatStringPart3 = "[M]M";

                    formatString4DigitYearPart1 = "[d]d";
                    formatString4DigitYearPart2 = "[M]M";
                    break;

                default:
                    date = SqlDateTime.Null;
                    return false;
            }

            var numericTimeFormatStrings = new[]
            {
                "",
                "HH:mm",
                "HH:mm:ss",
                "HH:mm:ss:fff",
                "HH:mm:ss.fff",
                "hhtt",
                "hh tt"
            };

            var numericFormatStrings = separators
                .SelectMany(s => new[] {
                    // Parts in the expected order
                    $"{formatStringPart1}{s}{formatStringPart2}{s}{formatStringPart3}",

                    // 4-digit year can come in any position and the other parts remain
                    // in their original relative order
                    $"yyyy{s}{formatString4DigitYearPart1}{s}{formatString4DigitYearPart2}",
                    $"{formatString4DigitYearPart1}{s}yyyy{s}{formatString4DigitYearPart2}",
                    $"{formatString4DigitYearPart1}{s}{formatString4DigitYearPart2}{s}yyyy",
                })
                .SelectMany(s => numericTimeFormatStrings.Select(t => s + " " + t));

            var alphaFormatStrings = new[]
            {
                "mon [dd][,] yyyy",
                "mon dd[,] [yy]",
                "mon yyyy [dd]",
                "[dd] mon[,] yyyy",
                "dd mon[,][yy]yy",
                "dd [yy]yy mon",
                "[dd] yyyy mon",
                "yyyy mon [dd]",
                "yyyy [dd] mon"
            };

            var isoFormatStrings = new[]
            {
                "yyyy-MM-ddTHH:mm:ss",
                "yyyy-MM-ddTHH:mm:ss.fff",
                "yyyyMMdd",
                "yyyyMMdd HH:mm:ss",
                "yyyyMMdd HH:mm:ss.fff"
            };

            var allFormats = numericFormatStrings
                .Concat(alphaFormatStrings)
                .Concat(isoFormatStrings)
                .SelectMany(f => SqlToNetFormatString(f))
                .ToArray();

            if (!DateTime.TryParseExact(value.Value.Trim(), allFormats, CultureInfo.InvariantCulture, DateTimeStyles.None, out var parsed))
            {
                date = SqlDateTime.Null;
                return false;
            }

            date = parsed;
            return true;
        }

        /// <summary>
        /// Converts a SQL format string from the SQL Server documentation to the corresponding .NET format strings
        /// </summary>
        /// <param name="formatString"></param>
        /// <returns></returns>
        public static string[] SqlToNetFormatString(string formatString)
        {
            var parts = new List<string[]>();

            // Parse the SQL format string into parts
            // dd and [d]d both indicate a 1- or 2-digit day
            // M and [M]M is a 1- or 2-digit month
            // MM is a 2-digit month
            // mon is an abbreviated or full month name
            // yy is a 2-digit year
            // yyyy is a 4-digit year
            // [yy]yy is a 2- or 4-digit year
            // HH is a 1- or 2-digit hour (24 hour clock)
            // hh is a 1- or 2-digit hour (12 hour clock)
            // mm is a 1- or 2-digit minute
            // ss is a 1- or 2-digit second
            // fff is a 1- to 3-digit fraction of a second
            // fffffff is a 1- to 7-digit fraction of a second
            // Anything else in [] is optional
            // Anything else is a literal
            //
            // Note docs sometimes use m or mm for month - we need to change this to M or MM to consistently
            // interpret it as a month instead of minutes
            var optional = false;

            for (var i = 0; i < formatString.Length; i++)
            {
                var length = formatString.Length - i;
                var setOptional = false;

                while (length >= 1)
                {
                    var part = formatString.Substring(i, length);

                    if (part == "d" || part == "dd" || part == "[d]d")
                    {
                        parts.Add(new[] { "d", "dd" });
                        break;
                    }
                    else if (part == "M" || part == "MM" || part == "[M]M")
                    {
                        parts.Add(new[] { "M", "MM" });
                        break;
                    }
                    else if (part == "mon")
                    {
                        parts.Add(new[] { "MMM", "MMMM" });
                        break;
                    }
                    else if (part == "yy")
                    {
                        parts.Add(new[] { "yy" });
                        break;
                    }
                    else if (part == "yyyy")
                    {
                        parts.Add(new[] { "yyyy" });
                        break;
                    }
                    else if (part == "[yy]yy")
                    {
                        parts.Add(new[] { "yy", "yyyy" });
                        break;
                    }
                    else if (part == "HH")
                    {
                        parts.Add(new[] { "H", "HH" });
                        break;
                    }
                    else if (part == "hh")
                    {
                        parts.Add(new[] { "h", "hh" });
                        break;
                    }
                    else if (part == "mm")
                    {
                        parts.Add(new[] { "m", "mm" });
                        break;
                    }
                    else if (part == "ss")
                    {
                        parts.Add(new[] { "s", "ss" });
                        break;
                    }
                    else if (part == "fff")
                    {
                        parts.Add(new[] { "FFF" });
                        break;
                    }
                    else if (part == "fffffff")
                    {
                        parts.Add(new[] { "FFFFFFF" });
                        break;
                    }
                    else if (part == "[")
                    {
                        optional = true;
                        setOptional = true;
                        break;
                    }
                    else if (length == 1)
                    {
                        // Literal
                        parts.Add(new[] { part });
                        break;
                    }
                    else
                    {
                        // Try a shorter part
                        length--;
                    }
                }

                if (length == 0)
                    throw new FormatException();

                i += length - 1;

                if (optional && !setOptional)
                {
                    parts[parts.Count - 1] = parts[parts.Count - 1].Concat(new[] { "" }).ToArray();
                    optional = false;

                    if (formatString[i + 1] == ']')
                        i++;
                    else
                        throw new FormatException();
                }
            }

            var formatStrings = parts[0];

            for (var i = 1; i < parts.Count; i++)
                formatStrings = formatStrings.SelectMany(s => parts[i].Select(p => s + p)).ToArray();

            return formatStrings.Select(s => s.Trim().Replace("  ", " ")).ToArray();
        }
    }
}
