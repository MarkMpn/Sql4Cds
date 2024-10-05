using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using Microsoft.Crm.Sdk.Messages;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;
using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Xml;
using System.Xml.XPath;
using Wmhelp.XPath2;
using Wmhelp.XPath2.Value;
using System.Text.Json;
using Microsoft.SqlServer.Server;

#if NETCOREAPP
using Microsoft.PowerPlatform.Dataverse.Client;
#else
using Microsoft.Xrm.Tooling.Connector;
#endif

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
        [SqlFunction(IsDeterministic = true)]
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
        [MaxLength(4000)]
        [CollationSensitive]
        [SqlFunction(IsDeterministic = true)]
        public static SqlString Json_Value(SqlString json, SqlString jpath)
        {
            if (json.IsNull || jpath.IsNull)
                return SqlString.Null;

            var path = jpath.Value;
            
            try
            {
                var jsonPath = new JsonPath(path);
                var jsonDoc = JsonDocument.Parse(json.Value);
                var jtoken = jsonPath.Evaluate(jsonDoc.RootElement);

                if (jtoken == null)
                {
                    if (jsonPath.Mode == JsonPathMode.Lax)
                        return SqlString.Null;
                    else
                        throw new QueryExecutionException(Sql4CdsError.JsonPropertyNotFound(null));
                }

                switch (jtoken.Value.ValueKind)
                {
                    case JsonValueKind.Object:
                    case JsonValueKind.Array:
                        if (jsonPath.Mode == JsonPathMode.Lax)
                            return SqlString.Null;
                        else
                            throw new QueryExecutionException(Sql4CdsError.JsonScalarValueNotFound(null));

                    case JsonValueKind.Null:
                        return SqlString.Null;
                }

                var value = jtoken.Value.ToString();

                if (value.Length > 4000)
                {
                    if (jsonPath.Mode == JsonPathMode.Lax)
                        return SqlString.Null;
                    else
                        throw new QueryExecutionException(Sql4CdsError.JsonStringTruncation(null));
                }

                return new SqlString(value, json.LCID, json.SqlCompareOptions);
            }
            catch (Newtonsoft.Json.JsonException ex)
            {
                throw new QueryExecutionException(ex.Message, ex);
            }
        }

        /// <summary>
        /// Extracts an object or an array from a JSON string
        /// </summary>
        /// <param name="json">An expression containing the JSON document to parse</param>
        /// <param name="jpath">A JSON path that specifies the property to extract</param>
        /// <returns>Returns a JSON fragment of type nvarchar(max)</returns>
        [CollationSensitive]
        [SqlFunction(IsDeterministic = true)]
        public static SqlString Json_Query(SqlString json, SqlString jpath)
        {
            if (json.IsNull || jpath.IsNull)
                return SqlString.Null;

            var path = jpath.Value;

            try
            {
                var jsonPath = new JsonPath(path);
                var jsonDoc = JsonDocument.Parse(json.Value);
                var jtoken = jsonPath.Evaluate(jsonDoc.RootElement);

                if (jtoken == null)
                {
                    if (jsonPath.Mode == JsonPathMode.Lax)
                        return SqlString.Null;
                    else
                        throw new QueryExecutionException(Sql4CdsError.JsonPropertyNotFound(null));
                }

                switch (jtoken.Value.ValueKind)
                {
                    case JsonValueKind.Object:
                    case JsonValueKind.Array:
                        return new SqlString(jtoken.Value.ToString(), json.LCID, json.SqlCompareOptions);

                    default:
                        if (jsonPath.Mode == JsonPathMode.Lax)
                            return SqlString.Null;
                        else
                            throw new QueryExecutionException(Sql4CdsError.JsonObjectOrArrayNotFound(null));
                }
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
        [SqlFunction(IsDeterministic = true)]
        public static SqlBoolean Json_Path_Exists(SqlString json, SqlString jpath)
        {
            if (json.IsNull || jpath.IsNull)
                return SqlBoolean.Null;

            var path = jpath.Value;

            try
            {
                var jsonPath = new JsonPath(path);
                var jsonDoc = JsonDocument.Parse(json.Value);
                var jtoken = jsonPath.Evaluate(jsonDoc.RootElement);

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
        [SqlFunction(IsDeterministic = true)]
        public static SqlDateTimeOffset DateAdd(SqlString datepart, SqlInt32 number, SqlDateTimeOffset date, [SourceType(nameof(date)), TargetType] DataTypeReference dateType)
        {
            if (number.IsNull || date.IsNull)
                return SqlDateTime.Null;

            if (!TryParseDatePart(datepart.Value, out var interval))
                throw new QueryExecutionException(Sql4CdsError.InvalidOptionValue(new StringLiteral { Value = datepart.Value }, "datepart"));

            if (interval == Engine.DatePart.TZOffset)
                throw new QueryExecutionException(Sql4CdsError.InvalidDatePart(null, datepart.Value, "dateadd", dateType));

            DateTimeOffset value;

            try
            {
                switch (interval)
                {
                    case Engine.DatePart.Year:
                        value = date.Value.AddYears(number.Value);
                        break;

                    case Engine.DatePart.Quarter:
                        value = date.Value.AddMonths(number.Value * 3);
                        break;

                    case Engine.DatePart.Month:
                        value = date.Value.AddMonths(number.Value);
                        break;

                    case Engine.DatePart.DayOfYear:
                    case Engine.DatePart.Day:
                    case Engine.DatePart.WeekDay:
                        value = date.Value.AddDays(number.Value);
                        break;

                    case Engine.DatePart.Week:
                        value = date.Value.AddDays(number.Value * 7);
                        break;

                    case Engine.DatePart.Hour:
                        value = date.Value.AddHours(number.Value);
                        break;

                    case Engine.DatePart.Minute:
                        value = date.Value.AddMinutes(number.Value);
                        break;

                    case Engine.DatePart.Second:
                        value = date.Value.AddSeconds(number.Value);
                        break;

                    case Engine.DatePart.Millisecond:
                        value = date.Value.AddMilliseconds(number.Value);

                        // https://learn.microsoft.com/en-us/sql/t-sql/functions/dateadd-transact-sql?view=sql-server-ver16#return-values-for-a-smalldatetime-date-and-a-second-or-fractional-seconds-datepart
                        if (dateType.IsSameAs(DataTypeHelpers.SmallDateTime))
                            value = value.AddMilliseconds(1);
                        break;

                    case Engine.DatePart.Microsecond:
                        // Check data type & precision
                        // https://learn.microsoft.com/en-us/sql/t-sql/functions/dateadd-transact-sql?view=sql-server-ver16#fractional-seconds-precision
                        if (dateType.IsSameAs(DataTypeHelpers.SmallDateTime) || dateType.IsSameAs(DataTypeHelpers.DateTime) || dateType.IsSameAs(DataTypeHelpers.Date))
                            throw new QueryExecutionException(Sql4CdsError.InvalidDatePart(null, datepart.Value, "dateadd", dateType));

                        value = date.Value.AddTicks(number.Value * 10);
                        break;

                    case Engine.DatePart.Nanosecond:
                        // Check data type & precision
                        // https://learn.microsoft.com/en-us/sql/t-sql/functions/dateadd-transact-sql?view=sql-server-ver16#fractional-seconds-precision
                        if (dateType.IsSameAs(DataTypeHelpers.SmallDateTime) || dateType.IsSameAs(DataTypeHelpers.DateTime) || dateType.IsSameAs(DataTypeHelpers.Date))
                            throw new QueryExecutionException(Sql4CdsError.InvalidDatePart(null, datepart.Value, "dateadd", dateType));

                        var ticks = (int)Math.Round(number.Value / 100M, MidpointRounding.AwayFromZero);
                        value = date.Value.AddTicks(ticks);
                        break;

                    default:
                        throw new QueryExecutionException(Sql4CdsError.InvalidOptionValue(new StringLiteral { Value = datepart.Value }, "datepart"));
                }
            }
            catch (ArgumentOutOfRangeException)
            {
                throw new QueryExecutionException(Sql4CdsError.AdditionOverflow(null, dateType));
            }

            if (dateType.IsSameAs(DataTypeHelpers.SmallDateTime) && (value.DateTime < SqlSmallDateTime.MinValue.Value || value.DateTime > SqlSmallDateTime.MaxValue.Value))
                throw new QueryExecutionException(Sql4CdsError.AdditionOverflow(null, dateType));

            if (dateType.IsSameAs(DataTypeHelpers.DateTime) && (value.DateTime < SqlDateTime.MinValue.Value || value.DateTime > SqlDateTime.MaxValue.Value))
                throw new QueryExecutionException(Sql4CdsError.AdditionOverflow(null, dateType));

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
        [SqlFunction(IsDeterministic = true)]
        public static SqlInt32 DateDiff(SqlString datepart, SqlDateTime startdate, SqlDateTime enddate)
        {
            if (startdate.IsNull || enddate.IsNull)
                return SqlInt32.Null;

            if (!TryParseDatePart(datepart.Value, out var interval))
                throw new QueryExecutionException(Sql4CdsError.InvalidOptionValue(new StringLiteral { Value = datepart.Value }, "datepart"));
            
            startdate = DateTrunc(datepart, startdate);
            enddate = DateTrunc(datepart, enddate);

            switch (interval)
            {
                case Engine.DatePart.Year:
                    return enddate.Value.Year - startdate.Value.Year;

                case Engine.DatePart.Quarter:
                    var endQuarter = enddate.Value.Year * 4 + (enddate.Value.Month - 1) / 3 + 1;
                    var startQuarter = startdate.Value.Year * 4 + (startdate.Value.Month - 1) / 3 + 1;
                    return endQuarter - startQuarter;

                case Engine.DatePart.Month:
                    return (enddate.Value.Year - startdate.Value.Year) * 12 + enddate.Value.Month - startdate.Value.Month;

                case Engine.DatePart.DayOfYear:
                case Engine.DatePart.Day:
                case Engine.DatePart.WeekDay:
                    return (enddate.Value - startdate.Value).Days;

                case Engine.DatePart.Week:
                case Engine.DatePart.ISOWeek:
                    return (enddate.Value - startdate.Value).Days / 7;

                case Engine.DatePart.Hour:
                    return (int)(enddate.Value - startdate.Value).TotalHours;

                case Engine.DatePart.Minute:
                    return (int)(enddate.Value - startdate.Value).TotalMinutes;

                case Engine.DatePart.Second:
                    return (int)(enddate.Value - startdate.Value).TotalSeconds;

                case Engine.DatePart.Millisecond:
                    return (int)(enddate.Value - startdate.Value).TotalMilliseconds;

                case Engine.DatePart.Microsecond:
                    return (int)((enddate.Value - startdate.Value).Ticks / 10);

                case Engine.DatePart.Nanosecond:
                    return (int)((enddate.Value - startdate.Value).Ticks * 100);

                default:
                    throw new QueryExecutionException(Sql4CdsError.InvalidOptionValue(new StringLiteral { Value = datepart.Value }, "datepart"));
            }
        }

        /// <summary>
        /// Implements the DATETRUNC function
        /// </summary>
        /// <param name="datepart">Specifies the precision for truncation</param>
        /// <param name="date">The date value to be truncated</param>
        /// <returns>The truncated version of the date</returns>
        /// <see href="https://learn.microsoft.com/en-us/sql/t-sql/functions/datetrunc-transact-sql?view=sql-server-ver16"/>
        [SqlFunction(IsDeterministic = true)]
        public static SqlDateTime DateTrunc(SqlString datepart, SqlDateTime date)
        {
            if (date.IsNull)
                return SqlDateTime.Null;

            if (!TryParseDatePart(datepart.Value, out var interval))
                throw new QueryExecutionException(Sql4CdsError.InvalidOptionValue(new StringLiteral { Value = datepart.Value }, "datepart"));

            switch (interval)
            {
                case Engine.DatePart.Year:
                    return new DateTime(date.Value.Year, 1, 1);

                case Engine.DatePart.Quarter:
                    return new DateTime(date.Value.Year, ((date.Value.Month - 1) / 3 + 1) * 3, 1);

                case Engine.DatePart.Month:
                    return new DateTime(date.Value.Year, date.Value.Month, 1);

                case Engine.DatePart.DayOfYear:
                case Engine.DatePart.Day:
                    return date.Value.Date;

                case Engine.DatePart.Week:
                    return date.Value.Date.AddDays(-(int)date.Value.DayOfWeek);

                case Engine.DatePart.ISOWeek:
                    var day = (int)date.Value.DayOfWeek;
                    if (day == 0)
                        day = 7;

                    return date.Value.Date.AddDays(-day + 1);

                case Engine.DatePart.Hour:
                    return new DateTime(date.Value.Year, date.Value.Month, date.Value.Day, date.Value.Hour, 0, 0);

                case Engine.DatePart.Minute:
                    return new DateTime(date.Value.Year, date.Value.Month, date.Value.Day, date.Value.Hour, date.Value.Minute, 0);

                case Engine.DatePart.Second:
                    return new DateTime(date.Value.Year, date.Value.Month, date.Value.Day, date.Value.Hour, date.Value.Minute, date.Value.Second);

                case Engine.DatePart.Millisecond:
                    return new DateTime(date.Value.Year, date.Value.Month, date.Value.Day, date.Value.Hour, date.Value.Minute, date.Value.Second, date.Value.Millisecond);

                case Engine.DatePart.Microsecond:
                    // TODO: Check data type & precision

                default:
                    throw new QueryExecutionException(Sql4CdsError.InvalidOptionValue(new StringLiteral { Value = datepart.Value }, "datepart"));
            }
        }

        /// <summary>
        /// Implements the DATEPART function
        /// </summary>
        /// <param name="datepart">The specific part of the <paramref name="date"/> argument for which DATEPART will return an integer</param>
        /// <param name="date">The date to extract the <paramref name="datepart"/> from</param>
        /// <returns>The <paramref name="datepart"/> of the <paramref name="date"/></returns>
        [SqlFunction(IsDeterministic = true)]
        public static SqlInt32 DatePart(SqlString datepart, SqlDateTimeOffset date, [SourceType(nameof(date))] DataTypeReference dateType)
        {
            if (date.IsNull)
                return SqlInt32.Null;

            if (!TryParseDatePart(datepart.Value, out var interval))
                throw new QueryExecutionException(Sql4CdsError.InvalidOptionValue(new StringLiteral { Value = datepart.Value }, "datepart"));

            var sqlDateType = dateType as SqlDataTypeReference;

            switch (interval)
            {
                case Engine.DatePart.Year:
                    if (sqlDateType?.SqlDataTypeOption == SqlDataTypeOption.Time)
                        throw new QueryExecutionException(Sql4CdsError.InvalidDatePart(null, datepart.Value, "datepart", dateType));

                    return date.Value.Year;

                case Engine.DatePart.Quarter:
                    if (sqlDateType?.SqlDataTypeOption == SqlDataTypeOption.Time)
                        throw new QueryExecutionException(Sql4CdsError.InvalidDatePart(null, datepart.Value, "datepart", dateType));

                    return (date.Value.Month - 1) / 3 + 1;

                case Engine.DatePart.Month:
                    if (sqlDateType?.SqlDataTypeOption == SqlDataTypeOption.Time)
                        throw new QueryExecutionException(Sql4CdsError.InvalidDatePart(null, datepart.Value, "datepart", dateType));

                    return date.Value.Month;

                case Engine.DatePart.DayOfYear:
                    if (sqlDateType?.SqlDataTypeOption == SqlDataTypeOption.Time)
                        throw new QueryExecutionException(Sql4CdsError.InvalidDatePart(null, datepart.Value, "datepart", dateType));

                    return date.Value.DayOfYear;

                case Engine.DatePart.Day:
                    if (sqlDateType?.SqlDataTypeOption == SqlDataTypeOption.Time)
                        throw new QueryExecutionException(Sql4CdsError.InvalidDatePart(null, datepart.Value, "datepart", dateType));

                    return date.Value.Day;

                case Engine.DatePart.Week:
                    if (sqlDateType?.SqlDataTypeOption == SqlDataTypeOption.Time)
                        throw new QueryExecutionException(Sql4CdsError.InvalidDatePart(null, datepart.Value, "datepart", dateType));

                    return CultureInfo.CurrentCulture.Calendar.GetWeekOfYear(date.Value.DateTime, CalendarWeekRule.FirstFourDayWeek, DayOfWeek.Sunday);

                case Engine.DatePart.WeekDay:
                    if (sqlDateType?.SqlDataTypeOption == SqlDataTypeOption.Time)
                        throw new QueryExecutionException(Sql4CdsError.InvalidDatePart(null, datepart.Value, "datepart", dateType));

                    return (int)date.Value.DayOfWeek + 1;

                case Engine.DatePart.Hour:
                    return date.Value.Hour;

                case Engine.DatePart.Minute:
                    return date.Value.Minute;

                case Engine.DatePart.Second:
                    return date.Value.Second;

                case Engine.DatePart.Millisecond:
                    return date.Value.Millisecond;

                case Engine.DatePart.Microsecond:
                    return (int)(date.Value.Ticks % 10_000_000) / 10;

                case Engine.DatePart.Nanosecond:
                    return (int)(date.Value.Ticks % 10_000_000) * 100;

                case Engine.DatePart.TZOffset:
                    if (sqlDateType?.SqlDataTypeOption != SqlDataTypeOption.DateTimeOffset && sqlDateType?.SqlDataTypeOption != SqlDataTypeOption.DateTime2 && sqlDateType?.SqlDataTypeOption.IsStringType() != true)
                        throw new QueryExecutionException(Sql4CdsError.InvalidDatePart(null, datepart.Value, "datepart", dateType));

                    return (int)date.Value.Offset.TotalMinutes;

                case Engine.DatePart.ISOWeek:
                    if (sqlDateType?.SqlDataTypeOption == SqlDataTypeOption.Time)
                        throw new QueryExecutionException(Sql4CdsError.InvalidDatePart(null, datepart.Value, "datepart", dateType));

                    return CultureInfo.CurrentCulture.Calendar.GetWeekOfYear(date.Value.DateTime, CalendarWeekRule.FirstFourDayWeek, DayOfWeek.Monday);

                default:
                    throw new QueryExecutionException(Sql4CdsError.InvalidOptionValue(new StringLiteral { Value = datepart.Value }, "datepart"));
            }
        }

        /// <summary>
        /// Converts the SQL datepart argument names to the equivalent enum value
        /// </summary>
        /// <param name="datepart">The SQL name for the datepart argument</param>
        /// <returns>The equivalent <see cref="DatePart"/> value</returns>
        internal static bool TryParseDatePart(string datepart, out DatePart parsed)
        {
            switch (datepart.ToLower())
            {
                case "year":
                case "yy":
                case "yyyy":
                    parsed = Engine.DatePart.Year;
                    return true;

                case "quarter":
                case "qq":
                case "q":
                    parsed = Engine.DatePart.Quarter;
                    return true;

                case "month":
                case "mm":
                case "m":
                    parsed = Engine.DatePart.Month;
                    return true;

                case "dayofyear":
                case "dy":
                case "y":
                    parsed = Engine.DatePart.DayOfYear;
                    return true;

                case "day":
                case "dd":
                case "d":
                    parsed = Engine.DatePart.Day;
                    return true;

                case "week":
                case "wk":
                case "ww":
                    parsed = Engine.DatePart.Week;
                    return true;

                case "weekday":
                case "dw":
                case "w": // Abbreviation is lised for DATEADD but not DATEPART
                    parsed = Engine.DatePart.WeekDay;
                    return true;

                case "hour":
                case "hh":
                    parsed = Engine.DatePart.Hour;
                    return true;

                case "minute":
                case "mi": // Abbreviation is lised for DATEADD but not DATEPART
                case "n":
                    parsed = Engine.DatePart.Minute;
                    return true;

                case "second":
                case "ss":
                case "s":
                    parsed = Engine.DatePart.Second;
                    return true;

                case "millisecond":
                case "ms":
                    parsed = Engine.DatePart.Millisecond;
                    return true;

                case "microsecond":
                case "mcs":
                    parsed = Engine.DatePart.Microsecond;
                    return true;

                case "nanosecond":
                case "ns":
                    parsed = Engine.DatePart.Nanosecond;
                    return true;

                case "tzoffset":
                case "tz":
                    parsed = Engine.DatePart.TZOffset;
                    return true;

                case "iso_week":
                case "isowk":
                case "isoww":
                    parsed = Engine.DatePart.ISOWeek;
                    return true;

                default:
                    parsed = Engine.DatePart.Year;
                    return false;
            }
        }

        /// <summary>
        /// Gets the current date/time in user-local timezone
        /// </summary>
        /// <returns>The current date/time in user-local timezone</returns>
        [SqlFunction(IsDeterministic = false)]
        public static SqlDateTime GetDate()
        {
            return DateTime.Now;
        }

        /// <summary>
        /// Gets the current date/time in user-local timezone
        /// </summary>
        /// <returns>The current date/time in user-local timezone</returns>
        [SqlFunction(IsDeterministic = false)]
        public static SqlDateTime SysDateTime()
        {
            return DateTime.Now;
        }

        /// <summary>
        /// Gets the current date/time in user-local timezone
        /// </summary>
        /// <returns>The current date/time in user-local timezone</returns>
        [SqlFunction(IsDeterministic = false)]
        public static SqlDateTime SysDateTimeOffset()
        {
            return DateTime.Now;
        }

        /// <summary>
        /// Gets the current date/time in UTC timezone
        /// </summary>
        /// <returns>The current date/time in UTC timezone</returns>
        [SqlFunction(IsDeterministic = false)]
        public static SqlDateTime GetUtcDate()
        {
            return DateTime.UtcNow;
        }

        /// <summary>
        /// Gets the current date/time in UTC timezone
        /// </summary>
        /// <returns>The current date/time in UTC timezone</returns>
        [SqlFunction(IsDeterministic = false)]
        public static SqlDateTime SysUtcDateTime()
        {
            return DateTime.UtcNow;
        }

        /// <summary>
        /// Gets the day of the month from the specified date
        /// </summary>
        /// <param name="date">The date to get the day number from</param>
        /// <returns>The day of the month</returns>
        [SqlFunction(IsDeterministic = true)]
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
        [SqlFunction(IsDeterministic = true)]
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
        [SqlFunction(IsDeterministic = true)]
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
        [CollationSensitive]
        [SqlFunction(IsDeterministic = true)]
        public static SqlString Left(SqlString s, [MaxLength] SqlInt32 length)
        {
            if (s.IsNull || length.IsNull)
                return SqlString.Null;

            if (s.Value.Length <= length)
                return s;

            return new SqlString(s.Value.Substring(0, length.Value), s.LCID, s.SqlCompareOptions);
        }

        /// <summary>
        /// Returns the suffix of a string
        /// </summary>
        /// <param name="s">The string to get the suffix of</param>
        /// <param name="length">The number of characters to return</param>
        /// <returns>The last <paramref name="length"/> characters of the string <paramref name="s"/></returns>
        [CollationSensitive]
        [SqlFunction(IsDeterministic = true)]
        public static SqlString Right(SqlString s, [MaxLength] SqlInt32 length)
        {
            if (s.IsNull || length.IsNull)
                return SqlString.Null;

            if (s.Value.Length <= length)
                return s;

            return new SqlString(s.Value.Substring(s.Value.Length - length.Value, length.Value), s.LCID, s.SqlCompareOptions);
        }

        /// <summary>
        /// Replaces all occurrences of a specified string value with another string value.
        /// </summary>
        /// <param name="input">The string expression to be searched</param>
        /// <param name="find">The substring to be found</param>
        /// <param name="replace">The replacement string</param>
        /// <returns>Replaces any instances of <paramref name="find"/> with <paramref name="replace"/> in the <paramref name="input"/></returns>
        [CollationSensitive]
        [SqlFunction(IsDeterministic = true)]
        public static SqlString Replace(SqlString input, SqlString find, SqlString replace)
        {
            if (input.IsNull || find.IsNull || replace.IsNull)
                return SqlString.Null;

            return new SqlString(Regex.Replace(input.Value, Regex.Escape(find.Value), replace.Value.Replace("$", "$$"), RegexOptions.IgnoreCase), input.LCID, input.SqlCompareOptions);
        }

        /// <summary>
        /// Returns the number of characters of the specified string expression, excluding trailing spaces
        /// </summary>
        /// <param name="s">The string expression to be evaluated</param>
        /// <returns></returns>
        [CollationSensitive]
        [SqlFunction(IsDeterministic = true)]
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
        [SqlFunction(IsDeterministic = true)]
        public static SqlInt32 DataLength<T>(T value, [SourceType(nameof(value))] DataTypeReference type)
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
        [CollationSensitive]
        [SqlFunction(IsDeterministic = true)]
        public static SqlString Substring(SqlString expression, SqlInt32 start, [MaxLength] SqlInt32 length)
        {
            if (expression.IsNull || start.IsNull || length.IsNull)
                return SqlString.Null;

            if (length < 0)
                throw new ArgumentOutOfRangeException(nameof(length));

            if (start < 1)
                start = 1;

            if (start > expression.Value.Length)
                return new SqlString(String.Empty, expression.LCID, expression.SqlCompareOptions);

            start -= 1;

            if (start + length > expression.Value.Length)
                length = expression.Value.Length - start;

            return new SqlString(expression.Value.Substring(start.Value, length.Value), expression.LCID, expression.SqlCompareOptions);
        }

        /// <summary>
        /// Removes the space character from the start and end of a string
        /// </summary>
        /// <param name="expression">A character expression where characters should be removed</param>
        /// <returns></returns>
        [SqlFunction(IsDeterministic = true)]
        public static SqlString Trim([MaxLength] SqlString expression)
        {
            if (expression.IsNull)
                return expression;

            return new SqlString(expression.Value.Trim(' '), expression.LCID, expression.SqlCompareOptions);
        }

        /// <summary>
        /// Removes the space character from the start of a string
        /// </summary>
        /// <param name="expression">A character expression where characters should be removed</param>
        /// <returns></returns>
        [SqlFunction(IsDeterministic = true)]
        public static SqlString LTrim([MaxLength] SqlString expression)
        {
            if (expression.IsNull)
                return expression;

            return new SqlString(expression.Value.TrimStart(' '), expression.LCID, expression.SqlCompareOptions);
        }

        /// <summary>
        /// Removes the space character from the end of a string
        /// </summary>
        /// <param name="expression">A character expression where characters should be removed</param>
        /// <returns></returns>
        [SqlFunction(IsDeterministic = true)]
        public static SqlString RTrim([MaxLength] SqlString expression)
        {
            if (expression.IsNull)
                return expression;

            return new SqlString(expression.Value.TrimEnd(' '), expression.LCID, expression.SqlCompareOptions);
        }

        /// <summary>
        /// Searches for one character expression inside a second character expression, returning the starting position of the first expression if found
        /// </summary>
        /// <param name="find">A character expression containing the sequence to find</param>
        /// <param name="search">A character expression to search.</param>
        /// <returns></returns>
        [CollationSensitive]
        [SqlFunction(IsDeterministic = true)]
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
        [CollationSensitive]
        [SqlFunction(IsDeterministic = true)]
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
        /// Returns the starting position of the first occurrence of a pattern in a specified expression, or zero if the pattern is not found, on all valid text and character data types.
        /// </summary>
        /// <param name="pattern">A character expression that contains the sequence to be found. Wildcard characters can be used; however, the % character must come before and follow <paramref name="pattern"/></param>
        /// <param name="expression">An expression that is searched for the specified <paramref name="pattern"/></param>
        /// <returns></returns>
        [CollationSensitive]
        [SqlFunction(IsDeterministic = true)]
        public static SqlInt32 PatIndex(SqlString pattern, SqlString expression)
        {
            if (pattern.IsNull || expression.IsNull)
                return 0;

            var regex = ExpressionExtensions.LikeToRegex(pattern, SqlString.Null, true);
            var value = expression.Value;

            if (expression.SqlCompareOptions.HasFlag(SqlCompareOptions.IgnoreNonSpace))
                value = ExpressionExtensions.RemoveDiacritics(value);

            var match = regex.Match(value);

            if (!match.Success)
                return 0;

            return match.Index + 1;
        }

        /// <summary>
        /// Returns the single-byte character with the specified integer code
        /// </summary>
        /// <param name="value">An integer from 0 through 255</param>
        /// <returns></returns>
        [SqlFunction(IsDeterministic = true)]
        public static SqlString Char(SqlInt32 value, ExpressionExecutionContext context)
        {
            if (value.IsNull || value.Value < 0 || value.Value > 255)
                return SqlString.Null;

            return context.PrimaryDataSource.DefaultCollation.ToSqlString(new string((char)value.Value, 1));
        }

        /// <summary>
        /// Returns the Unicode character with the specified integer code
        /// </summary>
        /// <param name="value">An integer from 0 through 255</param>
        /// <returns></returns>
        [SqlFunction(IsDeterministic = true)]
        public static SqlString NChar(SqlInt32 value, ExpressionExecutionContext context)
        {
            if (value.IsNull || value.Value < 0 || value.Value > 0x10FFFF)
                return SqlString.Null;

            return context.PrimaryDataSource.DefaultCollation.ToSqlString(new string((char)value.Value, 1));
        }

        /// <summary>
        /// Returns the ASCII code value of the leftmost character of a character expression.
        /// </summary>
        /// <param name="value">A string to convert</param>
        /// <returns></returns>
        [SqlFunction(IsDeterministic = true)]
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
        [SqlFunction(IsDeterministic = true)]
        public static SqlInt32 Unicode(SqlString value)
        {
            if (value.IsNull || value.Value.Length == 0)
                return SqlInt32.Null;

            return value.Value[0];
        }

        /// <summary>
        /// Returns the identifier of the user
        /// </summary>
        /// <param name="context">The context in which the expression is being executed</param>
        /// <returns></returns>
        [SqlFunction(IsDeterministic = false)]
        public static SqlEntityReference User_Name(ExpressionExecutionContext context)
        {
            return new SqlEntityReference(context.Options.PrimaryDataSource, "systemuser", context.Options.UserId);
        }

        /// <summary>
        /// The value of <paramref name="check"/> is returned if it is not NULL; otherwise, <paramref name="replacement"/> is returned
        /// </summary>
        /// <typeparam name="T">The type of values being compared</typeparam>
        /// <param name="check">The expression to be checked for NULL</param>
        /// <param name="replacement">The value to be returned if <paramref name="check"/> is NULL</param>
        /// <returns></returns>
        [SqlFunction(IsDeterministic = true)]
        public static T IsNull<T>(T check, T replacement)
            where T:INullable
        {
            if (!check.IsNull)
                return check;

            return replacement;
        }

        /// <summary>
        /// Returns a value formatted with the specified format and optional culture
        /// </summary>
        /// <typeparam name="T">The type of value to be formatted</typeparam>
        /// <param name="value">The value to be formatted</param>
        /// <param name="format">Format pattern</param>
        /// <param name="culture">Optional argument specifying a culture</param>
        /// <returns></returns>
        [SqlFunction(IsDeterministic = false)]
        public static SqlString Format<T>(T value, SqlString format, [Optional] SqlString culture, [SourceType(nameof(value))] DataTypeReference type, ExpressionExecutionContext context)
            where T : INullable
        {
            if (value.IsNull)
                return SqlString.Null;

            var valueProp = typeof(T).GetProperty("Value");

            if (!typeof(IFormattable).IsAssignableFrom(valueProp.PropertyType))
                throw new QueryExecutionException(Sql4CdsError.InvalidArgumentType(null, type, 1, "format"));

            var innerValue = (IFormattable)valueProp.GetValue(value);
            return Format(innerValue, format, culture, context);
        }

        private static SqlString Format(IFormattable value, SqlString format, SqlString culture, ExpressionExecutionContext context)
        {
            if (value == null)
                return SqlString.Null;

            if (format.IsNull)
                return SqlString.Null;

            var cultureInfo = CultureInfo.CurrentCulture;

            try
            {
                if (!culture.IsNull)
                    cultureInfo = CultureInfo.GetCultureInfo(culture.Value);

                var formatted = value.ToString(format.Value, cultureInfo);
                return context.PrimaryDataSource.DefaultCollation.ToSqlString(formatted);
            }
            catch
            {
                return SqlString.Null;
            }
        }

        /// <summary>
        /// Returns a character expression with lowercase character data converted to uppercase
        /// </summary>
        /// <param name="value">An expression of character data</param>
        /// <returns></returns>
        [CollationSensitive]
        [SqlFunction(IsDeterministic = true)]
        public static SqlString Upper([MaxLength] SqlString value)
        {
            if (value.IsNull)
                return value;

            return new SqlString(value.Value.ToUpper(value.CultureInfo), value.LCID, value.SqlCompareOptions);
        }

        /// <summary>
        /// Returns a character expression with uppercase character data converted to lowercase
        /// </summary>
        /// <param name="value">An expression of character data</param>
        /// <returns></returns>
        [CollationSensitive]
        [SqlFunction(IsDeterministic = true)]
        public static SqlString Lower([MaxLength] SqlString value)
        {
            if (value.IsNull)
                return value;

            return new SqlString(value.Value.ToLower(value.CultureInfo), value.LCID, value.SqlCompareOptions);
        }

        /// <summary>
        /// Returns the requested property of a specified collation
        /// </summary>
        /// <param name="collation">The name of the collation</param>
        /// <param name="property">The collation property</param>
        /// <returns></returns>
        [SqlFunction(IsDeterministic = true)]
        public static SqlInt32 CollationProperty(SqlString collation, SqlString property)
        {
            if (collation.IsNull || property.IsNull)
                return SqlInt32.Null;

            if (!Collation.TryParse(collation.Value, out var coll))
                return SqlInt32.Null;

            switch (property.Value.ToLowerInvariant())
            {
                case "codepage":
                    return 0;

                case "lcid":
                    return coll.LCID;

                case "comparisonstyle":
                    var compare = 0;

                    if (coll.CompareOptions.HasFlag(CompareOptions.IgnoreCase))
                        compare |= 1;

                    if (coll.CompareOptions.HasFlag(CompareOptions.IgnoreNonSpace))
                        compare |= 2;

                    if (coll.CompareOptions.HasFlag(CompareOptions.IgnoreKanaType))
                        compare |= 65536;

                    if (coll.CompareOptions.HasFlag(CompareOptions.IgnoreWidth))
                        compare |= 131072;

                    return compare;

                case "version":
                    if (coll.Name.Contains("140"))
                        return 3;

                    if (coll.Name.Contains("100"))
                        return 2;

                    if (coll.Name.Contains("90"))
                        return 1;

                    return 0;
            }

            return SqlInt32.Null;
        }

        /// <summary>
        /// Specifies an XQuery against an instance of the xml data type.
        /// </summary>
        /// <param name="value">The xml data to query</param>
        /// <param name="query">The XQuery expression to apply</param>
        /// <param name="context">The context the expression is evaluated in</param>
        /// <param name="schema">The schema of data available to the query</param>
        /// <returns></returns>
        [SqlFunction(IsDeterministic = true)]
        public static SqlXml Query(SqlXml value, XPath2Expression query, ExpressionExecutionContext context, INodeSchema schema)
        {
            if (value.IsNull)
                return value;

            var doc = new XPathDocument(value.CreateReader());
            var nav = doc.CreateNavigator();
            var result = query.Evaluate(new XPath2ExpressionContext(context, schema, nav), null);
            var stream = new MemoryStream();

            var xmlWriterSettings = new XmlWriterSettings
            {
                CloseOutput = false,
                ConformanceLevel = ConformanceLevel.Fragment,
                Encoding = Encoding.GetEncoding("utf-16"),
                OmitXmlDeclaration = true
            };
            var xmlWriter = XmlWriter.Create(stream, xmlWriterSettings);

            foreach (XPathNavigator r in (XPath2NodeIterator)result)
            {
                var reader = r.ReadSubtree();

                if (reader.ReadState == ReadState.Initial)
                    reader.Read();

                while (!reader.EOF)
                    xmlWriter.WriteNode(reader, defattr: true);
            }

            xmlWriter.Flush();
            stream.Position = 0;
            return new SqlXml(stream);
        }

        /// <summary>
        /// Performs an XQuery against the XML and returns a value of SQL type. This method returns a scalar value.
        /// </summary>
        /// <param name="value">The xml data to query</param>
        /// <param name="query">The XQuery expression to apply</param>
        /// <param name="targetType">The type of data to return</param>
        /// <param name="context">The context the expression is evaluated in</param>
        /// <param name="schema">The schema of data available to the query</param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        [SqlFunction(IsDeterministic = true)]
        public static object Value(SqlXml value, XPath2Expression query, [TargetType] DataTypeReference targetType, ExpressionExecutionContext context, INodeSchema schema)
        {
            if (value.IsNull)
                return value;

            var doc = new XPathDocument(value.CreateReader());
            var nav = doc.CreateNavigator();
            var result = query.Evaluate(new XPath2ExpressionContext(context, schema, nav), null);

            var targetNetType = targetType.ToNetType(out _);

            INullable sqlValue;

            if (result == null)
                sqlValue = SqlTypeConverter.GetNullValue(targetNetType);
            else if (result is INullable)
                sqlValue = (INullable)result;
            else if (result is Base64BinaryValue bin)
                sqlValue = new SqlBinary(bin.BinaryValue);
            else if (result is string s)
                sqlValue = context.PrimaryDataSource.DefaultCollation.ToSqlString(s);
            else if (result is double d)
                sqlValue = (SqlDouble)d;
            else if (result is XPath2NodeIterator nodeIterator)
                sqlValue = context.PrimaryDataSource.DefaultCollation.ToSqlString(nodeIterator.FirstOrDefault()?.Value);
            else
                throw new QueryExecutionException(Sql4CdsError.NotSupported(null, $"XPath return type '{result.GetType().Name}'"));

            if (sqlValue.GetType() != targetNetType)
                sqlValue = (INullable) SqlTypeConverter.ChangeType(sqlValue, targetNetType, context);

            return sqlValue;
        }

        /// <summary>
        /// Deletes a specified length of characters in the first string at the start position and then inserts the second string into the first string at the start position
        /// </summary>
        /// <param name="value">The first string to manipulate</param>
        /// <param name="start">The starting position within the first string to make the edits at</param>
        /// <param name="length">The number of characters to remove from the first string</param>
        /// <param name="replaceWith">The second string to insert into the first string</param>
        /// <returns></returns>
        [CollationSensitive]
        [SqlFunction(IsDeterministic = true)]
        public static SqlString Stuff(SqlString value, SqlInt32 start, SqlInt32 length, SqlString replaceWith)
        {
            if (value.IsNull || start.IsNull || length.IsNull || start.Value <= 0 || start.Value > value.Value.Length || length.Value < 0)
                return SqlString.Null;

            var sb = new StringBuilder(value.Value);

            if (length.Value > 0)
                sb.Remove(start.Value - 1, Math.Min(length.Value, value.Value.Length - start.Value + 1));

            if (!replaceWith.IsNull)
                sb.Insert(start.Value - 1, replaceWith.Value);

            return new SqlString(sb.ToString(), value.LCID, value.SqlCompareOptions);
        }

        [SqlFunction(IsDeterministic = false)]
        public static SqlVariant ServerProperty(SqlString propertyName, ExpressionExecutionContext context)
        {
            if (propertyName.IsNull)
                return SqlVariant.Null;

            var dataSource = context.PrimaryDataSource;

#if NETCOREAPP
            var svc = dataSource.Connection as ServiceClient;
#else
            var svc = dataSource.Connection as CrmServiceClient;
#endif

            switch (propertyName.Value.ToLowerInvariant())
            {
                case "collation":
                    return new SqlVariant(DataTypeHelpers.NVarChar(128, dataSource.DefaultCollation, CollationLabel.CoercibleDefault), dataSource.DefaultCollation.ToSqlString(dataSource.DefaultCollation.Name), context);

                case "collationid":
                    return new SqlVariant(DataTypeHelpers.Int, new SqlInt32(dataSource.DefaultCollation.LCID), context);

                case "comparisonstyle":
                    return new SqlVariant(DataTypeHelpers.Int, new SqlInt32((int)dataSource.DefaultCollation.CompareOptions), context);

                case "edition":
                    return new SqlVariant(DataTypeHelpers.NVarChar(128, dataSource.DefaultCollation, CollationLabel.CoercibleDefault), dataSource.DefaultCollation.ToSqlString("Enterprise Edition"), context);

                case "editionid":
                    return new SqlVariant(DataTypeHelpers.BigInt, new SqlInt64(1804890536), context);

                case "enginedition":
                    return new SqlVariant(DataTypeHelpers.Int, new SqlInt32(3), context);

                case "issingleuser":
                    return new SqlVariant(DataTypeHelpers.Int, new SqlInt32(0), context);

                case "machinename":
                case "servername":
                    string machineName = dataSource.Name;

#if NETCOREAPP
                    if (svc != null)
                        machineName = svc.ConnectedOrgUriActual.Host;
#else
                    if (svc != null)
                        machineName = svc.CrmConnectOrgUriActual.Host;
#endif
                    return new SqlVariant(DataTypeHelpers.NVarChar(128, dataSource.DefaultCollation, CollationLabel.CoercibleDefault), dataSource.DefaultCollation.ToSqlString(machineName), context);

                case "pathseparator":
                    return new SqlVariant(DataTypeHelpers.NVarChar(1, dataSource.DefaultCollation, CollationLabel.CoercibleDefault), dataSource.DefaultCollation.ToSqlString(Path.DirectorySeparatorChar.ToString()), context);

                case "processid":
                    return new SqlVariant(DataTypeHelpers.Int, new SqlInt32(System.Diagnostics.Process.GetCurrentProcess().Id), context);

                case "productversion":
                    string orgVersion = null;

#if NETCOREAPP
                    if (svc != null)
                        orgVersion = svc.ConnectedOrgVersion.ToString();
#else
                    if (svc != null)
                        orgVersion = svc.ConnectedOrgVersion.ToString();
#endif

                    if (orgVersion == null)
                        orgVersion = ((RetrieveVersionResponse)dataSource.Execute(new RetrieveVersionRequest())).Version;

                    return new SqlVariant(DataTypeHelpers.NVarChar(128, dataSource.DefaultCollation, CollationLabel.CoercibleDefault), dataSource.DefaultCollation.ToSqlString(orgVersion), context);
            }

            return SqlVariant.Null;
        }

        [SqlFunction(IsDeterministic = false)]
        public static SqlVariant Sql_Variant_Property(SqlVariant expression, SqlString property, ExpressionExecutionContext context)
        {
            if (property.IsNull)
                return SqlVariant.Null;

            switch (property.Value.ToLowerInvariant())
            {
                case "basetype":
                    if (expression.BaseType == null)
                        return new SqlVariant(DataTypeHelpers.NVarChar(128, Collation.USEnglish, CollationLabel.CoercibleDefault), SqlString.Null, context);

                    if (expression.BaseType is SqlDataTypeReference sqlType)
                        return new SqlVariant(DataTypeHelpers.NVarChar(128, Collation.USEnglish, CollationLabel.CoercibleDefault), Collation.USEnglish.ToSqlString(sqlType.SqlDataTypeOption.ToString().ToLowerInvariant()), context);

                    return new SqlVariant(DataTypeHelpers.NVarChar(128, Collation.USEnglish, CollationLabel.CoercibleDefault), Collation.USEnglish.ToSqlString(expression.BaseType.ToSql()), context);

                case "precision":
                    if (expression.BaseType == null)
                        return new SqlVariant(DataTypeHelpers.Int, SqlInt32.Null, context);

                    return new SqlVariant(DataTypeHelpers.Int, new SqlInt32(expression.BaseType.GetPrecision()), context);

                case "scale":
                    if (expression.BaseType == null)
                        return new SqlVariant(DataTypeHelpers.Int, SqlInt32.Null, context);

                    return new SqlVariant(DataTypeHelpers.Int, new SqlInt32(expression.BaseType.GetScale()), context);

                case "totalbytes":
                    if (expression.BaseType == null)
                        return new SqlVariant(DataTypeHelpers.Int, SqlInt32.Null, context);

                    return new SqlVariant(DataTypeHelpers.Int, DataLength<INullable>(expression.Value, expression.BaseType), context);

                case "collation":
                    if (!(expression.BaseType is SqlDataTypeReferenceWithCollation coll))
                        return new SqlVariant(DataTypeHelpers.NVarChar(128, Collation.USEnglish, CollationLabel.CoercibleDefault), SqlString.Null, context);

                    return new SqlVariant(DataTypeHelpers.NVarChar(128, Collation.USEnglish, CollationLabel.CoercibleDefault), Collation.USEnglish.ToSqlString(coll.Collation.Name), context);

                case "maxlength":
                    if (expression.BaseType == null)
                        return new SqlVariant(DataTypeHelpers.Int, SqlInt32.Null, context);

                    return new SqlVariant(DataTypeHelpers.Int, new SqlInt32(expression.BaseType.GetSize()), context);
            }

            return SqlVariant.Null;
        }

        /// <summary>
        /// Tests whether a string contains valid JSON
        /// </summary>
        /// <param name="json">The string to test</param>
        /// <returns><c>null</c> if <paramref name="json"/> is null, <c>true</c> if the input is a valid JSON object or array or <c>false</c> otherwise</returns>
        [SqlFunction(IsDeterministic = true)]
        public static SqlBoolean IsJson(SqlString json)
        {
            if (json.IsNull)
                return SqlBoolean.Null;

            var reader = new Utf8JsonReader(Encoding.UTF8.GetBytes(json.Value).AsSpan());
            JsonElement? element;

            try
            {
                if (!JsonElement.TryParseValue(ref reader, out element) || element == null)
                    return false;
            }
            catch (JsonException)
            {
                return false;
            }

            if (!reader.IsFinalBlock)
                return false;

            return element.Value.ValueKind == JsonValueKind.Array || element.Value.ValueKind == JsonValueKind.Object;
        }

        /// <summary>
        /// Tests whether a string contains valid JSON
        /// </summary>
        /// <param name="json">The string to test</param>
        /// <param name="type">Specifies the JSON type to check in the input</param>
        /// <returns>Returns <c>true</c> if the string contains valid JSON; otherwise, returns <c>false</c>. Returns <c>null</c> if expression is null</returns>
        [SqlFunction(IsDeterministic = true)]
        public static SqlBoolean IsJson(SqlString json, SqlString type)
        {
            if (json.IsNull)
                return SqlBoolean.Null;

            var reader = new Utf8JsonReader(Encoding.UTF8.GetBytes(json.Value).AsSpan());
            JsonElement? element;

            try
            {
                if (!JsonElement.TryParseValue(ref reader, out element) || element == null)
                    return false;
            }
            catch (JsonException)
            {
                return false;
            }

            if (!reader.IsFinalBlock)
                return false;

            if (type.Value.Equals("VALUE", StringComparison.OrdinalIgnoreCase))
                return true;

            if (type.Value.Equals("ARRAY", StringComparison.OrdinalIgnoreCase))
                return element.Value.ValueKind == JsonValueKind.Array;

            if (type.Value.Equals("OBJECT", StringComparison.OrdinalIgnoreCase))
                return element.Value.ValueKind == JsonValueKind.Object;

            if (type.Value.Equals("SCALAR", StringComparison.OrdinalIgnoreCase))
                return element.Value.ValueKind == JsonValueKind.String || element.Value.ValueKind == JsonValueKind.Number;

            return false;
        }

        [SqlFunction(IsDeterministic = false)]
        public static SqlInt32 Error_Severity(ExpressionExecutionContext context)
        {
            if (context.Error == null)
                return SqlInt32.Null;

            return context.Error.Class;
        }

        [SqlFunction(IsDeterministic = false)]
        public static SqlInt32 Error_State(ExpressionExecutionContext context)
        {
            if (context.Error == null)
                return SqlInt32.Null;

            return context.Error.State;
        }

        [SqlFunction(IsDeterministic = false)]
        public static SqlInt32 Error_Number(ExpressionExecutionContext context)
        {
            if (context.Error == null)
                return SqlInt32.Null;

            return context.Error.Number;
        }

        [MaxLength(4000)]
        [SqlFunction(IsDeterministic = false)]
        public static SqlString Error_Message(ExpressionExecutionContext context)
        {
            if (context.Error == null)
                return SqlString.Null;

            return context.PrimaryDataSource.DefaultCollation.ToSqlString(context.Error.Message);
        }

        [SqlFunction(IsDeterministic = false)]
        public static SqlInt32 Error_Line(ExpressionExecutionContext context)
        {
            if (context.Error == null)
                return SqlInt32.Null;

            return context.Error.LineNumber;
        }

        [MaxLength(128)]
        [SqlFunction(IsDeterministic = false)]
        public static SqlString Error_Procedure(ExpressionExecutionContext context)
        {
            if (context.Error == null || context.Error.Procedure == null)
                return SqlString.Null;

            return context.PrimaryDataSource.DefaultCollation.ToSqlString(context.Error.Procedure);
        }

        [MaxLength(2048)]
        [SqlFunction(IsDeterministic = false)]
        public static SqlString FormatMessage(SqlString message, ExpressionExecutionContext context, params INullable[] parameters)
        {
            if (message.IsNull)
                return SqlString.Null;

            var regex = new Regex("%(?<flag>[-+0# ])?(?<width>([0-9]+|\\*))?(\\.(?<precision>([0-9]+|\\*)))?(?<size>h|l)?(?<type>[diosuxXc]|I64d|S_MSG)");
            var paramIndex = 0;

            T GetValue<T>()
            {
                if (paramIndex >= parameters.Length)
                    throw new QueryExecutionException(Sql4CdsError.InvalidDataTypeForSubstitutionParameter(paramIndex + 1));

                if (!(parameters[paramIndex] is T val))
                    throw new QueryExecutionException(Sql4CdsError.InvalidDataTypeForSubstitutionParameter(paramIndex + 1));

                paramIndex++;
                return val;
            }

            var msg = regex.Replace(message.Value, match =>
            {
                var flag = match.Groups["flag"].Success ? match.Groups["flag"].Value : string.Empty;
                var width = match.Groups["width"].Success ? match.Groups["width"].Value : null;
                var precision = match.Groups["precision"].Success ? match.Groups["precision"].Value : null;
                var size = match.Groups["size"].Success ? match.Groups["size"].Value : null;
                var type = match.Groups["type"].Value;

                if (width == "*")
                    width = GetValue<SqlInt32>().Value.ToString();

                if (precision == "*")
                    precision = GetValue<SqlInt32>().Value.ToString();

                string formatted;

                var formatString = "0";

                if (flag.Contains("0") && width != null)
                    formatString = formatString.PadLeft(Int32.Parse(width), '0');

                if (precision != null)
                    formatString = formatString.PadLeft(Int32.Parse(precision), '0');

                var negativeFormatString = formatString == "0" ? "0" : formatString.Substring(0, formatString.Length - 1);

                if (flag.Contains("+"))
                    formatString = "+" + formatString + ";-" + negativeFormatString;
                else if (flag.Contains(" "))
                    formatString = " " + formatString + ";-" + negativeFormatString;
                else
                    formatString = formatString + ";-" + negativeFormatString;

                switch (type)
                {
                    case "d":
                    case "i":
                    case "o":
                    case "u":
                    case "x":
                    case "X":
                        var intValue = GetValue<SqlInt32>();

                        if (intValue.IsNull)
                            return "(null)";

                        if (type == "d" || type == "i")
                        {
                            formatted = intValue.Value.ToString(formatString);
                        }
                        else if (type == "o")
                        {
                            formatted = Convert.ToString(intValue.Value, 8);

                            if (flag.Contains("#") && intValue.Value != 0)
                                formatted = "0" + formatted;

                            if (precision != null)
                                formatted = formatted.PadLeft(Int32.Parse(precision), '0');
                        }
                        else if (type == "u")
                        {
                            formatted = ((uint)intValue.Value).ToString();

                            if (precision != null)
                                formatted = formatted.PadLeft(Int32.Parse(precision), '0');
                        }
                        else if (type == "x" || type == "X")
                        {
                            formatted = ((uint)intValue.Value).ToString(type);

                            if (precision != null)
                                formatted = formatted.PadLeft(Int32.Parse(precision), '0');

                            if (flag.Contains("#"))
                                formatted = "0" + type + formatted;
                        }
                        else
                        {
                            throw new QueryExecutionException(Sql4CdsError.InvalidFormatSpecification(match.Value));
                        }
                        break;

                    case "I64d":
                        var bigintValue = GetValue<SqlDecimal>();

                        if (bigintValue.IsNull)
                            return "(null)";

                        formatted = ((long)bigintValue.Value).ToString(formatString);
                        break;

                    case "s":
                    case "S_MSG":
                    case "c":
                        var strValue = GetValue<SqlString>();

                        if (strValue.IsNull)
                            return "(null)";

                        formatted = strValue.Value;

                        if (precision != null && formatted.Length > Int32.Parse(precision))
                            formatted = formatted.Substring(Int32.Parse(precision));
                        break;

                    default:
                        throw new QueryExecutionException(Sql4CdsError.InvalidFormatSpecification(match.Value));
                }

                if (width != null && formatted.Length < Int32.Parse(width))
                {
                    if (flag.Contains("-"))
                        formatted = formatted.PadRight(Int32.Parse(width));
                    else if (flag.Contains("0"))
                        formatted = formatted.PadLeft(Int32.Parse(width), '0');
                    else
                        formatted = formatted.PadLeft(Int32.Parse(width));
                }

                return formatted;
            });

            return (context?.PrimaryDataSource.DefaultCollation ?? Collation.USEnglish).ToSqlString(msg);
        }

        [SqlFunction(IsDeterministic = false)]
        public static SqlGuid NewId()
        {
            return Guid.NewGuid();
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
        /// Given a lambda expression that accesses a property, return the property info
        /// </summary>
        /// <typeparam name="T">The type of value returned by the property</typeparam>
        /// <param name="expression">The expression</param>
        /// <returns>The property details</returns>
        public static PropertyInfo GetPropertyInfo<T>(Expression<Func<T>> expression)
        {
            var lambda = (LambdaExpression)expression;
            var prop = lambda.Body as MemberExpression;

            if (prop == null)
                throw new ArgumentException("Invalid Expression. Expression should consist of a property access only.");

            return prop.Member as PropertyInfo;
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
    [AttributeUsage(AttributeTargets.Parameter | AttributeTargets.Method)]
    class MaxLengthAttribute : Attribute
    {
        /// <summary>
        /// Sets the maximum length of the result as the maximum length of this parameter
        /// </summary>
        public MaxLengthAttribute()
        {
        }

        /// <summary>
        /// Sets the maximum length of the result to a fixed value
        /// </summary>
        /// <param name="maxLength"></param>
        public MaxLengthAttribute(int maxLength)
        {
            MaxLength = maxLength;
        }

        public int? MaxLength { get; }
    }

    /// <summary>
    /// Indicates that the parameter gives the type of the returned value
    /// </summary>
    [AttributeUsage(AttributeTargets.Parameter)]
    class TargetTypeAttribute : Attribute
    {
    }

    /// <summary>
    /// Indicates that the parameter gives the orignal SQL type of another parameter
    /// </summary>
    [AttributeUsage(AttributeTargets.Parameter)]
    class SourceTypeAttribute : Attribute
    {
        public SourceTypeAttribute(string sourceParameter)
        {
            SourceParameter = sourceParameter;
        }

        /// <summary>
        /// Returns the name of the parameter this provides the original type of
        /// </summary>
        public string SourceParameter { get; }
    }

    /// <summary>
    /// Indicates that the parameter is optional
    /// </summary>
    [AttributeUsage(AttributeTargets.Parameter)]
    class OptionalAttribute : Attribute
    {
    }

    /// <summary>
    /// Indicates that a function is collation sensitive
    /// </summary>
    [AttributeUsage(AttributeTargets.Method)]
    class CollationSensitiveAttribute : Attribute
    {
    }

    /// <summary>
    /// The available date parts for the DATEPART function
    /// </summary>
    enum DatePart
    {
        Year,
        Quarter,
        Month,
        DayOfYear,
        Day,
        Week,
        WeekDay,
        Hour,
        Minute,
        Second,
        Millisecond,
        Microsecond,
        Nanosecond,
        TZOffset,
        ISOWeek,
    }
}
