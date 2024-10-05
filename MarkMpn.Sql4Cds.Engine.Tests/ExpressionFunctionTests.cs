using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace MarkMpn.Sql4Cds.Engine.Tests
{
    [TestClass]
    public class ExpressionFunctionTests
    {
        [TestMethod]
        public void DatePart_Week()
        {
            // https://learn.microsoft.com/en-us/sql/t-sql/functions/datepart-transact-sql?view=sql-server-ver16#week-and-weekday-datepart-arguments
            // Assuming default SET DATEFIRST 7 -- ( Sunday )
            var actual = ExpressionFunctions.DatePart("week", (SqlDateTime)new DateTime(2007, 4, 21), DataTypeHelpers.DateTime);
            Assert.AreEqual(16, actual);
        }

        [TestMethod]
        public void DatePart_WeekDay()
        {
            // https://learn.microsoft.com/en-us/sql/t-sql/functions/datepart-transact-sql?view=sql-server-ver16#week-and-weekday-datepart-arguments
            // Assuming default SET DATEFIRST 7 -- ( Sunday )
            var actual = ExpressionFunctions.DatePart("weekday", (SqlDateTime)new DateTime(2007, 4, 21), DataTypeHelpers.DateTime);
            Assert.AreEqual(7, actual);
        }

        [TestMethod]
        public void DatePart_TZOffset()
        {
            // https://learn.microsoft.com/en-us/sql/t-sql/functions/datepart-transact-sql?view=sql-server-ver16#tzoffset
            var actual = ExpressionFunctions.DatePart("tzoffset", (SqlDateTimeOffset)new DateTimeOffset(2007, 5, 10, 0, 0, 1, TimeSpan.FromMinutes(5 * 60 + 10)), DataTypeHelpers.DateTimeOffset);
            Assert.AreEqual(310, actual);
        }

        [TestMethod]
        public void DatePart_ErrorOnInvalidPartsForTimeValue()
        {
            // https://learn.microsoft.com/en-us/sql/t-sql/functions/datepart-transact-sql?view=sql-server-ver16#default-returned-for-a-datepart-that-isnt-in-a-date-argument
            try
            {
                ExpressionFunctions.DatePart("year", new SqlTime(new TimeSpan(0, 12, 10, 30, 123)), DataTypeHelpers.Time(7));
                Assert.Fail();
            }
            catch (QueryExecutionException ex)
            {
                Assert.AreEqual(9810, ex.Errors.Single().Number);
            }
        }

        [DataTestMethod]
        [DataRow("millisecond", 123)]
        [DataRow("microsecond", 123456)]
        [DataRow("nanosecond", 123456700)]
        public void DatePart_FractionalSeconds(string part, int expected)
        {
            // https://learn.microsoft.com/en-us/sql/t-sql/functions/datepart-transact-sql?view=sql-server-ver16#fractional-seconds
            var actual = ExpressionFunctions.DatePart(part, (SqlString)"00:00:01.1234567", DataTypeHelpers.VarChar(100, Collation.USEnglish, CollationLabel.CoercibleDefault));
            Assert.AreEqual(expected, actual);
        }

        [DataTestMethod]
        [DataRow("20240830")]
        [DataRow("2024-08-31")]
        public void DateAdd_MonthLimitedToDaysInFollowingMonth(string date)
        {
            // https://learn.microsoft.com/en-us/sql/t-sql/functions/dateadd-transact-sql?view=sql-server-ver16#datepart-argument
            SqlDateParsing.TryParse(date, DateFormat.mdy, out SqlDateTime startDate);
            var actual = ExpressionFunctions.DateAdd("month", 1, startDate, DataTypeHelpers.DateTime);
            Assert.AreEqual(new SqlDateTime(2024, 9, 30), (SqlDateTime)actual);
        }

        [DataTestMethod]
        [DataRow(2147483647)]
        [DataRow(-2147483647)]
        public void DateAdd_ThrowsIfResultIsOutOfRange(int number)
        {
            // https://learn.microsoft.com/en-us/sql/t-sql/functions/dateadd-transact-sql?view=sql-server-ver16#date-argument
            try
            {
                ExpressionFunctions.DateAdd("year", number, new SqlDateTime(2024, 7, 31), DataTypeHelpers.DateTime);
            }
            catch (QueryExecutionException ex)
            {
                Assert.AreEqual(517, ex.Errors.Single().Number);
            }
        }

        [DataTestMethod]
        [DataRow(-30, 0)]
        [DataRow(29, 0)]
        [DataRow(-31, -1)]
        [DataRow(30, 1)]
        public void DateAdd_SmallDateTimeSeconds(int number, int expectedMinutesDifference)
        {
            // https://learn.microsoft.com/en-us/sql/t-sql/functions/dateadd-transact-sql?view=sql-server-ver16#return-values-for-a-smalldatetime-date-and-a-second-or-fractional-seconds-datepart
            var startDateTime = new DateTime(2024, 10, 5);
            var actual = ((SqlSmallDateTime)ExpressionFunctions.DateAdd("second", number, new SqlSmallDateTime(startDateTime), DataTypeHelpers.SmallDateTime)).Value;
            var expected = startDateTime.AddMinutes(expectedMinutesDifference);
            Assert.AreEqual(expected, actual);
        }

        [DataTestMethod]
        [DataRow(-30001, 0)]
        [DataRow(29998, 0)]
        [DataRow(-30002, -1)]
        [DataRow(29999, 1)]
        public void DateAdd_SmallDateTimeMilliSeconds(int number, int expectedMinutesDifference)
        {
            // https://learn.microsoft.com/en-us/sql/t-sql/functions/dateadd-transact-sql?view=sql-server-ver16#return-values-for-a-smalldatetime-date-and-a-second-or-fractional-seconds-datepart
            var startDateTime = new DateTime(2024, 10, 5);
            var actual = ((SqlSmallDateTime)ExpressionFunctions.DateAdd("millisecond", number, new SqlSmallDateTime(startDateTime), DataTypeHelpers.SmallDateTime)).Value;
            var expected = startDateTime.AddMinutes(expectedMinutesDifference);
            Assert.AreEqual(expected, actual);
        }

        [DataTestMethod]
        [DataRow("millisecond", 1, 1121111)]
        [DataRow("millisecond", 2, 1131111)]
        [DataRow("microsecond", 1, 1111121)]
        [DataRow("microsecond", 2, 1111131)]
        [DataRow("nanosecond", 49, 1111111)]
        [DataRow("nanosecond", 50, 1111112)]
        [DataRow("nanosecond", 150, 1111113)]
        public void DateAdd_FractionalSeconds(string datepart, int number, int expected)
        {
            // https://learn.microsoft.com/en-us/sql/t-sql/functions/dateadd-transact-sql?view=sql-server-ver16#fractional-seconds-precision
            var startDateTime = new DateTime(2024, 1, 1, 13, 10, 10).AddTicks(1111111);
            var actual = ExpressionFunctions.DateAdd(datepart, number, new SqlDateTime2(startDateTime), DataTypeHelpers.DateTime2(7)).Value;
            Assert.AreEqual(expected, actual.Ticks % TimeSpan.TicksPerSecond);
        }

        [DataTestMethod]
        [DataRow("year", "2025-01-01 13:10:10.1111111")]
        [DataRow("quarter", "2024-04-01 13:10:10.1111111")]
        [DataRow("month", "2024-02-01 13:10:10.1111111")]
        [DataRow("dayofyear", "2024-01-02 13:10:10.1111111")]
        [DataRow("day", "2024-01-02 13:10:10.1111111")]
        [DataRow("week", "2024-01-08 13:10:10.1111111")]
        [DataRow("weekday", "2024-01-02 13:10:10.1111111")]
        [DataRow("hour", "2024-01-01 14:10:10.1111111")]
        [DataRow("minute", "2024-01-01 13:11:10.1111111")]
        [DataRow("second", "2024-01-01 13:10:11.1111111")]
        [DataRow("millisecond", "2024-01-01 13:10:10.1121111")]
        [DataRow("microsecond", "2024-01-01 13:10:10.1111121")]
        [DataRow("nanosecond", "2024-01-01 13:10:10.1111111")]
        public void DateAdd_DateParts(string datepart, string expected)
        {
            // https://learn.microsoft.com/en-us/sql/t-sql/functions/dateadd-transact-sql?view=sql-server-ver16#a-increment-datepart-by-an-interval-of-1
            var startDateTime = new DateTime(2024, 1, 1, 13, 10, 10).AddTicks(1111111);
            var actual = ExpressionFunctions.DateAdd(datepart, 1, new SqlDateTime2(startDateTime), DataTypeHelpers.DateTime2(7)).Value;
            Assert.AreEqual(expected, actual.ToString("yyyy-MM-dd HH:mm:ss.fffffff"));
        }

        [DataTestMethod]
        [DataRow("quarter", 4, "2025-01-01 01:01:01.1111111")]
        [DataRow("month", 13, "2025-02-01 01:01:01.1111111")]
        [DataRow("dayofyear", 366, "2025-01-01 01:01:01.1111111")] // NOTE: Docs used 365, but 2024 is a leap year
        [DataRow("day", 366, "2025-01-01 01:01:01.1111111")] // NOTE: Docs used 365, but 2024 is a leap year
        [DataRow("week", 5, "2024-02-05 01:01:01.1111111")]
        [DataRow("weekday", 31, "2024-02-01 01:01:01.1111111")]
        [DataRow("hour", 23, "2024-01-02 00:01:01.1111111")]
        [DataRow("minute", 59, "2024-01-01 02:00:01.1111111")]
        [DataRow("second", 59, "2024-01-01 01:02:00.1111111")]
        [DataRow("millisecond", 1, "2024-01-01 01:01:01.1121111")]
        public void DateAdd_Carry(string datepart, int number, string expected)
        {
            // https://learn.microsoft.com/en-us/sql/t-sql/functions/dateadd-transact-sql?view=sql-server-ver16#b-increment-more-than-one-level-of-datepart-in-one-statement
            var startDateTime = new DateTime(2024, 1, 1, 1, 1, 1).AddTicks(1111111);
            var actual = ExpressionFunctions.DateAdd(datepart, number, new SqlDateTime2(startDateTime), DataTypeHelpers.DateTime2(7)).Value;
            Assert.AreEqual(expected, actual.ToString("yyyy-MM-dd HH:mm:ss.fffffff"));
        }

        [DataTestMethod]
        [DataRow("microsecond")]
        [DataRow("nanosecond")]
        public void DateAdd_MicroSecondAndNanoSecondNotSupportedForSmallDateTime(string datepart)
        {
            // https://learn.microsoft.com/en-us/sql/t-sql/functions/dateadd-transact-sql?view=sql-server-ver16#fractional-seconds-precision
            try
            {
                ExpressionFunctions.DateAdd(datepart, 1, new SqlSmallDateTime(new DateTime(2024, 1, 1)), DataTypeHelpers.SmallDateTime);
                Assert.Fail();
            }
            catch (QueryExecutionException ex)
            {
                Assert.AreEqual(9810, ex.Errors.Single().Number);
            }
        }

        [DataTestMethod]
        [DataRow("microsecond")]
        [DataRow("nanosecond")]
        public void DateAdd_MicroSecondAndNanoSecondNotSupportedForDate(string datepart)
        {
            // https://learn.microsoft.com/en-us/sql/t-sql/functions/dateadd-transact-sql?view=sql-server-ver16#fractional-seconds-precision
            try
            {
                ExpressionFunctions.DateAdd(datepart, 1, new SqlDate(new DateTime(2024, 1, 1)), DataTypeHelpers.Date);
                Assert.Fail();
            }
            catch (QueryExecutionException ex)
            {
                Assert.AreEqual(9810, ex.Errors.Single().Number);
            }
        }

        [DataTestMethod]
        [DataRow("microsecond")]
        [DataRow("nanosecond")]
        public void DateAdd_MicroSecondAndNanoSecondNotSupportedForDateTime(string datepart)
        {
            // https://learn.microsoft.com/en-us/sql/t-sql/functions/dateadd-transact-sql?view=sql-server-ver16#fractional-seconds-precision
            try
            {
                ExpressionFunctions.DateAdd(datepart, 1, new SqlDateTime(new DateTime(2024, 1, 1)), DataTypeHelpers.DateTime);
                Assert.Fail();
            }
            catch (QueryExecutionException ex)
            {
                Assert.AreEqual(9810, ex.Errors.Single().Number);
            }
        }

        [DataTestMethod]
        [DataRow("weekday")]
        [DataRow("tzoffset")]
        [DataRow("nanosecond")]
        public void DateTrunc_InvalidDateParts(string datepart)
        {
            // https://learn.microsoft.com/en-us/sql/t-sql/functions/datetrunc-transact-sql?view=sql-server-ver16#datepart
            try
            {
                ExpressionFunctions.DateTrunc(datepart, new SqlDateTime(new DateTime(2024, 1, 1)), DataTypeHelpers.DateTime);
                Assert.Fail();
            }
            catch (QueryExecutionException ex)
            {
                Assert.AreEqual(9810, ex.Errors.Single().Number);
            }
        }

        [DataTestMethod]
        // date doesn't support any of the time-based dateparts
        [DataRow("date", "hour")]
        [DataRow("date", "minute")]
        [DataRow("date", "second")]
        [DataRow("date", "millisecond")]
        [DataRow("date", "microsecond")]

        // datetime doesn't support microsecond
        [DataRow("datetime", "microsecond")]

        // smalldatetime doesn't support millisecond or microsecond
        [DataRow("smalldatetime", "millisecond")]
        [DataRow("smalldatetime", "microsecond")]

        // datetime2, datetimeoffset and time vary depending on scale
        [DataRow("datetime2(1)", "millisecond")]
        [DataRow("datetime2(1)", "microsecond")]
        [DataRow("datetime2(2)", "millisecond")]
        [DataRow("datetime2(2)", "microsecond")]
        [DataRow("datetime2(3)", "microsecond")]
        [DataRow("datetime2(4)", "microsecond")]
        [DataRow("datetime2(5)", "microsecond")]

        [DataRow("datetimeoffset(1)", "millisecond")]
        [DataRow("datetimeoffset(1)", "microsecond")]
        [DataRow("datetimeoffset(2)", "millisecond")]
        [DataRow("datetimeoffset(2)", "microsecond")]
        [DataRow("datetimeoffset(3)", "microsecond")]
        [DataRow("datetimeoffset(4)", "microsecond")]
        [DataRow("datetimeoffset(5)", "microsecond")]

        [DataRow("time(1)", "millisecond")]
        [DataRow("time(1)", "microsecond")]
        [DataRow("time(2)", "millisecond")]
        [DataRow("time(2)", "microsecond")]
        [DataRow("time(3)", "microsecond")]
        [DataRow("time(4)", "microsecond")]
        [DataRow("time(5)", "microsecond")]

        // time also doesn't support any date-based dateparts
        [DataRow("time", "year")]
        [DataRow("time", "quarter")]
        [DataRow("time", "month")]
        [DataRow("time", "dayofyear")]
        [DataRow("time", "day")]
        [DataRow("time", "week")]
        public void DateTrunc_RequiresMinimalPrecision(string datetype, string datepart)
        {
            DataTypeHelpers.TryParse(null, datetype, out var type);

            // https://learn.microsoft.com/en-us/sql/t-sql/functions/datetrunc-transact-sql?view=sql-server-ver16#fractional-time-scale-precision
            try
            {
                ExpressionFunctions.DateTrunc(datepart, new SqlDateTime(new DateTime(2024, 1, 1)), type);
                Assert.Fail();
            }
            catch (QueryExecutionException ex)
            {
                Assert.AreEqual(9810, ex.Errors.Single().Number);
            }
        }

        [DataTestMethod]
        [DataRow("year", "2021-01-01 00:00:00.0000000")]
        [DataRow("quarter", "2021-10-01 00:00:00.0000000")]
        [DataRow("month", "2021-12-01 00:00:00.0000000")]
        [DataRow("week", "2021-12-05 00:00:00.0000000")]
        [DataRow("iso_week", "2021-12-06 00:00:00.0000000")]
        [DataRow("dayofyear", "2021-12-08 00:00:00.0000000")]
        [DataRow("day", "2021-12-08 00:00:00.0000000")]
        [DataRow("hour", "2021-12-08 11:00:00.0000000")]
        [DataRow("minute", "2021-12-08 11:30:00.0000000")]
        [DataRow("second", "2021-12-08 11:30:15.0000000")]
        [DataRow("millisecond", "2021-12-08 11:30:15.1230000")]
        [DataRow("microsecond", "2021-12-08 11:30:15.1234560")]
        public void DateTrunc_Values(string datepart, string expected)
        {
            // https://learn.microsoft.com/en-us/sql/t-sql/functions/datetrunc-transact-sql?view=sql-server-ver16#a-use-different-datepart-options
            var actual = ExpressionFunctions.DateTrunc(datepart, new SqlDateTime2(new DateTime(2021, 12, 8, 11, 30, 15).AddTicks(1234567)), DataTypeHelpers.DateTime2(7));
            Assert.AreEqual(expected, actual.Value.ToString("yyyy-MM-dd HH:mm:ss.fffffff"));
        }

        [DataTestMethod]
        [DataRow("year", "2005-12-31 23:59:59.9999999", "2006-01-01 00:00:00.0000000")]
        [DataRow("quarter", "2005-12-31 23:59:59.9999999", "2006-01-01 00:00:00.0000000")]
        [DataRow("month", "2005-12-31 23:59:59.9999999", "2006-01-01 00:00:00.0000000")]
        [DataRow("dayofyear", "2005-12-31 23:59:59.9999999", "2006-01-01 00:00:00.0000000")]
        [DataRow("day", "2005-12-31 23:59:59.9999999", "2006-01-01 00:00:00.0000000")]
        [DataRow("week", "2005-12-31 23:59:59.9999999", "2006-01-01 00:00:00.0000000")]
        [DataRow("weekday", "2005-12-31 23:59:59.9999999", "2006-01-01 00:00:00.0000000")]
        [DataRow("hour", "2005-12-31 23:59:59.9999999", "2006-01-01 00:00:00.0000000")]
        [DataRow("minute", "2005-12-31 23:59:59.9999999", "2006-01-01 00:00:00.0000000")]
        [DataRow("second", "2005-12-31 23:59:59.9999999", "2006-01-01 00:00:00.0000000")]
        [DataRow("millisecond", "2005-12-31 23:59:59.9999999", "2006-01-01 00:00:00.0000000")]
        [DataRow("microsecond", "2005-12-31 23:59:59.9999999", "2006-01-01 00:00:00.0000000")]
        public void DateDiff_1Boundary(string datepart, string startdate, string enddate)
        {
            // https://learn.microsoft.com/en-us/sql/t-sql/functions/datediff-transact-sql?view=sql-server-ver16#datepart-boundaries
            SqlDateParsing.TryParse(startdate, DateFormat.mdy, out SqlDateTimeOffset start);
            SqlDateParsing.TryParse(enddate, DateFormat.mdy, out SqlDateTimeOffset end);
            var actual = ExpressionFunctions.DateDiff(datepart, start, end, DataTypeHelpers.DateTimeOffset, DataTypeHelpers.DateTimeOffset);
            Assert.AreEqual(1, actual);
        }

        [TestMethod]
        public void DateDiff_TimeZone()
        {
            var dateTime = new DateTime(2024, 10, 5, 12, 0, 0);
            var offset = new DateTimeOffset(dateTime, TimeSpan.FromHours(1));
            var actual = ExpressionFunctions.DateDiff("hour", new SqlDateTime(dateTime), new SqlDateTimeOffset(offset), DataTypeHelpers.DateTime, DataTypeHelpers.DateTimeOffset);
            Assert.AreEqual(-1, actual);
        }
    }
}
