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
    }
}
