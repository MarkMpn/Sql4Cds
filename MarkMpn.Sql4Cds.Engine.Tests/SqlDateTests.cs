using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace MarkMpn.Sql4Cds.Engine.Tests
{
    [TestClass]
    public class SqlDateTests
    {
        [DataTestMethod]
        [DataRow("[M]M/dd/[yy]yy", "M/d/yy;M/d/yyyy;M/dd/yy;M/dd/yyyy;MM/d/yy;MM/d/yyyy;MM/dd/yy;MM/dd/yyyy")]
        public void ConvertSqlFormatStringToNet(string input, string expected)
        {
            var actual = SqlDateParsing.SqlToNetFormatString(input);
            CollectionAssert.AreEquivalent(expected.Split(';'), actual);
        }

        [DataTestMethod]
        [DataRow("4/21/2007")]
        [DataRow("4-21-2007")]
        [DataRow("4.21.2007")]
        [DataRow("Apr 21, 2007")]
        [DataRow("Apr 2007 21")]
        [DataRow("21 April, 2007")]
        [DataRow("21 2007 Apr")]
        [DataRow("2007 April 21")]
        [DataRow("2007-04-21")]
        [DataRow("20070421")]
        public void ParseMDY(string input)
        {
            Assert.IsTrue(SqlDate.TryParse(input, DateFormat.mdy, out var actual));
            Assert.AreEqual(new DateTime(2007, 4, 21), actual.Value);
        }

        [DataTestMethod]
        [DataRow("4/2007/21")]
        [DataRow("4-2007-21")]
        [DataRow("4.2007.21")]
        [DataRow("Apr 21, 2007")]
        [DataRow("Apr 2007 21")]
        [DataRow("21 April, 2007")]
        [DataRow("21 2007 Apr")]
        [DataRow("2007 April 21")]
        [DataRow("2007-04-21")]
        [DataRow("20070421")]
        public void ParseMYD(string input)
        {
            Assert.IsTrue(SqlDate.TryParse(input, DateFormat.myd, out var actual));
            Assert.AreEqual(new DateTime(2007, 4, 21), actual.Value);
        }

        [DataTestMethod]
        [DataRow("21/4/2007")]
        [DataRow("21-4-2007")]
        [DataRow("21.4.2007")]
        [DataRow("Apr 21, 2007")]
        [DataRow("Apr 2007 21")]
        [DataRow("21 April, 2007")]
        [DataRow("21 2007 Apr")]
        [DataRow("2007 April 21")]
        [DataRow("2007-04-21")]
        [DataRow("20070421")]
        public void ParseDMY(string input)
        {
            Assert.IsTrue(SqlDate.TryParse(input, DateFormat.dmy, out var actual));
            Assert.AreEqual(new DateTime(2007, 4, 21), actual.Value);
        }

        [DataTestMethod]
        [DataRow("21/2007/4")]
        [DataRow("21-2007-4")]
        [DataRow("21.2007.4")]
        [DataRow("Apr 21, 2007")]
        [DataRow("Apr 2007 21")]
        [DataRow("21 April, 2007")]
        [DataRow("21 2007 Apr")]
        [DataRow("2007 April 21")]
        [DataRow("2007-04-21")]
        [DataRow("20070421")]
        public void ParseDYM(string input)
        {
            Assert.IsTrue(SqlDate.TryParse(input, DateFormat.dym, out var actual));
            Assert.AreEqual(new DateTime(2007, 4, 21), actual.Value);
        }

        [DataTestMethod]
        [DataRow("2007/4/21")]
        [DataRow("2007-4-21")]
        [DataRow("2007.4.21")]
        [DataRow("Apr 21, 2007")]
        [DataRow("Apr 2007 21")]
        [DataRow("21 April, 2007")]
        [DataRow("21 2007 Apr")]
        [DataRow("2007 April 21")]
        [DataRow("2007-04-21")]
        [DataRow("20070421")]
        public void ParseYMD(string input)
        {
            Assert.IsTrue(SqlDate.TryParse(input, DateFormat.ymd, out var actual));
            Assert.AreEqual(new DateTime(2007, 4, 21), actual.Value);
        }

        [DataTestMethod]
        [DataRow("12:30:45")] // TIME only
        //[DataRow("+02:00")] // TIMEZONE only - mentioned as valid in documentation but doesn't work in practise
        [DataRow("12:30:45+02:00")] // TIME + TIMEZONE
        public void UseDefaultValuesForTimeStrings(string input)
        {
            // https://learn.microsoft.com/en-us/sql/t-sql/data-types/date-transact-sql?view=sql-server-ver16#convert-string-literals-to-date
            Assert.IsTrue(SqlDate.TryParse(input, DateFormat.ymd, out var actual));
            Assert.AreEqual(new DateTime(1900, 1, 1), actual.Value);
        }

        [DataTestMethod]
        [DataRow("2007-04-21 12:30:45")] // DATE + TIME
        [DataRow("2007-04-21 12:30:45+02:00")] // DATE + TIME + TIMEZONE
        public void IgnoreTime(string input)
        {
            // https://learn.microsoft.com/en-us/sql/t-sql/data-types/date-transact-sql?view=sql-server-ver16#convert-string-literals-to-date
            Assert.IsTrue(SqlDate.TryParse(input, DateFormat.ymd, out var actual));
            Assert.AreEqual(new DateTime(2007, 4, 21), actual.Value);
        }
    }
}
