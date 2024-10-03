using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace MarkMpn.Sql4Cds.Engine.Tests
{
    [TestClass]
    public class SqlDateTimeTests
    {
        [DataTestMethod]
        [DataRow("4/15/96", "mdy")]
        [DataRow("04/15/96", "mdy")]
        [DataRow("4/15/1996", "mdy")]
        [DataRow("04/15/1996", "mdy")]

        [DataRow("4-15-96", "mdy")]
        [DataRow("04-15-96", "mdy")]
        [DataRow("4-15-1996", "mdy")]
        [DataRow("04-15-1996", "mdy")]

        [DataRow("4.15.96", "mdy")]
        [DataRow("04.15.96", "mdy")]
        [DataRow("4.15.1996", "mdy")]
        [DataRow("04.15.1996", "mdy")]

        [DataRow("4/96/15", "myd")]
        [DataRow("04/96/15", "myd")]
        [DataRow("4/1996/15", "myd")]
        [DataRow("04/1996/15", "myd")]

        [DataRow("15/4/96", "dmy")]
        [DataRow("15/04/96", "dmy")]
        [DataRow("15/4/1996", "dmy")]
        [DataRow("15/04/1996", "dmy")]

        [DataRow("15/96/4", "dym")]
        [DataRow("15/96/04", "dym")]
        [DataRow("15/1996/4", "dym")]
        [DataRow("15/1996/04", "dym")]

        [DataRow("96/15/4", "ydm")]
        [DataRow("96/15/04", "ydm")]
        [DataRow("1996/15/4", "ydm")]
        [DataRow("1996/15/04", "ydm")]

        [DataRow("96/4/15", "ymd")]
        [DataRow("96/04/15", "ymd")]
        [DataRow("1996/4/15", "ymd")]
        [DataRow("1996/04/15", "ymd")]
        public void NumericDateFormat(string input, string order)
        {
            var format = (DateFormat)Enum.Parse(typeof(DateFormat), order);
            Assert.IsTrue(SqlDateParsing.TryParse(input, format, out SqlDateTime actual));
            Assert.AreEqual(new DateTime(1996, 4, 15), actual.Value);
        }

        [DataTestMethod]
        [DataRow("04/15/1996 14:30", "14:30:00")]
        [DataRow("04/15/1996 14:30:20", "14:30:20")]
        [DataRow("04/15/1996 14:30:20:997", "14:30:20.997")]
        [DataRow("04/15/1996 14:30:20.9", "14:30:20.9")]
        [DataRow("04/15/1996 4am", "04:00:00")]
        [DataRow("04/15/1996 4 PM", "16:00:00")]
        public void NumericDateFormatWithTime(string input, string expectedTime)
        {
            Assert.IsTrue(SqlDateParsing.TryParse(input, DateFormat.mdy, out SqlDateTime actual));
            Assert.AreEqual(new DateTime(1996, 4, 15), actual.Value.Date);
            Assert.AreEqual(TimeSpan.Parse(expectedTime), actual.Value.TimeOfDay);
        }

        [DataTestMethod]
        [DataRow("Apr 1996", false)]
        [DataRow("April 1996", false)]
        [DataRow("April 15 1996", true)]
        [DataRow("April 15, 1996", true)]
        [DataRow("April 15 96", true)]
        [DataRow("April 15, 96", true)]
        [DataRow("Apr 1996 15", true)]
        [DataRow("April 1996 15", true)]
        [DataRow("Apr, 1996", false)]
        [DataRow("April, 1996", false)]
        [DataRow("15 Apr, 1996", true)]
        [DataRow("15 April, 1996", true)]
        [DataRow("15 Apr,1996", true)]
        [DataRow("15 April,1996", true)]
        [DataRow("15 Apr,96", true)]
        [DataRow("15 April,96", true)]
        [DataRow("15 Apr96", true)]
        [DataRow("15 April96", true)]
        [DataRow("15 96 apr", true)]
        [DataRow("15 96 april", true)]
        [DataRow("15 1996 apr", true)]
        [DataRow("15 1996 april", true)]
        [DataRow("1996 apr", false)]
        [DataRow("1996 april", false)]
        [DataRow("1996 apr 15", true)]
        [DataRow("1996 april 15", true)]
        [DataRow("1996 15 apr", true)]
        [DataRow("1996 15 april", true)]
        public void AlphaFormat(string input, bool includesDay)
        {
            Assert.IsTrue(SqlDateParsing.TryParse(input, DateFormat.mdy, out SqlDateTime actual));

            if (includesDay)
                Assert.AreEqual(new DateTime(1996, 4, 15), actual.Value);
            else
                Assert.AreEqual(new DateTime(1996, 4, 1), actual.Value);
        }

        [DataTestMethod]
        [DataRow("2004-05-23T14:25:10", false)]
        [DataRow("2004-05-23T14:25:10.487", true)]
        [DataRow("20040523 14:25:10", false)]
        [DataRow("20040523 14:25:10.487", true)]
        public void IsoFormat(string input, bool milli)
        {
            Assert.IsTrue(SqlDateParsing.TryParse(input, DateFormat.mdy, out SqlDateTime actual));

            if (milli)
                Assert.AreEqual(new DateTime(2004, 5, 23, 14, 25, 10, 487), actual.Value);
            else
                Assert.AreEqual(new DateTime(2004, 5, 23, 14, 25, 10), actual.Value);
        }
    }
}
