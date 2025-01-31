using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.XTB;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace MarkMpn.Sql4Cds.Tests
{
    [TestClass]
    public class FormatterTests
    {
        [TestMethod]
        public void SimpleSelect()
        {
            var original = @"select * from tbl";
            var expected = @"SELECT *
FROM   tbl;";
            Assert.AreEqual(expected, Formatter.Format(original));
        }

        [TestMethod]
        public void SingleLineLeadingComment()
        {
            var original = @"-- comment here
select * from tbl";
            var expected = @"-- comment here
SELECT *
FROM   tbl;";
            Assert.AreEqual(expected, Formatter.Format(original));
        }

        [TestMethod]
        public void SingleLineTrailingComment()
        {
            var original = @"select * from tbl
-- comment here";
            var expected = @"SELECT *
FROM   tbl;
-- comment here";
            Assert.AreEqual(expected, Formatter.Format(original));
        }

        [TestMethod]
        public void SingleLineInterStatementComment()
        {
            var original = @"select * from tbl
-- comment here
insert into tbl (col) values ('foo')";
            var expected = @"SELECT *
FROM   tbl;
-- comment here
INSERT  INTO tbl (col)
VALUES          ('foo');";
            Assert.AreEqual(expected, Formatter.Format(original));
        }

        [TestMethod]
        public void SingleLineInterTokenComment()
        {
            var original = @"select *
-- comment here
from tbl";
            var expected = @"SELECT *
-- comment here
FROM   tbl;";
            Assert.AreEqual(expected, Formatter.Format(original));
        }

        [TestMethod]
        public void SlashStarInterTokenComment()
        {
            var original = @"select * /* all cols */
from tbl";
            var expected = @"SELECT * /* all cols */
FROM   tbl;";
            Assert.AreEqual(expected, Formatter.Format(original));
        }

        [TestMethod]
        public void Issue408()
        {
            var original = @"SELECT w.*
FROM   workflow AS w CROSS APPLY OPENJSON (JSON_QUERY(w.clientdata, '$.properties.connectionReferences')) AS wfcr
       LEFT OUTER JOIN
       connectionreference AS cr
       ON JSON_VALUE(wfcr.[value], '$.connection.connectionReferenceLogicalName') = cr.connectionreferencelogicalname
WHERE  w.category = 5
       --AND JSON_VALUE(wfcr.[value], '$.connection.connectionReferenceLogicalName') IS NULL;";

            var expected = @"SELECT w.*
FROM   workflow AS w CROSS APPLY OPENJSON (JSON_QUERY(w.clientdata, '$.properties.connectionReferences')) AS wfcr
       LEFT OUTER JOIN
       connectionreference AS cr
       ON JSON_VALUE(wfcr.[value], '$.connection.connectionReferenceLogicalName') = cr.connectionreferencelogicalname
WHERE  w.category = 5;
       --AND JSON_VALUE(wfcr.[value], '$.connection.connectionReferenceLogicalName') IS NULL;";

            Assert.AreEqual(expected, Formatter.Format(original));
        }

        [TestMethod]
        public void Issue525()
        {
            var original = @"SELECT sc1.uniquename AS [aaa]
FROM (
	SELECT sc.solutionidname AS uniquename,
		-- comment
		sc.objectid
	FROM solutioncomponent AS sc
	) AS sc1";

            var expected = @"SELECT sc1.uniquename AS [aaa]
FROM   (SELECT sc.solutionidname AS uniquename,
		-- comment
               sc.objectid
        FROM   solutioncomponent AS sc) AS sc1;";

            Assert.AreEqual(expected, Formatter.Format(original));
        }

        [TestMethod]
        public void Issue599()
        {
            var original = @"SELECT *
FROM businessunit
	--aaa
	--bbb";

            var expected = @"SELECT *
FROM   businessunit;
	--aaa
	--bbb";

            Assert.AreEqual(expected, Formatter.Format(original));
        }

        [TestMethod]
        public void Issue600()
        {
            var original = @"SELECT
	-- b.name,
	b.*
FROM businessunit as b";

            var expected = @"SELECT
	-- b.name,
	b.*
FROM   businessunit AS b;";

            Assert.AreEqual(expected, Formatter.Format(original));
        }

        [TestMethod]
        public void Issue622()
        {
            var original = @"SELECT
	-- b.name,
	-- b.name,
	b.name
FROM businessunit AS b
WHERE b.name = ''";

            var expected = @"SELECT
	-- b.name,
	-- b.name,
	b.name
FROM   businessunit AS b
WHERE  b.name = '';";

            Assert.AreEqual(expected, Formatter.Format(original));
        }
    }
}
