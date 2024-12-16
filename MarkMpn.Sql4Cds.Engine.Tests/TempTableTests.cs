using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace MarkMpn.Sql4Cds.Engine.Tests
{
    [TestClass]
    public class TempTableTests : FakeXrmEasyTestsBase
    {
        [TestMethod]
        public void BasicTempTableUsage()
        {
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandTimeout = 0;
                // Create a temp table, insert a record, read the record, delete it, check it's deleted, drop the table
                cmd.CommandText = @"
CREATE TABLE #TempTable (Id INT, Name NVARCHAR(100))
INSERT INTO #TempTable (Id, Name) VALUES (1, 'Hello'), (2, 'World')
SELECT * FROM #TempTable
UPDATE #TempTable SET Name = 'Updated' WHERE Id = 1
SELECT * FROM #TempTable
DELETE FROM #TempTable
SELECT * FROM #TempTable
DROP TABLE #TempTable";

                using (var reader = cmd.ExecuteReader())
                {
                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("Hello", reader["Name"]);
                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("World", reader["Name"]);
                    Assert.IsFalse(reader.Read());

                    Assert.IsTrue(reader.NextResult());

                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("Updated", reader["Name"]);
                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("World", reader["Name"]);
                    Assert.IsFalse(reader.Read());

                    Assert.IsTrue(reader.NextResult());

                    Assert.IsFalse(reader.Read());

                    Assert.IsFalse(reader.NextResult());
                }
            }
        }
    }
}
