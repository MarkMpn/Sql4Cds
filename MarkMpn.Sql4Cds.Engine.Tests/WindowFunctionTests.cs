using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace MarkMpn.Sql4Cds.Engine.Tests
{
    [TestClass]
    public class WindowFunctionTests : FakeXrmEasyTestsBase
    {
        [TestMethod]
        public void WindowFunctionsCanOnlyAppearInTheSelectOrOrderByClauses()
        {
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = "SELECT name FROM account WHERE ROW_NUMBER() OVER (ORDER BY name) = 1";

                try
                {
                    cmd.ExecuteNonQuery();
                    Assert.Fail("Expected exception");
                }
                catch (Sql4CdsException ex)
                {
                    Assert.AreEqual(4108, ex.Number);
                }
            }
        }

        [TestMethod]
        public void RowNumber()
        {
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = "INSERT INTO account (name, employees) VALUES ('Data8', 10), ('Data9', 20), ('Data10', 30)";
                cmd.ExecuteNonQuery();

                cmd.CommandText = "SELECT name, ROW_NUMBER() OVER (ORDER BY employees) AS rownum FROM account";

                using (var reader = cmd.ExecuteReader())
                {
                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("Data8", reader["name"]);
                    Assert.AreEqual(1L, reader["rownum"]);

                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("Data9", reader["name"]);
                    Assert.AreEqual(2L, reader["rownum"]);

                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("Data10", reader["name"]);
                    Assert.AreEqual(3L, reader["rownum"]);

                    Assert.IsFalse(reader.Read());
                }
            }
        }

        [TestMethod]
        public void RowNumberWithPartition()
        {
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = "INSERT INTO account (name, employees) VALUES ('Data8', 10), ('Data8', 20), ('Data9', 30)";
                cmd.ExecuteNonQuery();

                cmd.CommandText = "SELECT name, ROW_NUMBER() OVER (PARTITION BY name ORDER BY employees) AS rownum FROM account";

                using (var reader = cmd.ExecuteReader())
                {
                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("Data8", reader["name"]);
                    Assert.AreEqual(1L, reader["rownum"]);

                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("Data8", reader["name"]);
                    Assert.AreEqual(2L, reader["rownum"]);

                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("Data9", reader["name"]);
                    Assert.AreEqual(1L, reader["rownum"]);

                    Assert.IsFalse(reader.Read());
                }
            }
        }
    }
}
