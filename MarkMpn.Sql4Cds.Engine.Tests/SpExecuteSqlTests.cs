using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace MarkMpn.Sql4Cds.Engine.Tests
{
    [TestClass]
    public class SpExecuteSqlTests : FakeXrmEasyTestsBase
    {
        [TestMethod]
        public void SimpleExpression()
        {
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandTimeout = 0;
                cmd.CommandText = @"
EXEC sp_executesql N'SELECT 1'";

                Assert.AreEqual(1, cmd.ExecuteScalar());
            }
        }

        [TestMethod]
        public void MultipleResultSets()
        {
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandTimeout = 0;
                cmd.CommandText = @"
EXEC sp_executesql N'SELECT 1; SELECT 2'";

                using (var reader = cmd.ExecuteReader())
                {
                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual(1, reader.GetInt32(0));
                    Assert.IsFalse(reader.Read());

                    Assert.IsTrue(reader.NextResult());

                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual(2, reader.GetInt32(0));
                    Assert.IsFalse(reader.Read());

                    Assert.IsFalse(reader.NextResult());
                }
            }
        }

        [TestMethod]
        public void MultipleCallsInSameBatch()
        {
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandTimeout = 0;
                cmd.CommandText = @"
EXEC sp_executesql N'SELECT 1';
EXEC sp_executesql N'SELECT 2'";

                using (var reader = cmd.ExecuteReader())
                {
                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual(1, reader.GetInt32(0));
                    Assert.IsFalse(reader.Read());

                    Assert.IsTrue(reader.NextResult());

                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual(2, reader.GetInt32(0));
                    Assert.IsFalse(reader.Read());

                    Assert.IsFalse(reader.NextResult());
                }
            }
        }

        [TestMethod]
        public void MustUseUnicodeString()
        {
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandTimeout = 0;
                cmd.CommandText = @"
EXEC sp_executesql 'SELECT 1'";

                try
                {
                    cmd.ExecuteScalar();
                    Assert.Fail("Expected an exception");
                }
                catch (Sql4CdsException ex)
                {
                    Assert.AreEqual(214, ex.Number);
                }
            }
        }

        [TestMethod]
        public void MustUseLiteralString()
        {
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandTimeout = 0;
                cmd.CommandText = @"
EXEC sp_executesql N'SELECT ' + N'1'";

                try
                {
                    cmd.ExecuteScalar();
                    Assert.Fail("Expected an exception");
                }
                catch (Sql4CdsException ex)
                {
                    Assert.IsTrue(ex.Message.StartsWith("Incorrect syntax"));
                }
            }
        }
        [TestMethod]
        public void VariableSql()
        {
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandTimeout = 0;
                cmd.CommandText = @"
DECLARE @sql nvarchar(100) = N'SELECT 1';
EXEC sp_executesql @sql";

                Assert.AreEqual(1, cmd.ExecuteScalar());
            }
        }
    }
}