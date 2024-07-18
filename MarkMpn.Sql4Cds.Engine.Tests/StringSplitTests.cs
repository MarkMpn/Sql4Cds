using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace MarkMpn.Sql4Cds.Engine.Tests
{
    [TestClass]
    public class StringSplitTests : FakeXrmEasyTestsBase
    {
        [TestMethod]
        public void InsufficientParameters()
        {
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = @"SELECT * FROM string_split('hello,world')";

                try
                {
                    cmd.ExecuteNonQuery();
                    Assert.Fail("Expected an exception");
                }
                catch (Sql4CdsException ex)
                {
                    Assert.AreEqual(313, ex.Number);
                }
            }
        }

        [TestMethod]
        public void TooManyParameters()
        {
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = @"SELECT * FROM string_split('hello,world', ',', 1, 'test')";

                try
                {
                    cmd.ExecuteNonQuery();
                    Assert.Fail("Expected an exception");
                }
                catch (Sql4CdsException ex)
                {
                    Assert.AreEqual(8144, ex.Number);
                }
            }
        }

        [TestMethod]
        public void DefaultsToNoOrdinal()
        {
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = @"SELECT * FROM string_split('hello,world', ',')";

                using (var reader = cmd.ExecuteReader())
                {
                    Assert.AreEqual(1, reader.FieldCount);
                    Assert.AreEqual("value", reader.GetName(0));
                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("hello", reader.GetString(0));
                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("world", reader.GetString(0));
                    Assert.IsFalse(reader.Read());
                }
            }
        }

        [TestMethod]
        public void IncludesOrdinal()
        {
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = @"SELECT * FROM string_split('hello,world', ',', 1)";

                using (var reader = cmd.ExecuteReader())
                {
                    Assert.AreEqual(2, reader.FieldCount);
                    Assert.AreEqual("value", reader.GetName(0));
                    Assert.AreEqual("ordinal", reader.GetName(1));
                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("hello", reader.GetString(0));
                    Assert.AreEqual(1, reader.GetInt32(1));
                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("world", reader.GetString(0));
                    Assert.AreEqual(2, reader.GetInt32(1));
                    Assert.IsFalse(reader.Read());
                }
            }
        }

        [TestMethod]
        public void InputAndSeparatorCanBeParameters()
        {
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = @"SELECT * FROM string_split(@input, @separator, 1)";
                cmd.Parameters.Add(cmd.CreateParameter());
                cmd.Parameters.Add(cmd.CreateParameter());

                cmd.Parameters[0].ParameterName = "@input";
                cmd.Parameters[0].Value = "hello,world";
                cmd.Parameters[1].ParameterName = "@separator";
                cmd.Parameters[1].Value = ",";

                using (var reader = cmd.ExecuteReader())
                {
                    Assert.AreEqual(2, reader.FieldCount);
                    Assert.AreEqual("value", reader.GetName(0));
                    Assert.AreEqual("ordinal", reader.GetName(1));
                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("hello", reader.GetString(0));
                    Assert.AreEqual(1, reader.GetInt32(1));
                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("world", reader.GetString(0));
                    Assert.AreEqual(2, reader.GetInt32(1));
                    Assert.IsFalse(reader.Read());
                }
            }
        }

        [TestMethod]
        public void OrdinalCannotBeParameter()
        {
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = @"SELECT * FROM string_split('hello,world', ',', @ordinal)";
                cmd.Parameters.Add(cmd.CreateParameter());
                cmd.Parameters[0].ParameterName = "@ordinal";
                cmd.Parameters[0].Value = true;

                try
                {
                    cmd.ExecuteNonQuery();
                    Assert.Fail("Expected an exception");
                }
                catch (Sql4CdsException ex)
                {
                    Assert.AreEqual(8748, ex.Number);
                }
            }
        }

        [DataTestMethod]
        [DataRow("123", 4199)]
        [DataRow("'123'", 8116)]
        [DataRow("1.0", 8116)]
        public void OrdinalMustBeBit(string ordinal, int expectedError)
        {
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = $"SELECT * FROM string_split('hello,world', ',', {ordinal})";

                try
                {
                    cmd.ExecuteNonQuery();
                    Assert.Fail("Expected an exception");
                }
                catch (Sql4CdsException ex)
                {
                    Assert.AreEqual(expectedError, ex.Number);
                }
            }
        }

        [TestMethod]
        public void SeparatorCannotBeNull()
        {
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = @"SELECT * FROM string_split('hello,world', null)";

                try
                {
                    cmd.ExecuteNonQuery();
                    Assert.Fail("Expected an exception");
                }
                catch (Sql4CdsException ex)
                {
                    Assert.AreEqual(214, ex.Number);
                }
            }
        }

        [TestMethod]
        public void NullInputGivesEmptyResult()
        {
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = @"SELECT * FROM string_split(null, ',')";

                using (var reader = cmd.ExecuteReader())
                {
                    Assert.AreEqual(1, reader.FieldCount);
                    Assert.AreEqual("value", reader.GetName(0));
                    Assert.IsFalse(reader.Read());
                }
            }
        }

        [TestMethod]
        public void CrossApply()
        {
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = @"
select * from (values ('a;b'), ('c;d')) as t1 (col)
cross apply string_split(t1.col, ';', 1) s";

                using (var reader = cmd.ExecuteReader())
                {
                    Assert.AreEqual(3, reader.FieldCount);
                    Assert.AreEqual("col", reader.GetName(0));
                    Assert.AreEqual("value", reader.GetName(1));
                    Assert.AreEqual("ordinal", reader.GetName(2));
                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("a;b", reader.GetString(0));
                    Assert.AreEqual("a", reader.GetString(1));
                    Assert.AreEqual(1, reader.GetInt32(2));
                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("a;b", reader.GetString(0));
                    Assert.AreEqual("b", reader.GetString(1));
                    Assert.AreEqual(2, reader.GetInt32(2));
                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("c;d", reader.GetString(0));
                    Assert.AreEqual("c", reader.GetString(1));
                    Assert.AreEqual(1, reader.GetInt32(2));
                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("c;d", reader.GetString(0));
                    Assert.AreEqual("d", reader.GetString(1));
                    Assert.AreEqual(2, reader.GetInt32(2));
                    Assert.IsFalse(reader.Read());
                }
            }
        }
    }
}
