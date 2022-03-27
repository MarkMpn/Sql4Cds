using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlTypes;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.RegularExpressions;
using System.Threading;
using System.Xml.Serialization;
using FakeXrmEasy;
using FakeXrmEasy.FakeMessageExecutors;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Metadata;
using Microsoft.Xrm.Sdk.Metadata.Query;
using Microsoft.Xrm.Sdk.Query;

namespace MarkMpn.Sql4Cds.Engine.Tests
{
    [TestClass]
    public class AdoProviderTests : FakeXrmEasyTestsBase
    {
        [TestMethod]
        public void SelectArithmetic()
        {
            var query = "SELECT employees + 1 AS a, employees * 2 AS b, turnover / 3 AS c, turnover - 4 AS d, turnover / employees AS e FROM account";

            var id = Guid.NewGuid();
            _context.Data["account"] = new Dictionary<Guid, Entity>
            {
                [id] = new Entity("account", id)
                {
                    ["accountid"] = id,
                    ["employees"] = 2,
                    ["turnover"] = new Money(9)
                }
            };

            using (var con = new Sql4CdsConnection(_localDataSource))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = query;

                using (var reader = cmd.ExecuteReader())
                {
                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual(3, reader.GetInt32(0));
                    Assert.AreEqual(4, reader.GetInt32(1));
                    Assert.AreEqual(3M, reader.GetDecimal(2));
                    Assert.AreEqual(5M, reader.GetDecimal(3));
                    Assert.AreEqual(4.5M, reader.GetDecimal(4));
                    Assert.IsFalse(reader.Read());
                }
            }
        }

        [TestMethod]
        public void SelectParameters()
        {
            using (var con = new Sql4CdsConnection(_localDataSource))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = "SELECT @param1, @param2";

                cmd.Parameters.Add(new Sql4CdsParameter("@param1", 1));
                cmd.Parameters.Add(new Sql4CdsParameter("@param2", "text"));

                using (var reader = cmd.ExecuteReader())
                {
                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual(1, reader.GetInt32(0));
                    Assert.AreEqual("text", reader.GetString(1));
                    Assert.IsFalse(reader.Read());
                }
            }
        }

        [TestMethod]
        public void InsertRecordsAffected()
        {
            using (var con = new Sql4CdsConnection(_localDataSource))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = "INSERT INTO account (name) VALUES (@name)";
                cmd.Parameters.Add(new Sql4CdsParameter("@name", "'Data8'"));
                var affected = cmd.ExecuteNonQuery();

                Assert.AreEqual(1, affected);
                Assert.AreEqual(1, _context.Data["account"].Count);
                Assert.AreEqual("'Data8'", _context.Data["account"].Values.Single().GetAttributeValue<string>("name"));
            }
        }

        [TestMethod]
        public void InsertRecordsAffectedMultipleCommands()
        {
            using (var con = new Sql4CdsConnection(_localDataSource))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = "INSERT INTO account (name) VALUES (@name); INSERT INTO account (name) VALUES (@name)";
                cmd.Parameters.Add(new Sql4CdsParameter("@name", "'Data8'"));
                var affected = cmd.ExecuteNonQuery();

                Assert.AreEqual(2, affected);
                Assert.AreEqual(2, _context.Data["account"].Count);
                CollectionAssert.AreEqual(new[] { "'Data8'", "'Data8'" }, _context.Data["account"].Values.Select(a => a.GetAttributeValue<string>("name")).ToArray());
            }
        }

        [TestMethod]
        public void CombinedInsertSelect()
        {
            using (var con = new Sql4CdsConnection(_localDataSource))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = "INSERT INTO account (name) VALUES (@name); SELECT accountid FROM account WHERE name = @name";
                cmd.Parameters.Add(new Sql4CdsParameter("@name", "'Data8'"));

                using (var reader = cmd.ExecuteReader())
                {
                    Assert.AreEqual(1, reader.RecordsAffected);
                    Assert.IsTrue(reader.Read());
                    var id = reader.GetGuid(0);
                    Assert.IsFalse(reader.Read());

                    Assert.AreEqual(1, _context.Data["account"].Count);
                    Assert.AreEqual("'Data8'", _context.Data["account"][id].GetAttributeValue<string>("name"));
                }
            }
        }

        [TestMethod]
        public void MultipleResultSets()
        {
            using (var con = new Sql4CdsConnection(_localDataSource))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = "INSERT INTO account (name) VALUES (@name); SELECT accountid FROM account WHERE name = @name; SELECT name FROM account";
                cmd.Parameters.Add(new Sql4CdsParameter("@name", "'Data8'"));

                using (var reader = cmd.ExecuteReader())
                {
                    Assert.AreEqual(1, reader.RecordsAffected);
                    Assert.IsTrue(reader.Read());
                    var id = (SqlEntityReference) reader.GetValue(0);
                    Assert.AreEqual("account", id.LogicalName);
                    Assert.IsFalse(reader.Read());

                    Assert.IsTrue(reader.NextResult());
                    Assert.IsTrue(reader.Read());
                    var name = reader.GetString(0);
                    Assert.IsFalse(reader.Read());
                    Assert.IsFalse(reader.NextResult());

                    Assert.AreEqual(1, _context.Data["account"].Count);
                    Assert.AreEqual("'Data8'", _context.Data["account"][id.Id].GetAttributeValue<string>("name"));
                    Assert.AreEqual("'Data8'", name);
                }
            }
        }

        [TestMethod]
        public void GetLastInsertId()
        {
            using (var con = new Sql4CdsConnection(_localDataSource))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = "INSERT INTO account (name) VALUES (@name); SELECT @@IDENTITY";
                cmd.Parameters.Add(new Sql4CdsParameter("@name", "'Data8'"));

                var id = (SqlEntityReference)cmd.ExecuteScalar();

                Assert.AreEqual("account", id.LogicalName);
                Assert.AreEqual(1, _context.Data["account"].Count);
                Assert.AreEqual("'Data8'", _context.Data["account"][id.Id].GetAttributeValue<string>("name"));
            }
        }

        [TestMethod]
        public void RowCount()
        {
            using (var con = new Sql4CdsConnection(_localDataSource))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = "INSERT INTO account (name) VALUES ('1'), ('2'), ('3'); SELECT @@ROWCOUNT; SELECT @@ROWCOUNT";
                
                using (var reader = cmd.ExecuteReader())
                {
                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual(3, reader.GetInt32(0));
                    Assert.IsFalse(reader.Read());

                    Assert.IsTrue(reader.NextResult());
                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual(1, reader.GetInt32(0));
                    Assert.IsFalse(reader.Read());
                    Assert.IsFalse(reader.NextResult());
                }
            }
        }

        [TestMethod]
        public void LoadToDataTable()
        {
            using (var con = new Sql4CdsConnection(_localDataSource))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = "SELECT 1, 'hello world'";

                using (var reader = cmd.ExecuteReader())
                {
                    var table = new DataTable();
                    table.Load(reader);

                    Assert.AreEqual(1, table.Rows.Count);
                    Assert.AreEqual(2, table.Columns.Count);
                    Assert.AreEqual(1, table.Rows[0][0]);
                    Assert.AreEqual("hello world", table.Rows[0][1]);
                }
            }
        }

        [TestMethod]
        public void ControlOfFlow()
        {
            using (var con = new Sql4CdsConnection(_localDataSource))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = @"
                    IF @param1 = 1
                        SELECT 'a'

                    IF @param1 = 2
                        SELECT 'b'

                    WHILE @param1 < 10
                    BEGIN
                        SELECT @param1
                        SET @param1 += 1
                    END";
                cmd.Parameters.Add(new Sql4CdsParameter("@param1", 1));

                var log = "";
                var results = new List<string>();

                ((Sql4CdsCommand)cmd).StatementCompleted += (s, e) => log += e.Statement.Sql + "\r\n";

                using (var reader = cmd.ExecuteReader())
                {
                    while (!reader.IsClosed)
                    {
                        var table = new DataTable();
                        table.Load(reader);

                        Assert.AreEqual(1, table.Columns.Count);
                        Assert.AreEqual(1, table.Rows.Count);
                        results.Add(table.Rows[0][0].ToString());
                    }
                }

                Assert.AreEqual("SELECT 'a'\r\nSELECT @param1\r\nSET @param1 += 1\r\nSELECT @param1\r\nSET @param1 += 1\r\nSELECT @param1\r\nSET @param1 += 1\r\nSELECT @param1\r\nSET @param1 += 1\r\nSELECT @param1\r\nSET @param1 += 1\r\nSELECT @param1\r\nSET @param1 += 1\r\nSELECT @param1\r\nSET @param1 += 1\r\nSELECT @param1\r\nSET @param1 += 1\r\nSELECT @param1\r\nSET @param1 += 1\r\n", log);
                CollectionAssert.AreEqual(new[] { "a", "1", "2", "3", "4", "5", "6", "7", "8", "9" }, results);
            }
        }

        [TestMethod]
        public void Print()
        {
            using (var con = new Sql4CdsConnection(_localDataSource))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = "PRINT @param1";
                cmd.Parameters.Add(new Sql4CdsParameter("@param1", 1));

                var log = "";
                con.InfoMessage += (s, e) => log += e.Message;

                cmd.ExecuteNonQuery();

                Assert.AreEqual("1", log);
            }
        }

        [TestMethod]
        public void GoTo()
        {
            using (var con = new Sql4CdsConnection(_localDataSource))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = @"
                    declare @param1 int = 1
                    goto label1

                    while @param1 < 10
                    begin
                        select @param1
                        label2:
                        set @param1 += 1
                    end

                    goto label3

                    label1:
                    set @param1 = 2
                    goto label2

                    label3:
                    select 'end'";

                var results = new List<string>();

                using (var reader = cmd.ExecuteReader())
                {
                    while (!reader.IsClosed)
                    {
                        var table = new DataTable();
                        table.Load(reader);

                        Assert.AreEqual(1, table.Columns.Count);
                        Assert.AreEqual(1, table.Rows.Count);
                        results.Add(table.Rows[0][0].ToString());
                    }
                }

                CollectionAssert.AreEqual(new[] {"3", "4", "5", "6", "7", "8", "9", "end" }, results);
            }
        }

        [TestMethod]
        public void ContinueBreak()
        {
            using (var con = new Sql4CdsConnection(_localDataSource))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = @"
                    declare @param1 int = 1

                    while @param1 < 10
                    begin
                        set @param1 += 1

                        if @param1 = 2
                            continue

                        if @param1 = 5
                            break

                        select @param1
                    end

                    select 'end'";

                var results = new List<string>();

                using (var reader = cmd.ExecuteReader())
                {
                    while (!reader.IsClosed)
                    {
                        var table = new DataTable();
                        table.Load(reader);

                        Assert.AreEqual(1, table.Columns.Count);
                        Assert.AreEqual(1, table.Rows.Count);
                        results.Add(table.Rows[0][0].ToString());
                    }
                }

                CollectionAssert.AreEqual(new[] { "3", "4", "end" }, results);
            }
        }
        
        [TestMethod]
        public void GlobalVariablesPreservedBetweenCommands()
        {
            using (var con = new Sql4CdsConnection(_localDataSource))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = "INSERT INTO account (name) VALUES ('test')";
                cmd.ExecuteNonQuery();

                cmd.CommandText = "SELECT @@IDENTITY";
                var accountId = (SqlEntityReference)cmd.ExecuteScalar();

                cmd.CommandText = "SELECT @@ROWCOUNT";
                var rowCount = (int)cmd.ExecuteScalar();

                Assert.AreEqual("test", _context.Data["account"][accountId.Id].GetAttributeValue<string>("name"));
                Assert.AreEqual(1, rowCount);
            }
        }
    }
}
