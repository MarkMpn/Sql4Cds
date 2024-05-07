using System;
using System.Activities.Expressions;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.RegularExpressions;
using System.Threading;
using System.Windows.Controls.Primitives;
using System.Xml.Serialization;
using Dapper;
using FakeItEasy;
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

            using (var con = new Sql4CdsConnection(_localDataSources))
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
            using (var con = new Sql4CdsConnection(_localDataSources))
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
            using (var con = new Sql4CdsConnection(_localDataSources))
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
            using (var con = new Sql4CdsConnection(_localDataSources))
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
            using (var con = new Sql4CdsConnection(_localDataSources))
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
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = "INSERT INTO account (name) VALUES (@name); SELECT accountid FROM account WHERE name = @name; SELECT name FROM account";
                cmd.Parameters.Add(new Sql4CdsParameter("@name", "'Data8'"));

                using (var reader = cmd.ExecuteReader())
                {
                    Assert.AreEqual(1, reader.RecordsAffected);
                    Assert.IsTrue(reader.Read());
                    var id = (SqlEntityReference)reader.GetValue(0);
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
            using (var con = new Sql4CdsConnection(_localDataSources))
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
            using (var con = new Sql4CdsConnection(_localDataSources))
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
            using (var con = new Sql4CdsConnection(_localDataSources))
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
            using (var con = new Sql4CdsConnection(_localDataSources))
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
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = "PRINT @param1";
                cmd.Parameters.Add(new Sql4CdsParameter("@param1", 1));

                var log = "";
                con.InfoMessage += (s, e) => log += e.Message.Message;

                cmd.ExecuteNonQuery();

                Assert.AreEqual("1", log);
            }
        }

        [TestMethod]
        public void GoTo()
        {
            using (var con = new Sql4CdsConnection(_localDataSources))
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

                CollectionAssert.AreEqual(new[] { "3", "4", "5", "6", "7", "8", "9", "end" }, results);
            }
        }

        [TestMethod]
        public void ContinueBreak()
        {
            using (var con = new Sql4CdsConnection(_localDataSources))
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
            using (var con = new Sql4CdsConnection(_localDataSources))
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

        [TestMethod]
        public void CaseInsensitiveDml()
        {
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = "INSERT INTO account (Name) VALUES ('ProperCase')";
                cmd.ExecuteNonQuery();

                cmd.CommandText = "SELECT @@IDENTITY";
                var accountId = (SqlEntityReference)cmd.ExecuteScalar();

                Assert.AreEqual("ProperCase", _context.Data["account"][accountId.Id].GetAttributeValue<string>("name"));

                cmd.CommandText = "UPDATE account SET NAME = 'UpperCase' WHERE AccountId = @AccountId";
                var param = cmd.CreateParameter();
                param.ParameterName = "@accountid";
                param.Value = accountId.Id;
                cmd.Parameters.Add(param);
                cmd.ExecuteNonQuery();

                Assert.AreEqual("UpperCase", _context.Data["account"][accountId.Id].GetAttributeValue<string>("name"));
            }
        }

        [TestMethod]
        public void ExecuteReaderSchemaOnly()
        {
            var id = Guid.NewGuid();

            _context.Data["account"] = new Dictionary<Guid, Entity>
            {
                [id] = new Entity("account", id)
                {
                    ["accountid"] = id,
                    ["name"] = "Test"
                }
            };

            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = "SELECT name FROM account";

                using (var reader = cmd.ExecuteReader(CommandBehavior.SchemaOnly))
                {
                    var schema = reader.GetSchemaTable();
                    Assert.AreEqual(1, schema.Rows.Count);
                    Assert.AreEqual("name", schema.Rows[0]["ColumnName"]);
                    Assert.AreEqual(typeof(string), schema.Rows[0]["DataType"]);

                    Assert.IsFalse(reader.Read());
                }
            }
        }

        [TestMethod]
        public void ExecuteReaderSingleRow()
        {
            var id1 = Guid.NewGuid();
            var id2 = Guid.NewGuid();

            _context.Data["account"] = new Dictionary<Guid, Entity>
            {
                [id1] = new Entity("account", id1)
                {
                    ["accountid"] = id1,
                    ["name"] = "Test1"
                },
                [id2] = new Entity("account", id2)
                {
                    ["accountid"] = id2,
                    ["name"] = "Test2"
                }
            };

            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = "SELECT name FROM account; SELECT accountid FROM account";

                using (var reader = cmd.ExecuteReader(CommandBehavior.SingleRow))
                {
                    Assert.IsTrue(reader.Read());
                    Assert.IsFalse(reader.Read());

                    Assert.IsFalse(reader.NextResult());
                }
            }
        }

        [TestMethod]
        public void ExecuteReaderSingleResult()
        {
            var id1 = Guid.NewGuid();
            var id2 = Guid.NewGuid();

            _context.Data["account"] = new Dictionary<Guid, Entity>
            {
                [id1] = new Entity("account", id1)
                {
                    ["accountid"] = id1,
                    ["name"] = "Test1"
                },
                [id2] = new Entity("account", id2)
                {
                    ["accountid"] = id2,
                    ["name"] = "Test2"
                }
            };

            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = "SELECT name FROM account; SELECT accountid FROM account";

                using (var reader = cmd.ExecuteReader(CommandBehavior.SingleResult))
                {
                    Assert.IsTrue(reader.Read());
                    Assert.IsTrue(reader.Read());
                    Assert.IsFalse(reader.Read());

                    Assert.IsFalse(reader.NextResult());
                }
            }
        }

        [TestMethod]
        public void StringLengthInSchema()
        {
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = "SELECT accountid, name, name + 'foo', employees, left(name, 2) FROM account";

                using (var reader = cmd.ExecuteReader(CommandBehavior.SchemaOnly))
                {
                    var schema = reader.GetSchemaTable();
                    var accountIdSize = schema.Rows[0]["ColumnSize"];
                    var nameSize = schema.Rows[1]["ColumnSize"];
                    var stringContcatSize = schema.Rows[2]["ColumnSize"];
                    var employeesSize = schema.Rows[3]["ColumnSize"];
                    var leftSize = schema.Rows[4]["ColumnSize"];

                    Assert.AreEqual(16, accountIdSize);
                    Assert.AreEqual(100, nameSize);
                    Assert.AreEqual(103, stringContcatSize);
                    Assert.AreEqual(4, employeesSize);
                    Assert.AreEqual(2, leftSize);
                }
            }
        }

        [TestMethod]
        public void StringLengthUnion()
        {
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = @"
                    DECLARE @short nvarchar(10)
                    DECLARE @long nvarchar(20)

                    SELECT @short
                    UNION ALL
                    SELECT @long";

                using (var reader = cmd.ExecuteReader(CommandBehavior.SchemaOnly))
                {
                    var schema = reader.GetSchemaTable();
                    var size = schema.Rows[0]["ColumnSize"];

                    Assert.AreEqual(20, size);
                }
            }
        }

        [TestMethod]
        public void DecimalPrecisionScaleUnion()
        {
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = @"
                    DECLARE @number decimal(20, 10)

                    SELECT @number, 1.2
                    UNION ALL
                    SELECT 123.4, 2.34";

                using (var reader = cmd.ExecuteReader(CommandBehavior.SchemaOnly))
                {
                    var schema = reader.GetSchemaTable();
                    var precision = schema.Rows[0]["NumericPrecision"];
                    var scale = schema.Rows[0]["NumericScale"];

                    Assert.AreEqual((short)20, precision);
                    Assert.AreEqual((short)10, scale);

                    precision = schema.Rows[1]["NumericPrecision"];
                    scale = schema.Rows[1]["NumericScale"];

                    Assert.AreEqual((short)3, precision);
                    Assert.AreEqual((short)2, scale);
                }
            }
        }

        [TestMethod]
        public void DecimalPrecisionScaleCalculations()
        {
            // Examples from https://docs.microsoft.com/en-us/sql/t-sql/data-types/precision-scale-and-length-transact-sql?view=sql-server-ver15#examples
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = @"
                    select
                        cast(0.0000009000 as decimal(30,20)) * cast(1.0000000000 as decimal(30,20)),
                        CAST(0.0000009000 AS DECIMAL(30,10)) * CAST(1.0000000000 AS DECIMAL(30,10))";

                using (var reader = cmd.ExecuteReader())
                {
                    var schema = reader.GetSchemaTable();
                    var precision = schema.Rows[0]["NumericPrecision"];
                    var scale = schema.Rows[0]["NumericScale"];

                    Assert.AreEqual((short)38, precision);
                    Assert.AreEqual((short)17, scale);

                    precision = schema.Rows[1]["NumericPrecision"];
                    scale = schema.Rows[1]["NumericScale"];

                    Assert.AreEqual((short)38, precision);
                    Assert.AreEqual((short)6, scale);

                    Assert.IsTrue(reader.Read());
                    var val1 = (SqlDecimal)reader.GetProviderSpecificValue(0);
                    Assert.AreEqual(0.00000090000000000M, val1.Value);
                    Assert.AreEqual((short)38, val1.Precision);
                    Assert.AreEqual((short)17, val1.Scale);

                    var val2 = (SqlDecimal)reader.GetProviderSpecificValue(1);
                    Assert.AreEqual(0.000001M, val2.Value);
                    Assert.AreEqual((short)38, val2.Precision);
                    Assert.AreEqual((short)6, val2.Scale);
                }
            }
        }

        [TestMethod]
        public void DefaultStringLengths()
        {
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = @"
                    declare @long varchar(100) = 'this is a very long test string that is bigger than thirty characters'
                    declare @short varchar = 'test'
                    select convert(varchar, @long), @short";

                using (var reader = cmd.ExecuteReader(CommandBehavior.SchemaOnly))
                {
                    var schema = reader.GetSchemaTable();
                    var length = schema.Rows[0]["ColumnSize"];
                    Assert.AreEqual(30, length); // Default length of 30 for CAST and CONVERT
                    length = schema.Rows[1]["ColumnSize"];
                    Assert.AreEqual(1, length); // Default length of 1 for variable declaration
                }
            }
        }

        [TestMethod]
        public void DateTypeConversions()
        {
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = @"
                    select current_timestamp, convert(date, current_timestamp), convert(time, current_timestamp), convert(datetime, convert(date, current_timestamp)), convert(datetime, convert(time, current_timestamp))";

                using (var reader = cmd.ExecuteReader())
                {
                    var schema = reader.GetSchemaTable();

                    Assert.AreEqual(typeof(DateTime), schema.Rows[0]["DataType"]);
                    Assert.AreEqual("datetime", schema.Rows[0]["DataTypeName"]);
                    Assert.AreEqual(typeof(SqlDateTime), schema.Rows[0]["ProviderSpecificDataType"]);

                    Assert.AreEqual(typeof(DateTime), schema.Rows[1]["DataType"]);
                    Assert.AreEqual("date", schema.Rows[1]["DataTypeName"]);
                    Assert.AreEqual(typeof(SqlDate), schema.Rows[1]["ProviderSpecificDataType"]);

                    Assert.AreEqual(typeof(TimeSpan), schema.Rows[2]["DataType"]);
                    Assert.AreEqual("time", schema.Rows[2]["DataTypeName"]);
                    Assert.AreEqual(typeof(SqlTime), schema.Rows[2]["ProviderSpecificDataType"]);

                    Assert.AreEqual(typeof(DateTime), schema.Rows[3]["DataType"]);
                    Assert.AreEqual("datetime", schema.Rows[3]["DataTypeName"]);
                    Assert.AreEqual(typeof(SqlDateTime), schema.Rows[3]["ProviderSpecificDataType"]);

                    Assert.AreEqual(typeof(DateTime), schema.Rows[4]["DataType"]);
                    Assert.AreEqual("datetime", schema.Rows[4]["DataTypeName"]);
                    Assert.AreEqual(typeof(SqlDateTime), schema.Rows[4]["ProviderSpecificDataType"]);

                    reader.Read();
                    var values = new object[5];
                    reader.GetValues(values);

                    Assert.IsInstanceOfType(values[0], typeof(DateTime));
                    Assert.IsInstanceOfType(values[1], typeof(DateTime));
                    Assert.IsInstanceOfType(values[2], typeof(TimeSpan));
                    Assert.IsInstanceOfType(values[3], typeof(DateTime));
                    Assert.IsInstanceOfType(values[4], typeof(DateTime));

                    Assert.AreEqual(TimeSpan.Zero, ((DateTime)values[1]).TimeOfDay);
                    Assert.AreEqual(TimeSpan.Zero, ((DateTime)values[3]).TimeOfDay);
                    Assert.AreEqual(new DateTime(1900, 1, 1), ((DateTime)values[4]).Date);
                }
            }
        }

        [TestMethod]
        public void InsertNull()
        {
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = "INSERT INTO contact (firstname, lastname, parentcustomerid) VALUES (NULL, NULL, NULL)";
                cmd.ExecuteNonQuery();

                cmd.CommandText = "SELECT firstname, lastname FROM contact WHERE contactid = @@IDENTITY";

                using (var reader = cmd.ExecuteReader())
                {
                    Assert.IsTrue(reader.Read());
                    Assert.IsTrue(reader.IsDBNull(0));
                    Assert.IsTrue(reader.IsDBNull(1));
                }
            }
        }

        [TestMethod]
        public void UpdateNull()
        {
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = "INSERT INTO account (name, employees) VALUES ('Data8', 100); SELECT @@IDENTITY";
                var accountId = cmd.ExecuteScalar();

                cmd.CommandText = "SELECT name, employees FROM account WHERE accountid = @accountId";
                var param = cmd.CreateParameter();
                param.ParameterName = "@accountId";
                param.Value = accountId;
                cmd.Parameters.Add(param);

                using (var reader = cmd.ExecuteReader())
                {
                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("Data8", reader.GetValue(0));
                    Assert.AreEqual(100, reader.GetValue(1));
                }

                cmd.CommandText = "UPDATE account SET name = NULL, employees = NULL, ownerid = NULL WHERE accountid = @accountId";
                cmd.ExecuteNonQuery();

                cmd.CommandText = "SELECT name, employees FROM account WHERE accountid = @accountId";

                using (var reader = cmd.ExecuteReader())
                {
                    Assert.IsTrue(reader.Read());
                    Assert.IsTrue(reader.IsDBNull(0));
                    Assert.IsTrue(reader.IsDBNull(1));
                }
            }
        }

        [TestMethod]
        public void WaitFor()
        {
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = "INSERT INTO contact (firstname, lastname) VALUES ('Mark', 'Carrington');";
                cmd.ExecuteNonQuery();

                var stopwatch = new Stopwatch();
                stopwatch.Start();
                cmd.ExecuteNonQuery();
                stopwatch.Stop();
                Assert.IsTrue(stopwatch.ElapsedMilliseconds < 100, "Inserting record expected to take <100ms, actually took " + stopwatch.Elapsed);

                cmd.CommandText = "WAITFOR DELAY '00:00:02'; INSERT INTO contact (firstname, lastname) VALUES ('Mark', 'Carrington');";
                cmd.ExecuteNonQuery();

                stopwatch.Restart();
                cmd.ExecuteNonQuery();
                stopwatch.Stop();
                Assert.IsTrue(stopwatch.ElapsedMilliseconds >= 1990 && stopwatch.ElapsedMilliseconds < 2100, "WAITFOR + INSERT expected to take 1.99s - 2.1s, actually took " + stopwatch.Elapsed);
            }
        }

        [TestMethod]
        public void StoredProcedureCommandType()
        {
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = "SampleMessage";
                cmd.CommandType = CommandType.StoredProcedure;

                var param = cmd.CreateParameter();
                param.ParameterName = "@StringParam";
                param.Value = "1";
                cmd.Parameters.Add(param);

                var outputParam1 = cmd.CreateParameter();
                outputParam1.ParameterName = "@OutputParam1";
                outputParam1.DbType = DbType.String;
                outputParam1.Size = 100;
                outputParam1.Direction = ParameterDirection.Output;
                cmd.Parameters.Add(outputParam1);

                var outputParam2 = cmd.CreateParameter();
                outputParam2.ParameterName = "@OutputParam2";
                outputParam2.DbType = DbType.Int32;
                outputParam2.Direction = ParameterDirection.Output;
                cmd.Parameters.Add(outputParam2);

                cmd.ExecuteNonQuery();

                Assert.AreEqual("1", outputParam1.Value);
                Assert.AreEqual(1, outputParam2.Value);
            }
        }

        [TestMethod]
        public void AliasedTVF()
        {
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = "SELECT msg.* FROM SampleMessage('1') AS msg";

                using (var reader = cmd.ExecuteReader())
                {
                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("1", reader.GetString(0));
                    Assert.AreEqual(1, reader.GetInt32(1));

                    Assert.IsFalse(reader.Read());
                }
            }
        }

        [TestMethod]
        public void CorrelatedNotExistsTypeConversion()
        {
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = "SELECT * FROM (VALUES ('1'), ('2')) a (s) WHERE NOT EXISTS (SELECT TOP 1 1 FROM (VALUES (1)) b (i) WHERE a.s = b.i)";

                using (var reader = cmd.ExecuteReader())
                {
                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("2", reader.GetString(0));

                    Assert.IsFalse(reader.Read());
                }
            }
        }

        [TestMethod]
        public void CharAscii()
        {
            // Using example from
            // https://docs.microsoft.com/en-us/sql/t-sql/functions/char-transact-sql?view=sql-server-ver16#a-using-ascii-and-char-to-print-ascii-values-from-a-string
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = @"
-- Create variables for the character string and for the current   
-- position in the string.  
DECLARE @position INT, @string CHAR(8);  
-- Initialize the current position and the string variables.  
SET @position = 1;  
SET @string = 'New Moon';  
WHILE @position <= DATALENGTH(@string)  
   BEGIN  
   SELECT ASCII(SUBSTRING(@string, @position, 1)),   
      CHAR(ASCII(SUBSTRING(@string, @position, 1)))  
   SET @position = @position + 1  
   END;  
GO";

                var s = "New Moon";

                using (var reader = cmd.ExecuteReader())
                {
                    for (var i = 0; i < s.Length; i++)
                    {
                        var ch = s[i];

                        Assert.IsTrue(reader.Read());
                        Assert.AreEqual((int)ch, reader.GetInt32(0));
                        Assert.AreEqual(ch.ToString(), reader.GetString(1));
                        Assert.IsFalse(reader.Read());

                        if (i == s.Length - 1)
                            Assert.IsFalse(reader.NextResult());
                        else
                            Assert.IsTrue(reader.NextResult());
                    }
                }
            }
        }

        [TestMethod]
        public void NCharUnicode()
        {
            // Using example from
            // https://docs.microsoft.com/en-us/sql/t-sql/functions/nchar-transact-sql?view=sql-server-ver16#b-using-substring-unicode-convert-and-nchar
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = @"
-- The @position variable holds the position of the character currently  
-- being processed. The @nstring variable is the Unicode character   
-- string to process.  
DECLARE @position INT, @nstring NCHAR(9);  
-- Initialize the current position variable to the first character in   
-- the string.  
SET @position = 1;  
-- Initialize the character string variable to the string to process.  
-- Notice that there is an N before the start of the string. This   
-- indicates that the data following the N is Unicode data.  
SET @nstring = N'København';  
-- Print the character number of the position of the string you are at,   
-- the actual Unicode character you are processing, and the UNICODE   
-- value for this particular character.  
PRINT 'Character #' + ' ' + 'Unicode Character' + ' ' + 'UNICODE Value';  
WHILE @position <= DATALENGTH(@nstring)  
   BEGIN  
   SELECT @position,   
      NCHAR(UNICODE(SUBSTRING(@nstring, @position, 1))),  
      CONVERT(NCHAR(17), SUBSTRING(@nstring, @position, 1)),  
      UNICODE(SUBSTRING(@nstring, @position, 1))  
   SELECT @position = @position + 1  
   END;  
GO";

                var s = "København";

                using (var reader = cmd.ExecuteReader())
                {
                    for (var i = 0; i < s.Length * 2; i++)
                    {
                        Assert.IsTrue(reader.Read());

                        Assert.AreEqual(i + 1, reader.GetInt32(0));

                        if (i < s.Length)
                        {
                            var ch = s[i].ToString();
                            Assert.AreEqual(ch, reader.GetString(1));
                            Assert.AreEqual(ch, reader.GetString(2));
                            Assert.AreEqual((int)s[i], reader.GetInt32(3));
                        }
                        else
                        {
                            Assert.IsTrue(reader.IsDBNull(1));
                            Assert.AreEqual("", reader.GetString(2));
                            Assert.IsTrue(reader.IsDBNull(3));
                        }

                        Assert.IsFalse(reader.Read());

                        if (i == s.Length * 2 - 1)
                            Assert.IsFalse(reader.NextResult());
                        else
                            Assert.IsTrue(reader.NextResult());
                    }
                }
            }
        }

        [TestMethod]
        [ExpectedException(typeof(Sql4CdsException))]
        public void Timeout()
        {
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandTimeout = 1;
                cmd.CommandText = "WAITFOR DELAY '00:00:02'; SELECT 1";

                using (var reader = cmd.ExecuteReader())
                {
                    Assert.IsFalse(reader.Read());
                    Assert.Fail();
                }
            }
        }

        [TestMethod]
        public void ReusedParameter()
        {
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = @"
DECLARE @name nvarchar(100) = 'Data8'
INSERT INTO account (name) VALUES (@name)
SELECT name FROM account WHERE name = @name OR name = @name";
                IRootExecutionPlanNode statement = null;
                cmd.StatementCompleted += (s, e) =>
                {
                    statement = e.Statement;
                };

                using (var reader = cmd.ExecuteReader())
                {
                    Assert.IsTrue(reader.Read());
                    Assert.IsFalse(reader.Read());

                    var select = (SelectNode)statement;
                    var fetch = (FetchXmlScan)select.Source;
                    foreach (var condition in fetch.Entity.GetConditions())
                        Assert.AreEqual("Data8", condition.value);
                }
            }
        }

        [TestMethod]
        public void SortByCollation()
        {
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = "INSERT INTO account (name) VALUES ('Chiapas'),('Colima'), ('Cinco Rios'), ('California')";
                cmd.ExecuteNonQuery();

                cmd.CommandText = "SELECT name FROM account ORDER BY name COLLATE Latin1_General_CS_AS ASC";

                using (var reader = cmd.ExecuteReader())
                {
                    var results = new List<string>();

                    while (reader.Read())
                        results.Add(reader.GetString(0));

                    var expected = new[] { "California", "Chiapas", "Cinco Rios", "Colima" };

                    for (var i = 0; i < expected.Length; i++)
                        Assert.AreEqual(expected[i], results[i]);
                }

                cmd.CommandText = "SELECT name FROM account ORDER BY name COLLATE Traditional_Spanish_ci_ai ASC";

                using (var reader = cmd.ExecuteReader())
                {
                    var results = new List<string>();

                    while (reader.Read())
                        results.Add(reader.GetString(0));

                    var expected = new[] { "California", "Cinco Rios", "Colima", "Chiapas" };

                    for (var i = 0; i < expected.Length; i++)
                        Assert.AreEqual(expected[i], results[i]);
                }
            }
        }

        [TestMethod]
        public void CollationSensitiveFunctions()
        {
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = "select case when 'test' like 't%' then 1 else 0 end";
                var actual = cmd.ExecuteScalar();

                Assert.AreEqual(1, actual);

                cmd.CommandText = "select case when 'TEST' collate latin1_general_cs_ai like 't%' then 1 else 0 end";
                actual = cmd.ExecuteScalar();

                Assert.AreEqual(0, actual);

                cmd.CommandText = "select case when upper('test' collate latin1_general_cs_ai) like 't%' then 1 else 0 end";
                actual = cmd.ExecuteScalar();

                Assert.AreEqual(0, actual);
            }
        }

        [TestMethod]
        public void MergeSemiJoin()
        {
            using (var con = new Sql4CdsConnection(_dataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandTimeout = 0;

                cmd.CommandText = "insert into account (name) values ('data8')";
                cmd.ExecuteNonQuery();
                cmd.ExecuteNonQuery();

                con.ChangeDatabase("prod");
                cmd.ExecuteNonQuery();

                cmd.CommandText = "SELECT name FROM account WHERE name IN (SELECT name FROM uat..account)";

                using (var reader = cmd.ExecuteReader())
                {
                    Assert.IsTrue(reader.Read());
                    Assert.IsFalse(reader.Read());
                }
            }
        }

        [TestMethod]
        public void XmlQuery()
        {
            using (var con = new Sql4CdsConnection(_dataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandTimeout = 0;

                cmd.CommandText = @"
DECLARE @x xml  
SET @x = '<ROOT><a>111</a><a>222</a></ROOT>'  
SELECT @x.query('/ROOT/a')";

                var actual = cmd.ExecuteScalar();

                Assert.AreEqual("<a>111</a><a>222</a>", actual);
            }
        }

        [DataTestMethod]
        [DataRow("/Root/ProductDescription/@ProductID", "int", 1)]
        [DataRow("/Root/ProductDescription/Features/Description", "int", null)]
        public void XmlValue(string xpath, string type, object expected)
        {
            using (var con = new Sql4CdsConnection(_dataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandTimeout = 0;

                cmd.CommandText = $@"DECLARE @myDoc XML  
DECLARE @ProdID INT  
SET @myDoc = '<Root>  
<ProductDescription ProductID=""1"" ProductName=""Road Bike"">  
<Features>
  <Warranty>1 year parts and labor</Warranty>
  <Maintenance>3 year parts and labor extended maintenance is available </Maintenance>
</Features>
</ProductDescription>
</Root>'  


SET @ProdID = @myDoc.value('{xpath}', '{type}')
SELECT @ProdID";

                var actual = cmd.ExecuteScalar();

                Assert.AreEqual(expected ?? DBNull.Value, actual);
            }
        }

        [TestMethod]
        public void Base64()
        {
            using (var con = new Sql4CdsConnection(_dataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandTimeout = 0;

                cmd.CommandText = @"
SELECT 
    CONVERT
    (
        VARCHAR(MAX), 
        CAST('' AS XML).value('xs:base64Binary(sql:column(""BASE64_COLUMN""))', 'VARBINARY(MAX)')
    ) AS RESULT
FROM
    (
        SELECT 'cm9sZToxIHByb2R1Y2VyOjEyIHRpbWVzdGFtcDoxNDY4NjQwMjIyNTcxMDAwIGxhdGxuZ3tsYXRpdHVkZV9lNzo0MTY5ODkzOTQgbG9uZ2l0dWRlX2U3Oi03Mzg5NjYyMTB9IHJhZGl1czoxOTc2NA==' AS BASE64_COLUMN
    ) A";

                var actual = cmd.ExecuteScalar();

                Assert.AreEqual("role:1 producer:12 timestamp:1468640222571000 latlng{latitude_e7:416989394 longitude_e7:-738966210} radius:19764", actual);
            }
        }

        [TestMethod]
        public void XmlQueryFromTable()
        {
            using (var con = new Sql4CdsConnection(_dataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandTimeout = 0;

                cmd.CommandText = "insert into account (name) values ('<ROOT><a>111</a><a>222</a></ROOT>')";
                cmd.ExecuteNonQuery();

                cmd.CommandText = "SELECT CAST(name AS xml).query('/ROOT/a') FROM account";
                var actual = cmd.ExecuteScalar();
                Assert.AreEqual("<a>111</a><a>222</a>", actual);
            }
        }

        [TestMethod]
        public void XmlQueryFromUsingSqlColumn()
        {
            using (var con = new Sql4CdsConnection(_dataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandTimeout = 0;

                cmd.CommandText = "insert into account (name) values ('SGVsbG8gd29ybGQh')";
                cmd.ExecuteNonQuery();

                cmd.CommandText = @"
SELECT
    CONVERT
    (
        VARCHAR(MAX),
        CAST('' AS XML).value('xs:base64Binary(sql:column(""name""))', 'VARBINARY(MAX)')
    ) AS RESULT
FROM
    account";

                var actual = cmd.ExecuteScalar();

                Assert.AreEqual("Hello world!", actual);
            }
        }

        [TestMethod]
        public void ForXmlRaw()
        {
            using (var con = new Sql4CdsConnection(_dataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandTimeout = 0;

                cmd.CommandText = @"
SELECT Name, Value
FROM (VALUES ('Name1', 'Value1'), ('Name2', 'Value2')) AS T(Name, Value)
FOR XML RAW";
                var actual = cmd.ExecuteScalar();
                Assert.AreEqual("<row Name=\"Name1\" Value=\"Value1\" /><row Name=\"Name2\" Value=\"Value2\" />", actual);
            }
        }

        [TestMethod]
        public void ForXmlPathColumnsWithNoName()
        {
            // https://learn.microsoft.com/en-us/sql/relational-databases/xml/columns-without-a-name?view=sql-server-ver16
            using (var con = new Sql4CdsConnection(_dataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandTimeout = 0;

                cmd.CommandText = @"
SELECT 2 + 2
FOR XML PATH";
                var actual = cmd.ExecuteScalar();
                Assert.AreEqual("<row>4</row>", actual);
            }
        }

        [TestMethod]
        public void UpdateCase()
        {
            // https://github.com/MarkMpn/Sql4Cds/issues/314
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandTimeout = 0;

                cmd.CommandText = "INSERT INTO account (employees) VALUES (1)";
                cmd.ExecuteNonQuery();

                cmd.CommandText = "UPDATE a SET a.employees = (CASE WHEN a.employees IS NULL THEN NULL ELSE a.employees + 1 END) FROM account AS a WHERE a.accountid IS NOT NULL";
                cmd.ExecuteNonQuery();

                cmd.CommandText = "SELECT employees FROM account";
                var actual = cmd.ExecuteScalar();
                Assert.AreEqual(2, actual);
            }
        }

        [TestMethod]
        public void FullOuterJoinNoEqijoinPredicate()
        {
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandTimeout = 0;

                cmd.CommandText = "INSERT INTO account (employees) VALUES (1)";
                cmd.ExecuteNonQuery();
                cmd.CommandText = "INSERT INTO account (employees) VALUES (1)";
                cmd.ExecuteNonQuery();

                cmd.CommandText = "SELECT a1.employees, a2.employees FROM account a1 FULL OUTER JOIN account a2 ON a1.employees <> a2.employees";

                using (var reader = cmd.ExecuteReader())
                {
                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual(1, reader.GetInt32(0));
                    Assert.AreEqual(DBNull.Value, reader.GetValue(1));

                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual(1, reader.GetInt32(0));
                    Assert.AreEqual(DBNull.Value, reader.GetValue(1));

                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual(DBNull.Value, reader.GetValue(0));
                    Assert.AreEqual(1, reader.GetInt32(1));

                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual(DBNull.Value, reader.GetValue(0));
                    Assert.AreEqual(1, reader.GetInt32(1));

                    Assert.IsFalse(reader.Read());
                }
            }
        }

        [TestMethod]
        public void StringAgg()
        {
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = "INSERT INTO account (name) VALUES ('A')";
                cmd.ExecuteNonQuery();

                cmd.CommandText = "INSERT INTO account (name) VALUES ('B')";
                cmd.ExecuteNonQuery();

                cmd.CommandText = "INSERT INTO account (name) VALUES ('C')";
                cmd.ExecuteNonQuery();

                cmd.CommandText = "SELECT STRING_AGG(name, ', ') WITHIN GROUP (ORDER BY name DESC) FROM account";
                var actual = cmd.ExecuteScalar();
                Assert.AreEqual("C, B, A", actual);
            }
        }

        [TestMethod]
        public void VariantType()
        {
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                // Can select two variant values of different types in the same column
                cmd.CommandText = "SELECT SERVERPROPERTY('edition') UNION ALL SELECT SERVERPROPERTY('editionid')";

                using (var reader = cmd.ExecuteReader())
                {
                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("Enterprise Edition", reader.GetString(0));
                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual(1804890536, reader.GetInt64(0));
                    Assert.IsFalse(reader.Read());
                }
            }
        }

        [TestMethod]
        public void VariantComparisons()
        {
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                // Variant values are compared according to the type family hierarchy
                // https://dba.stackexchange.com/questions/56722/why-does-implicit-conversion-from-sql-variant-basetype-decimal-not-work-well-w
                cmd.CommandText = @"
declare
    @v sql_variant = convert(decimal(28,8), 20.0);

select sql_variant_property(@v, 'BaseType') as BaseType,         -- 'decimal',
       iif(convert(int, 10.0)     < @v, 1, 0) as ResultInt,      -- 1
       iif(convert(decimal, 10.0) < @v, 1, 0) as  ResultDecimal, -- 1
       iif(convert(float, 10.0)   < @v, 1, 0) as  ResultFloat,   -- 0 !
       iif(convert(float, 10.0)   < convert(float, @v), 1, 0) as  ResultFloatFloat,  -- 1              
       iif(convert(float, 10.0)   < convert(decimal(28,8), @v), 1, 0) as  ResultFloatDecimal;   -- 1";

                using (var reader = cmd.ExecuteReader())
                {
                    Assert.IsTrue(reader.Read());

                    var i = 0;
                    Assert.AreEqual("decimal", reader.GetString(i++));
                    Assert.AreEqual(1, reader.GetInt32(i++));
                    Assert.AreEqual(1, reader.GetInt32(i++));
                    Assert.AreEqual(0, reader.GetInt32(i++));
                    Assert.AreEqual(1, reader.GetInt32(i++));
                    Assert.AreEqual(1, reader.GetInt32(i++));

                    Assert.IsFalse(reader.Read());
                }
            }
        }

        [TestMethod]
        public void SqlVariantProperty()
        {
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                // Variant values are compared according to the type family hierarchy
                // https://dba.stackexchange.com/questions/56722/why-does-implicit-conversion-from-sql-variant-basetype-decimal-not-work-well-w
                cmd.CommandText = @"
declare
    @v sql_variant = cast (46279.1 as decimal(8,2));

SELECT   SQL_VARIANT_PROPERTY(@v,'BaseType') AS 'Base Type',  
         SQL_VARIANT_PROPERTY(@v,'Precision') AS 'Precision',  
         SQL_VARIANT_PROPERTY(@v,'Scale') AS 'Scale' ";

                using (var reader = cmd.ExecuteReader())
                {
                    Assert.IsTrue(reader.Read());

                    var i = 0;
                    Assert.AreEqual("decimal", reader.GetString(i++));
                    Assert.AreEqual(8, reader.GetInt32(i++));
                    Assert.AreEqual(2, reader.GetInt32(i++));

                    Assert.IsFalse(reader.Read());
                }
            }
        }

        [TestMethod]
        public void VariantTypes()
        {
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                // Variant values are compared according to the type family hierarchy
                // https://dba.stackexchange.com/questions/56722/why-does-implicit-conversion-from-sql-variant-basetype-decimal-not-work-well-w
                cmd.CommandText = @"
declare
    @v sql_variant = 1;
declare
    @n sql_variant;

SELECT   @v
UNION ALL
SELECT   @n";

                using (var reader = (Sql4CdsDataReader) cmd.ExecuteReader())
                {
                    Assert.AreEqual(typeof(object), reader.GetProviderSpecificFieldType(0));
                    Assert.AreEqual(typeof(object), reader.GetFieldType(0));
                    Assert.AreEqual("sql_variant", reader.GetDataTypeName(0));

                    var schema = reader.GetSchemaTable();
                    Assert.AreEqual(typeof(object), schema.Rows[0]["DataType"]);
                    Assert.AreEqual(typeof(object), schema.Rows[0]["ProviderSpecificDataType"]);

                    Assert.IsTrue(reader.Read());

                    Assert.AreEqual(1, reader.GetInt32(0));
                    Assert.AreEqual(1, reader.GetValue(0));
                    Assert.AreEqual(new SqlInt32(1), reader.GetProviderSpecificValue(0));

                    Assert.IsTrue(reader.Read());

                    Assert.AreEqual(DBNull.Value, reader.GetValue(0));
                    Assert.AreEqual(DBNull.Value, reader.GetProviderSpecificValue(0));

                    Assert.IsFalse(reader.Read());
                }
            }
        }

        [TestMethod]
        public void ExecSetState()
        {
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = "INSERT INTO contact (firstname, lastname) VALUES ('Test', 'User'); SELECT @@IDENTITY";
                var id = cmd.ExecuteScalar();

                cmd.CommandText = @"
DECLARE @id EntityReference
SELECT TOP 1 @id = contactid FROM contact
EXEC SetState @id, 1, 2";

                cmd.ExecuteNonQuery();

                cmd.CommandText = "SELECT statecode, statuscode FROM contact WHERE contactid = @id";
                cmd.Parameters.Add(new Sql4CdsParameter("@id", id));

                using (var reader = cmd.ExecuteReader())
                {
                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual(1, reader.GetInt32(0));
                    Assert.AreEqual(2, reader.GetInt32(1));
                    Assert.IsFalse(reader.Read());
                }
            }
        }

        [TestMethod]
        public void ComplexFetchXmlAlias()
        {
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = "INSERT INTO account (name) VALUES ('Data8')";
                cmd.ExecuteNonQuery();

                cmd.CommandText = "SELECT name AS [acc. name] FROM account AS [acc. table]";

                using (var reader = cmd.ExecuteReader())
                {
                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("acc. name", reader.GetName(0));
                    Assert.AreEqual("Data8", reader.GetString(0));
                    Assert.IsFalse(reader.Read());
                }
            }
        }

        [TestMethod]
        public void CheckForMissingTable()
        {
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = @"
IF EXISTS(SELECT * FROM metadata.entity WHERE logicalname = 'missing')
    SELECT * FROM missing
ELSE
    SELECT 0";

                Assert.AreEqual(0, cmd.ExecuteScalar());
            }
        }

        [TestMethod]
        public void Throw()
        {
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = "THROW 51000, 'The record does not exist.', 1;";

                try
                {
                    cmd.ExecuteNonQuery();
                    Assert.Fail();
                }
                catch (Sql4CdsException ex)
                {
                    var error = ex.Errors.Single();

                    Assert.AreEqual(51000, error.Number);
                    Assert.AreEqual(16, error.Class);
                    Assert.AreEqual(1, error.State);
                    Assert.AreEqual(1, error.LineNumber);
                    Assert.AreEqual("The record does not exist.", error.Message);
                }
            }
        }

        [TestMethod]
        public void Catch()
        {
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = @"
BEGIN TRY
    THROW 51000, 'Test', 1;
END TRY
BEGIN CATCH
    SELECT ERROR_NUMBER(), ERROR_SEVERITY(), ERROR_STATE(), ERROR_PROCEDURE(), ERROR_LINE(), ERROR_MESSAGE()
END CATCH";

                using (var reader = cmd.ExecuteReader())
                {
                    Assert.IsTrue(reader.Read());

                    Assert.AreEqual(51000, reader.GetInt32(0));
                    Assert.AreEqual(16, reader.GetInt32(1));
                    Assert.AreEqual(1, reader.GetInt32(2));
                    Assert.IsTrue(reader.IsDBNull(3));
                    Assert.AreEqual(3, reader.GetInt32(4));
                    Assert.AreEqual("Test", reader.GetString(5));

                    Assert.IsFalse(reader.Read());
                }
            }
        }

        [TestMethod]
        public void RaiseError()
        {
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = @"
BEGIN TRY
    RAISERROR('Custom message %s', 16, 1, 'test')
END TRY
BEGIN CATCH
    SELECT ERROR_NUMBER(), ERROR_SEVERITY(), ERROR_STATE(), ERROR_PROCEDURE(), ERROR_LINE(), ERROR_MESSAGE()
END CATCH";

                using (var reader = cmd.ExecuteReader())
                {
                    Assert.IsTrue(reader.Read());

                    Assert.AreEqual(50000, reader.GetInt32(0));
                    Assert.AreEqual(16, reader.GetInt32(1));
                    Assert.AreEqual(1, reader.GetInt32(2));
                    Assert.IsTrue(reader.IsDBNull(3));
                    Assert.AreEqual(3, reader.GetInt32(4));
                    Assert.AreEqual("Custom message test", reader.GetString(5));

                    Assert.IsFalse(reader.Read());
                }
            }
        }

        [TestMethod]
        public void NestedCatch()
        {
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = @"
BEGIN TRY
    THROW 51000, 'Test', 1;
END TRY
BEGIN CATCH
    BEGIN TRY
        THROW 51001, 'Test2', 2;
    END TRY
    BEGIN CATCH
        SELECT ERROR_NUMBER(), ERROR_SEVERITY(), ERROR_STATE(), ERROR_PROCEDURE(), ERROR_LINE(), ERROR_MESSAGE()
    END CATCH
    SELECT ERROR_NUMBER(), ERROR_SEVERITY(), ERROR_STATE(), ERROR_PROCEDURE(), ERROR_LINE(), ERROR_MESSAGE()
END CATCH
SELECT ERROR_NUMBER(), ERROR_SEVERITY(), ERROR_STATE(), ERROR_PROCEDURE(), ERROR_LINE(), ERROR_MESSAGE()";

                using (var reader = cmd.ExecuteReader())
                {
                    Assert.IsTrue(reader.Read());

                    Assert.AreEqual(51001, reader.GetInt32(0));
                    Assert.AreEqual(16, reader.GetInt32(1));
                    Assert.AreEqual(2, reader.GetInt32(2));
                    Assert.IsTrue(reader.IsDBNull(3));
                    Assert.AreEqual(7, reader.GetInt32(4));
                    Assert.AreEqual("Test2", reader.GetString(5));

                    Assert.IsFalse(reader.Read());

                    Assert.IsTrue(reader.NextResult());
                    Assert.IsTrue(reader.Read());

                    Assert.AreEqual(51000, reader.GetInt32(0));
                    Assert.AreEqual(16, reader.GetInt32(1));
                    Assert.AreEqual(1, reader.GetInt32(2));
                    Assert.IsTrue(reader.IsDBNull(3));
                    Assert.AreEqual(3, reader.GetInt32(4));
                    Assert.AreEqual("Test", reader.GetString(5));

                    Assert.IsFalse(reader.Read());

                    Assert.IsTrue(reader.NextResult());
                    Assert.IsTrue(reader.Read());

                    Assert.IsTrue(reader.IsDBNull(0));
                    Assert.IsTrue(reader.IsDBNull(1));
                    Assert.IsTrue(reader.IsDBNull(2));
                    Assert.IsTrue(reader.IsDBNull(3));
                    Assert.IsTrue(reader.IsDBNull(4));
                    Assert.IsTrue(reader.IsDBNull(5));

                    Assert.IsFalse(reader.Read());

                    Assert.IsFalse(reader.NextResult());
                }
            }
        }

        [TestMethod]
        public void GotoOutOfCatchBlockClearsError()
        {
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = @"
BEGIN TRY
    THROW 51000, 'Test', 1;
END TRY
BEGIN CATCH
    BEGIN TRY
        THROW 51001, 'Test2', 2;
    END TRY
    BEGIN CATCH
        GOTO label1
    END CATCH
    label1:
    SELECT @@ERROR, ERROR_NUMBER(), ERROR_SEVERITY(), ERROR_STATE(), ERROR_PROCEDURE(), ERROR_LINE(), ERROR_MESSAGE()
    GOTO label2
END CATCH
label2:
SELECT @@ERROR, ERROR_NUMBER(), ERROR_SEVERITY(), ERROR_STATE(), ERROR_PROCEDURE(), ERROR_LINE(), ERROR_MESSAGE()";

                using (var reader = cmd.ExecuteReader())
                {
                    Assert.IsTrue(reader.Read());

                    Assert.AreEqual(51001, reader.GetInt32(0));
                    Assert.AreEqual(51000, reader.GetInt32(1));
                    Assert.AreEqual(16, reader.GetInt32(2));
                    Assert.AreEqual(1, reader.GetInt32(3));
                    Assert.IsTrue(reader.IsDBNull(4));
                    Assert.AreEqual(3, reader.GetInt32(5));
                    Assert.AreEqual("Test", reader.GetString(6));

                    Assert.IsFalse(reader.Read());

                    Assert.IsTrue(reader.NextResult());
                    Assert.IsTrue(reader.Read());

                    Assert.AreEqual(0, reader.GetInt32(0));
                    Assert.IsTrue(reader.IsDBNull(1));
                    Assert.IsTrue(reader.IsDBNull(2));
                    Assert.IsTrue(reader.IsDBNull(3));
                    Assert.IsTrue(reader.IsDBNull(4));
                    Assert.IsTrue(reader.IsDBNull(5));
                    Assert.IsTrue(reader.IsDBNull(6));

                    Assert.IsFalse(reader.Read());

                    Assert.IsFalse(reader.NextResult());
                }
            }
        }

        [TestMethod]
        public void Rethrow()
        {
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = @"
BEGIN TRY
    SELECT * FROM invalid_table;
END TRY
BEGIN CATCH
    SELECT @@ERROR, ERROR_NUMBER(), ERROR_SEVERITY(), ERROR_STATE(), ERROR_PROCEDURE(), ERROR_LINE(), ERROR_MESSAGE();
    THROW;
END CATCH";

                using (var reader = cmd.ExecuteReader())
                {
                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual(208, reader.GetInt32(0));
                    Assert.AreEqual(208, reader.GetInt32(1));
                    Assert.AreEqual(16, reader.GetInt32(2));
                    Assert.AreEqual(1, reader.GetInt32(3));
                    Assert.IsTrue(reader.IsDBNull(4));
                    Assert.AreEqual(2, reader.GetInt32(5));
                    Assert.AreEqual("Invalid object name 'invalid_table'.", reader.GetString(6));
                    Assert.IsFalse(reader.Read());

                    try
                    {
                        reader.NextResult();
                        Assert.Fail();
                    }
                    catch (Sql4CdsException ex)
                    {
                        Assert.AreEqual(208, ex.Number);
                        Assert.AreEqual(2, ex.LineNumber);
                    }
                }
            }
        }

        [DataTestMethod]
        [DataRow("SELECT FORMATMESSAGE('Signed int %i, %d %i, %d, %+i, %+d, %+i, %+d', 5, -5, 50, -50, -11, -11, 11, 11)", "Signed int 5, -5 50, -50, -11, -11, +11, +11")]
        [DataRow("SELECT FORMATMESSAGE('Signed int with up to 3 leading zeros %03i', 5)", "Signed int with up to 3 leading zeros 005")]
        [DataRow("SELECT FORMATMESSAGE('Signed int with up to 20 leading zeros %020i', 5)", "Signed int with up to 20 leading zeros 00000000000000000005")]
        [DataRow("SELECT FORMATMESSAGE('Signed int with leading zero 0 %020i', -55)", "Signed int with leading zero 0 -0000000000000000055")]
        [DataRow("SELECT FORMATMESSAGE('Bigint %I64d', 3000000000)", "Bigint 3000000000")]
        [DataRow("SELECT FORMATMESSAGE('Unsigned int %u, %u', 50, -50)", "Unsigned int 50, 4294967246")]
        [DataRow("SELECT FORMATMESSAGE('Unsigned octal %o, %o', 50, -50)", "Unsigned octal 62, 37777777716")]
        [DataRow("SELECT FORMATMESSAGE('Unsigned hexadecimal %x, %X, %X, %X, %x', 11, 11, -11, 50, -50)", "Unsigned hexadecimal b, B, FFFFFFF5, 32, ffffffce")]
        [DataRow("SELECT FORMATMESSAGE('Unsigned octal with prefix: %#o, %#o', 50, -50)", "Unsigned octal with prefix: 062, 037777777716")]
        [DataRow("SELECT FORMATMESSAGE('Unsigned hexadecimal with prefix: %#x, %#X, %#X, %X, %x', 11, 11, -11, 50, -50)", "Unsigned hexadecimal with prefix: 0xb, 0XB, 0XFFFFFFF5, 32, ffffffce")]
        [DataRow("SELECT FORMATMESSAGE('Hello %s!', 'TEST')", "Hello TEST!")]
        [DataRow("SELECT FORMATMESSAGE('Hello %20s!', 'TEST')", "Hello                 TEST!")]
        [DataRow("SELECT FORMATMESSAGE('Hello %-20s!', 'TEST')", "Hello TEST                !")]
        public void FormatMessage(string query, string expected)
        {
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = query;
                var actual = (string)cmd.ExecuteScalar();
                Assert.AreEqual(expected, actual);
            }
        }

        [DataTestMethod]
        [DataRow("accountid", "uniqueidentifier", 8169)]
        [DataRow("employees", "int", 245)]
        [DataRow("createdon", "datetime", 241)]
        [DataRow("turnover", "money", 235)]
        [DataRow("new_decimalprop", "decimal", 8114)]
        [DataRow("new_doubleprop", "float", 8114)]
        public void ConversionErrors(string column, string type, int expectedError)
        {
            var tableName = column.StartsWith("new_") ? "new_customentity" : "account";

            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                var accountId = Guid.NewGuid();
                _context.Data["account"] = new Dictionary<Guid, Entity>
                {
                    [accountId] = new Entity("account", accountId)
                    {
                        ["accountid"] = accountId,
                        ["employees"] = 10,
                        ["createdon"] = DateTime.Now,
                        ["turnover"] = new Money(1_000_000),
                        ["address1_latitude"] = 45.0D
                    }
                };
                _context.Data["new_customentity"] = new Dictionary<Guid, Entity>
                {
                    [accountId] = new Entity("new_customentity", accountId)
                    {
                        ["new_customentityid"] = accountId,
                        ["new_decimalprop"] = 123.45M,
                        ["new_doubleprop"] = 123.45D
                    }
                };

                var queries = new[]
                {
                    // The error should be thrown when filtering by a column in FetchXML
                    $"SELECT * FROM {tableName} WHERE {column} = 'test'",

                    // The same error should also be thrown when comparing the values in an expression
                    $"SELECT CASE WHEN {column} = 'test' then 1 else 0 end FROM {tableName}",

                    // And also when converting a value directly without a comparison
                    $"SELECT CAST('test' AS {type})"
                };

                foreach (var query in queries)
                {
                    // The error should not be thrown when generating an estimated plan
                    cmd.CommandText = query;
                    cmd.Prepare();

                    try
                    {
                        cmd.ExecuteNonQuery();
                        Assert.Fail();
                    }
                    catch (Sql4CdsException ex)
                    {
                        Assert.AreEqual(expectedError, ex.Number);
                    }
                }
            }
        }

        [TestMethod]
        public void MetadataGuidConversionErrors()
        {
            // Failures converting string to guid should be handled in the same way for metadata queries as for FetchXML
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = "SELECT logicalname FROM metadata.entity WHERE metadataid = 'test'";
                cmd.Prepare();

                try
                {
                    cmd.ExecuteNonQuery();
                    Assert.Fail();
                }
                catch (Sql4CdsException ex)
                {
                    Assert.AreEqual(8169, ex.Number);
                }
            }
        }

        [TestMethod]
        public void MetadataEnumConversionErrors()
        {
            // Enum values are presented as simple strings, so there should be no error when converting invalid values
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = "SELECT logicalname FROM metadata.entity WHERE ownershiptype = 'test'";
                cmd.Prepare();

                using (var reader = cmd.ExecuteReader())
                {
                    Assert.IsFalse(reader.Read());
                }
            }
        }

        class Account<TId>
        {
            public TId AccountId { get; set; }
            public string Name { get; set; }
            public int? Employees { get; set; }
        }

        class EntityReferenceTypeHandler : SqlMapper.TypeHandler<EntityReference>
        {
            public override EntityReference Parse(object value)
            {
                if (value is SqlEntityReference ser)
                    return ser;

                throw new NotSupportedException();
            }

            public override void SetValue(IDbDataParameter parameter, EntityReference value)
            {
                parameter.Value = (SqlEntityReference)value;
            }
        }

        [TestMethod]
        public void DapperQueryEntityReference()
        {
            // reader.GetValue() returns a SqlEntityReference value - need a custom type handler to convert it to the EntityReference
            // property type
            SqlMapper.AddTypeHandler(new EntityReferenceTypeHandler());

            DapperQuery<EntityReference>(id => id.Id);
        }

        [TestMethod]
        public void DapperQuerySqlEntityReference()
        {
            DapperQuery<SqlEntityReference>(id => id.Id);
        }

        [TestMethod]
        public void DapperQueryGuid()
        {
            DapperQuery<Guid>(id => id);
        }

        private void DapperQuery<TId>(Func<TId,Guid> selector)
        {
            using (var con = new Sql4CdsConnection(_localDataSources))
            {
                if (typeof(TId) == typeof(Guid))
                    con.ReturnEntityReferenceAsGuid = true;

                var accountId1 = Guid.NewGuid();
                var accountId2 = Guid.NewGuid();
                _context.Data["account"] = new Dictionary<Guid, Entity>
                {
                    [accountId1] = new Entity("account", accountId1)
                    {
                        ["accountid"] = accountId1,
                        ["name"] = "Account 1",
                        ["employees"] = 10,
                        ["createdon"] = DateTime.Now,
                        ["turnover"] = new Money(1_000_000),
                        ["address1_latitude"] = 45.0D
                    },
                    [accountId2] = new Entity("account", accountId2)
                    {
                        ["accountid"] = accountId2,
                        ["name"] = "Account 2",
                        ["createdon"] = DateTime.Now,
                        ["turnover"] = new Money(1_000_000),
                        ["address1_latitude"] = 45.0D
                    }
                };

                var accounts = con.Query<Account<TId>>("SELECT accountid, name, employees FROM account").AsList();
                Assert.AreEqual(2, accounts.Count);
                var account1 = accounts.Single(a => selector(a.AccountId) == accountId1);
                var account2 = accounts.Single(a => selector(a.AccountId) == accountId2);
                Assert.AreEqual("Account 1", account1.Name);
                Assert.AreEqual("Account 2", account2.Name);
                Assert.AreEqual(10, account1.Employees);
                Assert.IsNull(account2.Employees);
            }
        }

        class SqlEntityReferenceTypeHandler : SqlMapper.TypeHandler<SqlEntityReference>
        {
            public override SqlEntityReference Parse(object value)
            {
                if (value is SqlEntityReference ser)
                    return ser;

                throw new NotSupportedException();
            }

            public override void SetValue(IDbDataParameter parameter, SqlEntityReference value)
            {
                parameter.Value = value;
            }
        }

        [TestMethod]
        public void DapperParameters()
        {
            // Dapper wants to set the DbType of parameters but doesn't understand the SqlEntityReference type, need a custom
            // type handler to set the paramete
            SqlMapper.AddTypeHandler(new SqlEntityReferenceTypeHandler());

            using (var con = new Sql4CdsConnection(_localDataSources))
            {
                con.Execute("INSERT INTO account (name) VALUES (@name)", new { name = "Dapper" });
                var id = con.ExecuteScalar<SqlEntityReference>("SELECT @@IDENTITY");

                var name = con.ExecuteScalar<string>("SELECT name FROM account WHERE accountid = @id", new { id });

                Assert.AreEqual("Dapper", name);
            }
        }

        [TestMethod]
        public void IfExists()
        {
            using (var con = new Sql4CdsConnection(_localDataSources))
            {
                string message = null;

                con.InfoMessage += (sender, e) =>
                {
                    message = e.Message.Message;
                };

                con.Execute(@"
                    IF EXISTS(SELECT * FROM account WHERE name = 'Data8')
                    BEGIN
                        PRINT 'Exists'
                    END
                    ELSE
                    BEGIN
                        PRINT 'Not Exists'
                    END");

                Assert.AreEqual("Not Exists", message);
            }
        }
    }
}
