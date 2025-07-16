using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace MarkMpn.Sql4Cds.Engine.Tests
{
    [TestClass]
    public class OpenJsonTests : FakeXrmEasyTestsBase
    {
        [TestMethod]
        public void OpenJsonDefaultSchema()
        {
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = @"
DECLARE @json NVARCHAR(MAX)

SET @json='{""name"":""John"",""surname"":""Doe"",""age"":45,""skills"":[""SQL"",""C#"",""MVC""]}';

SELECT *
FROM OPENJSON(@json);";

                using (var reader = cmd.ExecuteReader())
                {
                    Assert.AreEqual("key", reader.GetName(0));
                    Assert.AreEqual("value", reader.GetName(1));
                    Assert.AreEqual("type", reader.GetName(2));

                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("name", reader.GetString(0));
                    Assert.AreEqual("John", reader.GetString(1));
                    Assert.AreEqual(1, reader.GetInt32(2));

                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("surname", reader.GetString(0));
                    Assert.AreEqual("Doe", reader.GetString(1));
                    Assert.AreEqual(1, reader.GetInt32(2));

                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("age", reader.GetString(0));
                    Assert.AreEqual("45", reader.GetString(1));
                    Assert.AreEqual(2, reader.GetInt32(2));

                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("skills", reader.GetString(0));
                    Assert.AreEqual("[\"SQL\",\"C#\",\"MVC\"]", reader.GetString(1));
                    Assert.AreEqual(4, reader.GetInt32(2));

                    Assert.IsFalse(reader.Read());
                }
            }
        }

        [TestMethod]
        public void OpenJsonDefaultSchemaWithPath()
        {
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = @"
DECLARE @json NVARCHAR(4000) = N'{  
      ""path"": {  
            ""to"":{  
                 ""sub-object"":[""en-GB"", ""en-UK"",""de-AT"",""es-AR"",""sr-Cyrl""]  
                 }  
              }  
 }';

SELECT [key], value
FROM OPENJSON(@json,'$.path.to.""sub-object""')";

                using (var reader = cmd.ExecuteReader())
                {
                    Assert.AreEqual("key", reader.GetName(0));
                    Assert.AreEqual("value", reader.GetName(1));

                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("0", reader.GetString(0));
                    Assert.AreEqual("en-GB", reader.GetString(1));

                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("1", reader.GetString(0));
                    Assert.AreEqual("en-UK", reader.GetString(1));

                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("2", reader.GetString(0));
                    Assert.AreEqual("de-AT", reader.GetString(1));

                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("3", reader.GetString(0));
                    Assert.AreEqual("es-AR", reader.GetString(1));

                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("4", reader.GetString(0));
                    Assert.AreEqual("sr-Cyrl", reader.GetString(1));

                    Assert.IsFalse(reader.Read());
                }
            }
        }

        [TestMethod]
        public void OpenJsonDefaultSchemaDataTypes()
        {
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = @"
DECLARE @json NVARCHAR(2048) = N'{
   ""String_value"": ""John"",
   ""DoublePrecisionFloatingPoint_value"": 45,
   ""DoublePrecisionFloatingPoint_value"": 2.3456,
   ""BooleanTrue_value"": true,
   ""BooleanFalse_value"": false,
   ""Null_value"": null,
   ""Array_value"": [""a"",""r"",""r"",""a"",""y""],
   ""Object_value"": {""obj"":""ect""}
}';

SELECT * FROM OpenJson(@json);";

                using (var reader = cmd.ExecuteReader())
                {
                    Assert.AreEqual("key", reader.GetName(0));
                    Assert.AreEqual("value", reader.GetName(1));
                    Assert.AreEqual("type", reader.GetName(2));

                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("String_value", reader.GetString(0));
                    Assert.AreEqual("John", reader.GetString(1));
                    Assert.AreEqual(1, reader.GetInt32(2));

                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("DoublePrecisionFloatingPoint_value", reader.GetString(0));
                    Assert.AreEqual("45", reader.GetString(1));
                    Assert.AreEqual(2, reader.GetInt32(2));

                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("DoublePrecisionFloatingPoint_value", reader.GetString(0));
                    Assert.AreEqual("2.3456", reader.GetString(1));
                    Assert.AreEqual(2, reader.GetInt32(2));

                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("BooleanTrue_value", reader.GetString(0));
                    Assert.AreEqual("true", reader.GetString(1));
                    Assert.AreEqual(3, reader.GetInt32(2));

                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("BooleanFalse_value", reader.GetString(0));
                    Assert.AreEqual("false", reader.GetString(1));
                    Assert.AreEqual(3, reader.GetInt32(2));

                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("Null_value", reader.GetString(0));
                    Assert.IsTrue(reader.IsDBNull(1));
                    Assert.AreEqual(0, reader.GetInt32(2));

                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("Array_value", reader.GetString(0));
                    Assert.AreEqual("[\"a\",\"r\",\"r\",\"a\",\"y\"]", reader.GetString(1));
                    Assert.AreEqual(4, reader.GetInt32(2));

                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("Object_value", reader.GetString(0));
                    Assert.AreEqual("{\"obj\":\"ect\"}", reader.GetString(1));
                    Assert.AreEqual(5, reader.GetInt32(2));

                    Assert.IsFalse(reader.Read());
                }
            }
        }

        [TestMethod]
        public void OpenJsonExplicitSchema()
        {
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = @"
DECLARE @json NVARCHAR(MAX) = N'[  
  {  
    ""Order"": {  
      ""Number"":""SO43659"",  
      ""Date"":""2011-05-31T00:00:00""  
    },  
    ""AccountNumber"":""AW29825"",  
    ""Item"": {  
      ""Price"":2024.9940,  
      ""Quantity"":1  
    }  
  },  
  {  
    ""Order"": {  
      ""Number"":""SO43661"",  
      ""Date"":""2011-06-01T00:00:00""  
    },  
    ""AccountNumber"":""AW73565"",  
    ""Item"": {  
      ""Price"":2024.9940,  
      ""Quantity"":3  
    }  
  }
]'  
   
SELECT *
FROM OPENJSON ( @json )  
WITH (   
              Number   VARCHAR(200)   '$.Order.Number',  
              Date     DATETIME       '$.Order.Date',  
              Customer VARCHAR(200)   '$.AccountNumber',  
              Quantity INT            '$.Item.Quantity',  
              [Order]  NVARCHAR(MAX)  AS JSON  
 )";

                using (var reader = cmd.ExecuteReader())
                {
                    Assert.AreEqual("Number", reader.GetName(0));
                    Assert.AreEqual("Date", reader.GetName(1));
                    Assert.AreEqual("Customer", reader.GetName(2));
                    Assert.AreEqual("Quantity", reader.GetName(3));
                    Assert.AreEqual("Order", reader.GetName(4));

                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("SO43659", reader.GetString(0));
                    Assert.AreEqual(new DateTime(2011, 5, 31), reader.GetDateTime(1));
                    Assert.AreEqual("AW29825", reader.GetString(2));
                    Assert.AreEqual(1, reader.GetInt32(3));
                    Assert.AreEqual(@"{  
      ""Number"":""SO43659"",  
      ""Date"":""2011-05-31T00:00:00""  
    }", reader.GetString(4));

                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("SO43661", reader.GetString(0));
                    Assert.AreEqual(new DateTime(2011, 6, 1), reader.GetDateTime(1));
                    Assert.AreEqual("AW73565", reader.GetString(2));
                    Assert.AreEqual(3, reader.GetInt32(3));
                    Assert.AreEqual(@"{  
      ""Number"":""SO43661"",  
      ""Date"":""2011-06-01T00:00:00""  
    }", reader.GetString(4));

                    Assert.IsFalse(reader.Read());
                }
            }
        }

        [TestMethod]
        public void MergeJson()
        {
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = @"
DECLARE @json1 NVARCHAR(MAX),@json2 NVARCHAR(MAX)

SET @json1=N'{""name"": ""John"", ""surname"":""Doe""}'

SET @json2=N'{""name"": ""John"", ""age"":45}'

SELECT *
FROM OPENJSON(@json1)
UNION ALL
SELECT *
FROM OPENJSON(@json2)
WHERE [key] NOT IN (SELECT [key] FROM OPENJSON(@json1))";

                using (var reader = cmd.ExecuteReader())
                {
                    Assert.AreEqual("key", reader.GetName(0));
                    Assert.AreEqual("value", reader.GetName(1));
                    Assert.AreEqual("type", reader.GetName(2));

                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("name", reader.GetString(0));
                    Assert.AreEqual("John", reader.GetString(1));
                    Assert.AreEqual(1, reader.GetInt32(2));

                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("surname", reader.GetString(0));
                    Assert.AreEqual("Doe", reader.GetString(1));
                    Assert.AreEqual(1, reader.GetInt32(2));

                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("age", reader.GetString(0));
                    Assert.AreEqual("45", reader.GetString(1));
                    Assert.AreEqual(2, reader.GetInt32(2));

                    Assert.IsFalse(reader.Read());
                }
            }
        }

        [TestMethod]
        public void NestedJson()
        {
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = @"
--simple cross apply example
DECLARE @JSON NVARCHAR(MAX) = N'[
{
""OrderNumber"":""SO43659"",
""OrderDate"":""2011-05-31T00:00:00"",
""AccountNumber"":""AW29825"",
""ItemPrice"":2024.9940,
""ItemQuantity"":1
},
{
""OrderNumber"":""SO43661"",
""OrderDate"":""2011-06-01T00:00:00"",
""AccountNumber"":""AW73565"",
""ItemPrice"":2024.9940,
""ItemQuantity"":3
}
]'

SELECT root.[key] AS [Order],TheValues.[key], TheValues.[value]
FROM OPENJSON ( @JSON ) AS root
CROSS APPLY OPENJSON ( root.value) AS TheValues";

                using (var reader = cmd.ExecuteReader())
                {
                    Assert.AreEqual("Order", reader.GetName(0));
                    Assert.AreEqual("key", reader.GetName(1));
                    Assert.AreEqual("value", reader.GetName(2));

                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("0", reader.GetString(0));
                    Assert.AreEqual("OrderNumber", reader.GetString(1));
                    Assert.AreEqual("SO43659", reader.GetString(2));

                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("0", reader.GetString(0));
                    Assert.AreEqual("OrderDate", reader.GetString(1));
                    Assert.AreEqual("2011-05-31T00:00:00", reader.GetString(2));

                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("0", reader.GetString(0));
                    Assert.AreEqual("AccountNumber", reader.GetString(1));
                    Assert.AreEqual("AW29825", reader.GetString(2));

                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("0", reader.GetString(0));
                    Assert.AreEqual("ItemPrice", reader.GetString(1));
                    Assert.AreEqual("2024.9940", reader.GetString(2));

                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("0", reader.GetString(0));
                    Assert.AreEqual("ItemQuantity", reader.GetString(1));
                    Assert.AreEqual("1", reader.GetString(2));

                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("1", reader.GetString(0));
                    Assert.AreEqual("OrderNumber", reader.GetString(1));
                    Assert.AreEqual("SO43661", reader.GetString(2));

                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("1", reader.GetString(0));
                    Assert.AreEqual("OrderDate", reader.GetString(1));
                    Assert.AreEqual("2011-06-01T00:00:00", reader.GetString(2));

                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("1", reader.GetString(0));
                    Assert.AreEqual("AccountNumber", reader.GetString(1));
                    Assert.AreEqual("AW73565", reader.GetString(2));

                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("1", reader.GetString(0));
                    Assert.AreEqual("ItemPrice", reader.GetString(1));
                    Assert.AreEqual("2024.9940", reader.GetString(2));

                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("1", reader.GetString(0));
                    Assert.AreEqual("ItemQuantity", reader.GetString(1));
                    Assert.AreEqual("3", reader.GetString(2));

                    Assert.IsFalse(reader.Read());
                }
            }
        }

        [TestMethod]
        public void RecursiveCTEJson()
        {
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = @"
DECLARE @JSON NVARCHAR(MAX) = N'[
{
""OrderNumber"":""SO43659"",
""OrderDate"":""2011-05-31T00:00:00"",
""AccountNumber"":""AW29825"",
""ItemPrice"":2024.9940,
""ItemQuantity"":1
},
{
""OrderNumber"":""SO43661"",
""OrderDate"":""2011-06-01T00:00:00"",
""AccountNumber"":""AW73565"",
""ItemPrice"":2024.9940,
""ItemQuantity"":3
}
]';


with cte ([key], value, type) as (
    select '$[' + [key] + ']', value, type from OPENJSON(@json)

    union all

    select cte.[key] + case when cte.type = 4 then '[' + childvalues.[key] + ']' else '.' + childvalues.[key] end, childvalues.value, childvalues.type from cte cross apply OPENJSON(cte.value) as childvalues WHERE cte.type in (4, 5)
)

SELECT * from CTE";

                using (var reader = cmd.ExecuteReader())
                {
                    Assert.AreEqual("key", reader.GetName(0));
                    Assert.AreEqual("value", reader.GetName(1));
                    Assert.AreEqual("type", reader.GetName(2));

                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("$[0]", reader.GetString(0));
                    Assert.AreEqual(@"{
""OrderNumber"":""SO43659"",
""OrderDate"":""2011-05-31T00:00:00"",
""AccountNumber"":""AW29825"",
""ItemPrice"":2024.9940,
""ItemQuantity"":1
}", reader.GetString(1));
                    Assert.AreEqual(5, reader.GetInt32(2));

                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("$[1]", reader.GetString(0));
                    Assert.AreEqual(@"{
""OrderNumber"":""SO43661"",
""OrderDate"":""2011-06-01T00:00:00"",
""AccountNumber"":""AW73565"",
""ItemPrice"":2024.9940,
""ItemQuantity"":3
}", reader.GetString(1));
                    Assert.AreEqual(5, reader.GetInt32(2));

                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("$[1].OrderNumber", reader.GetString(0));
                    Assert.AreEqual("SO43661", reader.GetString(1));
                    Assert.AreEqual(1, reader.GetInt32(2));

                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("$[1].OrderDate", reader.GetString(0));
                    Assert.AreEqual("2011-06-01T00:00:00", reader.GetString(1));
                    Assert.AreEqual(1, reader.GetInt32(2));

                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("$[1].AccountNumber", reader.GetString(0));
                    Assert.AreEqual("AW73565", reader.GetString(1));
                    Assert.AreEqual(1, reader.GetInt32(2));

                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("$[1].ItemPrice", reader.GetString(0));
                    Assert.AreEqual("2024.9940", reader.GetString(1));
                    Assert.AreEqual(2, reader.GetInt32(2));

                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("$[1].ItemQuantity", reader.GetString(0));
                    Assert.AreEqual("3", reader.GetString(1));
                    Assert.AreEqual(2, reader.GetInt32(2));

                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("$[0].OrderNumber", reader.GetString(0));
                    Assert.AreEqual("SO43659", reader.GetString(1));
                    Assert.AreEqual(1, reader.GetInt32(2));

                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("$[0].OrderDate", reader.GetString(0));
                    Assert.AreEqual("2011-05-31T00:00:00", reader.GetString(1));
                    Assert.AreEqual(1, reader.GetInt32(2));

                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("$[0].AccountNumber", reader.GetString(0));
                    Assert.AreEqual("AW29825", reader.GetString(1));
                    Assert.AreEqual(1, reader.GetInt32(2));

                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("$[0].ItemPrice", reader.GetString(0));
                    Assert.AreEqual("2024.9940", reader.GetString(1));
                    Assert.AreEqual(2, reader.GetInt32(2));

                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual("$[0].ItemQuantity", reader.GetString(0));
                    Assert.AreEqual("1", reader.GetString(1));
                    Assert.AreEqual(2, reader.GetInt32(2));

                    Assert.IsFalse(reader.Read());
                }
            }
        }

        [TestMethod]
        public void OpenJsonResultsNotSpooled()
        {
            // https://github.com/MarkMpn/Sql4Cds/issues/682
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = @"
CREATE TABLE #tmp (
    ID INT NOT NULL,
    JSON NVARCHAR(MAX) NOT NULL
);

INSERT INTO #tmp (ID, JSON) VALUES (1, '{""key"":""A""}');
INSERT INTO #tmp (ID, JSON) VALUES (2, '{""key"":""B""}');

SELECT t.ID, (SELECT TOP 1 [value] FROM OPENJSON(t.JSON)) AS [key] from #tmp t";

                using (var reader = cmd.ExecuteReader())
                {
                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual(1, reader.GetInt32(0));
                    Assert.AreEqual("A", reader.GetString(1));

                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual(2, reader.GetInt32(0));
                    Assert.AreEqual("B", reader.GetString(1));

                    Assert.IsFalse(reader.Read());
                }
            }
        }
    }
}
