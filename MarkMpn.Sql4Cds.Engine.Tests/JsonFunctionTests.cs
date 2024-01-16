using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace MarkMpn.Sql4Cds.Engine.Tests
{
    [TestClass]
    public class JsonFunctionTests : FakeXrmEasyTestsBase
    {
        [TestMethod]
        [DataRow("lax $", null, false)]
        [DataRow("strict $", null, true)]
        [DataRow("lax $.info.type", "1", false)]
        [DataRow("strict $.info.type", "1", false)]
        [DataRow("lax $.info.address.town", "Bristol", false)]
        [DataRow("strict $.info.address.town", "Bristol", false)]
        [DataRow("lax $.info.\"address\"", null, false)]
        [DataRow("strict $.info.\"address\"", null, true)]
        [DataRow("lax $.info.tags", null, false)]
        [DataRow("strict $.info.tags", null, true)]
        [DataRow("lax $.info.type[0]", null, false)]
        [DataRow("strict $.info.type[0]", null, true)]
        [DataRow("lax $.info.none", null, false)]
        [DataRow("strict $.info.none", null, true)]
        public void JsonValue(string path, string expectedValue, bool expectedError)
        {
            using (var con = new Sql4CdsConnection(_localDataSource))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = @"
DECLARE @jsonInfo NVARCHAR(MAX)

SET @jsonInfo=N'{  
     ""info"":{    
       ""type"":1,  
       ""address"":{    
         ""town"":""Bristol"",  
         ""county"":""Avon"",  
         ""country"":""England""  
       },  
       ""tags"":[""Sport"", ""Water polo""]  
    },  
    ""type"":""Basic""  
 }'

SELECT JSON_VALUE(@jsonInfo, @path)";

                var param = cmd.CreateParameter();
                param.ParameterName = "@path";
                param.Value = path;
                cmd.Parameters.Add(param);

                if (expectedError)
                {
                    Assert.ThrowsException<Sql4CdsException>(() => cmd.ExecuteScalar());
                }
                else
                {
                    if (expectedValue == null)
                        Assert.AreEqual(DBNull.Value, cmd.ExecuteScalar());
                    else
                        Assert.AreEqual(expectedValue, cmd.ExecuteScalar());
                }
            }
        }

        [TestMethod]
        public void JsonValueNull()
        {
            using (var con = new Sql4CdsConnection(_localDataSource))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = "SELECT JSON_VALUE('{ \"changedAttributes\": [ { \"logicalName\": \"column1\", \"oldValue\": null, \"newValue\": \"\" } ] }', '$.changedAttributes[0].oldValue')";
                Assert.AreEqual(DBNull.Value, cmd.ExecuteScalar());
            }
        }

        [TestMethod]
        [DataRow("lax $", null, true, false)]
        [DataRow("strict $", null, true, false)]
        [DataRow("lax $.info.type", null, false, false)]
        [DataRow("strict $.info.type", null, false, true)]
        [DataRow("lax $.info.address.town", null, false, false)]
        [DataRow("strict $.info.address.town", null, false, true)]
        [DataRow("lax $.info.\"address\"", @"{
         ""town"": ""Cheltenham"",
         ""county"": ""Gloucestershire"",
         ""country"": ""England""
      }", false, false)]
        [DataRow("strict $.info.\"address\"", @"{
         ""town"": ""Cheltenham"",
         ""county"": ""Gloucestershire"",
         ""country"": ""England""
      }", false, false)]
        [DataRow("lax $.info.tags", "[\"Sport\", \"Water polo\"]", false, false)]
        [DataRow("strict $.info.tags", "[\"Sport\", \"Water polo\"]", false, false)]
        [DataRow("lax $.info.type[0]", null, false, false)]
        [DataRow("strict $.info.type[0]", null, false, true)]
        [DataRow("lax $.info.none", null, false, false)]
        [DataRow("strict $.info.none", null, false, true)]
        public void JsonQuery(string path, string expectedValue, bool expectedFullValue, bool expectedError)
        {
            using (var con = new Sql4CdsConnection(_localDataSource))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = "SELECT JSON_QUERY(@json, @path)";

                var jsonParam = cmd.CreateParameter();
                jsonParam.ParameterName = "@json";
                jsonParam.Value = @"{
   ""info"": {
      ""type"": 1,
      ""address"": {
         ""town"": ""Cheltenham"",
         ""county"": ""Gloucestershire"",
         ""country"": ""England""
      },
      ""tags"": [""Sport"", ""Water polo""]
   },
   ""type"": ""Basic""
}";
                cmd.Parameters.Add(jsonParam);

                var pathParam = cmd.CreateParameter();
                pathParam.ParameterName = "@path";
                pathParam.Value = path;
                cmd.Parameters.Add(pathParam);

                if (expectedFullValue)
                    expectedValue = (string)jsonParam.Value;

                if (expectedError)
                {
                    Assert.ThrowsException<Sql4CdsException>(() => cmd.ExecuteScalar());
                }
                else
                {
                    if (expectedValue == null)
                        Assert.AreEqual(DBNull.Value, cmd.ExecuteScalar());
                    else
                        Assert.AreEqual(expectedValue, cmd.ExecuteScalar());
                }
            }
        }

        [TestMethod]
        public void IsJsonTrue()
        {
            using (var con = new Sql4CdsConnection(_localDataSource))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = "SELECT ISJSON('true', VALUE)";
                Assert.AreEqual(true, cmd.ExecuteScalar());
            }
        }

        [TestMethod]
        public void IsJsonUnquotedString()
        {
            using (var con = new Sql4CdsConnection(_localDataSource))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = "SELECT ISJSON('test string', VALUE)";
                Assert.AreEqual(false, cmd.ExecuteScalar());
            }
        }

        [TestMethod]
        public void IsJsonQuotedString()
        {
            using (var con = new Sql4CdsConnection(_localDataSource))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = "SELECT ISJSON('\"test string\"', SCALAR)";
                Assert.AreEqual(true, cmd.ExecuteScalar());
            }
        }
    }
}
