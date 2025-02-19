using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace MarkMpn.Sql4Cds.Engine.Tests
{
    [TestClass]
    public class XmlTests : FakeXrmEasyTestsBase
    {
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
        public void CannotUseDynamicColumnName()
        {
            using (var con = new Sql4CdsConnection(_dataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = @"
SELECT CAST('' AS XML).value('sql:column(sql:column(""name""))', 'VARBINARY(MAX)')
FROM account";

                try
                {
                    cmd.ExecuteNonQuery();
                    Assert.Fail();
                }
                catch (Sql4CdsException ex)
                {
                    Assert.AreEqual(2225, ex.Number);
                }
            }
        }

        [TestMethod]
        public void ReferenceColumnInOuterApply()
        {
            using (var con = new Sql4CdsConnection(_dataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = "INSERT INTO account (name) VALUES ('SGVsbG8gd29ybGQh')";
                cmd.ExecuteNonQuery();

                cmd.CommandText = @"
SELECT x.base64
FROM   account a
       OUTER APPLY (
              SELECT CONVERT(VARCHAR(MAX), CAST('' AS XML).value('xs:base64Binary(sql:column(""name""))', 'VARBINARY(MAX)')) AS base64
         ) x";

                var reader = cmd.ExecuteReader();
                Assert.IsTrue(reader.Read());
                Assert.AreEqual("Hello world!", reader["base64"]);
            }
        }
    }
}
