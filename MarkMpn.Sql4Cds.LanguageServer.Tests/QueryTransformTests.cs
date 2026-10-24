using System;
using System.Reflection;
using System.Collections.Generic;
using MarkMpn.Sql4Cds.Engine;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using MarkMpn.Sql4Cds.LanguageServer.Pptb;
using MarkMpn.Sql4Cds.XTB;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json.Linq;

namespace MarkMpn.Sql4Cds.LanguageServer.Tests
{
    [TestClass]
    public class QueryTransformTests
    {
        private static object Invoke(string method, params object[] args)
        {
            var handler = typeof(WebApiMetadataQuery).Assembly.GetType("MarkMpn.Sql4Cds.LanguageServer.Scripting.QueryTransformHandler", true);
            return handler.GetMethod(method, BindingFlags.NonPublic | BindingFlags.Static).Invoke(null, args);
        }

        [TestMethod]
        public void FormatPreservesCommentsAndFormatsSql()
        {
            var formatted = (string)Invoke("Format", "-- Keep this comment\nselect name from account where name='Contoso'", null);
            StringAssert.Contains(formatted, "-- Keep this comment");
            StringAssert.Contains(formatted, "SELECT");
            StringAssert.Contains(formatted, "FROM");
            StringAssert.Contains(formatted, "'Contoso'");
            Assert.AreEqual(formatted, Invoke("Format", formatted, null));
        }

        [TestMethod]
        public void FormatReportsInvalidSqlInsteadOfReturningUnchangedText()
        {
            var error = Assert.ThrowsException<TargetInvocationException>(() => Invoke("Format", "select from", null));
            Assert.IsInstanceOfType<ArgumentException>(error.InnerException);
            StringAssert.Contains(error.InnerException.Message, "Line 1");
        }

        [DataTestMethod]
        [DataRow("DELETE FROM account; SELECT name FROM account")]
        [DataRow("SELECT name FROM account; UPDATE account SET name = 'x'")]
        [DataRow("EXEC WhoAmI")]
        [DataRow("SELECT name INTO #temp FROM account")]
        public void DisplayNamesRejectsNonSelectScriptsBeforeExecutingAnything(string query)
        {
            var error = Assert.ThrowsException<TargetInvocationException>(() =>
                Invoke("Format", query, new FormatterOptions { AddDisplayNames = true, ThrowOnError = true }));
            Assert.IsInstanceOfType<NotSupportedException>(error.InnerException);
            StringAssert.Contains(error.InnerException.Message, "SELECT statements only");
        }

        [TestMethod]
        public void ConvertToMEscapesQuotesNewlinesAndLiteralEscapeSequences()
        {
            var query = "-- comment\r\nSELECT \"name\", '#(lf)' FROM account\tWHERE name = 'A'";
            var m = (string)Invoke("ConvertToM", query, "contoso.crm.dynamics.com");
            StringAssert.Contains(m, "CommonDataService.Database(\"contoso.crm.dynamics.com\")");
            StringAssert.Contains(m, "-- comment#(cr)#(lf)SELECT \"\"name\"\", '#(#)(lf)' FROM account#(tab)WHERE");
            StringAssert.Contains(m, "Value.NativeQuery");
            StringAssert.Contains(m, "[EnableFolding=true]");
        }

        [TestMethod]
        public void FetchXmlConversionUsesMetadataWithoutExecutingQueriesAndWarnsAboutProcessing()
        {
            using var connection = CreateConnection(out _);
            var results = (string[])Invoke("ConvertToFetchXml", "SELECT name FROM account", connection);
            Assert.AreEqual(1, results.Length);
            StringAssert.Contains(results[0], "<fetch");
            StringAssert.Contains(results[0], "<entity name=\"account\"");
            StringAssert.Contains(results[0], "<attribute name=\"name\"");
            Assert.IsFalse(results[0].Contains("additional processing"));

            results = (string[])Invoke("ConvertToFetchXml", "SELECT UPPER(name) FROM account", connection);
            StringAssert.Contains(results[0], "additional processing");
        }

        [TestMethod]
        public void DisplayNamesUsesMetadataAliasesAndPreservesExistingAliases()
        {
            using var connection = CreateConnection(out var dataSources);
            var options = new FormatterOptions
            {
                AddDisplayNames = true,
                ThrowOnError = true,
                Connection = connection,
                DataSources = dataSources
            };
            var formatted = (string)Invoke("Format", "SELECT name, name AS Existing FROM account", options);
            StringAssert.Contains(formatted, "[Account Name]");
            StringAssert.Contains(formatted, "Existing");
        }

        private static Sql4CdsConnection CreateConnection(out Dictionary<string, DataSource> dataSources)
        {
            var metadata = MetadataTestHelper.Cache(new JObject
            {
                ["LogicalName"] = "account",
                ["PrimaryIdAttribute"] = "accountid",
                ["PrimaryNameAttribute"] = "name",
                ["DisplayName"] = new JObject { ["UserLocalizedLabel"] = new JObject { ["Label"] = "Account", ["LanguageCode"] = 1033 } },
                ["Attributes"] = new JArray(
                    new JObject { ["@odata.type"] = "#Microsoft.Dynamics.CRM.UniqueIdentifierAttributeMetadata", ["LogicalName"] = "accountid", ["AttributeType"] = "Uniqueidentifier", ["ColumnNumber"] = 1, ["IsPrimaryId"] = true, ["IsValidForRead"] = true },
                    new JObject { ["@odata.type"] = "#Microsoft.Dynamics.CRM.StringAttributeMetadata", ["LogicalName"] = "name", ["AttributeType"] = "String", ["ColumnNumber"] = 2, ["MaxLength"] = 100, ["IsValidForRead"] = true,
                        ["DisplayName"] = new JObject { ["UserLocalizedLabel"] = new JObject { ["Label"] = "Account Name", ["LanguageCode"] = 1033 } } })
            });
            var dataSource = new DataSource
            {
                Name = "test",
                Metadata = metadata,
                TableSizeCache = new TableSizes()
            };
            var collation = typeof(Collation).GetProperty("USEnglish", BindingFlags.Static | BindingFlags.NonPublic).GetValue(null);
            typeof(DataSource).GetProperty("DefaultCollation", BindingFlags.Instance | BindingFlags.NonPublic).SetValue(dataSource, collation);
            dataSources = new Dictionary<string, DataSource> { ["test"] = dataSource };
            return new Sql4CdsConnection(dataSources) { UseTDSEndpoint = false };
        }

        private sealed class TableSizes : ITableSizeCache
        {
            public int this[string logicalName] => 100;
        }
    }
}
