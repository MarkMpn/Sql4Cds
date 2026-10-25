using System;
using System.Reflection;
using System.Collections.Generic;
using MarkMpn.Sql4Cds.Engine;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using MarkMpn.Sql4Cds.LanguageServer.Pptb;
using MarkMpn.Sql4Cds.XTB;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json.Linq;
using Microsoft.VisualStudio.LanguageServer.Protocol;

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
        public void DocumentFormattingReadsSyncedDocumentAndReturnsStandardTextEdits()
        {
            const string query = "-- comment \ud83d\ude00\r\nselect 1";
            var edits = FormatDocument(query, new FormattingOptions { TabSize = 4, InsertSpaces = true });
            Assert.AreEqual(1, edits.Length);
            Assert.AreEqual(0, edits[0].Range.Start.Line);
            Assert.AreEqual(0, edits[0].Range.Start.Character);
            Assert.AreEqual(1, edits[0].Range.End.Line);
            Assert.AreEqual(8, edits[0].Range.End.Character);
            StringAssert.Contains(edits[0].NewText, "-- comment \ud83d\ude00");
            StringAssert.Contains(edits[0].NewText, "SELECT");
            Assert.AreEqual(0, FormatDocument(edits[0].NewText, new FormattingOptions()).Length);
            Assert.AreEqual(0, FormatDocument(" \n", new FormattingOptions()).Length);
        }

        [TestMethod]
        public void DocumentFormattingReadsDisplayNamesFromOptionsAndRequiresAConnection()
        {
            var options = Newtonsoft.Json.JsonConvert.DeserializeObject<FormattingOptions>(
                "{\"tabSize\":4,\"insertSpaces\":true,\"addDisplayNames\":true}");
            Assert.IsTrue((bool)options.OtherOptions["addDisplayNames"]);
            var error = Assert.ThrowsException<TargetInvocationException>(() => FormatDocument("SELECT name FROM account", options));
            Assert.IsInstanceOfType<InvalidOperationException>(error.InnerException);
            StringAssert.Contains(error.InnerException.Message, "connection");
            options.OtherOptions["addDisplayNames"] = false;
            Assert.AreEqual(1, FormatDocument("select 1", options).Length);
            options.OtherOptions["addDisplayNames"] = "true";
            error = Assert.ThrowsException<TargetInvocationException>(() => FormatDocument("select 1", options));
            Assert.IsInstanceOfType<ArgumentException>(error.InnerException);
        }

        [TestMethod]
        public void InitializeAdvertisesStandardDocumentFormattingCapability()
        {
            var assembly = typeof(WebApiMetadataQuery).Assembly;
            var chunking = Activator.CreateInstance(assembly.GetType("MarkMpn.Sql4Cds.LanguageServer.Pptb.PptbRpcChunking", true));
            var type = assembly.GetType("MarkMpn.Sql4Cds.LanguageServer.Capabilities.CapabilitiesHandler", true);
            var handler = Activator.CreateInstance(type, new object[] { null, chunking });
            var result = type.GetMethod("HandleInitialize", BindingFlags.Instance | BindingFlags.NonPublic)
                .Invoke(handler, new object[] { new InitializeParams() });
            var json = JObject.FromObject(result, Newtonsoft.Json.JsonSerializer.Create(new Newtonsoft.Json.JsonSerializerSettings
            {
                ContractResolver = new Newtonsoft.Json.Serialization.CamelCasePropertyNamesContractResolver()
            }));
            Assert.IsTrue(json["capabilities"]["documentFormattingProvider"].Value<bool>());
        }

        private static TextEdit[] FormatDocument(string query, FormattingOptions options)
        {
            var assembly = typeof(WebApiMetadataQuery).Assembly;
            var documentsType = assembly.GetType("MarkMpn.Sql4Cds.LanguageServer.Workspace.TextDocumentManager", true);
            var documents = Activator.CreateInstance(documentsType);
            const string uri = "pptb://query/1";
            documentsType.GetMethod("SetContent").Invoke(documents, new object[] { uri, query });
            var connectionsType = assembly.GetType("MarkMpn.Sql4Cds.LanguageServer.Connection.ConnectionManager", true);
            var connections = Activator.CreateInstance(connectionsType, new object[] { null, null, null });
            var handlerType = assembly.GetType("MarkMpn.Sql4Cds.LanguageServer.Scripting.QueryTransformHandler", true);
            var handler = Activator.CreateInstance(handlerType, new[] { connections, documents });
            return (TextEdit[])handlerType.GetMethod("FormatDocument", BindingFlags.NonPublic | BindingFlags.Instance).Invoke(handler,
                new object[] { new DocumentFormattingParams { TextDocument = new TextDocumentIdentifier { Uri = new Uri(uri) }, Options = options } });
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
