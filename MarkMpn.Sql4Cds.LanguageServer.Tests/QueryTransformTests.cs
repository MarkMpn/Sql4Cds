using System;
using System.Collections.Generic;
using MarkMpn.Sql4Cds.Engine;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using MarkMpn.Sql4Cds.LanguageServer.Capabilities;
using MarkMpn.Sql4Cds.LanguageServer.Connection;
using MarkMpn.Sql4Cds.LanguageServer.Pptb;
using MarkMpn.Sql4Cds.LanguageServer.QueryExecution;
using MarkMpn.Sql4Cds.LanguageServer.Scripting;
using MarkMpn.Sql4Cds.LanguageServer.Workspace;
using MarkMpn.Sql4Cds.XTB;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json.Linq;
using Microsoft.VisualStudio.LanguageServer.Protocol;
using MarkMpn.Sql4Cds.LanguageServer.Capabilities.Contracts;

namespace MarkMpn.Sql4Cds.LanguageServer.Tests
{
    [TestClass]
    public class QueryTransformTests
    {
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
            var error = Assert.ThrowsException<InvalidOperationException>(() => FormatDocument("SELECT name FROM account", options));
            StringAssert.Contains(error.Message, "connection");
            options.OtherOptions["addDisplayNames"] = false;
            Assert.AreEqual(1, FormatDocument("select 1", options).Length);
            options.OtherOptions["addDisplayNames"] = "true";
            Assert.ThrowsException<ArgumentException>(() => FormatDocument("select 1", options));
        }

        [TestMethod]
        public void InitializeAdvertisesStandardDocumentFormattingCapability()
        {
            var handler = new CapabilitiesHandler(null, new PptbRpcChunking(), new QueryEventStream(), new ConnectionManager(null, null, null));
            var result = handler.HandleInitialize(new ClientInitializeParams());
            var json = JObject.FromObject(result, Newtonsoft.Json.JsonSerializer.Create(new Newtonsoft.Json.JsonSerializerSettings
            {
                ContractResolver = new Newtonsoft.Json.Serialization.CamelCasePropertyNamesContractResolver()
            }));
            Assert.IsTrue(json["capabilities"]["documentFormattingProvider"].Value<bool>());
        }

        private static TextEdit[] FormatDocument(string query, FormattingOptions options)
        {
            var documents = new TextDocumentManager();
            const string uri = "pptb://query/1";
            documents.SetContent(uri, query);
            var handler = new QueryTransformHandler(new ConnectionManager(null, null, null), documents);
            return handler.FormatDocument(new DocumentFormattingParams { TextDocument = new TextDocumentIdentifier { Uri = new Uri(uri) }, Options = options });
        }

        [TestMethod]
        public void FormatPreservesCommentsAndFormatsSql()
        {
            var formatted = QueryTransformHandler.Format("-- Keep this comment\nselect name from account where name='Contoso'", null);
            StringAssert.Contains(formatted, "-- Keep this comment");
            StringAssert.Contains(formatted, "SELECT");
            StringAssert.Contains(formatted, "FROM");
            StringAssert.Contains(formatted, "'Contoso'");
            Assert.AreEqual(formatted, QueryTransformHandler.Format(formatted, null));
        }

        [TestMethod]
        public void FormatReportsInvalidSqlInsteadOfReturningUnchangedText()
        {
            var error = Assert.ThrowsException<ArgumentException>(() => QueryTransformHandler.Format("select from", null));
            StringAssert.Contains(error.Message, "Line 1");
        }

        [TestMethod]
        public void ConvertToMEscapesQuotesNewlinesAndLiteralEscapeSequences()
        {
            var query = "-- comment\r\nSELECT \"name\", '#(lf)' FROM account\tWHERE name = 'A'";
            var m = QueryTransformHandler.ConvertToM(query, "contoso.crm.dynamics.com");
            StringAssert.Contains(m, "CommonDataService.Database(\"contoso.crm.dynamics.com\")");
            StringAssert.Contains(m, "-- comment#(cr)#(lf)SELECT \"\"name\"\", '#(#)(lf)' FROM account#(tab)WHERE");
            StringAssert.Contains(m, "Value.NativeQuery");
            StringAssert.Contains(m, "[EnableFolding=true]");
        }

        [TestMethod]
        public void FetchXmlConversionUsesMetadataWithoutExecutingQueriesAndWarnsAboutProcessing()
        {
            using var connection = CreateConnection(out _);
            var results = QueryTransformHandler.ConvertToFetchXml("SELECT name FROM account", connection);
            Assert.AreEqual(1, results.Length);
            StringAssert.Contains(results[0], "<fetch");
            StringAssert.Contains(results[0], "<entity name=\"account\"");
            StringAssert.Contains(results[0], "<attribute name=\"name\"");
            Assert.IsFalse(results[0].Contains("additional processing"));

            results = QueryTransformHandler.ConvertToFetchXml("SELECT UPPER(name) FROM account", connection);
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
            var formatted = QueryTransformHandler.Format("SELECT name, name AS Existing FROM account", options);
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
                TableSizeCache = new TableSizes(),
                DefaultCollation = Collation.USEnglish
            };
            dataSources = new Dictionary<string, DataSource> { ["test"] = dataSource };
            return new Sql4CdsConnection(dataSources) { UseTDSEndpoint = false };
        }

        private sealed class TableSizes : ITableSizeCache
        {
            public int this[string logicalName] => 100;
        }
    }
}
