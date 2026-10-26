using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using MarkMpn.Sql4Cds.Export.Contracts;
using MarkMpn.Sql4Cds.LanguageServer.Configuration;
using MarkMpn.Sql4Cds.LanguageServer.Pptb;
using MarkMpn.Sql4Cds.LanguageServer.QueryExecution.Contracts;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Serialization;

namespace MarkMpn.Sql4Cds.LanguageServer.Tests
{
    [TestClass]
    public class QuerySubsetTests
    {
        private const string OwnerUri = "test://query/subset";
        private static readonly Type HandlerType = typeof(WebApiMetadataQuery).Assembly.GetType(
            "MarkMpn.Sql4Cds.LanguageServer.QueryExecution.QueryExecutionHandler", true);
        private Sql4CdsSettings _previousSettings;

        [TestInitialize]
        public void InitializeSettings()
        {
            _previousSettings = Sql4CdsSettings.Instance;
            Sql4CdsSettings.Instance = new Sql4CdsSettings();
        }

        [TestCleanup]
        public void RestoreSettings() => Sql4CdsSettings.Instance = _previousSettings;

        [DataTestMethod]
        [DataRow(0L, 2, "Zulu,Alpha")]
        [DataRow(1L, 10, "Alpha,Mike")]
        [DataRow(3L, 2, "")]
        [DataRow(Int64.MaxValue, 2, "")]
        [DataRow(-1L, 1, "Zulu")]
        [DataRow(0L, 0, "")]
        [DataRow(0L, -1, "")]
        public void SubsetsPageOriginalRowsWithoutAConnection(long start, int count, string expected)
        {
            var handler = CreateHandler(out _);
            var response = Subset(handler, start, count);
            Assert.AreEqual(expected, String.Join(",", response.ResultSubset.Rows.Select(row => row[0].DisplayValue)));
            Assert.AreEqual(response.ResultSubset.Rows.Length, response.ResultSubset.RowCount);
            var json = JObject.FromObject(response, JsonSerializer.Create(new JsonSerializerSettings
            {
                ContractResolver = new CamelCasePropertyNamesContractResolver()
            }));
            CollectionAssert.AreEquivalent(new[] { "rowCount", "rows" },
                ((JObject)json["resultSubset"]).Properties().Select(property => property.Name).ToArray());
        }

        [TestMethod]
        public void SubsetsFormatNullValuesAndIncludeNewlyRetrievedRows()
        {
            var handler = CreateHandler(out var result);
            Assert.AreEqual(3, Subset(handler, 0, 200).ResultSubset.RowCount);
            Values(result).Add(new object[] { null });
            result.RowCount++;
            var subset = Subset(handler, 2, 200).ResultSubset;
            Assert.AreEqual(2, subset.RowCount);
            Assert.AreEqual("Mike", subset.Rows[0][0].DisplayValue);
            Assert.IsTrue(subset.Rows[1][0].IsNull);
        }

        [TestMethod]
        public void PagingDoesNotChangeFullResultExportOrder()
        {
            var handler = CreateHandler(out _);
            Subset(handler, 1, 1);
            var path = Path.GetTempFileName();
            try
            {
                var response = (SaveResultRequestResult)HandlerType.GetMethod("HandleExportJson").Invoke(handler,
                    new object[] { new SaveResultsAsJsonRequestParams { OwnerUri = OwnerUri, ResultSetIndex = 0, FilePath = path } });
                Assert.IsTrue(String.IsNullOrEmpty(response.Messages), response.Messages);
                var rows = JArray.Parse(File.ReadAllText(path));
                CollectionAssert.AreEqual(new[] { "Zulu", "Alpha", "Mike" },
                    rows.Select(row => row["name"].Value<string>()).ToArray());
            }
            finally
            {
                File.Delete(path);
            }
        }

        [TestMethod]
        public void ExecutionPlanSubsetKeepsItsPlaceholderWithoutCustomViewFields()
        {
            var handler = CreateHandler(out var result);
            result.SpecialAction.ExpectYukonXMLShowPlan = true;
            var subset = Subset(handler, 0, 200).ResultSubset;
            Assert.AreEqual(1, subset.RowCount);
            Assert.AreEqual("", subset.Rows[0][0].DisplayValue);
            Assert.IsFalse(subset.Rows[0][0].IsNull);
        }

        private static object CreateHandler(out ResultSetSummary result)
        {
            var handler = Activator.CreateInstance(HandlerType, new object[] { null, null, null, null });
            result = new ResultSetSummary
            {
                Id = 0, BatchId = 0, RowCount = 3, SpecialAction = new SpecialAction(),
                ColumnInfo = new[] { new DbColumnWrapper("name", "nvarchar", null) }
            };
            Values(result).AddRange(new[] { new object[] { "Zulu" }, new object[] { "Alpha" }, new object[] { "Mike" } });
            var results = (ConcurrentDictionary<string, List<ResultSetSummary>>)HandlerType
                .GetField("_resultSets", BindingFlags.Instance | BindingFlags.NonPublic).GetValue(handler);
            results[OwnerUri] = new List<ResultSetSummary> { result };
            return handler;
        }

        private static List<object[]> Values(ResultSetSummary result) => (List<object[]>)typeof(ResultSetSummary)
            .GetProperty("Values", BindingFlags.Instance | BindingFlags.NonPublic).GetValue(result);

        private static SubsetResult Subset(object handler, long start, int count) =>
            (SubsetResult)HandlerType.GetMethod("HandleSubset").Invoke(handler, new object[]
            {
                new SubsetParams { OwnerUri = OwnerUri, ResultSetIndex = 0, RowsStartIndex = start, RowsCount = count }
            });
    }
}
