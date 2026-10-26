using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using MarkMpn.Sql4Cds.Export.Contracts;
using MarkMpn.Sql4Cds.LanguageServer.Pptb;
using MarkMpn.Sql4Cds.LanguageServer.QueryExecution.Contracts;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace MarkMpn.Sql4Cds.LanguageServer.Tests
{
    [TestClass]
    public class CompletionMessageTests
    {
        private static readonly MethodInfo GetCompletionMessages = typeof(WebApiMetadataQuery).Assembly
            .GetType("MarkMpn.Sql4Cds.LanguageServer.QueryExecution.QueryExecutionHandler", true)
            .GetMethod("GetCompletionMessages", BindingFlags.NonPublic | BindingFlags.Static);

        private static readonly DateTimeOffset CompletionTime = new DateTimeOffset(2026, 10, 26, 19, 48, 24, 681, TimeSpan.FromHours(1)).AddTicks(1234);

        private static string[] Messages(int messageCount, params ResultSetSummary[] resultSets)
        {
            return ((IEnumerable<string>)GetCompletionMessages.Invoke(null, new object[] { messageCount, resultSets, CompletionTime })).ToArray();
        }

        private static ResultSetSummary Plan() => new ResultSetSummary { SpecialAction = new SpecialAction { ExpectYukonXMLShowPlan = true } };

        [TestMethod]
        public void NoResultsOrMessagesReportsCommandsCompleted()
        {
            CollectionAssert.AreEqual(
                new[] { "Commands completed successfully.", "Completion time: 2026-10-26T19:48:24.6811234+01:00" },
                Messages(0));
        }

        [TestMethod]
        public void ExecutionPlansAreNotTreatedAsResults()
        {
            Assert.AreEqual("Commands completed successfully.", Messages(0, Plan()).First());
        }

        [TestMethod]
        public void ResultSetsOrMessagesOnlyReportCompletionTime()
        {
            CollectionAssert.AreEqual(new[] { "Completion time: 2026-10-26T19:48:24.6811234+01:00" }, Messages(0, Plan(), new ResultSetSummary()));
            CollectionAssert.AreEqual(new[] { "Completion time: 2026-10-26T19:48:24.6811234+01:00" }, Messages(1));
        }
    }
}
