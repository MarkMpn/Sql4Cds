using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlTypes;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.Remoting.Contexts;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Services.Description;
using System.Xml.Serialization;
using FakeXrmEasy;
using FakeXrmEasy.Extensions;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Metadata;
using Microsoft.Xrm.Sdk.Metadata.Query;
using Microsoft.Xrm.Sdk.Query;
using Microsoft.Xrm.Tooling.Connector;

namespace MarkMpn.Sql4Cds.Engine.Tests
{
    [TestClass]
    public class CteTests : FakeXrmEasyTestsBase, IQueryExecutionOptions
    {
        private List<JoinOperator> _supportedJoins = new List<JoinOperator>
        {
            JoinOperator.Inner,
            JoinOperator.LeftOuter
        };

        CancellationToken IQueryExecutionOptions.CancellationToken => CancellationToken.None;

        bool IQueryExecutionOptions.BlockUpdateWithoutWhere => false;

        bool IQueryExecutionOptions.BlockDeleteWithoutWhere => false;

        bool IQueryExecutionOptions.UseBulkDelete => false;

        int IQueryExecutionOptions.BatchSize => 1;

        bool IQueryExecutionOptions.UseTDSEndpoint => false;

        int IQueryExecutionOptions.MaxDegreeOfParallelism => 10;

        bool IQueryExecutionOptions.ColumnComparisonAvailable => true;

        bool IQueryExecutionOptions.UseLocalTimeZone => true;

        List<JoinOperator> IQueryExecutionOptions.JoinOperatorsAvailable => _supportedJoins;

        bool IQueryExecutionOptions.BypassCustomPlugins => false;

        void IQueryExecutionOptions.ConfirmInsert(ConfirmDmlStatementEventArgs e)
        {
        }

        void IQueryExecutionOptions.ConfirmDelete(ConfirmDmlStatementEventArgs e)
        {
        }

        void IQueryExecutionOptions.ConfirmUpdate(ConfirmDmlStatementEventArgs e)
        {
        }

        bool IQueryExecutionOptions.ContinueRetrieve(int count)
        {
            return true;
        }

        void IQueryExecutionOptions.Progress(double? progress, string message)
        {
        }

        string IQueryExecutionOptions.PrimaryDataSource => "local";

        Guid IQueryExecutionOptions.UserId => Guid.NewGuid();

        bool IQueryExecutionOptions.QuotedIdentifiers => true;

        public ColumnOrdering ColumnOrdering => ColumnOrdering.Alphabetical;

        [TestMethod]
        public void SimpleSelect()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = @"
                WITH cte AS (SELECT accountid, name FROM account)
                SELECT * FROM cte";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var fetch = AssertNode<FetchXmlScan>(select.Source);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='accountid' />
                        <attribute name='name' />
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void ColumnAliases()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = @"
                WITH cte (id, n) AS (SELECT accountid, name FROM account)
                SELECT * FROM cte";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var fetch = AssertNode<FetchXmlScan>(select.Source);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='accountid' alias='id' />
                        <attribute name='name' alias='n' />
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void MultipleAnchorQueries()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = @"
                WITH cte (id, n) AS (SELECT accountid, name FROM account UNION ALL select contactid, fullname FROM contact)
                SELECT * FROM cte";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var concat = AssertNode<ConcatenateNode>(select.Source);
            var account = AssertNode<FetchXmlScan>(concat.Sources[0]);
            AssertFetchXml(account, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='accountid' />
                        <attribute name='name' />
                    </entity>
                </fetch>");
            var contact = AssertNode<FetchXmlScan>(concat.Sources[1]);
            AssertFetchXml(contact, @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='contactid' />
                        <attribute name='fullname' />
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void MergeFilters()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = @"
                WITH cte AS (SELECT contactid, firstname, lastname FROM contact WHERE firstname = 'Mark')
                SELECT * FROM cte WHERE lastname = 'Carrington'";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var fetch = AssertNode<FetchXmlScan>(select.Source);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='contactid' />
                        <attribute name='firstname' />
                        <attribute name='lastname' />
                        <filter>
                            <condition attribute='firstname' operator='eq' value='Mark' />
                            <condition attribute='lastname' operator='eq' value='Carrington' />
                        </filter>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void MultipleReferencesWithAliases()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = @"
                WITH cte AS (SELECT contactid, firstname, lastname FROM contact WHERE firstname = 'Mark')
                SELECT * FROM cte a INNER JOIN cte b ON a.lastname = b.lastname";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var fetch = AssertNode<FetchXmlScan>(select.Source);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='contactid' />
                        <attribute name='firstname' />
                        <attribute name='lastname' />
                        <link-entity name='contact' from='lastname' to='lastname' alias='b' link-type='inner'>
                            <attribute name='contactid' />
                            <attribute name='firstname' />
                            <attribute name='lastname' />
                            <filter>
                                <condition attribute='firstname' operator='eq' value='Mark' />
                            </filter>
                            <order attribute='contactid' />
                        </link-entity>
                        <filter>
                            <condition attribute='firstname' operator='eq' value='Mark' />
                        </filter>
                        <order attribute='contactid' />
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void MultipleReferencesInUnionAll()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = @"
                WITH cte AS (SELECT contactid, firstname, lastname FROM contact WHERE firstname = 'Mark')
                SELECT * FROM cte UNION ALL SELECT cte.* FROM cte";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var concat = AssertNode<ConcatenateNode>(select.Source);
            var fetch1 = AssertNode<FetchXmlScan>(concat.Sources[0]);
            var fetch2 = AssertNode<FetchXmlScan>(concat.Sources[1]);
            Assert.AreNotEqual(fetch1, fetch2);

            foreach (var fetch in new[] { fetch1, fetch2 })
            {
                AssertFetchXml(fetch, @"
                    <fetch>
                        <entity name='contact'>
                            <attribute name='contactid' />
                            <attribute name='firstname' />
                            <attribute name='lastname' />
                            <filter>
                                <condition attribute='firstname' operator='eq' value='Mark' />
                            </filter>
                        </entity>
                    </fetch>");
            }
        }

        [TestMethod]
        [ExpectedException(typeof(NotSupportedQueryFragmentException))]
        public void MultipleRecursiveReferences()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = @"
                WITH cte AS (
                    SELECT contactid, firstname, lastname FROM contact WHERE firstname = 'Mark'
                    UNION ALL
                    SELECT cte.* FROM cte a INNER JOIN cte b ON a.lastname = b.lastname
                )
                SELECT * FROM cte";

            planBuilder.Build(query, null, out _);
        }

        [TestMethod]
        [ExpectedException(typeof(NotSupportedQueryFragmentException))]
        public void HintsOnRecursiveReference()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = @"
                WITH cte AS (
                    SELECT contactid, firstname, lastname FROM contact WHERE firstname = 'Mark'
                    UNION ALL
                    SELECT cte.* FROM cte WITH (NOLOCK)
                )
                SELECT * FROM cte";

            planBuilder.Build(query, null, out _);
        }

        [TestMethod]
        [ExpectedException(typeof(NotSupportedQueryFragmentException))]
        public void RecursionWithoutUnionAll()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = @"
                WITH cte AS (
                    SELECT contactid, firstname, lastname FROM contact WHERE firstname = 'Mark'
                    UNION
                    SELECT cte.* FROM cte
                )
                SELECT * FROM cte";

            planBuilder.Build(query, null, out _);
        }

        [TestMethod]
        [ExpectedException(typeof(QueryParseException))]
        public void OrderByWithoutTop()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = @"
                WITH cte AS (
                    SELECT contactid, firstname, lastname FROM contact WHERE firstname = 'Mark'
                    UNION ALL
                    SELECT cte.* FROM cte
                    ORDER BY firstname
                )
                SELECT * FROM cte";

            planBuilder.Build(query, null, out _);
        }

        [TestMethod]
        [ExpectedException(typeof(NotSupportedQueryFragmentException))]
        public void GroupByOnRecursiveReference()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = @"
                WITH cte AS (
                    SELECT contactid, firstname, lastname FROM contact WHERE firstname = 'Mark'
                    UNION ALL
                    SELECT cte.* FROM cte GROUP BY contactid, firstname, lastname
                )
                SELECT * FROM cte";

            planBuilder.Build(query, null, out _);
        }

        [TestMethod]
        [ExpectedException(typeof(NotSupportedQueryFragmentException))]
        public void AggregateOnRecursiveReference()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = @"
                WITH cte AS (
                    SELECT contactid, firstname, lastname FROM contact WHERE firstname = 'Mark'
                    UNION ALL
                    SELECT MIN(contactid), MIN(firstname), MIN(lastname) FROM cte
                )
                SELECT * FROM cte";

            planBuilder.Build(query, null, out _);
        }

        [TestMethod]
        [ExpectedException(typeof(NotSupportedQueryFragmentException))]
        public void TopOnRecursiveReference()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = @"
                WITH cte AS (
                    SELECT contactid, firstname, lastname FROM contact WHERE firstname = 'Mark'
                    UNION ALL
                    SELECT TOP 10 cte.* FROM cte
                )
                SELECT * FROM cte";

            planBuilder.Build(query, null, out _);
        }

        [TestMethod]
        [ExpectedException(typeof(NotSupportedQueryFragmentException))]
        public void OuterJoinOnRecursiveReference()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = @"
                WITH cte AS (
                    SELECT contactid, firstname, lastname FROM contact WHERE firstname = 'Mark'
                    UNION ALL
                    SELECT cte.* FROM contact LEFT OUTER JOIN cte ON contact.lastname = cte.lastname
                )
                SELECT * FROM cte";

            planBuilder.Build(query, null, out _);
        }

        [TestMethod]
        [ExpectedException(typeof(NotSupportedQueryFragmentException))]
        public void SubqueryOnRecursiveReference()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = @"
                WITH cte AS (
                    SELECT contactid, firstname, lastname FROM contact WHERE firstname = 'Mark'
                    UNION ALL
                    SELECT contactid, firstname, lastname FROM contact WHERE lastname IN (SELECT lastname FROM cte)
                )
                SELECT * FROM cte";

            planBuilder.Build(query, null, out _);
        }

        [TestMethod]
        [ExpectedException(typeof(NotSupportedQueryFragmentException))]
        public void IncorrectColumnCount()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = @"
                WITH cte (id, fname) AS (
                    SELECT contactid, firstname, lastname FROM contact WHERE firstname = 'Mark'
                )
                SELECT * FROM cte";

            planBuilder.Build(query, null, out _);
        }

        [TestMethod]
        [ExpectedException(typeof(NotSupportedQueryFragmentException))]
        public void AnonymousColumn()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = @"
                WITH cte AS (
                    SELECT contactid, firstname + '', lastname FROM contact WHERE firstname = 'Mark'
                )
                SELECT * FROM cte";

            planBuilder.Build(query, null, out _);
        }

        [TestMethod]
        public void AliasedAnonymousColumn()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = @"
                WITH cte (id, fname, lname) AS (
                    SELECT contactid, firstname + '', lastname FROM contact WHERE firstname = 'Mark'
                )
                SELECT * FROM cte";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            Assert.AreEqual("id", select.ColumnSet[0].OutputColumn);
            Assert.AreEqual("contact.contactid", select.ColumnSet[0].SourceColumn);
            Assert.AreEqual("fname", select.ColumnSet[1].OutputColumn);
            Assert.AreEqual("Expr1", select.ColumnSet[1].SourceColumn);
            Assert.AreEqual("lname", select.ColumnSet[2].OutputColumn);
            Assert.AreEqual("contact.lastname", select.ColumnSet[2].SourceColumn);
            var compute = AssertNode<ComputeScalarNode>(select.Source);
            Assert.AreEqual("firstname + ''", compute.Columns["Expr1"].ToSql());
            var fetch = AssertNode<FetchXmlScan>(compute.Source);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='contactid' />
                        <attribute name='lastname' />
                        <attribute name='firstname' />
                        <filter>
                            <condition attribute=""firstname"" operator=""eq"" value=""Mark"" />
                        </filter>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void SimpleRecursion()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = @"
                WITH cte AS (
                    SELECT contactid, firstname, lastname FROM contact WHERE firstname = 'Mark'
                    UNION ALL
                    SELECT c.contactid, c.firstname, c.lastname FROM contact c INNER JOIN cte ON c.parentcustomerid = cte.contactid
                )
                SELECT * FROM cte";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            Assert.AreEqual("contactid", select.ColumnSet[0].OutputColumn);
            Assert.AreEqual("firstname", select.ColumnSet[1].OutputColumn);
            Assert.AreEqual("lastname", select.ColumnSet[2].OutputColumn);
            var spoolProducer = AssertNode<IndexSpoolNode>(select.Source);
            var concat = AssertNode<ConcatenateNode>(spoolProducer.Source);
            var depth0 = AssertNode<ComputeScalarNode>(concat.Sources[0]);
            var anchor = AssertNode<FetchXmlScan>(depth0.Source);

            AssertFetchXml(anchor, @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='contactid' />
                        <attribute name='firstname' />
                        <attribute name='lastname' />
                        <filter>
                            <condition attribute=""firstname"" operator=""eq"" value=""Mark"" />
                        </filter>
                    </entity>
                </fetch>");

            var assert = AssertNode<AssertNode>(concat.Sources[1]);
            var nestedLoop = AssertNode<NestedLoopNode>(assert.Source);
            var depthPlus1 = AssertNode<ComputeScalarNode>(nestedLoop.LeftSource);
            var spoolConsumer = AssertNode<TableSpoolNode>(depthPlus1.Source);
            var children = AssertNode<FetchXmlScan>(nestedLoop.RightSource);

            AssertFetchXml(children, @"
                <fetch xmlns:generator=""MarkMpn.SQL4CDS"">
                    <entity name='contact'>
                        <attribute name='contactid' />
                        <attribute name='firstname' />
                        <attribute name='lastname' />
                        <filter>
                            <condition attribute=""parentcustomerid"" operator=""eq"" value=""@Expr3"" generator:IsVariable=""true"" />
                        </filter>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void FactorialCalc()
        {
            using (var con = new Sql4CdsConnection(_localDataSource))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = @"
                    WITH Factorial (N, Factorial) AS (
                        SELECT 1, 1
                        UNION ALL
                        SELECT N + 1, (N + 1) * Factorial FROM Factorial WHERE N < 5)
                    SELECT N, Factorial FROM Factorial";

                using (var reader = cmd.ExecuteReader())
                {
                    var n = 1;
                    var factorial = 1;

                    while (n <= 5)
                    {
                        Assert.IsTrue(reader.Read());
                        Assert.AreEqual(n, reader.GetInt32(0));
                        Assert.AreEqual(factorial, reader.GetInt32(1));

                        n++;
                        factorial *= n;
                    }

                    Assert.IsFalse(reader.Read());
                }
            }
        }

        [TestMethod]
        public void FactorialCalcFiltered()
        {

            using (var con = new Sql4CdsConnection(_localDataSource))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = @"
                    WITH Factorial (N, Factorial) AS (
                        SELECT 1, 1
                        UNION ALL
                        SELECT N + 1, (N + 1) * Factorial FROM Factorial WHERE N < 5)
                    SELECT Factorial FROM Factorial WHERE N = 3";

                Assert.AreEqual(6, cmd.ExecuteScalar());
            }
        }

        [TestMethod]
        public void FactorialCalcFilteredCaseInsensitive()
        {
            using (var con = new Sql4CdsConnection(_localDataSource))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = @"
                    with factorial (N, Factorial) as (
                      select 1, 1
                      union all
                      select N + 1, (N + 1) * factorial from factorial where n < 10
                      )
                    select factorial from factorial where n = 3";

                Assert.AreEqual(6, cmd.ExecuteScalar());
            }
        }

        private T AssertNode<T>(IExecutionPlanNode node) where T : IExecutionPlanNode
        {
            Assert.IsInstanceOfType(node, typeof(T));
            return (T)node;
        }

        private void AssertFetchXml(FetchXmlScan node, string fetchXml)
        {
            try
            {
                var serializer = new XmlSerializer(typeof(FetchXml.FetchType));
                using (var reader = new StringReader(fetchXml))
                {
                    var fetch = (FetchXml.FetchType)serializer.Deserialize(reader);
                    PropertyEqualityAssert.Equals(fetch, node.FetchXml);
                }
            }
            catch (AssertFailedException ex)
            {
                Assert.Fail($"Expected:\r\n{fetchXml}\r\n\r\nActual:\r\n{node.FetchXmlString}\r\n\r\n{ex.Message}");
            }
        }
    }
}
