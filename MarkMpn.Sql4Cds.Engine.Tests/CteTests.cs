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
