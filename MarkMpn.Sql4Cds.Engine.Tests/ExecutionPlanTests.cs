using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;
using FakeXrmEasy;
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
    public class ExecutionPlanTests : FakeXrmEasyTestsBase, IQueryExecutionOptions
    {
        private List<JoinOperator> _supportedJoins = new List<JoinOperator>
        {
            JoinOperator.Inner,
            JoinOperator.LeftOuter
        };

        bool IQueryExecutionOptions.Cancelled => false;

        bool IQueryExecutionOptions.BlockUpdateWithoutWhere => false;

        bool IQueryExecutionOptions.BlockDeleteWithoutWhere => false;

        bool IQueryExecutionOptions.UseBulkDelete => false;

        int IQueryExecutionOptions.BatchSize => 1;

        bool IQueryExecutionOptions.UseTDSEndpoint => false;

        bool IQueryExecutionOptions.UseRetrieveTotalRecordCount => true;

        int IQueryExecutionOptions.MaxDegreeOfParallelism => 10;

        bool IQueryExecutionOptions.ColumnComparisonAvailable => true;

        bool IQueryExecutionOptions.UseLocalTimeZone => true;

        List<JoinOperator> IQueryExecutionOptions.JoinOperatorsAvailable => _supportedJoins;

        bool IQueryExecutionOptions.BypassCustomPlugins => false;

        bool IQueryExecutionOptions.ConfirmInsert(int count, EntityMetadata meta)
        {
            return true;
        }

        bool IQueryExecutionOptions.ConfirmDelete(int count, EntityMetadata meta)
        {
            return true;
        }

        bool IQueryExecutionOptions.ConfirmUpdate(int count, EntityMetadata meta)
        {
            return true;
        }

        bool IQueryExecutionOptions.ContinueRetrieve(int count)
        {
            return true;
        }

        void IQueryExecutionOptions.Progress(double? progress, string message)
        {
        }

        void IQueryExecutionOptions.RetrievingNextPage()
        {
        }

        string IQueryExecutionOptions.PrimaryDataSource => "local";

        Guid IQueryExecutionOptions.UserId => Guid.NewGuid();

        bool IQueryExecutionOptions.QuotedIdentifiers => true;

        [TestMethod]
        public void SimpleSelect()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = "SELECT accountid, name FROM account";

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
        public void SimpleSelectStar()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = "SELECT * FROM account";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            CollectionAssert.AreEqual(new[]
            {
                "accountid",
                "createdon",
                "employees",
                "name",
                "ownerid",
                "owneridname",
                "primarycontactid",
                "primarycontactidname",
                "turnover"
            }, select.ColumnSet.Select(col => col.OutputColumn).ToArray());
            var fetch = AssertNode<FetchXmlScan>(select.Source);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='account'>
                        <all-attributes />
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void Join()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = "SELECT accountid, name FROM account INNER JOIN contact ON account.accountid = contact.parentcustomerid";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var fetch = AssertNode<FetchXmlScan>(select.Source);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='contact'>
                        <link-entity name='account' alias='account' from='accountid' to='parentcustomerid' link-type='inner'>
                            <attribute name='accountid' />
                            <attribute name='name' />
                        </link-entity>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void JoinWithExtraCondition()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT
                    accountid,
                    name
                FROM
                    account
                    INNER JOIN contact ON account.accountid = contact.parentcustomerid AND contact.firstname = 'Mark'";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var fetch = AssertNode<FetchXmlScan>(select.Source);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='contact'>
                        <link-entity name='account' alias='account' from='accountid' to='parentcustomerid' link-type='inner'>
                            <attribute name='accountid' />
                            <attribute name='name' />
                        </link-entity>
                        <filter>
                            <condition attribute='firstname' operator='eq' value='Mark' />
                        </filter>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void NonUniqueJoin()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = "SELECT accountid, name FROM account INNER JOIN contact ON account.name = contact.fullname";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var fetch = AssertNode<FetchXmlScan>(select.Source);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='accountid' />
                        <attribute name='name' />
                        <link-entity name='contact' alias='contact' from='fullname' to='name' link-type='inner'>
                        </link-entity>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void NonUniqueJoinExpression()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = "SELECT accountid, name FROM account INNER JOIN contact ON account.name = (contact.firstname + ' ' + contact.lastname)";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var join = AssertNode<HashJoinNode>(select.Source);
            var accountFetch = AssertNode<FetchXmlScan>(join.LeftSource);
            AssertFetchXml(accountFetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='accountid' />
                        <attribute name='name' />
                    </entity>
                </fetch>");
            var contactComputeScalar = AssertNode<ComputeScalarNode>(join.RightSource);
            var contactFetch = AssertNode<FetchXmlScan>(contactComputeScalar.Source);
            AssertFetchXml(contactFetch, @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='firstname' />
                        <attribute name='lastname' />
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void SimpleWhere()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT
                    accountid,
                    name
                FROM
                    account
                WHERE
                    name = 'Data8'";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var fetch = AssertNode<FetchXmlScan>(select.Source);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='accountid' />
                        <attribute name='name' />
                        <filter>
                            <condition attribute='name' operator='eq' value='Data8' />
                        </filter>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void SimpleSort()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT
                    accountid,
                    name
                FROM
                    account
                ORDER BY
                    name ASC";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var fetch = AssertNode<FetchXmlScan>(select.Source);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='accountid' />
                        <attribute name='name' />
                        <order attribute='name' />
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void SimpleSortIndex()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT
                    accountid,
                    name
                FROM
                    account
                ORDER BY
                    2 ASC";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var fetch = AssertNode<FetchXmlScan>(select.Source);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='accountid' />
                        <attribute name='name' />
                        <order attribute='name' />
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void SimpleDistinct()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT DISTINCT
                    name
                FROM
                    account";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var fetch = AssertNode<FetchXmlScan>(select.Source);
            AssertFetchXml(fetch, @"
                <fetch distinct='true'>
                    <entity name='account'>
                        <attribute name='name' />
                        <order attribute='name' />
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void SimpleTop()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT TOP 10
                    accountid,
                    name
                FROM
                    account";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var fetch = AssertNode<FetchXmlScan>(select.Source);
            AssertFetchXml(fetch, @"
                <fetch top='10'>
                    <entity name='account'>
                        <attribute name='accountid' />
                        <attribute name='name' />
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void SimpleOffset()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT
                    accountid,
                    name
                FROM
                    account
                ORDER BY name
                OFFSET 100 ROWS FETCH NEXT 50 ROWS ONLY";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var fetch = AssertNode<FetchXmlScan>(select.Source);
            AssertFetchXml(fetch, @"
                <fetch count='50' page='3'>
                    <entity name='account'>
                        <attribute name='accountid' />
                        <attribute name='name' />
                        <order attribute='name' />
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void SimpleGroupAggregate()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT
                    name,
                    count(*)
                FROM
                    account
                GROUP BY name";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var tryCatch1 = AssertNode<TryCatchNode>(select.Source);
            var tryCatch2 = AssertNode<TryCatchNode>(tryCatch1.TrySource);
            var aggregateFetch = AssertNode<FetchXmlScan>(tryCatch2.TrySource);
            AssertFetchXml(aggregateFetch, @"
                <fetch aggregate='true'>
                    <entity name='account'>
                        <attribute name='name' groupby='true' alias='name' />
                        <attribute name='accountid' aggregate='count' alias='count' />
                        <order alias='name' />
                    </entity>
                </fetch>");
            var partitionAggregate = AssertNode<PartitionedAggregateNode>(tryCatch2.CatchSource);
            var partitionFetch = AssertNode<FetchXmlScan>(partitionAggregate.Source);
            AssertFetchXml(partitionFetch, @"
                <fetch aggregate='true'>
                    <entity name='account'>
                        <attribute name='name' groupby='true' alias='name' />
                        <attribute name='accountid' aggregate='count' alias='count' />
                        <order alias='name' />
                    </entity>
                </fetch>");
            var aggregate = AssertNode<StreamAggregateNode>(tryCatch1.CatchSource);
            var scalarFetch = AssertNode<FetchXmlScan>(aggregate.Source);
            AssertFetchXml(scalarFetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='name' />
                        <order attribute='name' />
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void AliasedAggregate()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT
                    name,
                    count(*) AS test
                FROM
                    account
                GROUP BY name";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var tryCatch1 = AssertNode<TryCatchNode>(select.Source);
            var tryCatch2 = AssertNode<TryCatchNode>(tryCatch1.TrySource);
            var aggregateFetch = AssertNode<FetchXmlScan>(tryCatch2.TrySource);
            AssertFetchXml(aggregateFetch, @"
                <fetch aggregate='true'>
                    <entity name='account'>
                        <attribute name='name' groupby='true' alias='name' />
                        <attribute name='accountid' aggregate='count' alias='test' />
                        <order alias='name' />
                    </entity>
                </fetch>");
            var partitionAggregate = AssertNode<PartitionedAggregateNode>(tryCatch2.CatchSource);
            var partitionFetch = AssertNode<FetchXmlScan>(partitionAggregate.Source);
            AssertFetchXml(partitionFetch, @"
                <fetch aggregate='true'>
                    <entity name='account'>
                        <attribute name='name' groupby='true' alias='name' />
                        <attribute name='accountid' aggregate='count' alias='test' />
                        <order alias='name' />
                    </entity>
                </fetch>");
            var aggregate = AssertNode<StreamAggregateNode>(tryCatch1.CatchSource);
            var scalarFetch = AssertNode<FetchXmlScan>(aggregate.Source);
            AssertFetchXml(scalarFetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='name' />
                        <order attribute='name' />
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void AliasedGroupingAggregate()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT
                    name AS test,
                    count(*)
                FROM
                    account
                GROUP BY name";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var tryCatch1 = AssertNode<TryCatchNode>(select.Source);
            var tryCatch2 = AssertNode<TryCatchNode>(tryCatch1.TrySource);
            var aggregateFetch = AssertNode<FetchXmlScan>(tryCatch2.TrySource);
            AssertFetchXml(aggregateFetch, @"
                <fetch aggregate='true'>
                    <entity name='account'>
                        <attribute name='name' groupby='true' alias='name' />
                        <attribute name='accountid' aggregate='count' alias='count' />
                        <order alias='name' />
                    </entity>
                </fetch>");
            var partitionAggregate = AssertNode<PartitionedAggregateNode>(tryCatch2.CatchSource);
            var partitionFetch = AssertNode<FetchXmlScan>(partitionAggregate.Source);
            AssertFetchXml(partitionFetch, @"
                <fetch aggregate='true'>
                    <entity name='account'>
                        <attribute name='name' groupby='true' alias='name' />
                        <attribute name='accountid' aggregate='count' alias='count' />
                        <order alias='name' />
                    </entity>
                </fetch>");
            var aggregate = AssertNode<StreamAggregateNode>(tryCatch1.CatchSource);
            var scalarFetch = AssertNode<FetchXmlScan>(aggregate.Source);
            AssertFetchXml(scalarFetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='name' />
                        <order attribute='name' />
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void SimpleAlias()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = "SELECT accountid, name AS test FROM account";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var fetch = AssertNode<FetchXmlScan>(select.Source);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='accountid' />
                        <attribute name='name' alias='test' />
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void SimpleHaving()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT
                    name,
                    count(*)
                FROM
                    account
                GROUP BY name
                HAVING
                    count(*) > 1";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            Assert.AreEqual("account.name", select.ColumnSet[0].SourceColumn);
            Assert.AreEqual("count", select.ColumnSet[1].SourceColumn);
            var filter = AssertNode<FilterNode>(select.Source);
            Assert.IsInstanceOfType(filter.Filter, typeof(BooleanComparisonExpression));
            var gt = (BooleanComparisonExpression)filter.Filter;
            Assert.IsInstanceOfType(gt.FirstExpression, typeof(ColumnReferenceExpression));
            var col = (ColumnReferenceExpression)gt.FirstExpression;
            Assert.AreEqual("count", col.MultiPartIdentifier.Identifiers.Single().Value);
            Assert.AreEqual(BooleanComparisonType.GreaterThan, gt.ComparisonType);
            Assert.IsInstanceOfType(gt.SecondExpression, typeof(IntegerLiteral));
            var val = (IntegerLiteral)gt.SecondExpression;
            Assert.AreEqual("1", val.Value);
            var tryCatch1 = AssertNode<TryCatchNode>(filter.Source);
            var tryCatch2 = AssertNode<TryCatchNode>(tryCatch1.TrySource);
            var aggregateFetch = AssertNode<FetchXmlScan>(tryCatch2.TrySource);
            AssertFetchXml(aggregateFetch, @"
                <fetch aggregate='true'>
                    <entity name='account'>
                        <attribute name='name' groupby='true' alias='name' />
                        <attribute name='accountid' aggregate='count' alias='count' />
                        <order alias='name' />
                    </entity>
                </fetch>");
            var partitionAggregate = AssertNode<PartitionedAggregateNode>(tryCatch2.CatchSource);
            var partitionFetch = AssertNode<FetchXmlScan>(partitionAggregate.Source);
            AssertFetchXml(partitionFetch, @"
                <fetch aggregate='true'>
                    <entity name='account'>
                        <attribute name='name' groupby='true' alias='name' />
                        <attribute name='accountid' aggregate='count' alias='count' />
                        <order alias='name' />
                    </entity>
                </fetch>");
            var aggregate = AssertNode<StreamAggregateNode>(tryCatch1.CatchSource);
            Assert.AreEqual("account.name", aggregate.GroupBy[0].ToSql());
            Assert.AreEqual("count", aggregate.Aggregates.Single().Key);
            var scalarFetch = AssertNode<FetchXmlScan>(aggregate.Source);
            AssertFetchXml(scalarFetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='name' />
                        <order attribute='name' />
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void GroupByDatePart()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT
                    DATEPART(month, createdon),
                    count(*)
                FROM
                    account
                GROUP BY DATEPART(month, createdon)";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            Assert.AreEqual("createdon_month", select.ColumnSet[0].SourceColumn);
            Assert.AreEqual("count", select.ColumnSet[1].SourceColumn);
            var tryCatch1 = AssertNode<TryCatchNode>(select.Source);
            var tryCatch2 = AssertNode<TryCatchNode>(tryCatch1.TrySource);
            var aggregateFetch = AssertNode<FetchXmlScan>(tryCatch2.TrySource);
            AssertFetchXml(aggregateFetch, @"
                <fetch aggregate='true'>
                    <entity name='account'>
                        <attribute name='createdon' groupby='true' alias='createdon_month' dategrouping='month' />
                        <attribute name='accountid' aggregate='count' alias='count' />
                        <order alias='createdon_month' />
                    </entity>
                </fetch>");
            var partitionAggregate = AssertNode<PartitionedAggregateNode>(tryCatch2.CatchSource);
            Assert.AreEqual("createdon_month", partitionAggregate.GroupBy[0].ToSql());
            Assert.AreEqual("count", partitionAggregate.Aggregates.Single().Key);
            var partitionFetch = AssertNode<FetchXmlScan>(partitionAggregate.Source);
            AssertFetchXml(partitionFetch, @"
                <fetch aggregate='true'>
                    <entity name='account'>
                        <attribute name='createdon' groupby='true' alias='createdon_month' dategrouping='month' />
                        <attribute name='accountid' aggregate='count' alias='count' />
                        <order alias='createdon_month' />
                    </entity>
                </fetch>");
            var aggregate = AssertNode<HashMatchAggregateNode>(tryCatch1.CatchSource);
            Assert.AreEqual("createdon_month", aggregate.GroupBy[0].ToSql());
            Assert.AreEqual("count", aggregate.Aggregates.Single().Key);
            var computeScalar = AssertNode<ComputeScalarNode>(aggregate.Source);
            Assert.AreEqual(1, computeScalar.Columns.Count);
            Assert.AreEqual("DATEPART(month, createdon)", computeScalar.Columns["createdon_month"].ToSql());
            var scalarFetch = AssertNode<FetchXmlScan>(computeScalar.Source);
            AssertFetchXml(scalarFetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='createdon' />
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void PartialOrdering()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT
                    name,
                    firstname
                FROM
                    account
                    INNER JOIN contact ON account.accountid = contact.parentcustomerid
                ORDER BY
                    firstname,
                    name,
                    accountid";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var order = AssertNode<SortNode>(select.Source);
            var fetch = AssertNode<FetchXmlScan>(order.Source);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='firstname' />
                        <link-entity name='account' alias='account' from='accountid' to='parentcustomerid' link-type='inner'>
                            <attribute name='name' />
                            <attribute name='accountid' />
                        </link-entity>
                        <order attribute='firstname' />
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void PartialOrderingAvoidingLegacyPagingWithTop()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT TOP 100
                    name,
                    firstname
                FROM
                    account
                    INNER JOIN contact ON account.accountid = contact.parentcustomerid
                ORDER BY
                    firstname,
                    name,
                    accountid";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var top = AssertNode<TopNode>(select.Source);
            var order = AssertNode<SortNode>(top.Source);
            var fetch = AssertNode<FetchXmlScan>(order.Source);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='firstname' />
                        <link-entity name='account' alias='account' from='accountid' to='parentcustomerid' link-type='inner'>
                            <attribute name='name' />
                            <attribute name='accountid' />
                            <order attribute='name' />
                        </link-entity>
                        <order attribute='firstname' />
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void PartialWhere()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT
                    accountid,
                    name
                FROM
                    account
                WHERE
                    name = 'Data8'
                    and (turnover + employees) = 100";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var filter = AssertNode<FilterNode>(select.Source);
            var fetch = AssertNode<FetchXmlScan>(filter.Source);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='accountid' />
                        <attribute name='name' />
                        <attribute name='turnover' />
                        <attribute name='employees' />
                        <filter>
                            <condition attribute='name' operator='eq' value='Data8' />
                        </filter>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void RetrieveTotalRecordCountRequest()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT count(*) FROM account";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var computeScalar = AssertNode<ComputeScalarNode>(select.Source);
            Assert.AreEqual(1, computeScalar.Columns.Count);
            Assert.AreEqual("account_count", computeScalar.Columns["count"].ToSql());
            var count = AssertNode<RetrieveTotalRecordCountNode>(computeScalar.Source);
            Assert.AreEqual("account", count.EntityName);
        }

        [TestMethod]
        public void ComputeScalarSelect()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT firstname + ' ' + lastname AS fullname FROM contact WHERE firstname = 'Mark'";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var computeScalar = AssertNode<ComputeScalarNode>(select.Source);
            Assert.AreEqual(1, computeScalar.Columns.Count);
            Assert.AreEqual("firstname + ' ' + lastname", computeScalar.Columns["Expr1"].ToSql());
            var fetch = AssertNode<FetchXmlScan>(computeScalar.Source);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='firstname' />
                        <attribute name='lastname' />
                        <filter>
                            <condition attribute='firstname' operator='eq' value='Mark' />
                        </filter>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void ComputeScalarFilter()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT contactid FROM contact WHERE firstname + ' ' + lastname = 'Mark Carrington'";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var filter = AssertNode<FilterNode>(select.Source);
            Assert.AreEqual("firstname + ' ' + lastname = 'Mark Carrington'", filter.Filter.ToSql());
            var fetch = AssertNode<FetchXmlScan>(filter.Source);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='contactid' />
                        <attribute name='firstname' />
                        <attribute name='lastname' />
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void SelectSubqueryWithMergeJoin()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT firstname + ' ' + lastname AS fullname, 'Account: ' + (SELECT name FROM account WHERE accountid = parentcustomerid) AS accountname FROM contact WHERE firstname = 'Mark'";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var computeScalar = AssertNode<ComputeScalarNode>(select.Source);
            Assert.AreEqual(2, computeScalar.Columns.Count);
            Assert.AreEqual("firstname + ' ' + lastname", computeScalar.Columns[select.ColumnSet[0].SourceColumn].ToSql());
            Assert.AreEqual("'Account: ' + Expr3.name", computeScalar.Columns[select.ColumnSet[1].SourceColumn].ToSql());
            var fetch = AssertNode<FetchXmlScan>(computeScalar.Source);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='firstname' />
                        <attribute name='lastname' />
                        <link-entity name='account' alias='Expr3' from='accountid' to='parentcustomerid' link-type='outer'>
                            <attribute name='name' />
                        </link-entity>
                        <filter>
                            <condition attribute='firstname' operator='eq' value='Mark' />
                        </filter>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void SelectSubqueryWithNestedLoop()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT firstname + ' ' + lastname AS fullname, 'Account: ' + (SELECT name FROM account WHERE createdon = contact.createdon) AS accountname FROM contact WHERE firstname = 'Mark'";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var computeScalar = AssertNode<ComputeScalarNode>(select.Source);
            Assert.AreEqual(2, computeScalar.Columns.Count);
            Assert.AreEqual("firstname + ' ' + lastname", computeScalar.Columns[select.ColumnSet[0].SourceColumn].ToSql());
            Assert.AreEqual("'Account: ' + Expr3", computeScalar.Columns[select.ColumnSet[1].SourceColumn].ToSql());
            var nestedLoop = AssertNode<NestedLoopNode>(computeScalar.Source);
            Assert.AreEqual("@Expr2", nestedLoop.OuterReferences["contact.createdon"]);
            var fetch = AssertNode<FetchXmlScan>(nestedLoop.LeftSource);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='firstname' />
                        <attribute name='lastname' />
                        <attribute name='createdon' />
                        <filter>
                            <condition attribute='firstname' operator='eq' value='Mark' />
                        </filter>
                    </entity>
                </fetch>");
            var subAssert = AssertNode<AssertNode>(nestedLoop.RightSource);
            var subAggregate = AssertNode<StreamAggregateNode>(subAssert.Source);
            var subIndexSpool = AssertNode<IndexSpoolNode>(subAggregate.Source);
            Assert.AreEqual("account.createdon", subIndexSpool.KeyColumn);
            Assert.AreEqual("@Expr2", subIndexSpool.SeekValue);
            var subAggregateFetch = AssertNode<FetchXmlScan>(subIndexSpool.Source);
            AssertFetchXml(subAggregateFetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='name' />
                        <attribute name='createdon' />
                        <filter>
                            <condition attribute='createdon' operator='not-null' />
                        </filter>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void SelectSubqueryWithSmallNestedLoop()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT TOP 10 firstname + ' ' + lastname AS fullname, 'Account: ' + (SELECT name FROM account WHERE createdon = contact.createdon) AS accountname FROM contact WHERE firstname = 'Mark'";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var computeScalar = AssertNode<ComputeScalarNode>(select.Source);
            Assert.AreEqual(2, computeScalar.Columns.Count);
            Assert.AreEqual("firstname + ' ' + lastname", computeScalar.Columns[select.ColumnSet[0].SourceColumn].ToSql());
            Assert.AreEqual("'Account: ' + Expr3", computeScalar.Columns[select.ColumnSet[1].SourceColumn].ToSql());
            var nestedLoop = AssertNode<NestedLoopNode>(computeScalar.Source);
            Assert.AreEqual("@Expr2", nestedLoop.OuterReferences["contact.createdon"]);
            var fetch = AssertNode<FetchXmlScan>(nestedLoop.LeftSource);
            AssertFetchXml(fetch, @"
                <fetch top='10'>
                    <entity name='contact'>
                        <attribute name='firstname' />
                        <attribute name='lastname' />
                        <attribute name='createdon' />
                        <filter>
                            <condition attribute='firstname' operator='eq' value='Mark' />
                        </filter>
                    </entity>
                </fetch>");
            var subAssert = AssertNode<AssertNode>(nestedLoop.RightSource);
            var subAggregate = AssertNode<StreamAggregateNode>(subAssert.Source);
            var subAggregateFetch = AssertNode<FetchXmlScan>(subAggregate.Source);
            AssertFetchXml(subAggregateFetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='name' />
                        <filter>
                            <condition attribute='createdon' operator='eq' value='@Expr2' />
                        </filter>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void SelectSubqueryWithNonCorrelatedNestedLoop()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT firstname + ' ' + lastname AS fullname, 'Account: ' + (SELECT TOP 1 name FROM account) AS accountname FROM contact WHERE firstname = 'Mark'";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var computeScalar = AssertNode<ComputeScalarNode>(select.Source);
            Assert.AreEqual(2, computeScalar.Columns.Count);
            Assert.AreEqual("firstname + ' ' + lastname", computeScalar.Columns[select.ColumnSet[0].SourceColumn].ToSql());
            Assert.AreEqual("'Account: ' + Expr2", computeScalar.Columns[select.ColumnSet[1].SourceColumn].ToSql());
            var nestedLoop = AssertNode<NestedLoopNode>(computeScalar.Source);
            Assert.AreEqual(QualifiedJoinType.LeftOuter, nestedLoop.JoinType);
            Assert.IsTrue(nestedLoop.SemiJoin);
            Assert.AreEqual("account.name", nestedLoop.DefinedValues["Expr2"]);
            var fetch = AssertNode<FetchXmlScan>(nestedLoop.LeftSource);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='firstname' />
                        <attribute name='lastname' />
                        <filter>
                            <condition attribute='firstname' operator='eq' value='Mark' />
                        </filter>
                    </entity>
                </fetch>");
            var subSpool = AssertNode<TableSpoolNode>(nestedLoop.RightSource);
            var subFetch = AssertNode<FetchXmlScan>(subSpool.Source);
            AssertFetchXml(subFetch, @"
                <fetch top='1'>
                    <entity name='account'>
                        <attribute name='name' />
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void SelectSubqueryWithCorrelatedSpooledNestedLoop()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT firstname + ' ' + lastname AS fullname, 'Account: ' + (SELECT name FROM account WHERE createdon = contact.createdon) AS accountname FROM contact WHERE firstname = 'Mark'";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var computeScalar = AssertNode<ComputeScalarNode>(select.Source);
            Assert.AreEqual(2, computeScalar.Columns.Count);
            Assert.AreEqual("firstname + ' ' + lastname", computeScalar.Columns[select.ColumnSet[0].SourceColumn].ToSql());
            Assert.AreEqual("'Account: ' + Expr3", computeScalar.Columns[select.ColumnSet[1].SourceColumn].ToSql());
            var nestedLoop = AssertNode<NestedLoopNode>(computeScalar.Source);
            Assert.AreEqual(QualifiedJoinType.LeftOuter, nestedLoop.JoinType);
            Assert.IsTrue(nestedLoop.SemiJoin);
            var fetch = AssertNode<FetchXmlScan>(nestedLoop.LeftSource);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='firstname' />
                        <attribute name='lastname' />
                        <attribute name='createdon' />
                        <filter>
                            <condition attribute='firstname' operator='eq' value='Mark' />
                        </filter>
                    </entity>
                </fetch>");
            var subAssert = AssertNode<AssertNode>(nestedLoop.RightSource);
            var subAggregate = AssertNode<StreamAggregateNode>(subAssert.Source);
            var subIndexSpool = AssertNode<IndexSpoolNode>(subAggregate.Source);
            Assert.AreEqual("account.createdon", subIndexSpool.KeyColumn);
            Assert.AreEqual("@Expr2", subIndexSpool.SeekValue);
            var subFetch = AssertNode<FetchXmlScan>(subIndexSpool.Source);
            AssertFetchXml(subFetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='name' />
                        <attribute name='createdon' />
                        <filter>
                            <condition attribute='createdon' operator='not-null' />
                        </filter>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void SelectSubqueryWithPartiallyCorrelatedSpooledNestedLoop()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT firstname + ' ' + lastname AS fullname, 'Account: ' + (SELECT name FROM account WHERE createdon = contact.createdon AND employees > 10) AS accountname FROM contact WHERE firstname = 'Mark'";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var computeScalar = AssertNode<ComputeScalarNode>(select.Source);
            Assert.AreEqual(2, computeScalar.Columns.Count);
            Assert.AreEqual("firstname + ' ' + lastname", computeScalar.Columns[select.ColumnSet[0].SourceColumn].ToSql());
            Assert.AreEqual("'Account: ' + Expr3", computeScalar.Columns[select.ColumnSet[1].SourceColumn].ToSql());
            var nestedLoop = AssertNode<NestedLoopNode>(computeScalar.Source);
            Assert.AreEqual(QualifiedJoinType.LeftOuter, nestedLoop.JoinType);
            Assert.IsTrue(nestedLoop.SemiJoin);
            var fetch = AssertNode<FetchXmlScan>(nestedLoop.LeftSource);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='firstname' />
                        <attribute name='lastname' />
                        <attribute name='createdon' />
                        <filter>
                            <condition attribute='firstname' operator='eq' value='Mark' />
                        </filter>
                    </entity>
                </fetch>");
            var subAssert = AssertNode<AssertNode>(nestedLoop.RightSource);
            var subAggregate = AssertNode<StreamAggregateNode>(subAssert.Source);
            var subIndexSpool = AssertNode<IndexSpoolNode>(subAggregate.Source);
            Assert.AreEqual("account.createdon", subIndexSpool.KeyColumn);
            Assert.AreEqual("@Expr2", subIndexSpool.SeekValue);
            var subFetch = AssertNode<FetchXmlScan>(subIndexSpool.Source);
            AssertFetchXml(subFetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='name' />
                        <attribute name='createdon' />
                        <filter>
                            <condition attribute='employees' operator='gt' value='10' />
                        </filter>
                        <filter>
                            <condition attribute='createdon' operator='not-null' />
                        </filter>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void SelectSubqueryUsingOuterReferenceInSelectClause()
        {
            var metadata = new AttributeMetadataCache(_service);
            var tableSize = new StubTableSizeCache();
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT firstname + ' ' + lastname AS fullname, 'Account: ' + (SELECT firstname + ' ' + name FROM account WHERE accountid = parentcustomerid) AS accountname FROM contact WHERE firstname = 'Mark'";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var computeScalar = AssertNode<ComputeScalarNode>(select.Source);
            Assert.AreEqual(2, computeScalar.Columns.Count);
            Assert.AreEqual("firstname + ' ' + lastname", computeScalar.Columns[select.ColumnSet[0].SourceColumn].ToSql());
            Assert.AreEqual("'Account: ' + Expr5", computeScalar.Columns[select.ColumnSet[1].SourceColumn].ToSql());
            var nestedLoop = AssertNode<NestedLoopNode>(computeScalar.Source);
            Assert.AreEqual("@Expr2", nestedLoop.OuterReferences["contact.parentcustomerid"]);
            Assert.AreEqual("@Expr3", nestedLoop.OuterReferences["contact.firstname"]);
            var fetch = AssertNode<FetchXmlScan>(nestedLoop.LeftSource);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='firstname' />
                        <attribute name='lastname' />
                        <attribute name='parentcustomerid' />
                        <filter>
                            <condition attribute='firstname' operator='eq' value='Mark' />
                        </filter>
                    </entity>
                </fetch>");
            var subAssert = AssertNode<AssertNode>(nestedLoop.RightSource);
            var subAggregate = AssertNode<StreamAggregateNode>(subAssert.Source);
            var subCompute = AssertNode<ComputeScalarNode>(subAggregate.Source);
            var subIndexSpool = AssertNode<IndexSpoolNode>(subCompute.Source);
            Assert.AreEqual("account.accountid", subIndexSpool.KeyColumn);
            Assert.AreEqual("@Expr2", subIndexSpool.SeekValue);
            var subAggregateFetch = AssertNode<FetchXmlScan>(subIndexSpool.Source);
            AssertFetchXml(subAggregateFetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='name' />
                        <attribute name='accountid' />
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void SelectSubqueryUsingOuterReferenceInOrderByClause()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT firstname FROM contact ORDER BY (SELECT TOP 1 name FROM account WHERE accountid = parentcustomerid ORDER BY firstname)";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var sort = AssertNode<SortNode>(select.Source);
            var nestedLoop = AssertNode<NestedLoopNode>(sort.Source);
            Assert.AreEqual("@Expr1", nestedLoop.OuterReferences["contact.parentcustomerid"]);
            Assert.AreEqual("@Expr2", nestedLoop.OuterReferences["contact.firstname"]);
            var fetch = AssertNode<FetchXmlScan>(nestedLoop.LeftSource);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='firstname' />
                        <attribute name='parentcustomerid' />
                    </entity>
                </fetch>");
            var subTop = AssertNode<TopNode>(nestedLoop.RightSource);
            var subSort = AssertNode<SortNode>(subTop.Source);
            var subIndexSpool = AssertNode<IndexSpoolNode>(subSort.Source);
            Assert.AreEqual("account.accountid", subIndexSpool.KeyColumn);
            Assert.AreEqual("@Expr1", subIndexSpool.SeekValue);
            var subAggregateFetch = AssertNode<FetchXmlScan>(subIndexSpool.Source);
            AssertFetchXml(subAggregateFetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='name' />
                        <attribute name='accountid' />
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void WhereSubquery()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT firstname + ' ' + lastname AS fullname FROM contact WHERE (SELECT name FROM account WHERE accountid = parentcustomerid) = 'Data8'";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var computeScalar = AssertNode<ComputeScalarNode>(select.Source);
            Assert.AreEqual(1, computeScalar.Columns.Count);
            Assert.AreEqual("firstname + ' ' + lastname", computeScalar.Columns[select.ColumnSet[0].SourceColumn].ToSql());
            var fetch = AssertNode<FetchXmlScan>(computeScalar.Source);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='firstname' />
                        <attribute name='lastname' />
                        <link-entity name='account' alias='Expr2' from='accountid' to='parentcustomerid' link-type='outer'>
                            <attribute name='name' />
                        </link-entity>
                        <filter>
                            <condition entityname='Expr2' attribute='name' operator='eq' value='Data8' />
                        </filter>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void ComputeScalarDistinct()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT DISTINCT TOP 10
                    name + '1'
                FROM
                    account";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var top = AssertNode<TopNode>(select.Source);
            Assert.AreEqual("10", top.Top.ToSql());
            var distinct = AssertNode<DistinctNode>(top.Source);
            CollectionAssert.AreEqual(new[] { "Expr1" }, distinct.Columns);
            var computeScalar = AssertNode<ComputeScalarNode>(distinct.Source);
            var fetch = AssertNode<FetchXmlScan>(computeScalar.Source);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='name' />
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void UnionAll()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT name FROM account
                UNION ALL
                SELECT fullname FROM contact";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            Assert.AreEqual(1, select.ColumnSet.Count);
            Assert.AreEqual("name", select.ColumnSet[0].OutputColumn);
            Assert.AreEqual("Expr1", select.ColumnSet[0].SourceColumn);
            var concat = AssertNode<ConcatenateNode>(select.Source);
            Assert.AreEqual(2, concat.Sources.Count);
            Assert.AreEqual("Expr1", concat.ColumnSet[0].OutputColumn);
            Assert.AreEqual("account.name", concat.ColumnSet[0].SourceColumns[0]);
            Assert.AreEqual("contact.fullname", concat.ColumnSet[0].SourceColumns[1]);
            var accountFetch = AssertNode<FetchXmlScan>(concat.Sources[0]);
            AssertFetchXml(accountFetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='name' />
                    </entity>
                </fetch>");
            var contactFetch = AssertNode<FetchXmlScan>(concat.Sources[1]);
            AssertFetchXml(contactFetch, @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='fullname' />
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void SimpleInFilter()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT
                    accountid,
                    name
                FROM
                    account
                WHERE
                    name in ('Data8', 'Mark Carrington')";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var fetch = AssertNode<FetchXmlScan>(select.Source);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='accountid' />
                        <attribute name='name' />
                        <filter>
                            <condition attribute='name' operator='in'>
                                <value>Data8</value>
                                <value>Mark Carrington</value>
                            </condition>
                        </filter>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void SubqueryInFilterUncorrelated()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT
                    accountid,
                    name
                FROM
                    account
                WHERE
                    name in (SELECT firstname FROM contact)";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var filter = AssertNode<FilterNode>(select.Source);
            var hashJoin = AssertNode<HashJoinNode>(filter.Source);
            var fetch = AssertNode<FetchXmlScan>(hashJoin.LeftSource);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='accountid' />
                        <attribute name='name' />
                    </entity>
                </fetch>");
            var subFetch = AssertNode<FetchXmlScan>(hashJoin.RightSource);
            AssertFetchXml(subFetch, @"
                <fetch distinct='true'>
                    <entity name='contact'>
                        <attribute name='firstname' />
                        <order attribute='firstname' />
                    </entity>
                </fetch>");
        }

        [TestMethod]
        [ExpectedException(typeof(NotSupportedQueryFragmentException))]
        public void SubqueryInFilterMultipleColumnsError()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT
                    accountid,
                    name
                FROM
                    account
                WHERE
                    name in (SELECT firstname, lastname FROM contact)";

            planBuilder.Build(query, null, out _);
        }

        [TestMethod]
        public void SubqueryInFilterUncorrelatedPrimaryKey()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT
                    accountid,
                    name
                FROM
                    account
                WHERE
                    primarycontactid in (SELECT contactid FROM contact WHERE firstname = 'Mark')";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var fetch = AssertNode<FetchXmlScan>(select.Source);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='accountid' />
                        <attribute name='name' />
                        <link-entity name='contact' alias='Expr1' from='contactid' to='primarycontactid' link-type='outer'>
                            <filter>
                                <condition attribute='firstname' operator='eq' value='Mark' />
                            </filter>
                        </link-entity>
                        <filter>
                            <condition entityname='Expr1' attribute='contactid' operator='not-null' />
                        </filter>
                    </entity>
                </fetch>
            ");
        }

        [TestMethod]
        public void SubqueryInFilterCorrelated()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT
                    accountid,
                    name
                FROM
                    account
                WHERE
                    name in (SELECT firstname FROM contact WHERE parentcustomerid = accountid)";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var filter = AssertNode<FilterNode>(select.Source);
            Assert.AreEqual("Expr2 IS NOT NULL", filter.Filter.ToSql());
            var nestedLoop = AssertNode<NestedLoopNode>(filter.Source);
            Assert.AreEqual(QualifiedJoinType.LeftOuter, nestedLoop.JoinType);
            Assert.IsTrue(nestedLoop.SemiJoin);
            var fetch = AssertNode<FetchXmlScan>(nestedLoop.LeftSource);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='accountid' />
                        <attribute name='name' />
                    </entity>
                </fetch>");
            var subIndexSpool = AssertNode<IndexSpoolNode>(nestedLoop.RightSource);
            Assert.AreEqual("contact.parentcustomerid", subIndexSpool.KeyColumn);
            Assert.AreEqual("@Expr1", subIndexSpool.SeekValue);
            var subFetch = AssertNode<FetchXmlScan>(subIndexSpool.Source);
            AssertFetchXml(subFetch, @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='firstname' />
                        <attribute name='parentcustomerid' />
                        <filter>
                            <condition attribute='parentcustomerid' operator='not-null' />
                        </filter>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void SubqueryNotInFilterCorrelated()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT
                    accountid,
                    name
                FROM
                    account
                WHERE
                    name not in (SELECT firstname FROM contact WHERE parentcustomerid = accountid)";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var filter = AssertNode<FilterNode>(select.Source);
            Assert.AreEqual("Expr2 IS NULL", filter.Filter.ToSql());
            var nestedLoop = AssertNode<NestedLoopNode>(filter.Source);
            Assert.AreEqual(QualifiedJoinType.LeftOuter, nestedLoop.JoinType);
            Assert.IsTrue(nestedLoop.SemiJoin);
            var fetch = AssertNode<FetchXmlScan>(nestedLoop.LeftSource);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='accountid' />
                        <attribute name='name' />
                    </entity>
                </fetch>");
            var subIndexSpool = AssertNode<IndexSpoolNode>(nestedLoop.RightSource);
            Assert.AreEqual("contact.parentcustomerid", subIndexSpool.KeyColumn);
            Assert.AreEqual("@Expr1", subIndexSpool.SeekValue);
            var subFetch = AssertNode<FetchXmlScan>(subIndexSpool.Source);
            AssertFetchXml(subFetch, @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='firstname' />
                        <attribute name='parentcustomerid' />
                        <filter>
                            <condition attribute='parentcustomerid' operator='not-null' />
                        </filter>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void ExistsFilterUncorrelated()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT
                    accountid,
                    name
                FROM
                    account
                WHERE
                    EXISTS (SELECT * FROM contact)";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var filter = AssertNode<FilterNode>(select.Source);
            var loop = AssertNode<NestedLoopNode>(filter.Source);
            var fetch = AssertNode<FetchXmlScan>(loop.LeftSource);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='accountid' />
                        <attribute name='name' />
                    </entity>
                </fetch>");
            var subSpool = AssertNode<TableSpoolNode>(loop.RightSource);
            var subFetch = AssertNode<FetchXmlScan>(subSpool.Source);
            AssertFetchXml(subFetch, @"
                <fetch top='1'>
                    <entity name='contact'>
                        <attribute name='contactid' />
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void ExistsFilterCorrelatedPrimaryKey()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT
                    accountid,
                    name
                FROM
                    account
                WHERE
                    EXISTS (SELECT * FROM contact WHERE contactid = primarycontactid)";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var fetch = AssertNode<FetchXmlScan>(select.Source);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='accountid' />
                        <attribute name='name' />
                        <link-entity name='contact' alias='Expr2' from='contactid' to='primarycontactid' link-type='outer'>
                        </link-entity>
                        <filter>
                            <condition entityname='Expr2' attribute='contactid' operator='not-null' />
                        </filter>
                    </entity>
                </fetch>
            ");
        }

        [TestMethod]
        public void ExistsFilterCorrelated()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT
                    accountid,
                    name
                FROM
                    account
                WHERE
                    EXISTS (SELECT * FROM contact WHERE parentcustomerid = accountid)";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var filter = AssertNode<FilterNode>(select.Source);
            Assert.AreEqual("Expr3 IS NOT NULL", filter.Filter.ToSql());
            var join = AssertNode<MergeJoinNode>(filter.Source);
            Assert.AreEqual(QualifiedJoinType.LeftOuter, join.JoinType);
            Assert.IsTrue(join.SemiJoin);
            var fetch = AssertNode<FetchXmlScan>(join.LeftSource);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='accountid' />
                        <attribute name='name' />
                        <order attribute='accountid' />
                    </entity>
                </fetch>");
            var sort = AssertNode<SortNode>(join.RightSource);
            Assert.AreEqual("Expr2.parentcustomerid ASC", sort.Sorts[0].ToSql());
            var subFetch = AssertNode<FetchXmlScan>(sort.Source);
            AssertFetchXml(subFetch, @"
                <fetch distinct='true'>
                    <entity name='contact'>
                        <attribute name='parentcustomerid' />
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void NotExistsFilterCorrelated()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT
                    accountid,
                    name
                FROM
                    account
                WHERE
                    NOT EXISTS (SELECT * FROM contact WHERE parentcustomerid = accountid)";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var filter = AssertNode<FilterNode>(select.Source);
            Assert.AreEqual("NOT Expr3 IS NOT NULL", filter.Filter.ToSql());
            var join = AssertNode<MergeJoinNode>(filter.Source);
            Assert.AreEqual(QualifiedJoinType.LeftOuter, join.JoinType);
            Assert.IsTrue(join.SemiJoin);
            var fetch = AssertNode<FetchXmlScan>(join.LeftSource);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='accountid' />
                        <attribute name='name' />
                        <order attribute='accountid' />
                    </entity>
                </fetch>");
            var sort = AssertNode<SortNode>(join.RightSource);
            Assert.AreEqual("Expr2.parentcustomerid ASC", sort.Sorts[0].ToSql());
            var subFetch = AssertNode<FetchXmlScan>(sort.Source);
            Assert.AreEqual("Expr2", subFetch.Alias);
            AssertFetchXml(subFetch, @"
                <fetch distinct='true'>
                    <entity name='contact'>
                        <attribute name='parentcustomerid' />
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void QueryDerivedTableSimple()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT TOP 10
                    name
                FROM
                    (SELECT accountid, name FROM account) a
                WHERE
                    name = 'Data8'";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var fetch = AssertNode<FetchXmlScan>(select.Source);
            AssertFetchXml(fetch, @"
                <fetch top='10'>
                    <entity name='account'>
                        <attribute name='accountid' />
                        <attribute name='name' />
                        <filter>
                            <condition attribute='name' operator='eq' value='Data8' />
                        </filter>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void QueryDerivedTableAlias()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT TOP 10
                    a.accountname
                FROM
                    (SELECT accountid, name AS accountname FROM account) a
                WHERE
                    a.accountname = 'Data8'";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var fetch = AssertNode<FetchXmlScan>(select.Source);
            AssertFetchXml(fetch, @"
                <fetch top='10'>
                    <entity name='account'>
                        <attribute name='accountid' />
                        <attribute name='name' alias='accountname' />
                        <filter>
                            <condition attribute='name' operator='eq' value='Data8' />
                        </filter>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void QueryDerivedTableValues()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT TOP 10
                    name
                FROM
                    (VALUES (1, 'Data8')) a (ID, name)
                WHERE
                    name = 'Data8'";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var top = AssertNode<TopNode>(select.Source);
            var filter = AssertNode<FilterNode>(top.Source);
            var constant = AssertNode<ConstantScanNode>(filter.Source);

            var schema = constant.GetSchema(_dataSources, null);
            Assert.AreEqual(typeof(SqlInt32), schema.Schema["a.ID"].ToNetType(out _));
            Assert.AreEqual(typeof(SqlString), schema.Schema["a.name"].ToNetType(out _));
        }

        [TestMethod]
        public void NoLockTableHint()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT TOP 10
                    name
                FROM
                    account (NOLOCK)
                WHERE
                    name = 'Data8'";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var fetch = AssertNode<FetchXmlScan>(select.Source);
            AssertFetchXml(fetch, @"
                <fetch top='10' no-lock='true'>
                    <entity name='account'>
                        <attribute name='name' />
                        <filter>
                            <condition attribute='name' operator='eq' value='Data8' />
                        </filter>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void CrossJoin()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT
                    name,
                    fullname
                FROM
                    account
                    CROSS JOIN
                    contact";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var loop = AssertNode<NestedLoopNode>(select.Source);
            Assert.AreEqual(QualifiedJoinType.Inner, loop.JoinType);
            Assert.IsNull(loop.JoinCondition);
            var outerFetch = AssertNode<FetchXmlScan>(loop.LeftSource);
            AssertFetchXml(outerFetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='name' />
                    </entity>
                </fetch>");
            var innerSpool = AssertNode<TableSpoolNode>(loop.RightSource);
            var innerFetch = AssertNode<FetchXmlScan>(innerSpool.Source);
            AssertFetchXml(innerFetch, @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='fullname' />
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void CrossApply()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT
                    name,
                    firstname,
                    lastname
                FROM
                    account
                    CROSS APPLY
                    (
                        SELECT *
                        FROM   contact
                        WHERE  primarycontactid = contactid
                    ) a";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var fetch = AssertNode<FetchXmlScan>(select.Source);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='name' />
                        <link-entity name='contact' alias='a' from='contactid' to='primarycontactid' link-type='inner'>
                            <attribute name='firstname' />
                            <attribute name='lastname' />
                        </link-entity>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void OuterApply()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT
                    name,
                    firstname,
                    lastname
                FROM
                    account
                    OUTER APPLY
                    (
                        SELECT *
                        FROM   contact
                        WHERE  primarycontactid = contactid
                    ) a";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var fetch = AssertNode<FetchXmlScan>(select.Source);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='name' />
                        <link-entity name='contact' alias='a' from='contactid' to='primarycontactid' link-type='outer'>
                            <attribute name='firstname' />
                            <attribute name='lastname' />
                        </link-entity>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void OuterApplyNestedLoop()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT
                    name,
                    firstname,
                    lastname
                FROM
                    account
                    OUTER APPLY
                    (
                        SELECT TOP 1 *
                        FROM   contact
                        WHERE  parentcustomerid = accountid
                        ORDER BY firstname
                    ) a";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var loop = AssertNode<NestedLoopNode>(select.Source);
            Assert.AreEqual(QualifiedJoinType.LeftOuter, loop.JoinType);
            Assert.IsNull(loop.JoinCondition);
            Assert.AreEqual(1, loop.OuterReferences.Count);
            Assert.AreEqual("@Expr1", loop.OuterReferences["account.accountid"]);
            var outerFetch = AssertNode<FetchXmlScan>(loop.LeftSource);
            AssertFetchXml(outerFetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='name' />
                        <attribute name='accountid' />
                    </entity>
                </fetch>");
            var innerAlias = AssertNode<AliasNode>(loop.RightSource);
            var innerTop = AssertNode<TopNode>(innerAlias.Source);
            var innerSort = AssertNode<SortNode>(innerTop.Source);
            var innerIndexSpool = AssertNode<IndexSpoolNode>(innerSort.Source);
            Assert.AreEqual("contact.parentcustomerid", innerIndexSpool.KeyColumn);
            Assert.AreEqual("@Expr1", innerIndexSpool.SeekValue);
            var innerFetch = AssertNode<FetchXmlScan>(innerIndexSpool.Source);
            AssertFetchXml(innerFetch, @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='firstname' />
                        <attribute name='lastname' />
                        <attribute name='parentcustomerid' />
                        <filter>
                            <condition attribute='parentcustomerid' operator='not-null' />
                        </filter>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void FetchXmlNativeWhere()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT
                    accountid,
                    name
                FROM
                    account
                WHERE
                    createdon = lastxdays(1)";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var fetch = AssertNode<FetchXmlScan>(select.Source);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='accountid' />
                        <attribute name='name' />
                        <filter>
                            <condition attribute='createdon' operator='last-x-days' value='1' />
                        </filter>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void SimpleMetadataSelect()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT logicalname
                FROM   metadata.entity";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var meta = AssertNode<MetadataQueryNode>(select.Source);

            Assert.AreEqual(MetadataSource.Entity, meta.MetadataSource);
            Assert.AreEqual("entity", meta.EntityAlias);
            CollectionAssert.AreEqual(new[] { "LogicalName" }, meta.Query.Properties.PropertyNames);
        }

        [TestMethod]
        public void SimpleMetadataWhere()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT logicalname
                FROM   metadata.entity
                WHERE  objecttypecode = 1";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var meta = AssertNode<MetadataQueryNode>(select.Source);

            Assert.AreEqual(MetadataSource.Entity, meta.MetadataSource);
            Assert.AreEqual("entity", meta.EntityAlias);
            CollectionAssert.AreEqual(new[] { "LogicalName" }, meta.Query.Properties.PropertyNames);
            Assert.AreEqual(1, meta.Query.Criteria.Conditions.Count);
            Assert.AreEqual("ObjectTypeCode", meta.Query.Criteria.Conditions[0].PropertyName);
            Assert.AreEqual(MetadataConditionOperator.Equals, meta.Query.Criteria.Conditions[0].ConditionOperator);
            Assert.AreEqual(1, meta.Query.Criteria.Conditions[0].Value);
        }

        [TestMethod]
        public void SimpleUpdate()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = "UPDATE account SET name = 'foo' WHERE name = 'bar'";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var update = AssertNode<UpdateNode>(plans[0]);
            Assert.AreEqual("account", update.LogicalName);
            Assert.AreEqual("account.accountid", update.PrimaryIdSource);
            Assert.AreEqual("Expr1", update.ColumnMappings["name"]);
            var computeScalar = AssertNode<ComputeScalarNode>(update.Source);
            Assert.AreEqual("'foo'", computeScalar.Columns["Expr1"].ToSql());
            var fetch = AssertNode<FetchXmlScan>(computeScalar.Source);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='accountid' />
                        <filter>
                            <condition attribute='name' operator='eq' value='bar' />
                        </filter>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void UpdateFromJoin()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = "UPDATE a SET name = 'foo' FROM account a INNER JOIN contact c ON a.accountid = c.parentcustomerid WHERE name = 'bar'";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var update = AssertNode<UpdateNode>(plans[0]);
            Assert.AreEqual("account", update.LogicalName);
            Assert.AreEqual("a.accountid", update.PrimaryIdSource);
            Assert.AreEqual("Expr1", update.ColumnMappings["name"]);
            var distinct = AssertNode<DistinctNode>(update.Source);
            var computeScalar = AssertNode<ComputeScalarNode>(distinct.Source);
            Assert.AreEqual("'foo'", computeScalar.Columns["Expr1"].ToSql());
            var fetch = AssertNode<FetchXmlScan>(computeScalar.Source);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='contact'>
                        <link-entity name='account' alias='a' from='accountid' to='parentcustomerid' link-type='inner'>
                            <attribute name='accountid' />
                        </link-entity>
                        <filter>
                            <condition entityname='a' attribute='name' operator='eq' value='bar' />
                        </filter>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void QueryHints()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = "SELECT accountid, name FROM account OPTION (OPTIMIZE FOR UNKNOWN, FORCE ORDER, RECOMPILE, USE HINT('DISABLE_OPTIMIZER_ROWGOAL'), USE HINT('ENABLE_QUERY_OPTIMIZER_HOTFIXES'), LOOP JOIN, MERGE JOIN, HASH JOIN, NO_PERFORMANCE_SPOOL, MAXRECURSION 2)";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var fetch = AssertNode<FetchXmlScan>(select.Source);
            AssertFetchXml(fetch, @"
                <fetch options='OptimizeForUnknown,ForceOrder,Recompile,DisableRowGoal,EnableOptimizerHotfixes,LoopJoin,MergeJoin,HashJoin,NO_PERFORMANCE_SPOOL,MaxRecursion=2'>
                    <entity name='account'>
                        <attribute name='accountid' />
                        <attribute name='name' />
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void AggregateSort()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = "SELECT name, count(*) from account group by name order by 2 desc";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            Assert.AreEqual("account.name", select.ColumnSet[0].SourceColumn);
            Assert.AreEqual("count", select.ColumnSet[1].SourceColumn);
            var tryCatch1 = AssertNode<TryCatchNode>(select.Source);
            var tryCatch2 = AssertNode<TryCatchNode>(tryCatch1.TrySource);
            var aggregateFetch = AssertNode<FetchXmlScan>(tryCatch2.TrySource);
            AssertFetchXml(aggregateFetch, @"
                <fetch aggregate='true'>
                    <entity name='account'>
                        <attribute name='name' groupby='true' alias='name' />
                        <attribute name='accountid' aggregate='count' alias='count' />
                        <order alias='count' descending='true' />
                    </entity>
                </fetch>");
            var partitionSort = AssertNode<SortNode>(tryCatch2.CatchSource);
            Assert.AreEqual("count", partitionSort.Sorts.Single().Expression.ToSql());
            Assert.AreEqual(SortOrder.Descending, partitionSort.Sorts.Single().SortOrder);
            var partitionAggregate = AssertNode<PartitionedAggregateNode>(partitionSort.Source);
            var partitionFetch = AssertNode<FetchXmlScan>(partitionAggregate.Source);
            AssertFetchXml(partitionFetch, @"
                <fetch aggregate='true'>
                    <entity name='account'>
                        <attribute name='name' groupby='true' alias='name' />
                        <attribute name='accountid' aggregate='count' alias='count' />
                        <order alias='name' />
                    </entity>
                </fetch>");
            var sort = AssertNode<SortNode>(tryCatch1.CatchSource);
            Assert.AreEqual("count", sort.Sorts.Single().Expression.ToSql());
            Assert.AreEqual(SortOrder.Descending, sort.Sorts.Single().SortOrder);
            var aggregate = AssertNode<StreamAggregateNode>(sort.Source);
            var fetch = AssertNode<FetchXmlScan>(aggregate.Source);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='name' />
                        <order attribute='name' />
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void FoldFilterWithNonFoldedJoin()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = "SELECT name from account INNER JOIN contact ON left(name, 4) = left(firstname, 4) where name like 'Data8%' and firstname like 'Mark%'";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var join = AssertNode<HashJoinNode>(select.Source);
            var leftCompute = AssertNode<ComputeScalarNode>(join.LeftSource);
            var leftFetch = AssertNode<FetchXmlScan>(leftCompute.Source);
            AssertFetchXml(leftFetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='name' />
                        <filter>
                            <condition attribute='name' operator='like' value='Data8%' />
                        </filter>
                    </entity>
                </fetch>");
            var rightCompute = AssertNode<ComputeScalarNode>(join.RightSource);
            var rightFetch = AssertNode<FetchXmlScan>(rightCompute.Source);
            AssertFetchXml(rightFetch, @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='firstname' />
                        <filter>
                            <condition attribute='firstname' operator='like' value='Mark%' />
                        </filter>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void FoldFilterWithInClause()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = "SELECT name from account where name like 'Data8%' and primarycontactid in (select contactid from contact where firstname = 'Mark')";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var fetch = AssertNode<FetchXmlScan>(select.Source);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='name' />
                        <link-entity name='contact' alias='Expr1' from='contactid' to='primarycontactid' link-type='outer'>
                            <filter>
                                <condition attribute='firstname' operator='eq' value='Mark' />
                            </filter>
                        </link-entity>
                        <filter>
                            <condition attribute='name' operator='like' value='Data8%' />
                            <condition entityname='Expr1' attribute='contactid' operator='not-null' />
                        </filter>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void FoldFilterWithInClauseWithoutPrimaryKey()
        {
            _supportedJoins.Add(JoinOperator.Any);

            try
            {
                var metadata = new AttributeMetadataCache(_service);
                var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

                var query = "SELECT name from account where name like 'Data8%' and createdon in (select createdon from contact where firstname = 'Mark')";

                var plans = planBuilder.Build(query, null, out _);

                Assert.AreEqual(1, plans.Length);

                var select = AssertNode<SelectNode>(plans[0]);
                var fetch = AssertNode<FetchXmlScan>(select.Source);
                AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='name' />
                        <link-entity name='contact' alias='Expr1' from='createdon' to='createdon' link-type='in'>
                            <filter>
                                <condition attribute='firstname' operator='eq' value='Mark' />
                            </filter>
                        </link-entity>
                        <filter>
                            <condition attribute='name' operator='like' value='Data8%' />
                        </filter>
                    </entity>
                </fetch>");
            }
            finally
            {
                _supportedJoins.Remove(JoinOperator.Any);
            }
        }

        [TestMethod]
        public void FoldFilterWithInClauseOnLinkEntityWithoutPrimaryKey()
        {
            _supportedJoins.Add(JoinOperator.Any);

            try
            {
                var metadata = new AttributeMetadataCache(_service);
                var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

                var query = "SELECT name from account inner join contact on account.accountid = contact.parentcustomerid where name like 'Data8%' and contact.createdon in (select createdon from contact where firstname = 'Mark')";

                var plans = planBuilder.Build(query, null, out _);

                Assert.AreEqual(1, plans.Length);

                var select = AssertNode<SelectNode>(plans[0]);
                var fetch = AssertNode<FetchXmlScan>(select.Source);
                AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='contact'>
                        <link-entity name='account' alias='account' from='accountid' to='parentcustomerid' link-type='inner'>
                            <attribute name='name' />
                        </link-entity>
                        <link-entity name='contact' alias='Expr1' from='createdon' to='createdon' link-type='in'>
                            <filter>
                                <condition attribute='firstname' operator='eq' value='Mark' />
                            </filter>
                        </link-entity>
                        <filter>
                            <condition entityname='account' attribute='name' operator='like' value='Data8%' />
                        </filter>
                    </entity>
                </fetch>");
            }
            finally
            {
                _supportedJoins.Remove(JoinOperator.Any);
            }
        }

        [TestMethod]
        public void FoldFilterWithExistsClauseWithoutPrimaryKey()
        {
            _supportedJoins.Add(JoinOperator.Exists);

            try
            {
                var metadata = new AttributeMetadataCache(_service);
                var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

                var query = "SELECT name from account where name like 'Data8%' and exists (select * from contact where firstname = 'Mark' and createdon = account.createdon)";

                var plans = planBuilder.Build(query, null, out _);

                Assert.AreEqual(1, plans.Length);

                var select = AssertNode<SelectNode>(plans[0]);
                var fetch = AssertNode<FetchXmlScan>(select.Source);
                AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='name' />
                        <link-entity name='contact' alias='contact' from='createdon' to='createdon' link-type='exists'>
                            <filter>
                                <condition attribute='firstname' operator='eq' value='Mark' />
                            </filter>
                            <filter>
                                <condition attribute='createdon' operator='not-null' />
                            </filter>
                        </link-entity>
                        <filter>
                            <condition attribute='name' operator='like' value='Data8%' />
                        </filter>
                    </entity>
                </fetch>");
            }
            finally
            {
                _supportedJoins.Remove(JoinOperator.Exists);
            }
        }

        [TestMethod]
        public void DistinctNotRequiredWithPrimaryKey()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = "SELECT DISTINCT accountid, name from account";

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
        public void DistinctRequiredWithoutPrimaryKey()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = "SELECT DISTINCT accountid, name from account INNER JOIN contact ON account.accountid = contact.parentcustomerid";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var fetch = AssertNode<FetchXmlScan>(select.Source);
            AssertFetchXml(fetch, @"
                <fetch distinct='true'>
                    <entity name='contact'>
                        <link-entity name='account' alias='account' from='accountid' to='parentcustomerid' link-type='inner'>
                            <attribute name='accountid' />
                            <attribute name='name' />
                            <order attribute='accountid' />
                            <order attribute='name' />
                        </link-entity>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void SimpleDelete()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = "DELETE FROM account WHERE name = 'bar'";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var delete = AssertNode<DeleteNode>(plans[0]);
            Assert.AreEqual("account", delete.LogicalName);
            Assert.AreEqual("account.accountid", delete.PrimaryIdSource);
            var fetch = AssertNode<FetchXmlScan>(delete.Source);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='accountid' />
                        <filter>
                            <condition attribute='name' operator='eq' value='bar' />
                        </filter>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void SimpleInsertSelect()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = "INSERT INTO account (name) SELECT fullname FROM contact WHERE firstname = 'Mark'";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var insert = AssertNode<InsertNode>(plans[0]);
            Assert.AreEqual("account", insert.LogicalName);
            Assert.AreEqual("contact.fullname", insert.ColumnMappings["name"]);
            var fetch = AssertNode<FetchXmlScan>(insert.Source);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='fullname' />
                        <filter>
                            <condition attribute='firstname' operator='eq' value='Mark' />
                        </filter>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void SelectDuplicateColumnNames()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = "SELECT fullname, lastname + ', ' + firstname as fullname FROM contact WHERE firstname = 'Mark'";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            Assert.AreEqual(2, select.ColumnSet.Count);
            Assert.AreEqual("contact.fullname", select.ColumnSet[0].SourceColumn);
            Assert.AreEqual("fullname", select.ColumnSet[0].OutputColumn);
            Assert.AreEqual("fullname", select.ColumnSet[0].PhysicalOutputColumn);
            Assert.AreEqual("Expr1", select.ColumnSet[1].SourceColumn);
            Assert.AreEqual("fullname", select.ColumnSet[1].OutputColumn);
            Assert.AreEqual("fullname1", select.ColumnSet[1].PhysicalOutputColumn);
            var computeScalar = AssertNode<ComputeScalarNode>(select.Source);
            var fetch = AssertNode<FetchXmlScan>(computeScalar.Source);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='fullname' />
                        <attribute name='lastname' />
                        <attribute name='firstname' />
                        <filter>
                            <condition attribute='firstname' operator='eq' value='Mark' />
                        </filter>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        [ExpectedException(typeof(NotSupportedQueryFragmentException))]
        public void SubQueryDuplicateColumnNamesError()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = "SELECT * FROM (SELECT fullname, lastname + ', ' + firstname as fullname FROM contact WHERE firstname = 'Mark') a";

            planBuilder.Build(query, null, out _);
        }

        [TestMethod]
        public void UnionDuplicateColumnNames()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"SELECT fullname, lastname + ', ' + firstname as fullname FROM contact WHERE firstname = 'Mark'
                          UNION
                          SELECT fullname, lastname + ', ' + firstname as fullname FROM contact WHERE firstname = 'Mark'";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            Assert.AreEqual(2, select.ColumnSet.Count);
            Assert.AreEqual("Expr3", select.ColumnSet[0].SourceColumn);
            Assert.AreEqual("fullname", select.ColumnSet[0].OutputColumn);
            Assert.AreEqual("Expr4", select.ColumnSet[1].SourceColumn);
            Assert.AreEqual("fullname", select.ColumnSet[1].OutputColumn);
            var distinct = AssertNode<DistinctNode>(select.Source);
            var concat = AssertNode<ConcatenateNode>(distinct.Source);
            Assert.AreEqual(2, concat.ColumnSet.Count);
            Assert.AreEqual("contact.fullname", concat.ColumnSet[0].SourceColumns[0]);
            Assert.AreEqual("contact.fullname", concat.ColumnSet[0].SourceColumns[1]);
            Assert.AreEqual("Expr3", concat.ColumnSet[0].OutputColumn);
            Assert.AreEqual("Expr1", concat.ColumnSet[1].SourceColumns[0]);
            Assert.AreEqual("Expr2", concat.ColumnSet[1].SourceColumns[1]);
            Assert.AreEqual("Expr4", concat.ColumnSet[1].OutputColumn);
        }

        [TestMethod]
        [ExpectedException(typeof(NotSupportedQueryFragmentException))]
        public void SubQueryUnionDuplicateColumnNamesError()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);
            var query = @"SELECT * FROM (
                            SELECT fullname, lastname + ', ' + firstname as fullname FROM contact WHERE firstname = 'Mark'
                            UNION
                            SELECT fullname, lastname + ', ' + firstname as fullname FROM contact WHERE firstname = 'Mark'
                          ) a";

            planBuilder.Build(query, null, out _);
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

        [TestMethod]
        public void SelectStarInSubquery()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"SELECT * FROM account WHERE accountid IN (SELECT parentcustomerid FROM contact)";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var filter = AssertNode<FilterNode>(select.Source);
            Assert.AreEqual("Expr2 IS NOT NULL", filter.Filter.ToSql());
            var join = AssertNode<MergeJoinNode>(filter.Source);
            Assert.AreEqual(QualifiedJoinType.LeftOuter, join.JoinType);
            Assert.IsTrue(join.SemiJoin);
            Assert.AreEqual("Expr1.parentcustomerid", join.DefinedValues["Expr2"]);
            var accountFetch = AssertNode<FetchXmlScan>(join.LeftSource);
            Assert.AreEqual("account", accountFetch.Alias);
            AssertFetchXml(accountFetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='accountid' />
                        <attribute name='createdon' />
                        <attribute name='employees' />
                        <attribute name='name' />
                        <attribute name='ownerid' />
                        <attribute name='owneridname' />
                        <attribute name='primarycontactid' />
                        <attribute name='primarycontactidname' />
                        <attribute name='turnover' />
                        <order attribute='accountid' />
                    </entity>
                </fetch>");
            var contactSort = AssertNode<SortNode>(join.RightSource);
            Assert.AreEqual("Expr1.parentcustomerid", contactSort.Sorts[0].Expression.ToSql());
            var contactFetch = AssertNode<FetchXmlScan>(contactSort.Source);
            Assert.AreEqual("Expr1", contactFetch.Alias);
            AssertFetchXml(contactFetch, @"
                <fetch distinct='true'>
                    <entity name='contact'>
                        <attribute name='parentcustomerid' />
                    </entity>
                </fetch>");
        }

        [TestMethod]
        [ExpectedException(typeof(NotSupportedQueryFragmentException))]
        public void CannotSelectColumnsFromSemiJoin()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"SELECT contact.* FROM account WHERE accountid IN (SELECT parentcustomerid FROM contact)";

            planBuilder.Build(query, null, out _);
        }

        [TestMethod]
        public void MinAggregateNotFoldedToFetchXmlForOptionset()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"SELECT new_name, min(new_optionsetvalue) FROM new_customentity GROUP BY new_name";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var aggregate = AssertNode<StreamAggregateNode>(select.Source);
            var fetchXml = AssertNode<FetchXmlScan>(aggregate.Source);

            AssertFetchXml(fetchXml, @"
                <fetch>
                    <entity name='new_customentity'>
                        <attribute name='new_name' />
                        <attribute name='new_optionsetvalue' />
                        <order attribute='new_name' />
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void HelpfulErrorMessageOnMissingGroupBy()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"SELECT new_name, min(new_optionsetvalue) FROM new_customentity";

            try
            {
                planBuilder.Build(query, null, out _);
                Assert.Fail();
            }
            catch (NotSupportedQueryFragmentException ex)
            {
                Assert.AreEqual("Column is invalid in the select list because it is not contained in either an aggregate function or the GROUP BY clause: new_name", ex.Message);
            }
        }

        [TestMethod]
        public void AggregateInSubquery()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"SELECT firstname
                          FROM   contact
                          WHERE  firstname IN (SELECT   firstname
                                               FROM     contact
                                               GROUP BY firstname
                                               HAVING   count(*) > 1);";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            Assert.AreEqual("contact.firstname", select.ColumnSet[0].SourceColumn);
            var filter = AssertNode<FilterNode>(select.Source);
            Assert.AreEqual("Expr2 IS NOT NULL", filter.Filter.ToSql());
            var join = AssertNode<HashJoinNode>(filter.Source);
            Assert.AreEqual("contact.firstname", join.LeftAttribute.GetColumnName());
            Assert.AreEqual("Expr1.firstname", join.RightAttribute.GetColumnName());
            Assert.AreEqual(QualifiedJoinType.LeftOuter, join.JoinType);
            Assert.IsTrue(join.SemiJoin);
            var outerFetch = AssertNode<FetchXmlScan>(join.LeftSource);

            AssertFetchXml(outerFetch, @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='firstname' />
                    </entity>
                </fetch>");

            var innerAlias = AssertNode<AliasNode>(join.RightSource);
            Assert.AreEqual("Expr1", innerAlias.Alias);
            var innerFilter = AssertNode<FilterNode>(innerAlias.Source);
            Assert.AreEqual("count > 1", innerFilter.Filter.ToSql());
            var innerTry1 = AssertNode<TryCatchNode>(innerFilter.Source);
            var innerTry2 = AssertNode<TryCatchNode>(innerTry1.TrySource);
            var innerAggregateFetch = AssertNode<FetchXmlScan>(innerTry2.TrySource);

            AssertFetchXml(innerAggregateFetch, @"
                <fetch aggregate='true'>
                    <entity name='contact'>
                        <attribute name='firstname' groupby='true' alias='firstname' />
                        <attribute name='contactid' aggregate='count' alias='count' />
                        <order alias='firstname' />
                    </entity>
                </fetch>");

            var innerPartitionAggregate = AssertNode<PartitionedAggregateNode>(innerTry2.CatchSource);
            Assert.AreEqual("contact.firstname", innerPartitionAggregate.GroupBy[0].GetColumnName());
            Assert.AreEqual("count", innerPartitionAggregate.Aggregates.First().Key);
            Assert.AreEqual(AggregateType.CountStar, innerPartitionAggregate.Aggregates.First().Value.AggregateType);
            var innerPartitionFetch = AssertNode<FetchXmlScan>(innerPartitionAggregate.Source);

            AssertFetchXml(innerPartitionFetch, @"
                <fetch aggregate='true'>
                    <entity name='contact'>
                        <attribute name='firstname' groupby='true' alias='firstname' />
                        <attribute name='contactid' aggregate='count' alias='count' />
                        <order alias='firstname' />
                    </entity>
                </fetch>");

            var innerAggregate = AssertNode<StreamAggregateNode>(innerTry1.CatchSource);
            Assert.AreEqual("contact.firstname", innerAggregate.GroupBy[0].GetColumnName());
            Assert.AreEqual("count", innerAggregate.Aggregates.First().Key);
            Assert.AreEqual(AggregateType.CountStar, innerAggregate.Aggregates.First().Value.AggregateType);
            var innerFetch = AssertNode<FetchXmlScan>(innerAggregate.Source);

            AssertFetchXml(innerFetch, @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='firstname' />
                        <order attribute='firstname' />
                    </entity>
                </fetch>");

            _context.Data["contact"] = new Dictionary<Guid, Entity>
            {
                [Guid.NewGuid()] = new Entity("contact")
                {
                    ["firstname"] = "Mark"
                },
                [Guid.NewGuid()] = new Entity("contact")
                {
                    ["firstname"] = "Mark"
                },
                [Guid.NewGuid()] = new Entity("contact")
                {
                    ["firstname"] = "Matt"
                },
            };

            var result = select.Execute(_localDataSource, this, new Dictionary<string, DataTypeReference>(), new Dictionary<string, object>());
            Assert.AreEqual(2, result.Rows.Count);
            Assert.AreEqual(SqlTypeConverter.UseDefaultCollation("Mark"), result.Rows[0][0]);
            Assert.AreEqual(SqlTypeConverter.UseDefaultCollation("Mark"), result.Rows[1][0]);
        }

        [TestMethod]
        public void SelectVirtualNameAttributeFromLinkEntity()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = "SELECT parentcustomeridname FROM account INNER JOIN contact ON account.accountid = contact.parentcustomerid";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var fetch = AssertNode<FetchXmlScan>(select.Source);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='parentcustomerid' />
                        <link-entity name='account' alias='account' from='accountid' to='parentcustomerid' link-type='inner'>
                        </link-entity>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void DuplicatedDistinctColumns()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = "SELECT DISTINCT name AS n1, name AS n2 FROM account";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var fetch = AssertNode<FetchXmlScan>(select.Source);
            AssertFetchXml(fetch, @"
                <fetch distinct='true'>
                    <entity name='account'>
                        <attribute name='name' />
                        <order attribute='name' />
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void GroupByDatetimeWithoutDatePart()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = "SELECT createdon, COUNT(*) FROM account GROUP BY createdon";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var aggregate = AssertNode<StreamAggregateNode>(select.Source);
            var fetch = AssertNode<FetchXmlScan>(aggregate.Source);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='createdon' />
                        <order attribute='createdon' />
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void MetadataExpressions()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = "SELECT collectionschemaname + '.' + entitysetname FROM metadata.entity WHERE description LIKE '%test%'";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            Assert.AreEqual("Expr1", select.ColumnSet[0].SourceColumn);

            var computeScalar = AssertNode<ComputeScalarNode>(select.Source);
            Assert.AreEqual("collectionschemaname + '.' + entitysetname", computeScalar.Columns["Expr1"].ToSql());

            var filter = AssertNode<FilterNode>(computeScalar.Source);
            Assert.AreEqual("description LIKE '%test%'", filter.Filter.ToSql());

            var metadataQuery = AssertNode<MetadataQueryNode>(filter.Source);
            Assert.AreEqual(MetadataSource.Entity, metadataQuery.MetadataSource);
            CollectionAssert.AreEquivalent(new[] { "CollectionSchemaName", "EntitySetName", "Description" }, metadataQuery.Query.Properties.PropertyNames);
        }

        [TestMethod]
        public void MultipleAliases()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = "SELECT name AS n1, name AS n2 FROM account WHERE name = 'test'";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            Assert.AreEqual("account.n1", select.ColumnSet[0].SourceColumn);
            Assert.AreEqual("account.n1", select.ColumnSet[1].SourceColumn);

            var fetch = AssertNode<FetchXmlScan>(select.Source);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='name' alias='n1' />
                        <filter>
                            <condition attribute='name' operator='eq' value='test' />
                        </filter>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void CrossInstanceJoin()
        {
            var metadata1 = new AttributeMetadataCache(_service);
            var metadata2 = new AttributeMetadataCache(_service2);
            var datasources = new []
            {
                new DataSource
                {
                    Name = "uat",
                    Connection = _context.GetOrganizationService(),
                    Metadata = metadata1,
                    TableSizeCache = new StubTableSizeCache()
                },
                new DataSource
                {
                    Name = "prod",
                    Connection = _context2.GetOrganizationService(),
                    Metadata = metadata2,
                    TableSizeCache = new StubTableSizeCache()
                },
                new DataSource
                {
                    Name = "local" // Hack so that ((IQueryExecutionOptions)this).PrimaryDataSource = "local" doesn't cause test to fail
                }
            };
            var planBuilder = new ExecutionPlanBuilder(datasources, this);

            var query = "SELECT uat.name, prod.name FROM uat.dbo.account AS uat INNER JOIN prod.dbo.account AS prod ON uat.accountid = prod.accountid WHERE uat.name <> prod.name AND uat.name LIKE '%test%'";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);

            var filter = AssertNode<FilterNode>(select.Source);
            Assert.AreEqual("uat.name <> prod.name", filter.Filter.ToSql());

            var join = AssertNode<MergeJoinNode>(filter.Source);
            Assert.AreEqual("uat.accountid", join.LeftAttribute.ToSql());
            Assert.AreEqual("prod.accountid", join.RightAttribute.ToSql());

            var uatFetch = AssertNode<FetchXmlScan>(join.LeftSource);
            Assert.AreEqual("uat", uatFetch.DataSource);
            AssertFetchXml(uatFetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='name' />
                        <attribute name='accountid' />
                        <filter>
                            <condition attribute='name' operator='like' value='%test%' />
                        </filter>
                        <order attribute='accountid' />
                    </entity>
                </fetch>");

            var prodFetch = AssertNode<FetchXmlScan>(join.RightSource);
            Assert.AreEqual("prod", prodFetch.DataSource);
            AssertFetchXml(prodFetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='name' />
                        <attribute name='accountid' />
                        <order attribute='accountid' />
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void FilterOnGroupByExpression()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
            SELECT
                DAY(a.createdon),
                MONTH(a.createdon),
                YEAR(a.createdon),
                COUNT(*)
            FROM
                account a
            WHERE
                YEAR(a.createdon) = 2021 AND MONTH(a.createdon) = 11
            GROUP BY
                DAY(a.createdon),
                MONTH(a.createdon),
                YEAR(a.createdon)";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var grouping = AssertNode<HashMatchAggregateNode>(select.Source);
            Assert.AreEqual("Expr1", grouping.GroupBy[0].ToSql());
            Assert.AreEqual("Expr2", grouping.GroupBy[1].ToSql());
            Assert.AreEqual("Expr3", grouping.GroupBy[2].ToSql());
            var calc = AssertNode<ComputeScalarNode>(grouping.Source);
            Assert.AreEqual("DAY(a.createdon)", calc.Columns["Expr1"].ToSql());
            Assert.AreEqual("MONTH(a.createdon)", calc.Columns["Expr2"].ToSql());
            Assert.AreEqual("YEAR(a.createdon)", calc.Columns["Expr3"].ToSql());
            var filter = AssertNode<FilterNode>(calc.Source);
            Assert.AreEqual("YEAR(a.createdon) = 2021 AND MONTH(a.createdon) = 11", filter.Filter.ToSql());
            var fetch = AssertNode<FetchXmlScan>(filter.Source);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='createdon' />
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void SystemFunctions()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = "SELECT CURRENT_TIMESTAMP, CURRENT_USER, GETDATE(), USER_NAME()";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var calc = AssertNode<ComputeScalarNode>(select.Source);
            var constant = AssertNode<ConstantScanNode>(calc.Source);
        }

        [TestMethod]
        public void FoldEqualsCurrentUser()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = "SELECT name FROM account WHERE ownerid = CURRENT_USER";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var fetch = AssertNode<FetchXmlScan>(select.Source);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='name' />
                        <filter>
                            <condition attribute='ownerid' operator='eq-userid' />
                        </filter>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void EntityReferenceInQuery()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = "SELECT name FROM account WHERE accountid IN ('0000000000000000-0000-0000-000000000000', '0000000000000000-0000-0000-000000000001')";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var fetch = AssertNode<FetchXmlScan>(select.Source);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='name' />
                        <filter>
                            <condition attribute='accountid' operator='in'>
                                <value>0000000000000000-0000-0000-000000000000</value>
                                <value>0000000000000000-0000-0000-000000000001</value>
                            </condition>
                        </filter>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void OrderBySelectExpression()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = "SELECT name + 'foo' FROM account ORDER BY 1";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            Assert.AreEqual("Expr1", select.ColumnSet.Single().SourceColumn);
            var sort = AssertNode<SortNode>(select.Source);
            Assert.AreEqual("Expr1", sort.Sorts.Single().ToSql());
            var compute = AssertNode<ComputeScalarNode>(sort.Source);
            Assert.AreEqual("name + 'foo'", compute.Columns["Expr1"].ToSql());
            var fetch = AssertNode<FetchXmlScan>(compute.Source);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='name' />
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void DistinctOrderByUsesScalarAggregate()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = "SELECT DISTINCT name + 'foo' FROM account ORDER BY 1";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            Assert.AreEqual("Expr1", select.ColumnSet.Single().SourceColumn);
            var aggregate = AssertNode<StreamAggregateNode>(select.Source);
            Assert.AreEqual("Expr1", aggregate.GroupBy.Single().ToSql());
            var sort = AssertNode<SortNode>(aggregate.Source);
            Assert.AreEqual("Expr1", sort.Sorts.Single().ToSql());
            var compute = AssertNode<ComputeScalarNode>(sort.Source);
            Assert.AreEqual("name + 'foo'", compute.Columns["Expr1"].ToSql());
            var fetch = AssertNode<FetchXmlScan>(compute.Source);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='name' />
                    </entity>
                </fetch>");
        }

        [TestMethod]
        [ExpectedException(typeof(NotSupportedQueryFragmentException))]
        public void WindowFunctionsNotSupported()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = "SELECT COUNT(accountid) OVER(PARTITION BY accountid) AS test FROM account";

            planBuilder.Build(query, null, out _);
        }

        [TestMethod]
        public void DeclareVariableSetLiteralSelect()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                DECLARE @test int
                SET @test = 1
                SELECT @test";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(3, plans.Length);

            var declare = AssertNode<DeclareVariablesNode>(plans[0]);
            Assert.AreEqual(1, declare.Variables.Count);
            Assert.AreEqual(typeof(SqlInt32), declare.Variables["@test"].ToNetType(out _));

            var setVariable = AssertNode<AssignVariablesNode>(plans[1]);
            Assert.AreEqual(1, setVariable.Variables.Count);
            Assert.AreEqual("@test", setVariable.Variables[0].VariableName);
            Assert.AreEqual("Expr1", setVariable.Variables[0].SourceColumn);
            var setCompute = AssertNode<ComputeScalarNode>(setVariable.Source);
            Assert.AreEqual("CONVERT (INT, 1)", setCompute.Columns["Expr1"].ToSql());
            var setConstantScan = AssertNode<ConstantScanNode>(setCompute.Source);
            Assert.AreEqual(1, setConstantScan.Values.Count);

            var select = AssertNode<SelectNode>(plans[2]);
            Assert.AreEqual("Expr2", select.ColumnSet[0].SourceColumn);
            var selectCompute = AssertNode<ComputeScalarNode>(select.Source);
            Assert.AreEqual("@test", selectCompute.Columns["Expr2"].ToSql());
            var selectConstantScan = AssertNode<ConstantScanNode>(selectCompute.Source);
            Assert.AreEqual(1, selectConstantScan.Values.Count);

            var parameterTypes = new Dictionary<string, DataTypeReference>();
            var parameterValues = new Dictionary<string, object>();

            foreach (var plan in plans)
            {
                if (plan is IDataSetExecutionPlanNode selectQuery)
                {
                    var results = selectQuery.Execute(_dataSources, this, parameterTypes, parameterValues);

                    Assert.AreEqual(1, results.Rows.Count);
                    Assert.AreEqual(1, results.Columns.Count);
                    Assert.AreEqual((SqlInt32)1, results.Rows[0][0]);
                }
                else if (plan is IDmlQueryExecutionPlanNode dmlQuery)
                {
                    dmlQuery.Execute(_dataSources, this, parameterTypes, parameterValues, out _);
                }
            }
        }

        [TestMethod]
        public void SetVariableInDeclaration()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                DECLARE @test int = 1
                SELECT @test";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(3, plans.Length);

            var declare = AssertNode<DeclareVariablesNode>(plans[0]);
            Assert.AreEqual(1, declare.Variables.Count);
            Assert.AreEqual(typeof(SqlInt32), declare.Variables["@test"].ToNetType(out _));

            var setVariable = AssertNode<AssignVariablesNode>(plans[1]);
            Assert.AreEqual(1, setVariable.Variables.Count);
            Assert.AreEqual("@test", setVariable.Variables[0].VariableName);
            Assert.AreEqual("Expr1", setVariable.Variables[0].SourceColumn);
            var setCompute = AssertNode<ComputeScalarNode>(setVariable.Source);
            Assert.AreEqual("CONVERT (INT, 1)", setCompute.Columns["Expr1"].ToSql());
            var setConstantScan = AssertNode<ConstantScanNode>(setCompute.Source);
            Assert.AreEqual(1, setConstantScan.Values.Count);

            var select = AssertNode<SelectNode>(plans[2]);
            Assert.AreEqual("Expr2", select.ColumnSet[0].SourceColumn);
            var selectCompute = AssertNode<ComputeScalarNode>(select.Source);
            Assert.AreEqual("@test", selectCompute.Columns["Expr2"].ToSql());
            var selectConstantScan = AssertNode<ConstantScanNode>(selectCompute.Source);
            Assert.AreEqual(1, selectConstantScan.Values.Count);

            var parameterTypes = new Dictionary<string, DataTypeReference>();
            var parameterValues = new Dictionary<string, object>();

            foreach (var plan in plans)
            {
                if (plan is IDataSetExecutionPlanNode selectQuery)
                {
                    var results = selectQuery.Execute(_dataSources, this, parameterTypes, parameterValues);

                    Assert.AreEqual(1, results.Rows.Count);
                    Assert.AreEqual(1, results.Columns.Count);
                    Assert.AreEqual((SqlInt32)1, results.Rows[0][0]);
                }
                else if (plan is IDmlQueryExecutionPlanNode dmlQuery)
                {
                    dmlQuery.Execute(_dataSources, this, parameterTypes, parameterValues, out _);
                }
            }
        }

        [TestMethod]
        [ExpectedException(typeof(NotSupportedQueryFragmentException))]
        public void UnknownVariable()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = "SET @test = 1";

            planBuilder.Build(query, null, out _);
        }

        [TestMethod]
        [ExpectedException(typeof(NotSupportedQueryFragmentException))]
        public void DuplicateVariable()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                DECLARE @test INT
                DECLARE @test INT";

            planBuilder.Build(query, null, out _);
        }

        [TestMethod]
        public void VariableTypeConversionIntToString()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                DECLARE @test varchar(3)
                SET @test = 100
                SELECT @test";

            var plans = planBuilder.Build(query, null, out _);

            var parameterTypes = new Dictionary<string, DataTypeReference>();
            var parameterValues = new Dictionary<string, object>();

            foreach (var plan in plans)
            {
                if (plan is IDataSetExecutionPlanNode selectQuery)
                {
                    var results = selectQuery.Execute(_dataSources, this, parameterTypes, parameterValues);

                    Assert.AreEqual(1, results.Rows.Count);
                    Assert.AreEqual(1, results.Columns.Count);
                    Assert.AreEqual(ToSqlString("100"), results.Rows[0][0]);
                }
                else if (plan is IDmlQueryExecutionPlanNode dmlQuery)
                {
                    dmlQuery.Execute(_dataSources, this, parameterTypes, parameterValues, out _);
                }
            }
        }

        [TestMethod]
        public void VariableTypeConversionStringTruncation()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                DECLARE @test varchar(3)
                SET @test = 'test'
                SELECT @test";

            var plans = planBuilder.Build(query, null, out _);

            var parameterTypes = new Dictionary<string, DataTypeReference>();
            var parameterValues = new Dictionary<string, object>();

            foreach (var plan in plans)
            {
                if (plan is IDataSetExecutionPlanNode selectQuery)
                {
                    var results = selectQuery.Execute(_dataSources, this, parameterTypes, parameterValues);

                    Assert.AreEqual(1, results.Rows.Count);
                    Assert.AreEqual(1, results.Columns.Count);
                    Assert.AreEqual(ToSqlString("tes"), results.Rows[0][0]);
                }
                else if (plan is IDmlQueryExecutionPlanNode dmlQuery)
                {
                    dmlQuery.Execute(_dataSources, this, parameterTypes, parameterValues, out _);
                }
            }
        }

        [TestMethod]
        [ExpectedException(typeof(NotSupportedQueryFragmentException))]
        public void CannotCombineSetVariableAndDataRetrievalInSelect()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            // A SELECT statement that assigns a value to a variable must not be combined with data-retrieval operations
            var query = @"
                DECLARE @test varchar(3)
                SELECT @test = name, accountid FROM account";

            planBuilder.Build(query, null, out _);
        }

        [TestMethod]
        public void SetVariableWithSelectUsesFinalValue()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                DECLARE @test varchar(3)
                SELECT @test = name FROM account ORDER BY name
                SELECT @test";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(3, plans.Length);

            var declare = AssertNode<DeclareVariablesNode>(plans[0]);
            Assert.AreEqual(1, declare.Variables.Count);
            Assert.AreEqual(typeof(SqlString), declare.Variables["@test"].ToNetType(out _));

            var setVariable = AssertNode<AssignVariablesNode>(plans[1]);
            Assert.AreEqual(1, setVariable.Variables.Count);
            Assert.AreEqual("@test", setVariable.Variables[0].VariableName);
            Assert.AreEqual("Expr1", setVariable.Variables[0].SourceColumn);
            var setCompute = AssertNode<ComputeScalarNode>(setVariable.Source);
            Assert.AreEqual("CONVERT (VARCHAR (3), name)", setCompute.Columns["Expr1"].ToSql());
            var setFetchXml = AssertNode<FetchXmlScan>(setCompute.Source);
            AssertFetchXml(setFetchXml, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='name' />
                        <order attribute='name' />
                    </entity>
                </fetch>");

            var select = AssertNode<SelectNode>(plans[2]);
            Assert.AreEqual("Expr2", select.ColumnSet[0].SourceColumn);
            var selectCompute = AssertNode<ComputeScalarNode>(select.Source);
            Assert.AreEqual("@test", selectCompute.Columns["Expr2"].ToSql());
            var selectConstantScan = AssertNode<ConstantScanNode>(selectCompute.Source);
            Assert.AreEqual(1, selectConstantScan.Values.Count);

            _context.Data["account"] = new Dictionary<Guid, Entity>
            {
                [Guid.NewGuid()] = new Entity("account")
                {
                    ["name"] = "X"
                },
                [Guid.NewGuid()] = new Entity("account")
                {
                    ["name"] = "Z"
                },
                [Guid.NewGuid()] = new Entity("account")
                {
                    ["name"] = "Y"
                },
            };

            var parameterTypes = new Dictionary<string, DataTypeReference>();
            var parameterValues = new Dictionary<string, object>();

            foreach (var plan in plans)
            {
                if (plan is IDataSetExecutionPlanNode selectQuery)
                {
                    var results = selectQuery.Execute(_localDataSource, this, parameterTypes, parameterValues);

                    Assert.AreEqual(1, results.Rows.Count);
                    Assert.AreEqual(1, results.Columns.Count);
                    Assert.AreEqual(ToSqlString("Z"), results.Rows[0][0]);
                }
                else if (plan is IDmlQueryExecutionPlanNode dmlQuery)
                {
                    dmlQuery.Execute(_localDataSource, this, parameterTypes, parameterValues, out _);
                }
            }
        }

        [TestMethod]
        public void VarCharLengthDefaultsTo1()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                DECLARE @test varchar
                SET @test = 'test'
                SELECT @test";

            var plans = planBuilder.Build(query, null, out _);

            var parameterTypes = new Dictionary<string, DataTypeReference>();
            var parameterValues = new Dictionary<string, object>();

            foreach (var plan in plans)
            {
                if (plan is IDataSetExecutionPlanNode selectQuery)
                {
                    var results = selectQuery.Execute(_dataSources, this, parameterTypes, parameterValues);

                    Assert.AreEqual(1, results.Rows.Count);
                    Assert.AreEqual(1, results.Columns.Count);
                    Assert.AreEqual(ToSqlString("t"), results.Rows[0][0]);
                }
                else if (plan is IDmlQueryExecutionPlanNode dmlQuery)
                {
                    dmlQuery.Execute(_dataSources, this, parameterTypes, parameterValues, out _);
                }
            }
        }

        [TestMethod]
        [ExpectedException(typeof(NotSupportedQueryFragmentException))]
        public void CursorVariableNotSupported()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                DECLARE @test CURSOR";

            planBuilder.Build(query, null, out _);
        }

        [TestMethod]
        [ExpectedException(typeof(NotSupportedQueryFragmentException))]
        public void TableVariableNotSupported()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                DECLARE @test TABLE (ID INT)";

            planBuilder.Build(query, null, out _);
        }

        [TestMethod]
        public void IfStatement()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this) { CompileConditions = false };

            var query = @"
                IF @param1 = 1
                BEGIN
                    INSERT INTO account (name) VALUES ('one')
                    DELETE FROM account WHERE accountid = @@IDENTITY
                END
                ELSE
                    SELECT name FROM account";

            var parameters = new Dictionary<string, DataTypeReference>
            {
                ["@param1"] = typeof(SqlInt32).ToSqlType()
            };
            var plans = planBuilder.Build(query, parameters, out _);

            Assert.AreEqual(1, plans.Length);

            var cond = AssertNode<ConditionalNode>(plans[0]);
            Assert.AreEqual("@param1 = 1", cond.Condition.ToSql());
            Assert.AreEqual(ConditionalNodeType.If, cond.Type);

            Assert.AreEqual(2, cond.TrueStatements.Length);
            AssertNode<InsertNode>(cond.TrueStatements[0]);
            AssertNode<DeleteNode>(cond.TrueStatements[1]);

            Assert.AreEqual(1, cond.FalseStatements.Length);
            AssertNode<SelectNode>(cond.FalseStatements[0]);
        }

        [TestMethod]
        public void WhileStatement()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this) { CompileConditions = false };

            var query = @"
                WHILE @param1 < 10
                BEGIN
                    INSERT INTO account (name) VALUES (@param1)
                    SET @param1 += 1
                END";

            var parameters = new Dictionary<string, DataTypeReference>
            {
                ["@param1"] = typeof(SqlInt32).ToSqlType()
            };
            var plans = planBuilder.Build(query, parameters, out _);

            Assert.AreEqual(1, plans.Length);

            var cond = AssertNode<ConditionalNode>(plans[0]);
            Assert.AreEqual("@param1 < 10", cond.Condition.ToSql());
            Assert.AreEqual(ConditionalNodeType.While, cond.Type);

            Assert.AreEqual(2, cond.TrueStatements.Length);
            AssertNode<InsertNode>(cond.TrueStatements[0]);
            AssertNode<AssignVariablesNode>(cond.TrueStatements[1]);
        }

        [TestMethod]
        public void IfNotExists()
        {
            var metadata = new AttributeMetadataCache(_service);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this) { CompileConditions = false };

            var query = @"
                IF NOT EXISTS(SELECT * FROM account WHERE name = @param1)
                BEGIN
                    INSERT INTO account (name) VALUES (@param1)
                END";

            var parameters = new Dictionary<string, DataTypeReference>
            {
                ["@param1"] = typeof(SqlString).ToSqlType()
            };
            var plans = planBuilder.Build(query, parameters, out _);

            Assert.AreEqual(1, plans.Length);

            var cond = AssertNode<ConditionalNode>(plans[0]);

            Assert.AreEqual(1, cond.TrueStatements.Length);
            AssertNode<InsertNode>(cond.TrueStatements[0]);
        }
    }
}
