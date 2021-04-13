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
using Microsoft.Xrm.Sdk.Metadata;
using Microsoft.Xrm.Sdk.Metadata.Query;
using Microsoft.Xrm.Sdk.Query;

namespace MarkMpn.Sql4Cds.Engine.Tests
{
    [TestClass]
    public class ExecutionPlanTests : IQueryExecutionOptions
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

        int IQueryExecutionOptions.LocaleId => 1033;

        int IQueryExecutionOptions.MaxDegreeOfParallelism => 10;

        bool IQueryExecutionOptions.ColumnComparisonAvailable => true;

        bool IQueryExecutionOptions.UseLocalTimeZone => false;

        List<JoinOperator> IQueryExecutionOptions.JoinOperatorsAvailable => _supportedJoins;

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

        [TestMethod]
        public void SimpleSelect()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = "SELECT accountid, name FROM account";

            var plans = planBuilder.Build(query);

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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = "SELECT * FROM account";

            var plans = planBuilder.Build(query);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            CollectionAssert.AreEqual(new[]
            {
                "accountid",
                "createdon",
                "employees",
                "name",
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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = "SELECT accountid, name FROM account INNER JOIN contact ON account.accountid = contact.parentcustomerid";

            var plans = planBuilder.Build(query);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var fetch = AssertNode<FetchXmlScan>(select.Source);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='accountid' />
                        <attribute name='name' />
                        <link-entity name='contact' alias='contact' from='parentcustomerid' to='accountid' link-type='inner'>
                        </link-entity>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void JoinWithExtraCondition()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT
                    accountid,
                    name
                FROM
                    account
                    INNER JOIN contact ON account.accountid = contact.parentcustomerid AND contact.firstname = 'Mark'";

            var plans = planBuilder.Build(query);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var fetch = AssertNode<FetchXmlScan>(select.Source);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='accountid' />
                        <attribute name='name' />
                        <link-entity name='contact' alias='contact' from='parentcustomerid' to='accountid' link-type='inner'>
                            <filter>
                                <condition attribute='firstname' operator='eq' value='Mark' />
                            </filter>
                        </link-entity>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void NonUniqueJoin()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = "SELECT accountid, name FROM account INNER JOIN contact ON account.name = contact.fullname";

            var plans = planBuilder.Build(query);

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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = "SELECT accountid, name FROM account INNER JOIN contact ON account.name = (contact.firstname + ' ' + contact.lastname)";

            var plans = planBuilder.Build(query);

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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT
                    accountid,
                    name
                FROM
                    account
                WHERE
                    name = 'Data8'";

            var plans = planBuilder.Build(query);

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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT
                    accountid,
                    name
                FROM
                    account
                ORDER BY
                    name ASC";

            var plans = planBuilder.Build(query);

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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT
                    accountid,
                    name
                FROM
                    account
                ORDER BY
                    2 ASC";

            var plans = planBuilder.Build(query);

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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT DISTINCT
                    name
                FROM
                    account";

            var plans = planBuilder.Build(query);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var fetch = AssertNode<FetchXmlScan>(select.Source);
            AssertFetchXml(fetch, @"
                <fetch distinct='true'>
                    <entity name='account'>
                        <attribute name='name' />
                        <order attribute='accountid' />
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void SimpleTop()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT TOP 10
                    accountid,
                    name
                FROM
                    account";

            var plans = planBuilder.Build(query);

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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT
                    accountid,
                    name
                FROM
                    account
                ORDER BY name
                OFFSET 100 ROWS FETCH NEXT 50 ROWS ONLY";

            var plans = planBuilder.Build(query);

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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT
                    name,
                    count(*)
                FROM
                    account
                GROUP BY name";

            var plans = planBuilder.Build(query);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var tryCatch = AssertNode<TryCatchNode>(select.Source);
            var aggregateFetch = AssertNode<FetchXmlScan>(tryCatch.TrySource);
            AssertFetchXml(aggregateFetch, @"
                <fetch aggregate='true'>
                    <entity name='account'>
                        <attribute name='name' groupby='true' alias='name' />
                        <attribute name='accountid' aggregate='count' alias='count' />
                    </entity>
                </fetch>");
            var aggregate = AssertNode<HashMatchAggregateNode>(tryCatch.CatchSource);
            var scalarFetch = AssertNode<FetchXmlScan>(aggregate.Source);
            AssertFetchXml(scalarFetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='name' />
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void AliasedAggregate()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT
                    name,
                    count(*) AS test
                FROM
                    account
                GROUP BY name";

            var plans = planBuilder.Build(query);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var tryCatch = AssertNode<TryCatchNode>(select.Source);
            var aggregateFetch = AssertNode<FetchXmlScan>(tryCatch.TrySource);
            AssertFetchXml(aggregateFetch, @"
                <fetch aggregate='true'>
                    <entity name='account'>
                        <attribute name='name' groupby='true' alias='name' />
                        <attribute name='accountid' aggregate='count' alias='test' />
                    </entity>
                </fetch>");
            var aggregate = AssertNode<HashMatchAggregateNode>(tryCatch.CatchSource);
            var scalarFetch = AssertNode<FetchXmlScan>(aggregate.Source);
            AssertFetchXml(scalarFetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='name' />
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void AliasedGroupingAggregate()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT
                    name AS test,
                    count(*)
                FROM
                    account
                GROUP BY name";

            var plans = planBuilder.Build(query);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var tryCatch = AssertNode<TryCatchNode>(select.Source);
            var aggregateFetch = AssertNode<FetchXmlScan>(tryCatch.TrySource);
            AssertFetchXml(aggregateFetch, @"
                <fetch aggregate='true'>
                    <entity name='account'>
                        <attribute name='name' groupby='true' alias='name' />
                        <attribute name='accountid' aggregate='count' alias='count' />
                    </entity>
                </fetch>");
            var aggregate = AssertNode<HashMatchAggregateNode>(tryCatch.CatchSource);
            var scalarFetch = AssertNode<FetchXmlScan>(aggregate.Source);
            AssertFetchXml(scalarFetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='name' />
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void SimpleAlias()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = "SELECT accountid, name AS test FROM account";

            var plans = planBuilder.Build(query);

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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
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

            var plans = planBuilder.Build(query);

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
            var tryCatch = AssertNode<TryCatchNode>(filter.Source);
            var aggregateFetch = AssertNode<FetchXmlScan>(tryCatch.TrySource);
            AssertFetchXml(aggregateFetch, @"
                <fetch aggregate='true'>
                    <entity name='account'>
                        <attribute name='name' groupby='true' alias='name' />
                        <attribute name='accountid' aggregate='count' alias='count' />
                    </entity>
                </fetch>");
            var aggregate = AssertNode<HashMatchAggregateNode>(tryCatch.CatchSource);
            Assert.AreEqual("account.name", aggregate.GroupBy[0].ToSql());
            Assert.AreEqual("count", aggregate.Aggregates.Single().Key);
            var scalarFetch = AssertNode<FetchXmlScan>(aggregate.Source);
            AssertFetchXml(scalarFetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='name' />
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void GroupByDatePart()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT
                    DATEPART(month, createdon),
                    count(*)
                FROM
                    account
                GROUP BY DATEPART(month, createdon)";

            var plans = planBuilder.Build(query);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            Assert.AreEqual("createdon_month", select.ColumnSet[0].SourceColumn);
            Assert.AreEqual("count", select.ColumnSet[1].SourceColumn);
            var tryCatch = AssertNode<TryCatchNode>(select.Source);
            var aggregateFetch = AssertNode<FetchXmlScan>(tryCatch.TrySource);
            AssertFetchXml(aggregateFetch, @"
                <fetch aggregate='true'>
                    <entity name='account'>
                        <attribute name='createdon' groupby='true' alias='createdon_month' dategrouping='month' />
                        <attribute name='accountid' aggregate='count' alias='count' />
                    </entity>
                </fetch>");
            var aggregate = AssertNode<HashMatchAggregateNode>(tryCatch.CatchSource);
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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT
                    name,
                    firstname
                FROM
                    account
                    INNER JOIN contact ON account.accountid = contact.parentcustomerid
                ORDER BY
                    name,
                    firstname,
                    accountid";

            var plans = planBuilder.Build(query);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var order = AssertNode<SortNode>(select.Source);
            var fetch = AssertNode<FetchXmlScan>(order.Source);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='name' />
                        <attribute name='accountid' />
                        <link-entity name='contact' alias='contact' from='parentcustomerid' to='accountid' link-type='inner'>
                            <attribute name='firstname' />
                        </link-entity>
                        <order attribute='name' />
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void PartialOrderingAvoidingLegacyPagingWithTop()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT TOP 100
                    name,
                    firstname
                FROM
                    account
                    INNER JOIN contact ON account.accountid = contact.parentcustomerid
                ORDER BY
                    name,
                    firstname,
                    accountid";

            var plans = planBuilder.Build(query);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var top = AssertNode<TopNode>(select.Source);
            var order = AssertNode<SortNode>(top.Source);
            var fetch = AssertNode<FetchXmlScan>(order.Source);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='name' />
                        <attribute name='accountid' />
                        <link-entity name='contact' alias='contact' from='parentcustomerid' to='accountid' link-type='inner'>
                            <attribute name='firstname' />
                            <order attribute='firstname' />
                        </link-entity>
                        <order attribute='name' />
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void PartialWhere()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
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

            var plans = planBuilder.Build(query);

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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT count(*) FROM account";

            var plans = planBuilder.Build(query);

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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT firstname + ' ' + lastname AS fullname FROM contact WHERE firstname = 'Mark'";

            var plans = planBuilder.Build(query);

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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT contactid FROM contact WHERE firstname + ' ' + lastname = 'Mark Carrington'";

            var plans = planBuilder.Build(query);

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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT firstname + ' ' + lastname AS fullname, 'Account: ' + (SELECT name FROM account WHERE accountid = parentcustomerid) AS accountname FROM contact WHERE firstname = 'Mark'";

            var plans = planBuilder.Build(query);

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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT firstname + ' ' + lastname AS fullname, 'Account: ' + (SELECT name FROM account WHERE createdon = contact.createdon) AS accountname FROM contact WHERE firstname = 'Mark'";

            var plans = planBuilder.Build(query);

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
            var subAggregate = AssertNode<HashMatchAggregateNode>(subAssert.Source);
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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT TOP 10 firstname + ' ' + lastname AS fullname, 'Account: ' + (SELECT name FROM account WHERE createdon = contact.createdon) AS accountname FROM contact WHERE firstname = 'Mark'";

            var plans = planBuilder.Build(query);

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
            var subAggregate = AssertNode<HashMatchAggregateNode>(subAssert.Source);
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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT firstname + ' ' + lastname AS fullname, 'Account: ' + (SELECT TOP 1 name FROM account) AS accountname FROM contact WHERE firstname = 'Mark'";

            var plans = planBuilder.Build(query);

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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT firstname + ' ' + lastname AS fullname, 'Account: ' + (SELECT name FROM account WHERE createdon = contact.createdon) AS accountname FROM contact WHERE firstname = 'Mark'";

            var plans = planBuilder.Build(query);

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
            var subAggregate = AssertNode<HashMatchAggregateNode>(subAssert.Source);
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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT firstname + ' ' + lastname AS fullname, 'Account: ' + (SELECT name FROM account WHERE createdon = contact.createdon AND employees > 10) AS accountname FROM contact WHERE firstname = 'Mark'";

            var plans = planBuilder.Build(query);

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
            var subAggregate = AssertNode<HashMatchAggregateNode>(subAssert.Source);
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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var tableSize = new StubTableSizeCache();
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT firstname + ' ' + lastname AS fullname, 'Account: ' + (SELECT firstname + ' ' + name FROM account WHERE accountid = parentcustomerid) AS accountname FROM contact WHERE firstname = 'Mark'";

            var plans = planBuilder.Build(query);

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
            var subAggregate = AssertNode<HashMatchAggregateNode>(subAssert.Source);
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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT firstname FROM contact ORDER BY (SELECT TOP 1 name FROM account WHERE accountid = parentcustomerid ORDER BY firstname)";

            var plans = planBuilder.Build(query);

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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT firstname + ' ' + lastname AS fullname FROM contact WHERE (SELECT name FROM account WHERE accountid = parentcustomerid) = 'Data8'";

            var plans = planBuilder.Build(query);

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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT DISTINCT TOP 10
                    name + '1'
                FROM
                    account";

            var plans = planBuilder.Build(query);

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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT name FROM account
                UNION ALL
                SELECT fullname FROM contact";

            var plans = planBuilder.Build(query);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var concat = AssertNode<ConcatenateNode>(select.Source);
            Assert.AreEqual(2, concat.Sources.Count);
            Assert.AreEqual("name", concat.ColumnSet[0].OutputColumn);
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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT
                    accountid,
                    name
                FROM
                    account
                WHERE
                    name in ('Data8', 'Mark Carrington')";

            var plans = planBuilder.Build(query);

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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT
                    accountid,
                    name
                FROM
                    account
                WHERE
                    name in (SELECT firstname FROM contact)";

            var plans = planBuilder.Build(query);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var filter = AssertNode<FilterNode>(select.Source);
            var hashJoin = AssertNode<MergeJoinNode>(filter.Source);
            var fetch = AssertNode<FetchXmlScan>(hashJoin.LeftSource);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='accountid' />
                        <attribute name='name' />
                        <order attribute='name' />
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
        public void SubqueryInFilterUncorrelatedPrimaryKey()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT
                    accountid,
                    name
                FROM
                    account
                WHERE
                    primarycontactid in (SELECT contactid FROM contact WHERE firstname = 'Mark')";

            var plans = planBuilder.Build(query);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var fetch = AssertNode<FetchXmlScan>(select.Source);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='accountid' />
                        <attribute name='name' />
                        <link-entity name='contact' alias='Expr1' from='contactid' to='primarycontactid' link-type='outer'>
                            <attribute name='contactid' />
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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT
                    accountid,
                    name
                FROM
                    account
                WHERE
                    name in (SELECT firstname FROM contact WHERE parentcustomerid = accountid)";

            var plans = planBuilder.Build(query);

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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT
                    accountid,
                    name
                FROM
                    account
                WHERE
                    name not in (SELECT firstname FROM contact WHERE parentcustomerid = accountid)";

            var plans = planBuilder.Build(query);

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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT
                    accountid,
                    name
                FROM
                    account
                WHERE
                    EXISTS (SELECT * FROM contact)";

            var plans = planBuilder.Build(query);

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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT
                    accountid,
                    name
                FROM
                    account
                WHERE
                    EXISTS (SELECT * FROM contact WHERE contactid = primarycontactid)";

            var plans = planBuilder.Build(query);

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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT
                    accountid,
                    name
                FROM
                    account
                WHERE
                    EXISTS (SELECT * FROM contact WHERE parentcustomerid = accountid)";

            var plans = planBuilder.Build(query);

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
            var subTop = AssertNode<TopNode>(nestedLoop.RightSource);
            var subIndexSpool = AssertNode<IndexSpoolNode>(subTop.Source);
            Assert.AreEqual("contact.parentcustomerid", subIndexSpool.KeyColumn);
            Assert.AreEqual("@Expr1", subIndexSpool.SeekValue);
            var subFetch = AssertNode<FetchXmlScan>(subIndexSpool.Source);
            AssertFetchXml(subFetch, @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='contactid' />
                        <attribute name='parentcustomerid' />
                        <filter>
                            <condition attribute='parentcustomerid' operator='not-null' />
                        </filter>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void NotExistsFilterCorrelated()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT
                    accountid,
                    name
                FROM
                    account
                WHERE
                    NOT EXISTS (SELECT * FROM contact WHERE parentcustomerid = accountid)";

            var plans = planBuilder.Build(query);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var filter = AssertNode<FilterNode>(select.Source);
            Assert.AreEqual("NOT Expr2 IS NOT NULL", filter.Filter.ToSql());
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
            var subTop = AssertNode<TopNode>(nestedLoop.RightSource);
            var subIndexSpool = AssertNode<IndexSpoolNode>(subTop.Source);
            Assert.AreEqual("contact.parentcustomerid", subIndexSpool.KeyColumn);
            Assert.AreEqual("@Expr1", subIndexSpool.SeekValue);
            var subFetch = AssertNode<FetchXmlScan>(subIndexSpool.Source);
            AssertFetchXml(subFetch, @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='contactid' />
                        <attribute name='parentcustomerid' />
                        <filter>
                            <condition attribute='parentcustomerid' operator='not-null' />
                        </filter>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void QueryDerivedTableSimple()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT TOP 10
                    name
                FROM
                    (SELECT accountid, name FROM account) a
                WHERE
                    name = 'Data8'";

            var plans = planBuilder.Build(query);

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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT TOP 10
                    a.accountname
                FROM
                    (SELECT accountid, name AS accountname FROM account) a
                WHERE
                    a.accountname = 'Data8'";

            var plans = planBuilder.Build(query);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var top = AssertNode<TopNode>(select.Source);
            var filter = AssertNode<FilterNode>(top.Source);
            var alias = AssertNode<AliasNode>(filter.Source);
            var fetch = AssertNode<FetchXmlScan>(alias.Source);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='accountid' />
                        <attribute name='name' alias='accountname' />
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void QueryDerivedTableValues()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT TOP 10
                    name
                FROM
                    (VALUES (1, 'Data8')) a (ID, name)
                WHERE
                    name = 'Data8'";

            var plans = planBuilder.Build(query);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var top = AssertNode<TopNode>(select.Source);
            var filter = AssertNode<FilterNode>(top.Source);
            var constant = AssertNode<ConstantScanNode>(filter.Source);

            var schema = constant.GetSchema(metadata, null);
            Assert.AreEqual(typeof(SqlInt32), schema.Schema["a.ID"]);
            Assert.AreEqual(typeof(SqlString), schema.Schema["a.name"]);
        }

        [TestMethod]
        public void NoLockTableHint()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT TOP 10
                    name
                FROM
                    account (NOLOCK)
                WHERE
                    name = 'Data8'";

            var plans = planBuilder.Build(query);

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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT
                    name,
                    fullname
                FROM
                    account
                    CROSS JOIN
                    contact";

            var plans = planBuilder.Build(query);

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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
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

            var plans = planBuilder.Build(query);

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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
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

            var plans = planBuilder.Build(query);

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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
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

            var plans = planBuilder.Build(query);

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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT
                    accountid,
                    name
                FROM
                    account
                WHERE
                    createdon = lastxdays(1)";

            var plans = planBuilder.Build(query);

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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT logicalname
                FROM   metadata.entity";

            var plans = planBuilder.Build(query);

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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = @"
                SELECT logicalname
                FROM   metadata.entity
                WHERE  objecttypecode = 1";

            var plans = planBuilder.Build(query);

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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = "UPDATE account SET name = 'foo' WHERE name = 'bar'";

            var plans = planBuilder.Build(query);

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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = "UPDATE a SET name = 'foo' FROM account a INNER JOIN contact c ON a.accountid = c.parentcustomerid WHERE name = 'bar'";

            var plans = planBuilder.Build(query);

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
                    <entity name='account'>
                        <attribute name='accountid' />
                        <link-entity name='contact' alias='c' from='parentcustomerid' to='accountid' link-type='inner' />
                        <filter>
                            <condition attribute='name' operator='eq' value='bar' />
                        </filter>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void QueryHints()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = "SELECT accountid, name FROM account OPTION (OPTIMIZE FOR UNKNOWN, FORCE ORDER, RECOMPILE, USE HINT('DISABLE_OPTIMIZER_ROWGOAL'), USE HINT('ENABLE_QUERY_OPTIMIZER_HOTFIXES'), LOOP JOIN, MERGE JOIN, HASH JOIN, NO_PERFORMANCE_SPOOL, MAXRECURSION 2)";

            var plans = planBuilder.Build(query);

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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = "SELECT name, count(*) from account group by name order by 2 desc";

            var plans = planBuilder.Build(query);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            Assert.AreEqual("account.name", select.ColumnSet[0].SourceColumn);
            Assert.AreEqual("count", select.ColumnSet[1].SourceColumn);
            var tryCatch = AssertNode<TryCatchNode>(select.Source);
            var aggregateFetch = AssertNode<FetchXmlScan>(tryCatch.TrySource);
            AssertFetchXml(aggregateFetch, @"
                <fetch aggregate='true'>
                    <entity name='account'>
                        <attribute name='name' groupby='true' alias='name' />
                        <attribute name='accountid' aggregate='count' alias='count' />
                        <order alias='count' descending='true' />
                    </entity>
                </fetch>");
            var sort = AssertNode<SortNode>(tryCatch.CatchSource);
            Assert.AreEqual("count", sort.Sorts.Single().Expression.ToSql());
            Assert.AreEqual(SortOrder.Descending, sort.Sorts.Single().SortOrder);
            var aggregate = AssertNode<HashMatchAggregateNode>(sort.Source);
            var fetch = AssertNode<FetchXmlScan>(aggregate.Source);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='name' />
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void FoldFilterWithNonFoldedJoin()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = "SELECT name from account INNER JOIN contact ON left(name, 4) = left(firstname, 4) where name like 'Data8%' and firstname like 'Mark%'";

            var plans = planBuilder.Build(query);

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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = "SELECT name from account where name like 'Data8%' and primarycontactid in (select contactid from contact where firstname = 'Mark')";

            var plans = planBuilder.Build(query);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var fetch = AssertNode<FetchXmlScan>(select.Source);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='name' />
                        <link-entity name='contact' alias='Expr1' from='contactid' to='primarycontactid' link-type='outer'>
                            <attribute name='contactid' />
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
                var context = new XrmFakedContext();
                context.InitializeMetadata(Assembly.GetExecutingAssembly());

                var org = context.GetOrganizationService();
                var metadata = new AttributeMetadataCache(org);
                var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

                var query = "SELECT name from account where name like 'Data8%' and createdon in (select createdon from contact where firstname = 'Mark')";

                var plans = planBuilder.Build(query);

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
                var context = new XrmFakedContext();
                context.InitializeMetadata(Assembly.GetExecutingAssembly());

                var org = context.GetOrganizationService();
                var metadata = new AttributeMetadataCache(org);
                var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

                var query = "SELECT name from account inner join contact on account.accountid = contact.parentcustomerid where name like 'Data8%' and contact.createdon in (select createdon from contact where firstname = 'Mark')";

                var plans = planBuilder.Build(query);

                Assert.AreEqual(1, plans.Length);

                var select = AssertNode<SelectNode>(plans[0]);
                var fetch = AssertNode<FetchXmlScan>(select.Source);
                AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='name' />
                        <link-entity name='contact' alias='contact' from='parentcustomerid' to='accountid' link-type='inner'>
                            <link-entity name='contact' alias='Expr1' from='createdon' to='createdon' link-type='in'>
                                <filter>
                                    <condition attribute='firstname' operator='eq' value='Mark' />
                                </filter>
                            </link-entity>
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
        public void FoldFilterWithExistsClauseWithoutPrimaryKey()
        {
            _supportedJoins.Add(JoinOperator.Exists);

            try
            {
                var context = new XrmFakedContext();
                context.InitializeMetadata(Assembly.GetExecutingAssembly());

                var org = context.GetOrganizationService();
                var metadata = new AttributeMetadataCache(org);
                var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

                var query = "SELECT name from account where name like 'Data8%' and exists (select * from contact where firstname = 'Mark' and createdon = account.createdon)";

                var plans = planBuilder.Build(query);

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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = "SELECT DISTINCT accountid, name from account";

            var plans = planBuilder.Build(query);

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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = "SELECT DISTINCT accountid, name from account INNER JOIN contact ON account.accountid = contact.parentcustomerid";

            var plans = planBuilder.Build(query);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var fetch = AssertNode<FetchXmlScan>(select.Source);
            AssertFetchXml(fetch, @"
                <fetch distinct='true'>
                    <entity name='account'>
                        <attribute name='accountid' />
                        <attribute name='name' />
                        <link-entity name='contact' alias='contact' from='parentcustomerid' to='accountid' link-type='inner'>
                        </link-entity>
                        <order attribute='accountid' />
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void SimpleDelete()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var planBuilder = new ExecutionPlanBuilder(metadata, new StubTableSizeCache(), this);

            var query = "DELETE FROM account WHERE name = 'bar'";

            var plans = planBuilder.Build(query);

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

        private T AssertNode<T>(IExecutionPlanNode node) where T : IExecutionPlanNode
        {
            Assert.IsInstanceOfType(node, typeof(T));
            return (T)node;
        }

        private void AssertFetchXml(FetchXmlScan node, string fetchXml)
        {
            var serializer = new XmlSerializer(typeof(FetchXml.FetchType));
            using (var reader = new StringReader(fetchXml))
            {
                var fetch = (FetchXml.FetchType)serializer.Deserialize(reader);
                PropertyEqualityAssert.Equals(fetch, node.FetchXml);
            }
        }
    }
}
