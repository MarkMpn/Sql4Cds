using System;
using System.Collections.Generic;
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

namespace MarkMpn.Sql4Cds.Engine.Tests
{
    [TestClass]
    public class ExecutionPlanTests : IQueryExecutionOptions
    {
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

        [TestMethod]
        public void SimpleSelect()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var planBuilder = new ExecutionPlanBuilder(metadata, this);

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
        public void Join()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var planBuilder = new ExecutionPlanBuilder(metadata, this);

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
            var planBuilder = new ExecutionPlanBuilder(metadata, this);

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
        public void SimpleWhere()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var planBuilder = new ExecutionPlanBuilder(metadata, this);

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
            var planBuilder = new ExecutionPlanBuilder(metadata, this);

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
        public void SimpleTop()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var planBuilder = new ExecutionPlanBuilder(metadata, this);

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
            var planBuilder = new ExecutionPlanBuilder(metadata, this);

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
            var planBuilder = new ExecutionPlanBuilder(metadata, this);

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
            var planBuilder = new ExecutionPlanBuilder(metadata, this);

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
            var planBuilder = new ExecutionPlanBuilder(metadata, this);

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
            var planBuilder = new ExecutionPlanBuilder(metadata, this);

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
            var planBuilder = new ExecutionPlanBuilder(metadata, this);

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
            var planBuilder = new ExecutionPlanBuilder(metadata, this);

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
            var planBuilder = new ExecutionPlanBuilder(metadata, this);

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
                            <order attribute='firstname' />
                        </link-entity>
                        <order attribute='name' />
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
            var planBuilder = new ExecutionPlanBuilder(metadata, this);

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
            var planBuilder = new ExecutionPlanBuilder(metadata, this);

            var query = @"
                SELECT firstname + ' ' + lastname AS fullname FROM contact WHERE firstname = 'Mark'";

            var plans = planBuilder.Build(query);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var computeScalar = AssertNode<ComputeScalarNode>(select.Source);
            Assert.AreEqual(1, computeScalar.Columns.Count);
            Assert.AreEqual("firstname + ' ' + lastname", computeScalar.Columns["fullname"].ToSql());
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
            var planBuilder = new ExecutionPlanBuilder(metadata, this);

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
