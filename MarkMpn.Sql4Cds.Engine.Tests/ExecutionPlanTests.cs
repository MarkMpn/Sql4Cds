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
    public class ExecutionPlanTests : FakeXrmEasyTestsBase, IQueryExecutionOptions
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
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
                "owneridtype",
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
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = "SELECT accountid, name FROM account INNER JOIN contact ON account.name = contact.fullname";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var fetch = AssertNode<FetchXmlScan>(select.Source);
            Assert.IsTrue(fetch.UsingCustomPaging);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='accountid' />
                        <attribute name='name' />
                        <link-entity name='contact' alias='contact' from='fullname' to='name' link-type='inner'>
                            <attribute name='contactid' />
                            <order attribute='contactid' />
                        </link-entity>
                        <order attribute='accountid' />
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void NonUniqueJoinExpression()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
                        <filter>
                            <condition attribute='name' operator='not-null' />
                        </filter>
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
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = @"
                SELECT
                    name,
                    count(*),
                    sum(employees)
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
                        <attribute name='employees' aggregate='sum' alias='employees_sum' />
                        <order alias='name' />
                    </entity>
                </fetch>");
            var partitionAggregate = AssertNode<PartitionedAggregateNode>(tryCatch2.CatchSource);
            Assert.AreEqual(1, partitionAggregate.GroupBy.Count);
            Assert.AreEqual("account.name", partitionAggregate.GroupBy[0].GetColumnName());
            Assert.AreEqual(2, partitionAggregate.Aggregates.Count);
            Assert.AreEqual(AggregateType.CountStar, partitionAggregate.Aggregates["count"].AggregateType);
            Assert.AreEqual(AggregateType.Sum, partitionAggregate.Aggregates["employees_sum"].AggregateType);
            Assert.AreEqual("employees_sum", partitionAggregate.Aggregates["employees_sum"].SqlExpression.ToSql());
            var partitionFetch = AssertNode<FetchXmlScan>(partitionAggregate.Source);
            AssertFetchXml(partitionFetch, @"
                <fetch aggregate='true'>
                    <entity name='account'>
                        <attribute name='name' groupby='true' alias='name' />
                        <attribute name='accountid' aggregate='count' alias='count' />
                        <attribute name='employees' aggregate='sum' alias='employees_sum' />
                        <order alias='name' />
                    </entity>
                </fetch>");
            var aggregate = AssertNode<StreamAggregateNode>(tryCatch1.CatchSource);
            Assert.AreEqual(1, aggregate.GroupBy.Count);
            Assert.AreEqual("account.name", aggregate.GroupBy[0].GetColumnName());
            Assert.AreEqual(2, aggregate.Aggregates.Count);
            Assert.AreEqual(AggregateType.CountStar, aggregate.Aggregates["count"].AggregateType);
            Assert.AreEqual(AggregateType.Sum, aggregate.Aggregates["employees_sum"].AggregateType);
            Assert.AreEqual("employees", aggregate.Aggregates["employees_sum"].SqlExpression.ToSql());
            var scalarFetch = AssertNode<FetchXmlScan>(aggregate.Source);
            AssertFetchXml(scalarFetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='name' />
                        <attribute name='employees' />
                        <order attribute='name' />
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void AliasedAggregate()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
        public void GroupByDatePartUsingYearMonthDayFunctions()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = @"
                SELECT
                    YEAR(createdon),
                    MONTH(createdon),
                    DAY(createdon),
                    count(*)
                FROM
                    account
                GROUP BY
                    YEAR(createdOn),
                    MONTH(createdOn),
                    DAY(createdOn)";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            Assert.AreEqual("createdon_year", select.ColumnSet[0].SourceColumn);
            Assert.AreEqual("createdon_month", select.ColumnSet[1].SourceColumn);
            Assert.AreEqual("createdon_day", select.ColumnSet[2].SourceColumn);
            Assert.AreEqual("count", select.ColumnSet[3].SourceColumn);
            var tryCatch1 = AssertNode<TryCatchNode>(select.Source);
            var tryCatch2 = AssertNode<TryCatchNode>(tryCatch1.TrySource);
            var aggregateFetch = AssertNode<FetchXmlScan>(tryCatch2.TrySource);
            AssertFetchXml(aggregateFetch, @"
                <fetch aggregate='true'>
                    <entity name='account'>
                        <attribute name='createdon' groupby='true' alias='createdon_year' dategrouping='year' />
                        <attribute name='createdon' groupby='true' alias='createdon_month' dategrouping='month' />
                        <attribute name='createdon' groupby='true' alias='createdon_day' dategrouping='day' />
                        <attribute name='accountid' aggregate='count' alias='count' />
                        <order alias='createdon_year' />
                        <order alias='createdon_month' />
                        <order alias='createdon_day' />
                    </entity>
                </fetch>");
            var partitionAggregate = AssertNode<PartitionedAggregateNode>(tryCatch2.CatchSource);
            Assert.AreEqual("createdon_year", partitionAggregate.GroupBy[0].ToSql());
            Assert.AreEqual("createdon_month", partitionAggregate.GroupBy[1].ToSql());
            Assert.AreEqual("createdon_day", partitionAggregate.GroupBy[2].ToSql());
            Assert.AreEqual("count", partitionAggregate.Aggregates.Single().Key);
            var partitionFetch = AssertNode<FetchXmlScan>(partitionAggregate.Source);
            AssertFetchXml(partitionFetch, @"
                <fetch aggregate='true'>
                    <entity name='account'>
                        <attribute name='createdon' groupby='true' alias='createdon_year' dategrouping='year' />
                        <attribute name='createdon' groupby='true' alias='createdon_month' dategrouping='month' />
                        <attribute name='createdon' groupby='true' alias='createdon_day' dategrouping='day' />
                        <attribute name='accountid' aggregate='count' alias='count' />
                        <order alias='createdon_year' />
                        <order alias='createdon_month' />
                        <order alias='createdon_day' />
                    </entity>
                </fetch>");
            var aggregate = AssertNode<HashMatchAggregateNode>(tryCatch1.CatchSource);
            Assert.AreEqual("createdon_year", aggregate.GroupBy[0].ToSql());
            Assert.AreEqual("createdon_month", aggregate.GroupBy[1].ToSql());
            Assert.AreEqual("createdon_day", aggregate.GroupBy[2].ToSql());
            Assert.AreEqual("count", aggregate.Aggregates.Single().Key);
            var computeScalar = AssertNode<ComputeScalarNode>(aggregate.Source);
            Assert.AreEqual(3, computeScalar.Columns.Count);
            Assert.AreEqual("YEAR(createdOn)", computeScalar.Columns["createdon_year"].ToSql());
            Assert.AreEqual("MONTH(createdOn)", computeScalar.Columns["createdon_month"].ToSql());
            Assert.AreEqual("DAY(createdOn)", computeScalar.Columns["createdon_day"].ToSql());
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
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
        public void ComputeScalarSelect()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
        public void SelectSubqueryWithChildRecordUsesNestedLoop()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = @"
                SELECT name, (SELECT TOP 1 fullname FROM contact WHERE parentcustomerid = account.accountid) FROM account WHERE name = 'Data8'";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var nestedLoop = AssertNode<NestedLoopNode>(select.Source);
            Assert.AreEqual("@Expr1", nestedLoop.OuterReferences["account.accountid"]);
            var fetch = AssertNode<FetchXmlScan>(nestedLoop.LeftSource);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='name' />
                        <attribute name='accountid' />
                        <filter>
                            <condition attribute='name' operator='eq' value='Data8' />
                        </filter>
                    </entity>
                </fetch>");
            var subTop = AssertNode<TopNode>(nestedLoop.RightSource);
            var subIndexSpool = AssertNode<IndexSpoolNode>(subTop.Source);
            Assert.AreEqual("contact.parentcustomerid", subIndexSpool.KeyColumn);
            Assert.AreEqual("@Expr1", subIndexSpool.SeekValue);
            var subAggregateFetch = AssertNode<FetchXmlScan>(subIndexSpool.Source);
            AssertFetchXml(subAggregateFetch, @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='fullname' />
                        <attribute name='parentcustomerid' />
                        <filter>
                            <condition attribute='parentcustomerid' operator='not-null' />
                        </filter>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void SelectSubqueryWithSmallNestedLoop()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
                <fetch xmlns:generator='MarkMpn.SQL4CDS'>
                    <entity name='account'>
                        <attribute name='name' />
                        <filter>
                            <condition attribute='createdon' operator='eq' value='@Expr2' generator:IsVariable='true' />
                        </filter>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void SelectSubqueryWithNonCorrelatedNestedLoop()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
                            <condition attribute='createdon' operator='not-null' />
                        </filter>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void SelectSubqueryUsingOuterReferenceInSelectClause()
        {
            var tableSize = new StubTableSizeCache();
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
                        <link-entity name='account' alias='Expr2' from='accountid' to='parentcustomerid' link-type='inner'>
                            <attribute name='name' />
                            <filter>
                                <condition attribute='name' operator='eq' value='Data8' />
                            </filter>
                        </link-entity>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void ComputeScalarDistinct()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
        [ExpectedException(typeof(NotSupportedQueryFragmentException))]
        public void SubqueryInFilterMultipleColumnsError()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
                        <link-entity name='contact' alias='Expr1' from='contactid' to='primarycontactid' link-type='inner'>
                            <filter>
                                <condition attribute='firstname' operator='eq' value='Mark' />
                            </filter>
                        </link-entity>
                    </entity>
                </fetch>
            ");
        }

        [TestMethod]
        public void SubqueryInFilterCorrelated()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
                        <link-entity name='contact' alias='Expr2' from='contactid' to='primarycontactid' link-type='inner'>
                        </link-entity>
                    </entity>
                </fetch>
            ");
        }

        [TestMethod]
        public void ExistsFilterCorrelatedPrimaryKeyOr()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = @"
                SELECT
                    accountid,
                    name
                FROM
                    account
                WHERE
                    EXISTS (SELECT * FROM contact WHERE contactid = primarycontactid)
                    OR name = 'Data8'";

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
                        <filter type='or'>
                            <condition entityname='Expr2' attribute='contactid' operator='not-null' />
                            <condition attribute='name' operator='eq' value='Data8' />
                        </filter>
                    </entity>
                </fetch>
            ");
        }

        [TestMethod]
        public void ExistsFilterCorrelated()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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

            var schema = constant.GetSchema(new NodeCompilationContext(_dataSources, this, null));
            Assert.AreEqual(typeof(SqlInt32), schema.Schema["a.ID"].Type.ToNetType(out _));
            Assert.AreEqual(typeof(SqlString), schema.Schema["a.name"].Type.ToNetType(out _));
        }

        [TestMethod]
        public void NoLockTableHint()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
        public void CaseSensitiveMetadataWhere()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = @"
                SELECT logicalname
                FROM   metadata.entity
                WHERE  logicalname = 'Account' AND schemaname = 'Account'";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var filter = AssertNode<FilterNode>(select.Source);
            var meta = AssertNode<MetadataQueryNode>(filter.Source);

            // We don't know the case of the schema name, so need to do this condition in a filter node to do it case insensitively
            Assert.AreEqual("schemaname = 'Account'", filter.Filter.ToSql());

            // We know logical names are lower case so we can fold this part of the filter into the data source.
            Assert.AreEqual(MetadataSource.Entity, meta.MetadataSource);
            Assert.AreEqual("entity", meta.EntityAlias);
            CollectionAssert.AreEqual(new[] { "LogicalName", "SchemaName" }, meta.Query.Properties.PropertyNames);
            Assert.AreEqual(1, meta.Query.Criteria.Conditions.Count);
            Assert.AreEqual("LogicalName", meta.Query.Criteria.Conditions[0].PropertyName);
            Assert.AreEqual(MetadataConditionOperator.Equals, meta.Query.Criteria.Conditions[0].ConditionOperator);
            Assert.AreEqual("account", meta.Query.Criteria.Conditions[0].Value);
        }

        [TestMethod]
        public void SimpleUpdate()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = "UPDATE account SET name = 'foo' WHERE name = 'bar'";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var update = AssertNode<UpdateNode>(plans[0]);
            Assert.AreEqual("account", update.LogicalName);
            Assert.AreEqual("account.accountid", update.PrimaryIdSource);
            Assert.AreEqual("Expr1", update.ColumnMappings["name"].NewValueColumn);
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
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = "UPDATE a SET name = 'foo' FROM account a INNER JOIN contact c ON a.accountid = c.parentcustomerid WHERE name = 'bar'";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var update = AssertNode<UpdateNode>(plans[0]);
            Assert.AreEqual("account", update.LogicalName);
            Assert.AreEqual("a.accountid", update.PrimaryIdSource);
            Assert.AreEqual("Expr1", update.ColumnMappings["name"].NewValueColumn);
            var distinct = AssertNode<DistinctNode>(update.Source);
            var computeScalar = AssertNode<ComputeScalarNode>(distinct.Source);
            Assert.AreEqual("'foo'", computeScalar.Columns["Expr1"].ToSql());
            var fetch = AssertNode<FetchXmlScan>(computeScalar.Source);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='contact'>
                        <link-entity name='account' alias='a' from='accountid' to='parentcustomerid' link-type='inner'>
                            <attribute name='accountid' />
                            <filter>
                                <condition attribute='name' operator='eq' value='bar' />
                            </filter>
                        </link-entity>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void QueryHints()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = "SELECT name from account where name like 'Data8%' and primarycontactid in (select contactid from contact where firstname = 'Mark')";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var fetch = AssertNode<FetchXmlScan>(select.Source);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='name' />
                        <link-entity name='contact' alias='Expr1' from='contactid' to='primarycontactid' link-type='inner'>
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

        [TestMethod]
        public void FoldFilterWithInClauseOr()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = "SELECT name from account where name like 'Data8%' or primarycontactid in (select contactid from contact where firstname = 'Mark')";

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
                        <filter type='or'>
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
                var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
                var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
                            <filter>
                                <condition attribute='name' operator='like' value='Data8%' />
                            </filter>
                        </link-entity>
                        <link-entity name='contact' alias='Expr1' from='createdon' to='createdon' link-type='in'>
                            <filter>
                                <condition attribute='firstname' operator='eq' value='Mark' />
                            </filter>
                        </link-entity>
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
                var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = "SELECT fullname, lastname + ', ' + firstname as fullname FROM contact WHERE firstname = 'Mark'";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            Assert.AreEqual(2, select.ColumnSet.Count);
            Assert.AreEqual("contact.fullname", select.ColumnSet[0].SourceColumn);
            Assert.AreEqual("fullname", select.ColumnSet[0].OutputColumn);
            Assert.AreEqual("Expr1", select.ColumnSet[1].SourceColumn);
            Assert.AreEqual("fullname", select.ColumnSet[1].OutputColumn);
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
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = "SELECT * FROM (SELECT fullname, lastname + ', ' + firstname as fullname FROM contact WHERE firstname = 'Mark') a";

            planBuilder.Build(query, null, out _);
        }

        [TestMethod]
        public void UnionDuplicateColumnNames()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
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
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
                        <all-attributes />
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
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = @"SELECT contact.* FROM account WHERE accountid IN (SELECT parentcustomerid FROM contact)";

            planBuilder.Build(query, null, out _);
        }

        [TestMethod]
        public void MinAggregateNotFoldedToFetchXmlForOptionset()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
            
            var result = select.Execute(new NodeExecutionContext(_localDataSource, this, new Dictionary<string, DataTypeReference>(), new Dictionary<string, object>()), CommandBehavior.Default);
            var dataTable = new DataTable();
            dataTable.Load(result);

            Assert.AreEqual(2, dataTable.Rows.Count);
            Assert.AreEqual("Mark", dataTable.Rows[0][0]);
            Assert.AreEqual("Mark", dataTable.Rows[1][0]);
        }

        [TestMethod]
        public void SelectVirtualNameAttributeFromLinkEntity()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
        public void AliasedAttribute()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = "SELECT name AS n1 FROM account WHERE name = 'test'";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            Assert.AreEqual("account.n1", select.ColumnSet[0].SourceColumn);
            Assert.AreEqual("n1", select.ColumnSet[0].OutputColumn);

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
        public void MultipleAliases()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = "SELECT name AS n1, name AS n2 FROM account WHERE name = 'test'";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            Assert.AreEqual("account.name", select.ColumnSet[0].SourceColumn);
            Assert.AreEqual("account.name", select.ColumnSet[1].SourceColumn);
            Assert.AreEqual("n1", select.ColumnSet[0].OutputColumn);
            Assert.AreEqual("n2", select.ColumnSet[1].OutputColumn);

            var fetch = AssertNode<FetchXmlScan>(select.Source);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='name' />
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
                    TableSizeCache = new StubTableSizeCache(),
                    MessageCache = new StubMessageCache(),
                    DefaultCollation = Collation.USEnglish
                },
                new DataSource
                {
                    Name = "prod",
                    Connection = _context2.GetOrganizationService(),
                    Metadata = metadata2,
                    TableSizeCache = new StubTableSizeCache(),
                    MessageCache = new StubMessageCache(),
                    DefaultCollation = Collation.USEnglish
                },
                new DataSource
                {
                    Name = "local", // Hack so that ((IQueryExecutionOptions)this).PrimaryDataSource = "local" doesn't cause test to fail
                    DefaultCollation = Collation.USEnglish
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
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
            Assert.AreEqual("createdon_day", grouping.GroupBy[0].ToSql());
            Assert.AreEqual("createdon_month", grouping.GroupBy[1].ToSql());
            Assert.AreEqual("createdon_year", grouping.GroupBy[2].ToSql());
            var calc = AssertNode<ComputeScalarNode>(grouping.Source);
            Assert.AreEqual("DAY(a.createdon)", calc.Columns["createdon_day"].ToSql());
            Assert.AreEqual("MONTH(a.createdon)", calc.Columns["createdon_month"].ToSql());
            Assert.AreEqual("YEAR(a.createdon)", calc.Columns["createdon_year"].ToSql());
            var filter = AssertNode<FilterNode>(calc.Source);
            Assert.AreEqual("YEAR(a.createdon) = 2021 AND MONTH(a.createdon) = 11", filter.Filter.ToSql().Replace("\r\n", " "));
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
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = "SELECT DISTINCT account.accountid FROM metadata.entity INNER JOIN account ON entity.metadataid = account.accountid";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            Assert.AreEqual("account.accountid", select.ColumnSet.Single().SourceColumn);
            var aggregate = AssertNode<StreamAggregateNode>(select.Source);
            Assert.AreEqual("account.accountid", aggregate.GroupBy.Single().ToSql());
            var merge = AssertNode<MergeJoinNode>(aggregate.Source);
            var fetch = AssertNode<FetchXmlScan>(merge.LeftSource);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='accountid' />
                        <order attribute='accountid' />
                    </entity>
                </fetch>");
            var sort = AssertNode<SortNode>(merge.RightSource);
            Assert.AreEqual("entity.metadataid ASC", sort.Sorts[0].ToSql());
            var meta = AssertNode<MetadataQueryNode>(sort.Source);
        }

        [TestMethod]
        [ExpectedException(typeof(NotSupportedQueryFragmentException))]
        public void WindowFunctionsNotSupported()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = "SELECT COUNT(accountid) OVER(PARTITION BY accountid) AS test FROM account";

            planBuilder.Build(query, null, out _);
        }

        [TestMethod]
        public void DeclareVariableSetLiteralSelect()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
            var setConstantScan = AssertNode<ConstantScanNode>(setVariable.Source);
            Assert.AreEqual(1, setConstantScan.Values.Count);
            Assert.AreEqual("CONVERT (INT, 1)", setConstantScan.Values[0]["Expr1"].ToSql());

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
                if (plan is IDataReaderExecutionPlanNode selectQuery)
                {
                    var results = selectQuery.Execute(new NodeExecutionContext(_dataSources, this, parameterTypes, parameterValues), CommandBehavior.Default);
                    var dataTable = new DataTable();
                    dataTable.Load(results);

                    Assert.AreEqual(1, dataTable.Rows.Count);
                    Assert.AreEqual(1, dataTable.Columns.Count);
                    Assert.AreEqual(1, dataTable.Rows[0][0]);
                }
                else if (plan is IDmlQueryExecutionPlanNode dmlQuery)
                {
                    dmlQuery.Execute(new NodeExecutionContext(_dataSources, this, parameterTypes, parameterValues), out _);
                }
            }
        }

        [TestMethod]
        public void SetVariableInDeclaration()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
            var setConstantScan = AssertNode<ConstantScanNode>(setVariable.Source);
            Assert.AreEqual(1, setConstantScan.Values.Count);
            Assert.AreEqual("CONVERT (INT, 1)", setConstantScan.Values[0]["Expr1"].ToSql());

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
                if (plan is IDataReaderExecutionPlanNode selectQuery)
                {
                    var results = selectQuery.Execute(new NodeExecutionContext(_dataSources, this, parameterTypes, parameterValues), CommandBehavior.Default);
                    var dataTable = new DataTable();
                    dataTable.Load(results);

                    Assert.AreEqual(1, dataTable.Rows.Count);
                    Assert.AreEqual(1, dataTable.Columns.Count);
                    Assert.AreEqual(1, dataTable.Rows[0][0]);
                }
                else if (plan is IDmlQueryExecutionPlanNode dmlQuery)
                {
                    dmlQuery.Execute(new NodeExecutionContext(_dataSources, this, parameterTypes, parameterValues), out _);
                }
            }
        }

        [TestMethod]
        [ExpectedException(typeof(NotSupportedQueryFragmentException))]
        public void UnknownVariable()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = "SET @test = 1";

            planBuilder.Build(query, null, out _);
        }

        [TestMethod]
        [ExpectedException(typeof(NotSupportedQueryFragmentException))]
        public void DuplicateVariable()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = @"
                DECLARE @test INT
                DECLARE @test INT";

            planBuilder.Build(query, null, out _);
        }

        [TestMethod]
        public void VariableTypeConversionIntToString()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = @"
                DECLARE @test varchar(3)
                SET @test = 100
                SELECT @test";

            var plans = planBuilder.Build(query, null, out _);

            var parameterTypes = new Dictionary<string, DataTypeReference>();
            var parameterValues = new Dictionary<string, object>();

            foreach (var plan in plans)
            {
                if (plan is IDataReaderExecutionPlanNode selectQuery)
                {
                    var results = selectQuery.Execute(new NodeExecutionContext(_localDataSource, this, parameterTypes, parameterValues), CommandBehavior.Default);
                    var dataTable = new DataTable();
                    dataTable.Load(results);

                    Assert.AreEqual(1, dataTable.Rows.Count);
                    Assert.AreEqual(1, dataTable.Columns.Count);
                    Assert.AreEqual("100", dataTable.Rows[0][0]);
                }
                else if (plan is IDmlQueryExecutionPlanNode dmlQuery)
                {
                    dmlQuery.Execute(new NodeExecutionContext(_localDataSource, this, parameterTypes, parameterValues), out _);
                }
            }
        }

        [TestMethod]
        public void VariableTypeConversionStringTruncation()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = @"
                DECLARE @test varchar(3)
                SET @test = 'test'
                SELECT @test";

            var plans = planBuilder.Build(query, null, out _);

            var parameterTypes = new Dictionary<string, DataTypeReference>();
            var parameterValues = new Dictionary<string, object>();

            foreach (var plan in plans)
            {
                if (plan is IDataReaderExecutionPlanNode selectQuery)
                {
                    var results = selectQuery.Execute(new NodeExecutionContext(_localDataSource, this, parameterTypes, parameterValues), CommandBehavior.Default);
                    var dataTable = new DataTable();
                    dataTable.Load(results);

                    Assert.AreEqual(1, dataTable.Rows.Count);
                    Assert.AreEqual(1, dataTable.Columns.Count);
                    Assert.AreEqual("tes", dataTable.Rows[0][0]);
                }
                else if (plan is IDmlQueryExecutionPlanNode dmlQuery)
                {
                    dmlQuery.Execute(new NodeExecutionContext(_localDataSource, this, parameterTypes, parameterValues), out _);
                }
            }
        }

        [TestMethod]
        [ExpectedException(typeof(NotSupportedQueryFragmentException))]
        public void CannotCombineSetVariableAndDataRetrievalInSelect()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            // A SELECT statement that assigns a value to a variable must not be combined with data-retrieval operations
            var query = @"
                DECLARE @test varchar(3)
                SELECT @test = name, accountid FROM account";

            planBuilder.Build(query, null, out _);
        }

        [TestMethod]
        public void SetVariableWithSelectUsesFinalValue()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

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
                if (plan is IDataReaderExecutionPlanNode selectQuery)
                {
                    var results = selectQuery.Execute(new NodeExecutionContext(_localDataSource, this, parameterTypes, parameterValues), CommandBehavior.Default);
                    var dataTable = new DataTable();
                    dataTable.Load(results);

                    Assert.AreEqual(1, dataTable.Rows.Count);
                    Assert.AreEqual(1, dataTable.Columns.Count);
                    Assert.AreEqual("Z", dataTable.Rows[0][0]);
                }
                else if (plan is IDmlQueryExecutionPlanNode dmlQuery)
                {
                    dmlQuery.Execute(new NodeExecutionContext(_localDataSource, this, parameterTypes, parameterValues), out _);
                }
            }
        }

        [TestMethod]
        public void VarCharLengthDefaultsTo1()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = @"
                DECLARE @test varchar
                SET @test = 'test'
                SELECT @test";

            var plans = planBuilder.Build(query, null, out _);

            var parameterTypes = new Dictionary<string, DataTypeReference>();
            var parameterValues = new Dictionary<string, object>();

            foreach (var plan in plans)
            {
                if (plan is IDataReaderExecutionPlanNode selectQuery)
                {
                    var results = selectQuery.Execute(new NodeExecutionContext(_localDataSource, this, parameterTypes, parameterValues), CommandBehavior.Default);
                    var dataTable = new DataTable();
                    dataTable.Load(results);

                    Assert.AreEqual(1, dataTable.Rows.Count);
                    Assert.AreEqual(1, dataTable.Columns.Count);
                    Assert.AreEqual("t", dataTable.Rows[0][0]);
                }
                else if (plan is IDmlQueryExecutionPlanNode dmlQuery)
                {
                    dmlQuery.Execute(new NodeExecutionContext(_localDataSource, this, parameterTypes, parameterValues), out _);
                }
            }
        }

        [TestMethod]
        [ExpectedException(typeof(NotSupportedQueryFragmentException))]
        public void CursorVariableNotSupported()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = @"
                DECLARE @test CURSOR";

            planBuilder.Build(query, null, out _);
        }

        [TestMethod]
        [ExpectedException(typeof(NotSupportedQueryFragmentException))]
        public void TableVariableNotSupported()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = @"
                DECLARE @test TABLE (ID INT)";

            planBuilder.Build(query, null, out _);
        }

        [TestMethod]
        public void IfStatement()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this) { EstimatedPlanOnly = true };

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
                ["@param1"] = typeof(SqlInt32).ToSqlType(null)
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
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this) { EstimatedPlanOnly = true };

            var query = @"
                WHILE @param1 < 10
                BEGIN
                    INSERT INTO account (name) VALUES (@param1)
                    SET @param1 += 1
                END";

            var parameters = new Dictionary<string, DataTypeReference>
            {
                ["@param1"] = typeof(SqlInt32).ToSqlType(null)
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
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this) { EstimatedPlanOnly = true };

            var query = @"
                IF NOT EXISTS(SELECT * FROM account WHERE name = @param1)
                BEGIN
                    INSERT INTO account (name) VALUES (@param1)
                END";

            var parameters = new Dictionary<string, DataTypeReference>
            {
                ["@param1"] = typeof(SqlString).ToSqlType(null)
            };
            var plans = planBuilder.Build(query, parameters, out _);

            Assert.AreEqual(1, plans.Length);

            var cond = AssertNode<ConditionalNode>(plans[0]);

            Assert.AreEqual(1, cond.TrueStatements.Length);
            AssertNode<InsertNode>(cond.TrueStatements[0]);
        }

        [TestMethod]
        public void DuplicatedAliases()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = "SELECT name, createdon AS name FROM account";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var fetch = AssertNode<FetchXmlScan>(select.Source);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='name' />
                        <attribute name='createdon' />
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void MetadataLeftJoinData()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = @"
                SELECT entity.logicalname, account.name, contact.firstname 
                FROM 
                    metadata.entity 
                    LEFT OUTER JOIN account ON entity.metadataid = account.accountid 
                    LEFT OUTER JOIN contact ON account.primarycontactid = contact.contactid
                WHERE
                    entity.logicalname = 'account'";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var nestedLoop = AssertNode<NestedLoopNode>(select.Source);
            Assert.AreEqual(QualifiedJoinType.LeftOuter, nestedLoop.JoinType);
            Assert.AreEqual(1, nestedLoop.OuterReferences.Count);
            Assert.AreEqual("@Cond1", nestedLoop.OuterReferences["entity.metadataid"]);
            var meta = AssertNode<MetadataQueryNode>(nestedLoop.LeftSource);
            var fetch = AssertNode<FetchXmlScan>(nestedLoop.RightSource);
            AssertFetchXml(fetch, @"
                <fetch xmlns:generator='MarkMpn.SQL4CDS'>
                    <entity name='account'>
                        <attribute name='name' />
                        <link-entity name='contact' alias='contact' from='contactid' to='primarycontactid' link-type='outer'>
                            <attribute name='firstname' />
                        </link-entity>
                        <filter>
                            <condition attribute='accountid' operator='eq' value='@Cond1' generator:IsVariable='true' />
                        </filter>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void NotEqualExcludesNull()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = @"
                SELECT name FROM account WHERE name <> 'Data8'";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var fetch = AssertNode<FetchXmlScan>(select.Source);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='name' />
                        <filter>
                            <condition attribute='name' operator='ne' value='Data8' />
                            <condition attribute='name' operator='not-null' />
                        </filter>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void DoNotFoldFilterOnNameVirtualAttributeWithTooManyJoins()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = @"
                select top 10 a.name
                from account a
                    join contact c1 on c1.parentcustomerid = a.accountid
                    join contact c2 on c2.parentcustomerid = a.accountid
                    join contact c3 on c3.parentcustomerid = a.accountid
                    join contact c4 on c4.parentcustomerid = a.accountid
                    join contact c5 on c5.parentcustomerid = a.accountid
                    join contact c6 on c6.parentcustomerid = a.accountid
                    join contact c7 on c7.parentcustomerid = a.accountid
                    join contact c8 on c8.parentcustomerid = a.accountid
                    join contact c9 on c9.parentcustomerid = a.accountid
                    join contact c10 on c10.parentcustomerid = a.accountid
                    join contact c11 on c11.parentcustomerid = a.accountid
                where a.primarycontactidname = 'Test'";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var top = AssertNode<TopNode>(select.Source);
            var join = AssertNode<HashJoinNode>(top.Source);
            var fetch1 = AssertNode<FetchXmlScan>(join.LeftSource);
            AssertFetchXml(fetch1, @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='parentcustomerid' />
                        <filter>
                            <condition attribute='parentcustomerid' operator='not-null' />
                        </filter>
                    </entity>
                </fetch>");
            var fetch2 = AssertNode<FetchXmlScan>(join.RightSource);
            Assert.IsTrue(fetch2.UsingCustomPaging);
            AssertFetchXml(fetch2, @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='contactid' />
                        <link-entity name='account' alias='a' from='accountid' to='parentcustomerid' link-type='inner'>
                            <attribute name='name' />
                            <attribute name='accountid' />
                            <link-entity name='contact' alias='c2' from='parentcustomerid' to='accountid' link-type='inner'>
                                <attribute name='contactid' />
                                <order attribute='contactid' />
                            </link-entity>
                            <link-entity name='contact' alias='c3' from='parentcustomerid' to='accountid' link-type='inner'>
                                <attribute name='contactid' />
                                <order attribute='contactid' />
                            </link-entity>
                            <link-entity name='contact' alias='c4' from='parentcustomerid' to='accountid' link-type='inner'>
                                <attribute name='contactid' />
                                <order attribute='contactid' />
                            </link-entity>
                            <link-entity name='contact' alias='c5' from='parentcustomerid' to='accountid' link-type='inner'>
                                <attribute name='contactid' />
                                <order attribute='contactid' />
                            </link-entity>
                            <link-entity name='contact' alias='c6' from='parentcustomerid' to='accountid' link-type='inner'>
                                <attribute name='contactid' />
                                <order attribute='contactid' />
                            </link-entity>
                            <link-entity name='contact' alias='c7' from='parentcustomerid' to='accountid' link-type='inner'>
                                <attribute name='contactid' />
                                <order attribute='contactid' />
                            </link-entity>
                            <link-entity name='contact' alias='c8' from='parentcustomerid' to='accountid' link-type='inner'>
                                <attribute name='contactid' />
                                <order attribute='contactid' />
                            </link-entity>
                            <link-entity name='contact' alias='c9' from='parentcustomerid' to='accountid' link-type='inner'>
                                <attribute name='contactid' />
                                <order attribute='contactid' />
                            </link-entity>
                            <link-entity name='contact' alias='c10' from='parentcustomerid' to='accountid' link-type='inner'>
                                <attribute name='contactid' />
                                <order attribute='contactid' />
                            </link-entity>
                            <filter>
                                <condition attribute='primarycontactidname' operator='eq' value='Test' />
                            </filter>
                            <order attribute='accountid' />
                        </link-entity>
                        <order attribute='contactid' />
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void FilterOnVirtualTypeAttributeEquals()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = @"SELECT firstname FROM contact WHERE parentcustomeridtype = 'contact'";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var fetch = AssertNode<FetchXmlScan>(select.Source);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='firstname' />
                        <filter>
                            <condition attribute='parentcustomeridtype' operator='eq' value='2' />
                        </filter>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void FilterOnVirtualTypeAttributeEqualsImpossible()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = @"SELECT firstname FROM contact WHERE parentcustomeridtype = 'non-existent-entity'";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var fetch = AssertNode<FetchXmlScan>(select.Source);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='firstname' />
                        <filter>
                            <condition attribute='parentcustomeridtype' operator='eq' value='0' />
                        </filter>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void FilterOnVirtualTypeAttributeNotEquals()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = @"SELECT firstname FROM contact WHERE parentcustomeridtype <> 'contact'";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var fetch = AssertNode<FetchXmlScan>(select.Source);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='firstname' />
                        <filter>
                            <condition attribute='parentcustomeridtype' operator='ne' value='2' />
                            <condition attribute='parentcustomeridtype' operator='not-null' />
                        </filter>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void FilterOnVirtualTypeAttributeNotInImpossible()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = @"SELECT firstname FROM contact WHERE parentcustomeridtype NOT IN ('account', 'contact')";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var fetch = AssertNode<FetchXmlScan>(select.Source);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='firstname' />
                        <filter>
                            <condition attribute='parentcustomeridtype' operator='not-in'>
                                <value>1</value>
                                <value>2</value>
                            </condition>
                        </filter>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void FilterOnVirtualTypeAttributeNull()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = @"SELECT firstname FROM contact WHERE parentcustomeridtype IS NULL";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var fetch = AssertNode<FetchXmlScan>(select.Source);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='firstname' />
                        <filter>
                            <condition attribute='parentcustomeridtype' operator='null' />
                        </filter>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void FilterOnVirtualTypeAttributeNotNull()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = @"SELECT firstname FROM contact WHERE parentcustomeridtype IS NOT NULL";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var fetch = AssertNode<FetchXmlScan>(select.Source);
            AssertFetchXml(fetch, @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='firstname' />
                        <filter>
                            <condition attribute='parentcustomeridtype' operator='not-null' />
                        </filter>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void SubqueriesInValueList()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = @"SELECT a FROM (VALUES ('a'), ((SELECT TOP 1 firstname FROM contact)), ('b'), (1)) AS MyTable (a)";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var concat = AssertNode<ConcatenateNode>(select.Source);
            var a = AssertNode<ConstantScanNode>(concat.Sources[0]);
            var firstname = AssertNode<ComputeScalarNode>(concat.Sources[1]);
            var loop = AssertNode<NestedLoopNode>(firstname.Source);
            var firstnamePlaceholder = AssertNode<ConstantScanNode>(loop.LeftSource);
            var fetch = AssertNode<FetchXmlScan>(loop.RightSource);
            var b = AssertNode<ConstantScanNode>(concat.Sources[2]);
            var one = AssertNode<ConstantScanNode>(concat.Sources[3]);

            AssertFetchXml(fetch, @"
                <fetch top='1'>
                    <entity name='contact'>
                        <attribute name='firstname' />
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void FoldFilterOnIdentity()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = @"SELECT name FROM account WHERE accountid = @@IDENTITY";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var fetch = AssertNode<FetchXmlScan>(select.Source);

            AssertFetchXml(fetch, @"
                <fetch xmlns:generator='MarkMpn.SQL4CDS'>
                    <entity name='account'>
                        <attribute name='name' />
                        <filter>
                            <condition attribute='accountid' operator='eq' value='@@IDENTITY' generator:IsVariable='true' />
                        </filter>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void FoldPrimaryIdInQuery()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = @"SELECT name FROM account WHERE accountid IN (SELECT accountid FROM account INNER JOIN contact ON account.primarycontactid = contact.contactid WHERE name = 'Data8')";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var fetch = AssertNode<FetchXmlScan>(select.Source);

            AssertFetchXml(fetch, @"
                <fetch xmlns:generator='MarkMpn.SQL4CDS'>
                    <entity name='account'>
                        <attribute name='name' />
                        <link-entity name='contact' alias='contact' from='contactid' to='primarycontactid' link-type='inner' />
                        <filter>
                            <condition attribute='name' operator='eq' value='Data8' />
                        </filter>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void FoldPrimaryIdInQueryWithTop()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = @"DELETE FROM account WHERE accountid IN (SELECT TOP 10 accountid FROM account ORDER BY createdon DESC)";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var delete = AssertNode<DeleteNode>(plans[0]);
            var fetch = AssertNode<FetchXmlScan>(delete.Source);

            AssertFetchXml(fetch, @"
                <fetch xmlns:generator='MarkMpn.SQL4CDS' top='10'>
                    <entity name='account'>
                        <attribute name='accountid' />
                        <order attribute='createdon' descending='true' />
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void InsertParameters()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = @"DECLARE @name varchar(100) = 'test'; INSERT INTO account (name) VALUES (@name)";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(3, plans.Length);

            AssertNode<DeclareVariablesNode>(plans[0]);
            AssertNode<AssignVariablesNode>(plans[1]);
            var insert = AssertNode<InsertNode>(plans[2]);
            var compute = AssertNode<ComputeScalarNode>(insert.Source);
            var constant = AssertNode<ConstantScanNode>(compute.Source);
        }

        [TestMethod]
        public void NotExistsParameters()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = @"DECLARE @firstname AS VARCHAR (100) = 'Mark', @lastname AS VARCHAR (100) = 'Carrington';

IF NOT EXISTS (SELECT * FROM contact WHERE firstname = @firstname AND lastname = @lastname)
BEGIN
    INSERT INTO contact (firstname, lastname)
    VALUES              (@firstname, @lastname);
END";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(4, plans.Length);

            AssertNode<DeclareVariablesNode>(plans[0]);
            AssertNode<AssignVariablesNode>(plans[1]);
            AssertNode<AssignVariablesNode>(plans[2]);
            var cond = AssertNode<ConditionalNode>(plans[3]);
            var compute = AssertNode<ComputeScalarNode>(cond.Source);
            var loop = AssertNode<NestedLoopNode>(compute.Source);
            var constant = AssertNode<ConstantScanNode>(loop.LeftSource);
            var fetch = AssertNode<FetchXmlScan>(loop.RightSource);
            var insert = AssertNode<InsertNode>(cond.TrueStatements[0]);
            var insertCompute = AssertNode<ComputeScalarNode>(insert.Source);
            var insertConstant = AssertNode<ConstantScanNode>(insertCompute.Source);

            AssertFetchXml(fetch, @"
                <fetch xmlns:generator='MarkMpn.SQL4CDS' top='1'>
                    <entity name='contact'>
                        <attribute name='contactid' />
                        <filter>
                            <condition attribute='firstname' operator='eq' value='@firstname' generator:IsVariable='true' />
                            <condition attribute='lastname' operator='eq' value='@lastname' generator:IsVariable='true' />
                        </filter>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void UpdateParameters()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = @"declare @name varchar(100) = 'Data8', @employees int = 10
UPDATE account SET employees = @employees WHERE name = @name";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(4, plans.Length);

            AssertNode<DeclareVariablesNode>(plans[0]);
            AssertNode<AssignVariablesNode>(plans[1]);
            AssertNode<AssignVariablesNode>(plans[2]);
            var update = AssertNode<UpdateNode>(plans[3]);
            var compute = AssertNode<ComputeScalarNode>(update.Source);
            Assert.AreEqual(compute.Columns[update.ColumnMappings["employees"].NewValueColumn].ToSql(), "@employees");
            var fetch = AssertNode<FetchXmlScan>(compute.Source);

            AssertFetchXml(fetch, @"
                <fetch xmlns:generator='MarkMpn.SQL4CDS'>
                    <entity name='account'>
                        <attribute name='accountid' />
                        <filter>
                            <condition attribute='name' operator='eq' value='@name' generator:IsVariable='true' />
                        </filter>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void CountUsesAggregateByDefault()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = @"SELECT count(*) FROM account";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var tryCatch1 = AssertNode<TryCatchNode>(select.Source);
            var tryCatch2 = AssertNode<TryCatchNode>(tryCatch1.TrySource);
            var fetch = AssertNode<FetchXmlScan>(tryCatch2.TrySource);

            AssertFetchXml(fetch, @"
                <fetch aggregate='true'>
                    <entity name='account'>
                        <attribute name='accountid' aggregate='count' alias='count' />
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void CountUsesRetrieveTotalRecordCountWithHint()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = @"SELECT count(*) FROM account OPTION (USE HINT ('RETRIEVE_TOTAL_RECORD_COUNT'))";

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
        public void MaxDOPUsesDefault()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = @"UPDATE account SET name = 'test'";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var update = AssertNode<UpdateNode>(plans[0]);
            Assert.AreEqual(((IQueryExecutionOptions)this).MaxDegreeOfParallelism, update.MaxDOP);
        }

        [TestMethod]
        public void MaxDOPUsesHint()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = @"UPDATE account SET name = 'test' OPTION (MAXDOP 7)";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var update = AssertNode<UpdateNode>(plans[0]);
            Assert.AreEqual(7, update.MaxDOP);
        }

        [TestMethod]
        public void SubqueryUsesSpoolByDefault()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = @"SELECT accountid, (SELECT TOP 1 fullname FROM contact) FROM account";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var loop = AssertNode<NestedLoopNode>(select.Source);
            var accountFetch = AssertNode<FetchXmlScan>(loop.LeftSource);
            var spool = AssertNode<TableSpoolNode>(loop.RightSource);
            var contactFetch = AssertNode<FetchXmlScan>(spool.Source);
        }

        [TestMethod]
        public void SubqueryDoesntUseSpoolWithHint()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = @"SELECT accountid, (SELECT TOP 1 fullname FROM contact) FROM account OPTION (NO_PERFORMANCE_SPOOL)";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var loop = AssertNode<NestedLoopNode>(select.Source);
            var accountFetch = AssertNode<FetchXmlScan>(loop.LeftSource);
            var contactFetch = AssertNode<FetchXmlScan>(loop.RightSource);
        }

        [TestMethod]
        public void BypassPluginExecutionUsesDefault()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = @"UPDATE account SET name = 'test'";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var update = AssertNode<UpdateNode>(plans[0]);
            Assert.AreEqual(false, update.BypassCustomPluginExecution);
        }

        [TestMethod]
        public void BypassPluginExecutionUsesHint()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = @"UPDATE account SET name = 'test' OPTION (USE HINT ('BYPASS_CUSTOM_PLUGIN_EXECUTION'))";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var update = AssertNode<UpdateNode>(plans[0]);
            Assert.AreEqual(true, update.BypassCustomPluginExecution);
        }

        [TestMethod]
        public void PageSizeUsesHint()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = @"SELECT name FROM account OPTION (USE HINT ('FETCHXML_PAGE_SIZE_100'))";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);
            var fetch = AssertNode<FetchXmlScan>(select.Source);

            AssertFetchXml(fetch, @"
                <fetch xmlns:generator='MarkMpn.SQL4CDS' count='100'>
                    <entity name='account'>
                        <attribute name='name' />
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void DistinctOrderByOptionSet()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = "SELECT DISTINCT new_optionsetvalue FROM new_customentity ORDER BY new_optionsetvalue";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);
            var select = AssertNode<SelectNode>(plans[0]);
            var sort = AssertNode<SortNode>(select.Source);
            var fetch = AssertNode<FetchXmlScan>(sort.Source);

            AssertFetchXml(fetch, @"
                <fetch xmlns:generator='MarkMpn.SQL4CDS' distinct='true'>
                    <entity name='new_customentity'>
                        <attribute name='new_optionsetvalue' />
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void DistinctVirtualAttribute()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = "SELECT DISTINCT new_optionsetvaluename FROM new_customentity";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);
            var select = AssertNode<SelectNode>(plans[0]);
            var aggregate = AssertNode<StreamAggregateNode>(select.Source);
            Assert.AreEqual(1, aggregate.GroupBy.Count);
            Assert.AreEqual("new_customentity.new_optionsetvaluename", aggregate.GroupBy[0].ToSql());
            var fetch = AssertNode<FetchXmlScan>(aggregate.Source);

            AssertFetchXml(fetch, @"
                <fetch xmlns:generator='MarkMpn.SQL4CDS' distinct='true'>
                    <entity name='new_customentity'>
                        <attribute name='new_optionsetvalue' />
                        <order attribute='new_optionsetvalue' />
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void TopAliasStar()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = "SELECT TOP 10 A.* FROM account A";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);
            var select = AssertNode<SelectNode>(plans[0]);
            var fetch = AssertNode<FetchXmlScan>(select.Source);

            AssertFetchXml(fetch, @"
                <fetch xmlns:generator='MarkMpn.SQL4CDS' top='10'>
                    <entity name='account'>
                        <all-attributes />
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void OrderByStar()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = "SELECT * FROM account ORDER BY primarycontactid";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);
            var select = AssertNode<SelectNode>(plans[0]);
            var sort = AssertNode<SortNode>(select.Source);
            var fetch = AssertNode<FetchXmlScan>(sort.Source);

            AssertFetchXml(fetch, @"
                <fetch xmlns:generator='MarkMpn.SQL4CDS'>
                    <entity name='account'>
                        <all-attributes />
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void UpdateColumnInWhereClause()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = "UPDATE account SET name = '1' WHERE name <> '1'";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);
            var update = AssertNode<UpdateNode>(plans[0]);
            Assert.AreEqual("Expr1", update.ColumnMappings["name"].NewValueColumn);

            var compute = AssertNode<ComputeScalarNode>(update.Source);
            Assert.AreEqual("'1'", compute.Columns["Expr1"].ToSql());

            var fetch = AssertNode<FetchXmlScan>(compute.Source);

            AssertFetchXml(fetch, @"
                <fetch xmlns:generator='MarkMpn.SQL4CDS'>
                    <entity name='account'>
                        <attribute name='accountid' />
                        <filter>
                            <condition attribute='name' operator='ne' value='1' />
                            <condition attribute='name' operator='not-null' />
                        </filter>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void NestedOrFilters()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = "SELECT * FROM account WHERE name = '1' OR name = '2' OR name = '3' OR name = '4'";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);
            var select = AssertNode<SelectNode>(plans[0]);
            var fetch = AssertNode<FetchXmlScan>(select.Source);

            AssertFetchXml(fetch, @"
                <fetch xmlns:generator='MarkMpn.SQL4CDS'>
                    <entity name='account'>
                        <all-attributes />
                        <filter type='or'>
                            <condition attribute='name' operator='eq' value='1' />
                            <condition attribute='name' operator='eq' value='2' />
                            <condition attribute='name' operator='eq' value='3' />
                            <condition attribute='name' operator='eq' value='4' />
                        </filter>
                    </entity>
                </fetch>");
        }

        [ExpectedException(typeof(NotSupportedQueryFragmentException))]
        [TestMethod]
        public void UnknownHint()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = "SELECT * FROM account OPTION(USE HINT('invalid'))";

            planBuilder.Build(query, null, out _);
        }

        [TestMethod]
        public void MultipleTablesJoinFromWhereClause()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = "SELECT firstname FROM account, contact WHERE accountid = parentcustomerid AND lastname = 'Carrington' AND name = 'Data8'";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);
            var select = AssertNode<SelectNode>(plans[0]);
            var fetch = AssertNode<FetchXmlScan>(select.Source);

            AssertFetchXml(fetch, @"
                <fetch xmlns:generator='MarkMpn.SQL4CDS'>
                    <entity name='contact'>
                        <attribute name='firstname' />
                        <link-entity name='account' from='accountid' to='parentcustomerid' alias='account' link-type='inner'>
                            <filter>
                                <condition attribute='name' operator='eq' value='Data8' />
                            </filter>
                        </link-entity>
                        <filter>
                            <condition attribute='lastname' operator='eq' value='Carrington' />
                        </filter>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void MultipleTablesJoinFromWhereClauseReversed()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = "SELECT firstname FROM account, contact WHERE lastname = 'Carrington' AND name = 'Data8' AND parentcustomerid = accountid";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);
            var select = AssertNode<SelectNode>(plans[0]);
            var fetch = AssertNode<FetchXmlScan>(select.Source);

            AssertFetchXml(fetch, @"
                <fetch xmlns:generator='MarkMpn.SQL4CDS'>
                    <entity name='contact'>
                        <attribute name='firstname' />
                        <link-entity name='account' from='accountid' to='parentcustomerid' alias='account' link-type='inner'>
                            <filter>
                                <condition attribute='name' operator='eq' value='Data8' />
                            </filter>
                        </link-entity>
                        <filter>
                            <condition attribute='lastname' operator='eq' value='Carrington' />
                        </filter>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void MultipleTablesJoinFromWhereClause3()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = "SELECT firstname FROM account, contact, systemuser WHERE accountid = parentcustomerid AND lastname = 'Carrington' AND name = 'Data8' AND account.ownerid = systemuserid";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);
            var select = AssertNode<SelectNode>(plans[0]);
            var fetch = AssertNode<FetchXmlScan>(select.Source);

            AssertFetchXml(fetch, @"
                <fetch xmlns:generator='MarkMpn.SQL4CDS'>
                    <entity name='contact'>
                        <attribute name='firstname' />
                        <link-entity name='account' from='accountid' to='parentcustomerid' alias='account' link-type='inner'>
                            <link-entity name='systemuser' from='systemuserid' to='ownerid' alias='systemuser' link-type='inner' />
                            <filter>
                                <condition attribute='name' operator='eq' value='Data8' />
                            </filter>
                        </link-entity>
                        <filter>
                            <condition attribute='lastname' operator='eq' value='Carrington' />
                        </filter>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void NestedInSubqueries()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = "SELECT firstname FROM contact WHERE parentcustomerid IN (SELECT accountid FROM account WHERE primarycontactid IN (SELECT contactid FROM contact WHERE lastname = 'Carrington'))";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);
            var select = AssertNode<SelectNode>(plans[0]);
            var fetch = AssertNode<FetchXmlScan>(select.Source);

            AssertFetchXml(fetch, @"
                <fetch xmlns:generator='MarkMpn.SQL4CDS'>
                    <entity name='contact'>
                        <attribute name='firstname' />
                        <link-entity name='account' from='accountid' to='parentcustomerid' alias='Expr3' link-type='inner'>
                            <link-entity name='contact' from='contactid' to='primarycontactid' alias='Expr1' link-type='inner'>
                                <filter>
                                    <condition attribute='lastname' operator='eq' value='Carrington' />
                                </filter>
                            </link-entity>
                        </link-entity>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void SpoolNestedLoop()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = "SELECT account.name, contact.fullname FROM account INNER JOIN contact ON account.accountid = contact.parentcustomerid OR account.createdon < contact.createdon";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);
            var select = AssertNode<SelectNode>(plans[0]);
            var loop = AssertNode<NestedLoopNode>(select.Source);
            var fetchAccounts = AssertNode<FetchXmlScan>(loop.LeftSource);
            var spoolContacts = AssertNode<TableSpoolNode>(loop.RightSource);
            var fetchContacts = AssertNode<FetchXmlScan>(spoolContacts.Source);

            AssertFetchXml(fetchAccounts, @"
                <fetch xmlns:generator='MarkMpn.SQL4CDS'>
                    <entity name='account'>
                        <attribute name='name' />
                        <attribute name='accountid' />
                        <attribute name='createdon' />
                    </entity>
                </fetch>");

            AssertFetchXml(fetchContacts, @"
                <fetch xmlns:generator='MarkMpn.SQL4CDS'>
                    <entity name='contact'>
                        <attribute name='fullname' />
                        <attribute name='parentcustomerid' />
                        <attribute name='createdon' />
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void SelectFromTVF()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = "SELECT * FROM SampleMessage('test')";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);
            var select = AssertNode<SelectNode>(plans[0]);
            var execute = AssertNode<ExecuteMessageNode>(select.Source);

            CollectionAssert.AreEqual(new[] { "OutputParam1", "OutputParam2" }, select.ColumnSet.Select(c => c.SourceColumn).ToArray());
            Assert.AreEqual("SampleMessage", execute.MessageName);
            Assert.AreEqual(1, execute.Values.Count);
            Assert.AreEqual("'test'", execute.Values["StringParam"].ToSql());
        }

        [TestMethod]
        public void OuterApplyCorrelatedTVF()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = "SELECT account.name, msg.OutputParam1 FROM account OUTER APPLY (SELECT * FROM SampleMessage(account.name)) AS msg WHERE account.name = 'Data8'";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);
            var select = AssertNode<SelectNode>(plans[0]);
            var loop = AssertNode<NestedLoopNode>(select.Source);
            var fetch = AssertNode<FetchXmlScan>(loop.LeftSource);
            var alias = AssertNode<AliasNode>(loop.RightSource);
            var execute = AssertNode<ExecuteMessageNode>(alias.Source);

            CollectionAssert.AreEqual(new[] { "account.name", "msg.OutputParam1" }, select.ColumnSet.Select(c => c.SourceColumn).ToArray());
            Assert.AreEqual(QualifiedJoinType.LeftOuter, loop.JoinType);
            Assert.IsNull(loop.JoinCondition);
            Assert.AreEqual(1, loop.OuterReferences.Count);
            Assert.AreEqual("@Expr1", loop.OuterReferences["account.name"]);
            Assert.AreEqual("msg", alias.Alias);
            Assert.AreEqual("SampleMessage", execute.MessageName);
            Assert.AreEqual(1, execute.Values.Count);
            Assert.AreEqual("CONVERT (NVARCHAR (MAX), @Expr1)", execute.Values["StringParam"].ToSql());
        }

        [TestMethod]
        public void OuterApplyUncorrelatedTVF()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = "SELECT account.name, msg.OutputParam1 FROM account OUTER APPLY (SELECT * FROM SampleMessage('test')) AS msg WHERE account.name = 'Data8'";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);
            var select = AssertNode<SelectNode>(plans[0]);
            var loop = AssertNode<NestedLoopNode>(select.Source);
            var fetch = AssertNode<FetchXmlScan>(loop.LeftSource);
            var spool = AssertNode<TableSpoolNode>(loop.RightSource);
            var alias = AssertNode<AliasNode>(spool.Source);
            var execute = AssertNode<ExecuteMessageNode>(alias.Source);

            CollectionAssert.AreEqual(new[] { "account.name", "msg.OutputParam1" }, select.ColumnSet.Select(c => c.SourceColumn).ToArray());
            Assert.AreEqual(QualifiedJoinType.LeftOuter, loop.JoinType);
            Assert.IsNull(loop.JoinCondition);
            Assert.AreEqual(0, loop.OuterReferences.Count);
            Assert.AreEqual("msg", alias.Alias);
            Assert.AreEqual("SampleMessage", execute.MessageName);
            Assert.AreEqual(1, execute.Values.Count);
            Assert.AreEqual("'test'", execute.Values["StringParam"].ToSql());
        }

        [TestMethod]
        public void TVFScalarSubqueryParameter()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = "SELECT * FROM SampleMessage((SELECT TOP 1 name FROM account))";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);
            var select = AssertNode<SelectNode>(plans[0]);
            var loop1 = AssertNode<NestedLoopNode>(select.Source);
            var loop2 = AssertNode<NestedLoopNode>(loop1.LeftSource);
            var constant = AssertNode<ConstantScanNode>(loop2.LeftSource);
            var fetch = AssertNode<FetchXmlScan>(loop2.RightSource);
            var execute = AssertNode<ExecuteMessageNode>(loop1.RightSource);

            CollectionAssert.AreEqual(new[] { "OutputParam1", "OutputParam2" }, select.ColumnSet.Select(c => c.SourceColumn).ToArray());
            Assert.AreEqual(QualifiedJoinType.Inner, loop1.JoinType);
            Assert.IsNull(loop1.JoinCondition);
            Assert.AreEqual(1, loop1.OuterReferences.Count);
            Assert.AreEqual("@Expr2", loop1.OuterReferences["Expr1"]);
            Assert.AreEqual(QualifiedJoinType.LeftOuter, loop2.JoinType);
            Assert.IsNull(loop2.JoinCondition);
            Assert.IsTrue(loop2.SemiJoin);
            Assert.AreEqual(1, loop2.DefinedValues.Count);
            Assert.AreEqual("account.name", loop2.DefinedValues["Expr1"]);
            Assert.AreEqual(0, constant.Schema.Count);
            Assert.AreEqual(1, constant.Values.Count);
            Assert.AreEqual("SampleMessage", execute.MessageName);
            Assert.AreEqual(1, execute.Values.Count);
            Assert.AreEqual("CONVERT (NVARCHAR (MAX), @Expr2)", execute.Values["StringParam"].ToSql());

            AssertFetchXml(fetch, @"
                <fetch xmlns:generator='MarkMpn.SQL4CDS' top='1'>
                    <entity name='account'>
                        <attribute name='name' />
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void ExecuteSproc()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = "EXEC SampleMessage 'test'";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);
            var execute = AssertNode<ExecuteMessageNode>(plans[0]);

            Assert.AreEqual("SampleMessage", execute.MessageName);
            Assert.AreEqual(1, execute.Values.Count);
            Assert.AreEqual("'test'", execute.Values["StringParam"].ToSql());
        }

        [TestMethod]
        public void ExecuteSprocNamedParameters()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = @"DECLARE @i int
                EXEC SampleMessage @StringParam = 'test', @OutputParam2 = @i OUTPUT
                SELECT @i";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(3, plans.Length);
            var declare = AssertNode<DeclareVariablesNode>(plans[0]);
            var assign = AssertNode<AssignVariablesNode>(plans[1]);
            var execute = AssertNode<ExecuteMessageNode>(assign.Source);
            var select = AssertNode<SelectNode>(plans[2]);
        }

        [TestMethod]
        public void FoldMultipleJoinConditionsWithKnownValue()
        {
            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);

            var query = @"SELECT a.name, c.fullname FROM account a INNER JOIN contact c ON a.accountid = c.parentcustomerid AND a.name = c.fullname WHERE a.name = 'Data8'";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);
            var select = AssertNode<SelectNode>(plans[0]);
            var fetch = AssertNode<FetchXmlScan>(select.Source);
            AssertFetchXml(fetch, @"
                <fetch xmlns:generator='MarkMpn.SQL4CDS'>
                    <entity name='contact'>
                        <attribute name='fullname' />
                        <link-entity name='account' alias='a' from='accountid' to='parentcustomerid' link-type='inner'>
                            <attribute name='name' />
                            <filter>
                                <condition attribute='name' operator='eq' value='Data8' />
                            </filter>
                        </link-entity>
                        <filter>
                            <condition attribute='fullname' operator='eq' value='Data8' />
                        </filter>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        [ExpectedException(typeof(NotSupportedQueryFragmentException))]
        public void CollationConflict()
        {
            var planBuilder = new ExecutionPlanBuilder(_dataSources.Values, new OptionsWrapper(this) { PrimaryDataSource = "prod" });
            var query = "SELECT * FROM prod.dbo.account p, french.dbo.account f WHERE p.name = f.name";
            planBuilder.Build(query, null, out _);
        }

        [TestMethod]
        public void ExplicitCollation()
        {
            var planBuilder = new ExecutionPlanBuilder(_dataSources.Values, new OptionsWrapper(this) { PrimaryDataSource = "prod" });
            var query = "SELECT * FROM prod.dbo.account p, french.dbo.account f WHERE p.name = f.name COLLATE French_CI_AS";
            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);
            var select = AssertNode<SelectNode>(plans[0]);
            var join = AssertNode<HashJoinNode>(select.Source);
            Assert.AreEqual("p.name", join.LeftAttribute.ToSql());
            Assert.AreEqual("Expr1", join.RightAttribute.ToSql());
            var fetch1 = AssertNode<FetchXmlScan>(join.LeftSource);
            var computeScalar = AssertNode<ComputeScalarNode>(join.RightSource);
            Assert.AreEqual("ExplicitCollation(f.name COLLATE French_CI_AS)", computeScalar.Columns["Expr1"].ToSql());
            var fetch2 = AssertNode<FetchXmlScan>(computeScalar.Source);
        }

        [TestMethod]
        [ExpectedException(typeof(NotSupportedQueryFragmentException))]
        public void NoCollationSelectListError()
        {
            var planBuilder = new ExecutionPlanBuilder(_dataSources.Values, new OptionsWrapper(this) { PrimaryDataSource = "prod" });
            var query = "SELECT (CASE WHEN p.employees > f.employees THEN p.name ELSE f.name END) FROM prod.dbo.account p, french.dbo.account f";
            planBuilder.Build(query, null, out _);
        }

        [TestMethod]
        public void NoCollationExprWithExplicitCollationSelectList()
        {
            var planBuilder = new ExecutionPlanBuilder(_dataSources.Values, new OptionsWrapper(this) { PrimaryDataSource = "prod" });
            var query = "SELECT (CASE WHEN p.employees > f.employees THEN p.name ELSE f.name END) COLLATE Latin1_General_CI_AS FROM prod.dbo.account p, french.dbo.account f";
            planBuilder.Build(query, null, out _);
        }

        [TestMethod]
        [ExpectedException(typeof(NotSupportedQueryFragmentException))]
        public void NoCollationCollationSensitiveFunctionError()
        {
            var planBuilder = new ExecutionPlanBuilder(_dataSources.Values, new OptionsWrapper(this) { PrimaryDataSource = "prod" });
            var query = "SELECT PATINDEX((CASE WHEN p.employees > f.employees THEN p.name ELSE f.name END), 'a') FROM prod.dbo.account p, french.dbo.account f";
            planBuilder.Build(query, null, out _);
        }

        [TestMethod]
        public void NoCollationExprWithExplicitCollationCollationSensitiveFunctionError()
        {
            var planBuilder = new ExecutionPlanBuilder(_dataSources.Values, new OptionsWrapper(this) { PrimaryDataSource = "prod" });
            var query = "SELECT PATINDEX((CASE WHEN p.employees > f.employees THEN p.name ELSE f.name END) COLLATE Latin1_General_CI_AS, 'a') FROM prod.dbo.account p, french.dbo.account f";
            planBuilder.Build(query, null, out _);
        }

        [TestMethod]
        public void CollationFunctions()
        {
            var planBuilder = new ExecutionPlanBuilder(_dataSources.Values, new OptionsWrapper(this) { PrimaryDataSource = "prod" });
            var query = "SELECT *, COLLATIONPROPERTY(name, 'lcid') FROM sys.fn_helpcollations()";
            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);
            var select = AssertNode<SelectNode>(plans[0]);
            CollectionAssert.AreEqual(new[] { "name", "description", null }, select.ColumnSet.Select(col => col.OutputColumn).ToArray());
            var computeScalar = AssertNode<ComputeScalarNode>(select.Source);
            var sysFunc = AssertNode<SystemFunctionNode>(computeScalar.Source);
            Assert.AreEqual(SystemFunction.fn_helpcollations, sysFunc.SystemFunction);
        }

        [TestMethod]
        [ExpectedException(typeof(NotSupportedQueryFragmentException))]
        public void DuplicatedTableName()
        {
            var planBuilder = new ExecutionPlanBuilder(_dataSources.Values, new OptionsWrapper(this) { PrimaryDataSource = "prod" });
            var query = "SELECT * FROM account, account";
            planBuilder.Build(query, null, out _);
        }

        [TestMethod]
        [ExpectedException(typeof(NotSupportedQueryFragmentException))]
        public void DuplicatedTableNameJoin()
        {
            var planBuilder = new ExecutionPlanBuilder(_dataSources.Values, new OptionsWrapper(this) { PrimaryDataSource = "prod" });
            var query = "SELECT * FROM account INNER JOIN contact ON contact.parentcustomerid = account.accountid INNER JOIN contact ON contact.parentcustomerid = account.accountid";
            planBuilder.Build(query, null, out _);
        }

        [TestMethod]
        [ExpectedException(typeof(NotSupportedQueryFragmentException))]
        public void DuplicatedAliasName()
        {
            var planBuilder = new ExecutionPlanBuilder(_dataSources.Values, new OptionsWrapper(this) { PrimaryDataSource = "prod" });
            var query = "SELECT * FROM account x INNER JOIN contact x ON x.parentcustomerid = x.accountid";
            planBuilder.Build(query, null, out _);
        }

        [TestMethod]
        [ExpectedException(typeof(NotSupportedQueryFragmentException))]
        public void TableNameMatchesAliasName()
        {
            var planBuilder = new ExecutionPlanBuilder(_dataSources.Values, new OptionsWrapper(this) { PrimaryDataSource = "prod" });
            var query = "SELECT * FROM account INNER JOIN contact AS account ON account.parentcustomerid = account.accountid";
            planBuilder.Build(query, null, out _);
        }

        [TestMethod]
        public void AuditJoinsToCallingUserIdAndUserId()
        {
            // https://learn.microsoft.com/en-us/power-apps/developer/data-platform/auditing/retrieve-audit-data?tabs=webapi#audit-table-relationships
            // Audit table can only be joined to systemuser on callinguserid or userid. Both joins together, or any other joins, are not valid.
            var planBuilder = new ExecutionPlanBuilder(_dataSources.Values, new OptionsWrapper(this) { PrimaryDataSource = "prod" });
            var query = "SELECT * FROM audit INNER JOIN systemuser cu ON audit.callinguserid = cu.systemuserid INNER JOIN systemuser u ON audit.userid = u.systemuserid";
            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);
            var select = AssertNode<SelectNode>(plans[0]);
            var join = AssertNode<MergeJoinNode>(select.Source);
            Assert.AreEqual("u.systemuserid", join.LeftAttribute.ToSql());
            Assert.AreEqual("audit.userid", join.RightAttribute.ToSql());
            var userFetch = AssertNode<FetchXmlScan>(join.LeftSource);
            AssertFetchXml(userFetch, @"
                <fetch xmlns:generator='MarkMpn.SQL4CDS'>
                    <entity name='systemuser'>
                        <all-attributes />
                        <order attribute='systemuserid' />
                    </entity>
                </fetch>");
            var sort = AssertNode<SortNode>(join.RightSource);
            var auditFetch = AssertNode<FetchXmlScan>(sort.Source);
            AssertFetchXml(auditFetch, @"
                <fetch xmlns:generator='MarkMpn.SQL4CDS'>
                    <entity name='audit'>
                        <all-attributes />
                        <link-entity name='systemuser' alias='cu' from='systemuserid' to='callinguserid' link-type='inner'>
                            <all-attributes />
                        </link-entity>
                        <filter>
                            <condition attribute='userid' operator='not-null' />
                        </filter>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void AuditJoinsToObjectId()
        {
            // https://learn.microsoft.com/en-us/power-apps/developer/data-platform/auditing/retrieve-audit-data?tabs=webapi#audit-table-relationships
            // Audit table can only be joined to systemuser on callinguserid or userid. Both joins together, or any other joins, are not valid.
            var planBuilder = new ExecutionPlanBuilder(_dataSources.Values, new OptionsWrapper(this) { PrimaryDataSource = "prod" });
            var query = "SELECT * FROM audit INNER JOIN account ON audit.objectid = account.accountid";
            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);
            var select = AssertNode<SelectNode>(plans[0]);
            var join = AssertNode<MergeJoinNode>(select.Source);
            Assert.AreEqual("account.accountid", join.LeftAttribute.ToSql());
            Assert.AreEqual("audit.objectid", join.RightAttribute.ToSql());
            var accountFetch = AssertNode<FetchXmlScan>(join.LeftSource);
            AssertFetchXml(accountFetch, @"
                <fetch xmlns:generator='MarkMpn.SQL4CDS'>
                    <entity name='account'>
                        <all-attributes />
                        <order attribute='accountid' />
                    </entity>
                </fetch>");
            var sort = AssertNode<SortNode>(join.RightSource);
            var auditFetch = AssertNode<FetchXmlScan>(sort.Source);
            AssertFetchXml(auditFetch, @"
                <fetch xmlns:generator='MarkMpn.SQL4CDS'>
                    <entity name='audit'>
                        <all-attributes />
                        <filter>
                            <condition attribute='objectid' operator='not-null' />
                        </filter>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void SelectAuditObjectId()
        {
            // https://github.com/MarkMpn/Sql4Cds/issues/296
            var planBuilder = new ExecutionPlanBuilder(_dataSources.Values, new OptionsWrapper(this) { PrimaryDataSource = "prod" });
            var query = "SELECT auditid, objectidtype AS o FROM audit";
            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);
            var select = AssertNode<SelectNode>(plans[0]);
            var fetch = AssertNode<FetchXmlScan>(select.Source);
            AssertFetchXml(fetch, @"
                <fetch xmlns:generator='MarkMpn.SQL4CDS'>
                    <entity name='audit'>
                        <attribute name='auditid' />
                        <attribute name='objectid' />
                        <attribute name='objecttypecode' />
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void FilterAuditOnLeftJoinColumn()
        {
            var planBuilder = new ExecutionPlanBuilder(_dataSources.Values, new OptionsWrapper(this) { PrimaryDataSource = "prod" });
            var query = "SELECT * FROM audit LEFT OUTER JOIN systemuser ON audit.userid = systemuser.systemuserid WHERE systemuser.domainname IS NULL";
            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);
            var select = AssertNode<SelectNode>(plans[0]);
            var filter = AssertNode<FilterNode>(select.Source);
            var fetch = AssertNode<FetchXmlScan>(filter.Source);
            AssertFetchXml(fetch, @"
                <fetch xmlns:generator='MarkMpn.SQL4CDS'>
                    <entity name='audit'>
                        <all-attributes />
                        <link-entity name='systemuser' from='systemuserid' to='userid' link-type='outer' alias='systemuser'>
                            <all-attributes />
                        </link-entity>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void FilterAuditOnInnerJoinColumn()
        {
            var planBuilder = new ExecutionPlanBuilder(_dataSources.Values, new OptionsWrapper(this) { PrimaryDataSource = "prod" });
            var query = "SELECT * FROM audit INNER JOIN systemuser ON audit.userid = systemuser.systemuserid WHERE systemuser.domainname <> 'SYSTEM'";
            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);
            var select = AssertNode<SelectNode>(plans[0]);
            var fetch = AssertNode<FetchXmlScan>(select.Source);
            AssertFetchXml(fetch, @"
                <fetch xmlns:generator='MarkMpn.SQL4CDS'>
                    <entity name='audit'>
                        <all-attributes />
                        <link-entity name='systemuser' from='systemuserid' to='userid' link-type='inner' alias='systemuser'>
                            <all-attributes />
                            <filter>
                                <condition attribute='domainname' operator='ne' value='SYSTEM' />
                                <condition attribute='domainname' operator='not-null' />
                            </filter>
                        </link-entity>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void SortAuditOnJoinColumn()
        {
            var planBuilder = new ExecutionPlanBuilder(_dataSources.Values, new OptionsWrapper(this) { PrimaryDataSource = "prod" });
            var query = "SELECT * FROM audit LEFT OUTER JOIN systemuser ON audit.userid = systemuser.systemuserid ORDER BY systemuser.domainname";
            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);
            var select = AssertNode<SelectNode>(plans[0]);
            var sort = AssertNode<SortNode>(select.Source);
            var fetch = AssertNode<FetchXmlScan>(sort.Source);
            AssertFetchXml(fetch, @"
                <fetch xmlns:generator='MarkMpn.SQL4CDS'>
                    <entity name='audit'>
                        <all-attributes />
                        <link-entity name='systemuser' from='systemuserid' to='userid' link-type='outer' alias='systemuser'>
                            <all-attributes />
                        </link-entity>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void NestedSubqueries()
        {
            var planBuilder = new ExecutionPlanBuilder(_dataSources.Values, new OptionsWrapper(this) { PrimaryDataSource = "prod" });
            var query = @"
                SELECT name,
                    (select STUFF((SELECT ', ' + fullname
                    FROM   contact
                    where parentcustomerid = account.accountid
                    FOR    XML PATH ('')), 1, 2, ''))
                FROM account";
            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);
            var select = AssertNode<SelectNode>(plans[0]);
            var nestedLoops = AssertNode<NestedLoopNode>(select.Source);
            var accountFetch = AssertNode<FetchXmlScan>(nestedLoops.LeftSource);
            var stuffComputeScalar = AssertNode<ComputeScalarNode>(nestedLoops.RightSource);
            var xml = AssertNode<XmlWriterNode>(stuffComputeScalar.Source);
            var commaComputeScalar = AssertNode<ComputeScalarNode>(xml.Source);
            var contactSpool = AssertNode<IndexSpoolNode>(commaComputeScalar.Source);
            var contactFetch = AssertNode<FetchXmlScan>(contactSpool.Source);
        }

        [TestMethod]
        public void CalculatedColumnUsesEmptyName()
        {
            var planBuilder = new ExecutionPlanBuilder(_dataSources.Values, new OptionsWrapper(this) { PrimaryDataSource = "prod" });
            var query = @"SELECT (select STUFF('abcdef', 2, 3, 'ijklmn'))";
            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);
            var select = AssertNode<SelectNode>(plans[0]);
            Assert.IsNull(select.ColumnSet[0].OutputColumn);
        }

        [TestMethod]
        public void OuterJoinWithFiltersConvertedToInnerJoin()
        {
            var planBuilder = new ExecutionPlanBuilder(_dataSources.Values, new OptionsWrapper(this) { PrimaryDataSource = "prod" });
            var outerJoinQuery = "SELECT * FROM account LEFT OUTER JOIN contact ON contact.parentcustomerid = account.accountid WHERE contact.firstname = 'Mark'";
            var outerJoinPlans = planBuilder.Build(outerJoinQuery, null, out _);

            Assert.AreEqual(1, outerJoinPlans.Length);
            var outerJoinSelect = AssertNode<SelectNode>(outerJoinPlans[0]);
            var outerJoinFetch = AssertNode<FetchXmlScan>(outerJoinSelect.Source);

            var innerJoinQuery = "SELECT * FROM account INNER JOIN contact ON contact.parentcustomerid = account.accountid WHERE contact.firstname = 'Mark'";
            var innerJoinPlans = planBuilder.Build(innerJoinQuery, null, out _);

            Assert.AreEqual(1, innerJoinPlans.Length);
            var innerJoinSelect = AssertNode<SelectNode>(innerJoinPlans[0]);
            var innerJoinFetch = AssertNode<FetchXmlScan>(innerJoinSelect.Source);

            AssertFetchXml(outerJoinFetch, innerJoinFetch.FetchXmlString);
        }

        [TestMethod]
        public void CorrelatedSubqueryWithMultipleConditions()
        {
            // https://github.com/MarkMpn/Sql4Cds/issues/316
            var planBuilder = new ExecutionPlanBuilder(_dataSources.Values, new OptionsWrapper(this) { PrimaryDataSource = "prod" });
            var query = @"
SELECT r.accountid,
       r.employees,
       r.ownerid,
       (SELECT count(*)
        FROM   account AS sub
        WHERE  
        sub.employees <= r.employees AND
        sub.ownerid = r.ownerid) AS cnt
FROM   account AS r;";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);
            var select = AssertNode<SelectNode>(plans[0]);
            Assert.AreEqual("r.accountid", select.ColumnSet[0].SourceColumn);
            Assert.AreEqual("accountid", select.ColumnSet[0].OutputColumn);
            Assert.AreEqual("r.employees", select.ColumnSet[1].SourceColumn);
            Assert.AreEqual("employees", select.ColumnSet[1].OutputColumn);
            Assert.AreEqual("r.ownerid", select.ColumnSet[2].SourceColumn);
            Assert.AreEqual("ownerid", select.ColumnSet[2].OutputColumn);
            Assert.AreEqual("Expr3", select.ColumnSet[3].SourceColumn);
            Assert.AreEqual("cnt", select.ColumnSet[3].OutputColumn);

            var loop = AssertNode<NestedLoopNode>(select.Source);
            Assert.IsNull(loop.JoinCondition);
            Assert.AreEqual("@Expr1", loop.OuterReferences["r.employees"]);
            Assert.AreEqual("@Expr2", loop.OuterReferences["r.ownerid"]);
            Assert.AreEqual("count", loop.DefinedValues["Expr3"]);

            var outerFetch = AssertNode<FetchXmlScan>(loop.LeftSource);
            AssertFetchXml(outerFetch, @"
                <fetch xmlns:generator='MarkMpn.SQL4CDS'>
                    <entity name='account'>
                        <attribute name='accountid' />
                        <attribute name='employees' />
                        <attribute name='ownerid' />
                    </entity>
                </fetch>");

            var aggregate = AssertNode<StreamAggregateNode>(loop.RightSource);
            Assert.AreEqual(AggregateType.CountStar, aggregate.Aggregates["count"].AggregateType);

            var filter = AssertNode<FilterNode>(aggregate.Source);
            Assert.AreEqual("sub.employees <= @Expr1", filter.Filter.ToSql());

            var indexSpool = AssertNode<IndexSpoolNode>(filter.Source);
            Assert.AreEqual("@Expr2", indexSpool.SeekValue);
            Assert.AreEqual("sub.ownerid", indexSpool.KeyColumn);

            var innerFetch = AssertNode<FetchXmlScan>(indexSpool.Source);
            AssertFetchXml(innerFetch, @"
                <fetch xmlns:generator='MarkMpn.SQL4CDS'>
                    <entity name='account'>
                        <attribute name='employees' />
                        <attribute name='ownerid' />
                        <filter>
                            <condition attribute=""ownerid"" operator=""not-null"" />
                        </filter>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void CrossInstanceJoinOnStringColumn()
        {
            // https://github.com/MarkMpn/Sql4Cds/issues/325
            var metadata1 = new AttributeMetadataCache(_service);
            var metadata2 = new AttributeMetadataCache(_service2);
            var datasources = new[]
            {
                new DataSource
                {
                    Name = "uat",
                    Connection = _context.GetOrganizationService(),
                    Metadata = metadata1,
                    TableSizeCache = new StubTableSizeCache(),
                    MessageCache = new StubMessageCache(),
                    DefaultCollation = new Collation(1033, false, false)
                },
                new DataSource
                {
                    Name = "prod",
                    Connection = _context2.GetOrganizationService(),
                    Metadata = metadata2,
                    TableSizeCache = new StubTableSizeCache(),
                    MessageCache = new StubMessageCache(),
                    DefaultCollation = new Collation(1033, false, false)
                },
                new DataSource
                {
                    Name = "local", // Hack so that ((IQueryExecutionOptions)this).PrimaryDataSource = "local" doesn't cause test to fail
                    DefaultCollation = Collation.USEnglish
                }
            };
            var planBuilder = new ExecutionPlanBuilder(datasources, this);

            var query = "SELECT uat.name, prod.name FROM uat.dbo.account AS uat INNER JOIN prod.dbo.account AS prod ON uat.name = prod.name";

            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);

            var select = AssertNode<SelectNode>(plans[0]);

            var join = AssertNode<HashJoinNode>(select.Source);
            Assert.AreEqual("uat.name", join.LeftAttribute.ToSql());
            Assert.AreEqual("prod.name", join.RightAttribute.ToSql());

            var uatFetch = AssertNode<FetchXmlScan>(join.LeftSource);
            Assert.AreEqual("uat", uatFetch.DataSource);
            AssertFetchXml(uatFetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='name' />
                        <filter>
                            <condition attribute='name' operator='not-null' />
                        </filter>
                    </entity>
                </fetch>");

            var prodFetch = AssertNode<FetchXmlScan>(join.RightSource);
            Assert.AreEqual("prod", prodFetch.DataSource);
            AssertFetchXml(prodFetch, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='name' />
                        <filter>
                            <condition attribute='name' operator='not-null' />
                        </filter>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void LiftOrFilterToLinkEntityWithInnerJoin()
        {
            var planBuilder = new ExecutionPlanBuilder(_dataSources.Values, new OptionsWrapper(this) { PrimaryDataSource = "prod" });
            var query = "SELECT * FROM contact INNER JOIN account ON contact.parentcustomerid = account.accountid WHERE account.name = 'Data8' OR account.name = 'Data 8'";
            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);
            var select = AssertNode<SelectNode>(plans[0]);
            var fetch = AssertNode<FetchXmlScan>(select.Source);
            AssertFetchXml(fetch, @"
                <fetch xmlns:generator='MarkMpn.SQL4CDS'>
                    <entity name='contact'>
                        <all-attributes />
                        <link-entity name='account' from='accountid' to='parentcustomerid' link-type='inner' alias='account'>
                            <all-attributes />
                            <filter type='or'>
                                <condition attribute='name' operator='eq' value='Data8' />
                                <condition attribute='name' operator='eq' value='Data 8' />
                            </filter>
                        </link-entity>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void DoNotLiftOrFilterToLinkEntityWithOuterJoin()
        {
            var planBuilder = new ExecutionPlanBuilder(_dataSources.Values, new OptionsWrapper(this) { PrimaryDataSource = "prod" });
            var query = "SELECT * FROM contact LEFT OUTER JOIN account ON contact.parentcustomerid = account.accountid WHERE account.name = 'Data8' OR account.name = 'Data 8'";
            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);
            var select = AssertNode<SelectNode>(plans[0]);
            var fetch = AssertNode<FetchXmlScan>(select.Source);
            AssertFetchXml(fetch, @"
                <fetch xmlns:generator='MarkMpn.SQL4CDS'>
                    <entity name='contact'>
                        <all-attributes />
                        <link-entity name='account' from='accountid' to='parentcustomerid' link-type='outer' alias='account'>
                            <all-attributes />
                        </link-entity>
                        <filter type='or'>
                            <condition entityname='account' attribute='name' operator='eq' value='Data8' />
                            <condition entityname='account' attribute='name' operator='eq' value='Data 8' />
                        </filter>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void DoNotLiftOrFilterToLinkEntityWithDifferentEntities()
        {
            var planBuilder = new ExecutionPlanBuilder(_dataSources.Values, new OptionsWrapper(this) { PrimaryDataSource = "prod" });
            var query = "SELECT * FROM contact LEFT OUTER JOIN account ON contact.parentcustomerid = account.accountid WHERE account.name = 'Data8' OR contact.fullname = 'Mark Carrington'";
            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);
            var select = AssertNode<SelectNode>(plans[0]);
            var fetch = AssertNode<FetchXmlScan>(select.Source);
            AssertFetchXml(fetch, @"
                <fetch xmlns:generator='MarkMpn.SQL4CDS'>
                    <entity name='contact'>
                        <all-attributes />
                        <link-entity name='account' from='accountid' to='parentcustomerid' link-type='outer' alias='account'>
                            <all-attributes />
                        </link-entity>
                        <filter type='or'>
                            <condition entityname='account' attribute='name' operator='eq' value='Data8' />
                            <condition attribute='fullname' operator='eq' value='Mark Carrington' />
                        </filter>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void FoldSortOrderToInnerJoinLeftInput()
        {
            var planBuilder = new ExecutionPlanBuilder(_dataSources.Values, new OptionsWrapper(this) { PrimaryDataSource = "prod" });
            var query = "SELECT TOP 10 audit.* FROM contact CROSS APPLY SampleMessage(firstname) AS audit WHERE firstname = 'Mark' ORDER BY contact.createdon;";
            var plans = planBuilder.Build(query, null, out _);

            Assert.AreEqual(1, plans.Length);
            var select = AssertNode<SelectNode>(plans[0]);
            var top = AssertNode<TopNode>(select.Source);
            var loop = AssertNode<NestedLoopNode>(top.Source);
            var fetch = AssertNode<FetchXmlScan>(loop.LeftSource);
            AssertFetchXml(fetch, @"
                <fetch xmlns:generator='MarkMpn.SQL4CDS'>
                    <entity name='contact'>
                        <attribute name='firstname' />
                        <filter>
                           <condition attribute='firstname' operator='eq' value='Mark' />
                        </filter>
                        <order attribute='createdon' />
                    </entity>
                </fetch>");
            var execute = AssertNode<ExecuteMessageNode>(loop.RightSource);
        }
    }
}
