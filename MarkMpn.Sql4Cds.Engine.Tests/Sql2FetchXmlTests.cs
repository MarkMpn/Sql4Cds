using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlTypes;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.RegularExpressions;
using System.Threading;
using System.Xml.Serialization;
using FakeXrmEasy;
using FakeXrmEasy.Extensions;
using FakeXrmEasy.FakeMessageExecutors;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Metadata;
using Microsoft.Xrm.Sdk.Metadata.Query;
using Microsoft.Xrm.Sdk.Query;

namespace MarkMpn.Sql4Cds.Engine.Tests
{
    [TestClass]
    public class Sql2FetchXmlTests : FakeXrmEasyTestsBase, IQueryExecutionOptions
    {
        CancellationToken IQueryExecutionOptions.CancellationToken => CancellationToken.None;

        bool IQueryExecutionOptions.BlockUpdateWithoutWhere => false;

        bool IQueryExecutionOptions.BlockDeleteWithoutWhere => false;

        bool IQueryExecutionOptions.UseBulkDelete => false;

        int IQueryExecutionOptions.BatchSize => 1;

        bool IQueryExecutionOptions.UseTDSEndpoint => false;

        int IQueryExecutionOptions.MaxDegreeOfParallelism => 10;

        bool IQueryExecutionOptions.ColumnComparisonAvailable => true;

        bool IQueryExecutionOptions.UseLocalTimeZone => false;

        List<JoinOperator> IQueryExecutionOptions.JoinOperatorsAvailable => new List<JoinOperator> { JoinOperator.Inner, JoinOperator.LeftOuter };

        bool IQueryExecutionOptions.BypassCustomPlugins => false;

        string IQueryExecutionOptions.PrimaryDataSource => "local";

        Guid IQueryExecutionOptions.UserId => Guid.NewGuid();

        bool IQueryExecutionOptions.QuotedIdentifiers => false;

        ColumnOrdering IQueryExecutionOptions.ColumnOrdering => ColumnOrdering.Alphabetical;

        [TestMethod]
        public void SimpleSelect()
        {
            var query = "SELECT accountid, name FROM account";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            AssertFetchXml(queries, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='accountid' />
                        <attribute name='name' />
                    </entity>
                </fetch>
            ");
        }

        [TestMethod]
        public void SelectSameFieldMultipleTimes()
        {
            var query = "SELECT accountid, name, name FROM account";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            AssertFetchXml(queries, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='accountid' />
                        <attribute name='name' />
                    </entity>
                </fetch>
            ");

            CollectionAssert.AreEqual(new[]
            {
                "accountid",
                "name",
                "name"
            }, ((SelectNode)queries[0]).ColumnSet.Select(c => c.OutputColumn).ToList());
        }

        [TestMethod]
        public void SelectStar()
        {
            var query = "SELECT * FROM account";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            AssertFetchXml(queries, @"
                <fetch>
                    <entity name='account'>
                        <all-attributes />
                    </entity>
                </fetch>
            ");

            CollectionAssert.AreEqual(new[]
            {
                "accountid",
                "createdon",
                "employees",
                "name",
                "ownerid",
                "owneridname",
                "owneridtype",
                "parentaccountid",
                "parentaccountidname",
                "primarycontactid",
                "primarycontactidname",
                "turnover"
            }, ((SelectNode)queries[0]).ColumnSet.Select(c => c.OutputColumn).ToList());
        }

        [TestMethod]
        public void SelectStarAndField()
        {
            var query = "SELECT *, name FROM account";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            AssertFetchXml(queries, @"
                <fetch>
                    <entity name='account'>
                        <all-attributes />
                    </entity>
                </fetch>
            ");

            CollectionAssert.AreEqual(new[]
            {
                "accountid",
                "createdon",
                "employees",
                "name",
                "ownerid",
                "owneridname",
                "owneridtype",
                "parentaccountid",
                "parentaccountidname",
                "primarycontactid",
                "primarycontactidname",
                "turnover",
                "name"
            }, ((SelectNode)queries[0]).ColumnSet.Select(c => c.OutputColumn).ToList());
        }

        [TestMethod]
        public void SimpleFilter()
        {
            var query = "SELECT accountid, name FROM account WHERE name = 'test'";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            AssertFetchXml(queries, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='accountid' />
                        <attribute name='name' />
                        <filter>
                            <condition attribute='name' operator='eq' value='test' />
                        </filter>
                    </entity>
                </fetch>
            ");
        }

        [TestMethod]
        public void BetweenFilter()
        {
            var query = "SELECT accountid, name FROM account WHERE employees BETWEEN 1 AND 10 AND turnover NOT BETWEEN 2 AND 20";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            AssertFetchXml(queries, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='accountid' />
                        <attribute name='name' />
                        <filter>
                            <condition attribute='employees' operator='ge' value='1' />
                            <condition attribute='employees' operator='le' value='10' />
                            <filter type='or'>
                                <condition attribute='turnover' operator='lt' value='2' />
                                <condition attribute='turnover' operator='gt' value='20' />
                            </filter>
                        </filter>
                    </entity>
                </fetch>
            ");
        }

        [TestMethod]
        public void FetchFilter()
        {
            var query = "SELECT contactid, firstname FROM contact WHERE createdon = lastxdays(7)";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            AssertFetchXml(queries, @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='contactid' />
                        <attribute name='firstname' />
                        <filter>
                            <condition attribute='createdon' operator='last-x-days' value='7' />
                        </filter>
                    </entity>
                </fetch>
            ");
        }

        [TestMethod]
        public void NestedFilters()
        {
            var query = "SELECT accountid, name FROM account WHERE name = 'test' OR (accountid is not null and name like 'foo%')";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            AssertFetchXml(queries, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='accountid' />
                        <attribute name='name' />
                        <filter type='or'>
                            <condition attribute='name' operator='eq' value='test' />
                            <filter type='and'>
                                <condition attribute='accountid' operator='not-null' />
                                <condition attribute='name' operator='like' value='foo%' />
                            </filter>
                        </filter>
                    </entity>
                </fetch>
            ");
        }

        [TestMethod]
        public void Sorts()
        {
            var query = "SELECT accountid, name FROM account ORDER BY name DESC, accountid";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            AssertFetchXml(queries, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='accountid' />
                        <attribute name='name' />
                        <order attribute='name' descending='true' />
                        <order attribute='accountid' />
                    </entity>
                </fetch>
            ");
        }

        [TestMethod]
        public void SortByColumnIndex()
        {
            var query = "SELECT accountid, name FROM account ORDER BY 2 DESC, 1";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            AssertFetchXml(queries, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='accountid' />
                        <attribute name='name' />
                        <order attribute='name' descending='true' />
                        <order attribute='accountid' />
                    </entity>
                </fetch>
            ");
        }

        [TestMethod]
        public void SortByAliasedColumn()
        {
            var query = "SELECT accountid, name as accountname FROM account ORDER BY name";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            AssertFetchXml(queries, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='accountid' />
                        <attribute name='name' />
                        <order attribute='name' />
                    </entity>
                </fetch>
            ");
        }

        [TestMethod]
        public void Top()
        {
            var query = "SELECT TOP 10 accountid, name FROM account";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            AssertFetchXml(queries, @"
                <fetch top='10'>
                    <entity name='account'>
                        <attribute name='accountid' />
                        <attribute name='name' />
                    </entity>
                </fetch>
            ");
        }

        [TestMethod]
        public void TopBrackets()
        {
            var query = "SELECT TOP (10) accountid, name FROM account";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            AssertFetchXml(queries, @"
                <fetch top='10'>
                    <entity name='account'>
                        <attribute name='accountid' />
                        <attribute name='name' />
                    </entity>
                </fetch>
            ");
        }

        [TestMethod]
        public void Top10KUsesExtension()
        {
            var query = "SELECT TOP 10000 accountid, name FROM account";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            AssertFetchXml(queries, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='accountid' />
                        <attribute name='name' />
                    </entity>
                </fetch>
            ");

            var converted = (SelectNode)queries[0];
            Assert.IsInstanceOfType(converted.Source, typeof(TopNode));
        }

        [TestMethod]
        public void NoLock()
        {
            var query = "SELECT accountid, name FROM account (NOLOCK)";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            AssertFetchXml(queries, @"
                <fetch no-lock='true'>
                    <entity name='account'>
                        <attribute name='accountid' />
                        <attribute name='name' />
                    </entity>
                </fetch>
            ");
        }

        [TestMethod]
        public void Distinct()
        {
            var query = "SELECT DISTINCT name FROM account";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            AssertFetchXml(queries, @"
                <fetch distinct='true'>
                    <entity name='account'>
                        <attribute name='name' />
                        <order attribute='name' />
                    </entity>
                </fetch>
            ");
        }

        [TestMethod]
        public void Offset()
        {
            var query = "SELECT accountid, name FROM account ORDER BY name OFFSET 100 ROWS FETCH NEXT 50 ROWS ONLY";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            AssertFetchXml(queries, @"
                <fetch count='50' page='3'>
                    <entity name='account'>
                        <attribute name='accountid' />
                        <attribute name='name' />
                        <order attribute='name' />
                    </entity>
                </fetch>
            ");
        }

        [TestMethod]
        public void SimpleJoin()
        {
            var query = "SELECT accountid, name FROM account INNER JOIN contact ON primarycontactid = contactid";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            AssertFetchXml(queries, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='accountid' />
                        <attribute name='name' />
                        <link-entity name='contact' from='contactid' to='primarycontactid' link-type='inner' alias='contact'>
                        </link-entity>
                    </entity>
                </fetch>
            ");
        }

        [TestMethod]
        public void SelfReferentialJoin()
        {
            var query = "SELECT contact.contactid, contact.firstname, manager.firstname FROM contact LEFT OUTER JOIN contact AS manager ON contact.parentcustomerid = manager.contactid";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            AssertFetchXml(queries, @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='contactid' />
                        <attribute name='firstname' />
                        <link-entity name='contact' from='contactid' to='parentcustomerid' link-type='outer' alias='manager'>
                            <attribute name='firstname' />
                        </link-entity>
                    </entity>
                </fetch>
            ");
        }

        [TestMethod]
        public void AdditionalJoinCriteria()
        {
            var query = "SELECT accountid, name FROM account INNER JOIN contact ON accountid = parentcustomerid AND (firstname = 'Mark' OR lastname = 'Carrington')";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            AssertFetchXml(queries, @"
                <fetch>
                    <entity name='contact'>
                        <link-entity name='account' from='accountid' to='parentcustomerid' link-type='inner' alias='account'>
                            <attribute name='accountid' />
                            <attribute name='name' />
                        </link-entity>
                        <filter type='or'>
                            <condition attribute='firstname' operator='eq' value='Mark' />
                            <condition attribute='lastname' operator='eq' value='Carrington' />
                        </filter>
                    </entity>
                </fetch>
            ");
        }

        [TestMethod]
        public void InvalidAdditionalJoinCriteria()
        {
            var query = "SELECT accountid, name FROM account INNER JOIN contact ON accountid = parentcustomerid OR (firstname = 'Mark' AND lastname = 'Carrington')";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            Assert.IsNotInstanceOfType(((SelectNode)queries[0]).Source, typeof(FetchXmlScan));
        }

        [TestMethod]
        public void SortOnLinkEntity()
        {
            var query = "SELECT TOP 100 accountid, name FROM account INNER JOIN contact ON primarycontactid = contactid ORDER BY name, firstname";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            AssertFetchXml(queries, @"
                <fetch top='100'>
                    <entity name='account'>
                        <attribute name='accountid' />
                        <attribute name='name' />
                        <link-entity name='contact' from='contactid' to='primarycontactid' link-type='inner' alias='contact'>
                            <order attribute='firstname' />
                        </link-entity>
                        <order attribute='name' />
                    </entity>
                </fetch>
            ");
        }

        [TestMethod]
        public void InvalidSortOnLinkEntity()
        {
            var query = "SELECT TOP 100 accountid, name FROM account INNER JOIN contact ON accountid = parentcustomerid ORDER BY name, firstname";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            AssertFetchXml(queries, @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='firstname' />
                        <link-entity name='account' from='accountid' to='parentcustomerid' alias='account' link-type='inner'>
                            <attribute name='accountid' />
                            <attribute name='name' />
                            <order attribute='name' />
                        </link-entity>
                    </entity>
                </fetch>
            ");

            Assert.IsInstanceOfType(((SelectNode)queries[0]).Source, typeof(TopNode));
            Assert.IsInstanceOfType(((SelectNode)queries[0]).Source.GetSources().First(), typeof(SortNode));
        }

        [TestMethod]
        public void SimpleAggregate()
        {
            var query = "SELECT count(*), count(name), count(DISTINCT name), max(name), min(name), avg(employees) FROM account";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            AssertFetchXml(queries, @"
                <fetch aggregate='true'>
                    <entity name='account'>
                        <attribute name='accountid' aggregate='count' alias='count' />
                        <attribute name='name' aggregate='countcolumn' alias='name_count' />
                        <attribute name='name' aggregate='countcolumn' distinct='true' alias='name_count_distinct' />
                        <attribute name='name' aggregate='max' alias='name_max' />
                        <attribute name='name' aggregate='min' alias='name_min' />
                        <attribute name='employees' aggregate='avg' alias='employees_avg' />
                    </entity>
                </fetch>
            ");
        }

        [TestMethod]
        public void GroupBy()
        {
            var query = "SELECT name, count(*) FROM account GROUP BY name";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            AssertFetchXml(queries, @"
                <fetch aggregate='true'>
                    <entity name='account'>
                        <attribute name='name' groupby='true' alias='name' />
                        <attribute name='accountid' aggregate='count' alias='count' />
                        <order alias='name' />
                    </entity>
                </fetch>
            ");
        }

        [TestMethod]
        public void GroupBySorting()
        {
            var query = "SELECT name, count(*) FROM account GROUP BY name ORDER BY name, count(*)";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            AssertFetchXml(queries, @"
                <fetch aggregate='true'>
                    <entity name='account'>
                        <attribute name='name' groupby='true' alias='name' />
                        <attribute name='accountid' aggregate='count' alias='count' />
                        <order alias='name' />
                        <order alias='count' />
                    </entity>
                </fetch>
            ");
        }

        [TestMethod]
        public void GroupBySortingOnLinkEntity()
        {
            var query = "SELECT name, firstname, count(*) FROM account INNER JOIN contact ON parentcustomerid = account.accountid GROUP BY name, firstname ORDER BY firstname, name";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            AssertFetchXml(queries, @"
                <fetch aggregate='true'>
                    <entity name='contact'>
                        <attribute name='firstname' groupby='true' alias='firstname' />
                        <attribute name='contactid' aggregate='count' alias='count' />
                        <link-entity name='account' from='accountid' to='parentcustomerid' link-type='inner' alias='account'>
                            <attribute name='name' groupby='true' alias='name' />
                            <order alias='name' />
                        </link-entity>
                        <order alias='firstname' />
                    </entity>
                </fetch>
            ");
        }

        [TestMethod]
        public void GroupBySortingOnAliasedAggregate()
        {
            var query = "SELECT name, firstname, count(*) as count FROM account INNER JOIN contact ON parentcustomerid = account.accountid GROUP BY name, firstname ORDER BY count";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            AssertFetchXml(queries, @"
                <fetch aggregate='true'>
                    <entity name='contact'>
                        <attribute name='firstname' groupby='true' alias='firstname' />
                        <attribute name='contactid' aggregate='count' alias='count' />
                        <link-entity name='account' from='accountid' to='parentcustomerid' link-type='inner' alias='account'>
                            <attribute name='name' groupby='true' alias='name' />
                        </link-entity>
                        <order alias='count' />
                    </entity>
                </fetch>
            ");
        }

        [TestMethod]
        public void UpdateFieldToValue()
        {
            var query = "UPDATE contact SET firstname = 'Mark'";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            AssertFetchXml(queries, @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='contactid' />
                    </entity>
                </fetch>
            ");
        }

        [TestMethod]
        public void SelectArithmetic()
        {
            var query = "SELECT employees + 1 AS a, employees * 2 AS b, turnover / 3 AS c, turnover - 4 AS d, turnover / employees AS e FROM account";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            AssertFetchXml(queries, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='employees' />
                        <attribute name='turnover' />
                    </entity>
                </fetch>
            ");

            var id = Guid.NewGuid();
            _context.Data["account"] = new Dictionary<Guid, Entity>
            {
                [id] = new Entity("account", id)
                {
                    ["accountid"] = id,
                    ["employees"] = 2,
                    ["turnover"] = new Money(9)
                }
            };

            var dataReader = ((SelectNode)queries[0]).Execute(new NodeExecutionContext(GetDataSources(_context), this, new Dictionary<string, DataTypeReference>(), new Dictionary<string, object>(), null), CommandBehavior.Default);
            var dataTable = new DataTable();
            dataTable.Load(dataReader);

            Assert.AreEqual(1, dataTable.Rows.Count);
            Assert.AreEqual(3, dataTable.Rows[0]["a"]);
            Assert.AreEqual(4, dataTable.Rows[0]["b"]);
            Assert.AreEqual(3M, dataTable.Rows[0]["c"]);
            Assert.AreEqual(5M, dataTable.Rows[0]["d"]);
            Assert.AreEqual(4.5M, dataTable.Rows[0]["e"]);
        }

        [TestMethod]
        public void WhereComparingTwoFields()
        {
            var query = "SELECT contactid FROM contact WHERE firstname = lastname";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, new OptionsWrapper(this) { ColumnComparisonAvailable = false });
            var queries = planBuilder.Build(query, null, out _);

            AssertFetchXml(queries, @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='contactid' />
                        <attribute name='firstname' />
                        <attribute name='lastname' />
                    </entity>
                </fetch>
            ");

            var guid1 = Guid.NewGuid();
            var guid2 = Guid.NewGuid();
            _context.Data["contact"] = new Dictionary<Guid, Entity>
            {
                [guid1] = new Entity("contact", guid1)
                {
                    ["contactid"] = guid1,
                    ["firstname"] = "Mark",
                    ["lastname"] = "Carrington"
                },
                [guid2] = new Entity("contact", guid2)
                {
                    ["contactid"] = guid2,
                    ["firstname"] = "Mark",
                    ["lastname"] = "Mark"
                }
            };

            var dataReader = ((SelectNode)queries[0]).Execute(new NodeExecutionContext(GetDataSources(_context), this, new Dictionary<string, DataTypeReference>(), new Dictionary<string, object>(), null), CommandBehavior.Default);
            var dataTable = new DataTable();
            dataTable.Load(dataReader);

            Assert.AreEqual(1, dataTable.Rows.Count);
            Assert.AreEqual(guid2, ((SqlEntityReference)dataTable.Rows[0]["contactid"]).Id);
        }

        [TestMethod]
        public void WhereComparingExpression()
        {
            var query = "SELECT contactid FROM contact WHERE lastname = firstname + 'rington'";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            AssertFetchXml(queries, @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='contactid' />
                        <attribute name='lastname' />
                        <attribute name='firstname' />
                    </entity>
                </fetch>
            ");

            var guid1 = Guid.NewGuid();
            var guid2 = Guid.NewGuid();
            _context.Data["contact"] = new Dictionary<Guid, Entity>
            {
                [guid1] = new Entity("contact", guid1)
                {
                    ["contactid"] = guid1,
                    ["firstname"] = "Car",
                    ["lastname"] = "Carrington"
                },
                [guid2] = new Entity("contact", guid2)
                {
                    ["contactid"] = guid2,
                    ["firstname"] = "Mark",
                    ["lastname"] = "Mark"
                }
            };

            var dataReader = ((SelectNode)queries[0]).Execute(new NodeExecutionContext(GetDataSources(_context), this, new Dictionary<string, DataTypeReference>(), new Dictionary<string, object>(), null), CommandBehavior.Default);
            var dataTable = new DataTable();
            dataTable.Load(dataReader);

            Assert.AreEqual(1, dataTable.Rows.Count);
            Assert.AreEqual(guid1, ((SqlEntityReference)dataTable.Rows[0]["contactid"]).Id);
        }

        [TestMethod]
        public void BackToFrontLikeExpression()
        {
            var query = "SELECT contactid FROM contact WHERE 'Mark' like firstname";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            AssertFetchXml(queries, @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='contactid' />
                        <attribute name='firstname' />
                    </entity>
                </fetch>
            ");

            var guid1 = Guid.NewGuid();
            var guid2 = Guid.NewGuid();
            _context.Data["contact"] = new Dictionary<Guid, Entity>
            {
                [guid1] = new Entity("contact", guid1)
                {
                    ["contactid"] = guid1,
                    ["firstname"] = "Foo"
                },
                [guid2] = new Entity("contact", guid2)
                {
                    ["contactid"] = guid2,
                    ["firstname"] = "M%"
                }
            };

            var dataReader = ((SelectNode)queries[0]).Execute(new NodeExecutionContext(GetDataSources(_context), this, new Dictionary<string, DataTypeReference>(), new Dictionary<string, object>(), null), CommandBehavior.Default);
            var dataTable = new DataTable();
            dataTable.Load(dataReader);

            Assert.AreEqual(1, dataTable.Rows.Count);
            Assert.AreEqual(guid2, ((SqlEntityReference)dataTable.Rows[0]["contactid"]).Id);
        }

        [TestMethod]
        public void UpdateFieldToField()
        {
            var query = "UPDATE contact SET firstname = lastname";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            AssertFetchXml(queries, @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='contactid' />
                        <attribute name='lastname' />
                    </entity>
                </fetch>
            ");

            var guid = Guid.NewGuid();
            _context.Data["contact"] = new Dictionary<Guid, Entity>
            {
                [guid] = new Entity("contact", guid)
                {
                    ["contactid"] = guid,
                    ["lastname"] = "Carrington"
                }
            };

            ((UpdateNode)queries[0]).Execute(new NodeExecutionContext(GetDataSources(_context), this, new Dictionary<string, DataTypeReference>(), new Dictionary<string, object>(), null), out _, out _);

            Assert.AreEqual("Carrington", _context.Data["contact"][guid]["firstname"]);
        }

        [TestMethod]
        public void UpdateFieldToExpression()
        {
            var query = "UPDATE contact SET firstname = 'Hello ' + lastname";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            AssertFetchXml(queries, @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='contactid' />
                        <attribute name='lastname' />
                    </entity>
                </fetch>
            ");

            var guid = Guid.NewGuid();
            _context.Data["contact"] = new Dictionary<Guid, Entity>
            {
                [guid] = new Entity("contact", guid)
                {
                    ["contactid"] = guid,
                    ["lastname"] = "Carrington"
                }
            };

            ((UpdateNode)queries[0]).Execute(new NodeExecutionContext(GetDataSources(_context), this, new Dictionary<string, DataTypeReference>(), new Dictionary<string, object>(), null), out _, out _);

            Assert.AreEqual("Hello Carrington", _context.Data["contact"][guid]["firstname"]);
        }

        [TestMethod]
        public void UpdateReplace()
        {
            var query = "UPDATE contact SET firstname = REPLACE(firstname, 'Dataflex Pro', 'CDS') WHERE lastname = 'Carrington'";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            AssertFetchXml(queries, @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='contactid' />
                        <attribute name='firstname' />
                        <filter>
                            <condition attribute='lastname' operator='eq' value='Carrington' />
                        </filter>
                    </entity>
                </fetch>
            ");

            var guid = Guid.NewGuid();
            _context.Data["contact"] = new Dictionary<Guid, Entity>
            {
                [guid] = new Entity("contact", guid)
                {
                    ["contactid"] = guid,
                    ["firstname"] = "--Dataflex Pro--",
                    ["lastname"] = "Carrington"
                }
            };

            ((UpdateNode)queries[0]).Execute(new NodeExecutionContext(GetDataSources(_context), this, new Dictionary<string, DataTypeReference>(), new Dictionary<string, object>(), null), out _, out _);

            Assert.AreEqual("--CDS--", _context.Data["contact"][guid]["firstname"]);
        }

        [TestMethod]
        public void StringFunctions()
        {
            var query = "SELECT trim(firstname) as trim, ltrim(firstname) as ltrim, rtrim(firstname) as rtrim, substring(firstname, 2, 3) as substring23, len(firstname) as len FROM contact";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            AssertFetchXml(queries, @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='firstname' />
                    </entity>
                </fetch>
            ");

            var guid1 = Guid.NewGuid();
            _context.Data["contact"] = new Dictionary<Guid, Entity>
            {
                [guid1] = new Entity("contact", guid1)
                {
                    ["contactid"] = guid1,
                    ["firstname"] = " Mark "
                }
            };

            var dataReader = ((SelectNode)queries[0]).Execute(new NodeExecutionContext(GetDataSources(_context), this, new Dictionary<string, DataTypeReference>(), new Dictionary<string, object>(), null), CommandBehavior.Default);
            var dataTable = new DataTable();
            dataTable.Load(dataReader);

            Assert.AreEqual(1, dataTable.Rows.Count);

            var row = dataTable.Rows[0];
            Assert.AreEqual("Mark", row["trim"]);
            Assert.AreEqual("Mark ", row["ltrim"]);
            Assert.AreEqual(" Mark", row["rtrim"]);
            Assert.AreEqual("Mar", row["substring23"]);
            Assert.AreEqual(5, row["len"]);
        }

        [TestMethod]
        public void SelectExpression()
        {
            var query = "SELECT firstname, 'Hello ' + firstname AS greeting FROM contact";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            AssertFetchXml(queries, @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='firstname' />
                    </entity>
                </fetch>
            ");

            var guid = Guid.NewGuid();
            _context.Data["contact"] = new Dictionary<Guid, Entity>
            {
                [guid] = new Entity("contact", guid)
                {
                    ["contactid"] = guid,
                    ["firstname"] = "Mark"
                }
            };

            var dataReader = ((SelectNode)queries[0]).Execute(new NodeExecutionContext(GetDataSources(_context), this, new Dictionary<string, DataTypeReference>(), new Dictionary<string, object>(), null), CommandBehavior.Default);
            var dataTable = new DataTable();
            dataTable.Load(dataReader);

            Assert.AreEqual(1, dataTable.Rows.Count);
            Assert.AreEqual("Mark", dataTable.Rows[0]["firstname"]);
            Assert.AreEqual("Hello Mark", dataTable.Rows[0]["greeting"]);
        }

        private IDictionary<string, DataSource> GetDataSources(XrmFakedContext context)
        {
            var dataSource = new DataSource
            {
                Name = "local",
                Connection = context.GetOrganizationService(),
                Metadata = new AttributeMetadataCache(context.GetOrganizationService()),
                TableSizeCache = new StubTableSizeCache(),
                MessageCache = new StubMessageCache(),
                DefaultCollation = Collation.USEnglish
            };

            return new Dictionary<string, DataSource> { ["local"] = dataSource };
        }

        [TestMethod]
        public void SelectExpressionNullValues()
        {
            var query = "SELECT firstname, 'Hello ' + firstname AS greeting, case when createdon > '2020-01-01' then 'new' else 'old' end AS age FROM contact";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            AssertFetchXml(queries, @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='firstname' />
                        <attribute name='createdon' />
                    </entity>
                </fetch>
            ");

            var guid = Guid.NewGuid();
            _context.Data["contact"] = new Dictionary<Guid, Entity>
            {
                [guid] = new Entity("contact", guid)
                {
                    ["contactid"] = guid
                }
            };

            var dataReader = ((SelectNode)queries[0]).Execute(new NodeExecutionContext(GetDataSources(_context), this, new Dictionary<string, DataTypeReference>(), new Dictionary<string, object>(), null), CommandBehavior.Default);
            var dataTable = new DataTable();
            dataTable.Load(dataReader);

            Assert.AreEqual(1, dataTable.Rows.Count);
            Assert.AreEqual(DBNull.Value, dataTable.Rows[0]["firstname"]);
            Assert.AreEqual(DBNull.Value, dataTable.Rows[0]["greeting"]);
            Assert.AreEqual("old", dataTable.Rows[0]["age"]);
        }

        [TestMethod]
        public void OrderByExpression()
        {
            var query = "SELECT firstname, lastname FROM contact ORDER BY lastname + ', ' + firstname";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            AssertFetchXml(queries, @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='firstname' />
                        <attribute name='lastname' />
                    </entity>
                </fetch>
            ");

            var guid1 = Guid.NewGuid();
            var guid2 = Guid.NewGuid();
            _context.Data["contact"] = new Dictionary<Guid, Entity>
            {
                [guid1] = new Entity("contact", guid1)
                {
                    ["contactid"] = guid1,
                    ["firstname"] = "Mark",
                    ["lastname"] = "Carrington"
                },
                [guid2] = new Entity("contact", guid2)
                {
                    ["contactid"] = guid2,
                    ["firstname"] = "Data",
                    ["lastname"] = "8"
                }
            };

            var dataReader = ((SelectNode)queries[0]).Execute(new NodeExecutionContext(GetDataSources(_context), this, new Dictionary<string, DataTypeReference>(), new Dictionary<string, object>(), null), CommandBehavior.Default);
            var dataTable = new DataTable();
            dataTable.Load(dataReader);

            Assert.AreEqual(2, dataTable.Rows.Count);
            Assert.AreEqual("Data", dataTable.Rows[0]["firstname"]);
            Assert.AreEqual("Mark", dataTable.Rows[1]["firstname"]);
        }

        [TestMethod]
        public void OrderByAliasedField()
        {
            var query = "SELECT firstname, lastname AS surname FROM contact ORDER BY surname";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            AssertFetchXml(queries, @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='firstname' />
                        <attribute name='lastname' />
                        <order attribute='lastname' />
                    </entity>
                </fetch>
            ");

            var guid1 = Guid.NewGuid();
            var guid2 = Guid.NewGuid();
            _context.Data["contact"] = new Dictionary<Guid, Entity>
            {
                [guid1] = new Entity("contact", guid1)
                {
                    ["contactid"] = guid1,
                    ["firstname"] = "Mark",
                    ["lastname"] = "Carrington"
                },
                [guid2] = new Entity("contact", guid2)
                {
                    ["contactid"] = guid2,
                    ["firstname"] = "Data",
                    ["lastname"] = "8"
                }
            };

            var dataReader = ((SelectNode)queries[0]).Execute(new NodeExecutionContext(GetDataSources(_context), this, new Dictionary<string, DataTypeReference>(), new Dictionary<string, object>(), null), CommandBehavior.Default);
            var dataTable = new DataTable();
            dataTable.Load(dataReader);

            Assert.AreEqual(2, dataTable.Rows.Count);
            Assert.AreEqual("8", dataTable.Rows[0]["surname"]);
            Assert.AreEqual("Carrington", dataTable.Rows[1]["surname"]);
        }

        [TestMethod]
        public void OrderByCalculatedField()
        {
            var query = "SELECT firstname, lastname, lastname + ', ' + firstname AS fullname FROM contact ORDER BY fullname";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            AssertFetchXml(queries, @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='firstname' />
                        <attribute name='lastname' />
                    </entity>
                </fetch>
            ");

            var guid1 = Guid.NewGuid();
            var guid2 = Guid.NewGuid();
            _context.Data["contact"] = new Dictionary<Guid, Entity>
            {
                [guid1] = new Entity("contact", guid1)
                {
                    ["contactid"] = guid1,
                    ["firstname"] = "Mark",
                    ["lastname"] = "Carrington"
                },
                [guid2] = new Entity("contact", guid2)
                {
                    ["contactid"] = guid2,
                    ["firstname"] = "Data",
                    ["lastname"] = "8"
                }
            };

            var dataReader = ((SelectNode)queries[0]).Execute(new NodeExecutionContext(GetDataSources(_context), this, new Dictionary<string, DataTypeReference>(), new Dictionary<string, object>(), null), CommandBehavior.Default);
            var dataTable = new DataTable();
            dataTable.Load(dataReader);

            Assert.AreEqual(2, dataTable.Rows.Count);
            Assert.AreEqual("8, Data", dataTable.Rows[0]["fullname"]);
            Assert.AreEqual("Carrington, Mark", dataTable.Rows[1]["fullname"]);
        }

        [TestMethod]
        public void OrderByCalculatedFieldByIndex()
        {
            var query = "SELECT firstname, lastname, lastname + ', ' + firstname AS fullname FROM contact ORDER BY 3";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            AssertFetchXml(queries, @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='firstname' />
                        <attribute name='lastname' />
                    </entity>
                </fetch>
            ");

            var guid1 = Guid.NewGuid();
            var guid2 = Guid.NewGuid();
            _context.Data["contact"] = new Dictionary<Guid, Entity>
            {
                [guid1] = new Entity("contact", guid1)
                {
                    ["contactid"] = guid1,
                    ["firstname"] = "Mark",
                    ["lastname"] = "Carrington"
                },
                [guid2] = new Entity("contact", guid2)
                {
                    ["contactid"] = guid2,
                    ["firstname"] = "Data",
                    ["lastname"] = "8"
                }
            };

            var dataReader = ((SelectNode)queries[0]).Execute(new NodeExecutionContext(GetDataSources(_context), this, new Dictionary<string, DataTypeReference>(), new Dictionary<string, object>(), null), CommandBehavior.Default);
            var dataTable = new DataTable();
            dataTable.Load(dataReader);

            Assert.AreEqual(2, dataTable.Rows.Count);
            Assert.AreEqual("8, Data", dataTable.Rows[0]["fullname"]);
            Assert.AreEqual("Carrington, Mark", dataTable.Rows[1]["fullname"]);
        }

        [TestMethod]
        public void DateCalculations()
        {
            var query = "SELECT contactid, DATEADD(day, 1, createdon) AS nextday, DATEPART(minute, createdon) AS minute FROM contact WHERE DATEDIFF(hour, '2020-01-01', createdon) < 1";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            AssertFetchXml(queries, @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='contactid' />
                        <attribute name='createdon' />
                    </entity>
                </fetch>
            ");

            var guid1 = Guid.NewGuid();
            var guid2 = Guid.NewGuid();
            _context.Data["contact"] = new Dictionary<Guid, Entity>
            {
                [guid1] = new Entity("contact", guid1)
                {
                    ["contactid"] = guid1,
                    ["createdon"] = new DateTime(2020, 2, 1)
                },
                [guid2] = new Entity("contact", guid2)
                {
                    ["contactid"] = guid2,
                    ["createdon"] = new DateTime(2020, 1, 1, 0, 30, 0)
                }
            };

            var dataReader = ((SelectNode)queries[0]).Execute(new NodeExecutionContext(GetDataSources(_context), this, new Dictionary<string, DataTypeReference>(), new Dictionary<string, object>(), null), CommandBehavior.Default);
            var dataTable = new DataTable();
            dataTable.Load(dataReader);

            Assert.AreEqual(1, dataTable.Rows.Count);
            Assert.AreEqual(guid2, ((SqlEntityReference)dataTable.Rows[0]["contactid"]).Id);
            Assert.AreEqual(new DateTime(2020, 1, 2, 0, 30, 0), dataTable.Rows[0]["nextday"]);
            Assert.AreEqual(30, dataTable.Rows[0]["minute"]);
        }

        [TestMethod]
        public void TopAppliedAfterCustomFilter()
        {
            var query = "SELECT TOP 10 contactid FROM contact WHERE firstname = lastname";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, new OptionsWrapper(this) { ColumnComparisonAvailable = false });
            var queries = planBuilder.Build(query, null, out _);

            AssertFetchXml(queries, @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='contactid' />
                        <attribute name='firstname' />
                        <attribute name='lastname' />
                    </entity>
                </fetch>
            ");

            Assert.IsInstanceOfType(((SelectNode)queries[0]).Source, typeof(TopNode));
            Assert.IsInstanceOfType(((SelectNode)queries[0]).Source.GetSources().Single(), typeof(FilterNode));
        }

        [TestMethod]
        public void CustomFilterAggregateHavingProjectionSortAndTop()
        {
            var query = "SELECT TOP 10 lastname, SUM(CASE WHEN firstname = 'Mark' THEN 1 ELSE 0 END) as nummarks, LEFT(lastname, 1) AS lastinitial FROM contact WHERE DATEDIFF(day, '2020-01-01', createdon) > 10 GROUP BY lastname HAVING count(*) > 1 ORDER BY 2 DESC";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            AssertFetchXml(queries, @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='lastname' />
                        <attribute name='firstname' />
                        <attribute name='createdon' />
                        <order attribute='lastname' />
                    </entity>
                </fetch>
            ");

            _context.Data["contact"] = new Dictionary<Guid, Entity>
            {
                [Guid.NewGuid()] = new Entity("contact")
                {
                    ["lastname"] = "Carrington",
                    ["firstname"] = "Mark",
                    ["createdon"] = DateTime.Parse("2020-01-01") // Ignored by WHERE
                },
                [Guid.NewGuid()] = new Entity("contact")
                {
                    ["lastname"] = "Carrington",
                    ["firstname"] = "Mark", // nummarks = 1
                    ["createdon"] = DateTime.Parse("2020-02-01") // Included by WHERE
                },
                [Guid.NewGuid()] = new Entity("contact")
                {
                    ["lastname"] = "Carrington", // Included by HAVING count(*) > 1
                    ["firstname"] = "Matt", // nummarks = 1
                    ["createdon"] = DateTime.Parse("2020-02-01") // Included by WHERE
                },
                [Guid.NewGuid()] = new Entity("contact")
                {
                    ["lastname"] = "Doe",
                    ["firstname"] = "Mark", // nummarks = 1
                    ["createdon"] = DateTime.Parse("2020-02-01") // Included by WHERE
                },
                [Guid.NewGuid()] = new Entity("contact")
                {
                    ["lastname"] = "Doe", // Included by HAVING count(*) > 1
                    ["firstname"] = "Mark", // nummarks = 2
                    ["createdon"] = DateTime.Parse("2020-02-01") // Included by WHERE
                }
            };

            var dataReader = ((SelectNode)queries[0]).Execute(new NodeExecutionContext(GetDataSources(_context), this, new Dictionary<string, DataTypeReference>(), new Dictionary<string, object>(), null), CommandBehavior.Default);
            var dataTable = new DataTable();
            dataTable.Load(dataReader);

            Assert.AreEqual(2, dataTable.Rows.Count);

            Assert.AreEqual("Doe", dataTable.Rows[0]["lastname"]);
            Assert.AreEqual(2, dataTable.Rows[0]["nummarks"]);
            Assert.AreEqual("D", dataTable.Rows[0]["lastinitial"]);

            Assert.AreEqual("Carrington", dataTable.Rows[1]["lastname"]);
            Assert.AreEqual(1, dataTable.Rows[1]["nummarks"]);
            Assert.AreEqual("C", dataTable.Rows[1]["lastinitial"]);
        }

        [TestMethod]
        public void FilterCaseInsensitive()
        {
            var query = "SELECT contactid FROM contact WHERE DATEDIFF(day, '2020-01-01', createdon) < 10 OR lastname = 'Carrington' ORDER BY createdon";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            AssertFetchXml(queries, @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='contactid' />
                        <attribute name='createdon' />
                        <attribute name='lastname' />
                        <order attribute='createdon' />
                    </entity>
                </fetch>
            ");

            var guid1 = Guid.NewGuid();
            var guid2 = Guid.NewGuid();
            var guid3 = Guid.NewGuid();

            _context.Data["contact"] = new Dictionary<Guid, Entity>
            {
                [guid1] = new Entity("contact", guid1)
                {
                    ["lastname"] = "Carrington",
                    ["firstname"] = "Mark",
                    ["createdon"] = DateTime.Parse("2020-02-01"),
                    ["contactid"] = guid1
                },
                [guid2] = new Entity("contact", guid2)
                {
                    ["lastname"] = "CARRINGTON",
                    ["firstname"] = "Mark",
                    ["createdon"] = DateTime.Parse("2020-03-01"),
                    ["contactid"] = guid2
                },
                [guid3] = new Entity("contact", guid3)
                {
                    ["lastname"] = "Bloggs",
                    ["firstname"] = "Joe",
                    ["createdon"] = DateTime.Parse("2020-04-01"),
                    ["contactid"] = guid3
                }
            };

            var dataReader = ((SelectNode)queries[0]).Execute(new NodeExecutionContext(GetDataSources(_context), this, new Dictionary<string, DataTypeReference>(), new Dictionary<string, object>(), null), CommandBehavior.Default);
            var dataTable = new DataTable();
            dataTable.Load(dataReader);

            Assert.AreEqual(2, dataTable.Rows.Count);

            Assert.AreEqual(guid1, ((SqlEntityReference)dataTable.Rows[0]["contactid"]).Id);
            Assert.AreEqual(guid2, ((SqlEntityReference)dataTable.Rows[1]["contactid"]).Id);
        }

        [TestMethod]
        public void GroupCaseInsensitive()
        {
            var query = "SELECT lastname, count(*) FROM contact WHERE DATEDIFF(day, '2020-01-01', createdon) > 10 GROUP BY lastname ORDER BY 2 DESC";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            AssertFetchXml(queries, @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='lastname' />
                        <attribute name='createdon' />
                        <order attribute='lastname' />
                    </entity>
                </fetch>
            ");

            var guid1 = Guid.NewGuid();
            var guid2 = Guid.NewGuid();
            var guid3 = Guid.NewGuid();

            _context.Data["contact"] = new Dictionary<Guid, Entity>
            {
                [guid1] = new Entity("contact", guid1)
                {
                    ["lastname"] = "Carrington",
                    ["firstname"] = "Mark",
                    ["createdon"] = DateTime.Parse("2020-02-01"),
                    ["contactid"] = guid1
                },
                [guid2] = new Entity("contact", guid2)
                {
                    ["lastname"] = "CARRINGTON",
                    ["firstname"] = "Mark",
                    ["createdon"] = DateTime.Parse("2020-03-01"),
                    ["contactid"] = guid2
                },
                [guid3] = new Entity("contact", guid3)
                {
                    ["lastname"] = "Bloggs",
                    ["firstname"] = "Joe",
                    ["createdon"] = DateTime.Parse("2020-04-01"),
                    ["contactid"] = guid3
                }
            };

            var dataReader = ((SelectNode)queries[0]).Execute(new NodeExecutionContext(GetDataSources(_context), this, new Dictionary<string, DataTypeReference>(), new Dictionary<string, object>(), null), CommandBehavior.Default);
            var dataTable = new DataTable();
            dataTable.Load(dataReader);

            Assert.AreEqual(2, dataTable.Rows.Count);

            Assert.AreEqual("Carrington", dataTable.Rows[0]["lastname"]);
            Assert.AreEqual("Bloggs", dataTable.Rows[1]["lastname"]);
        }

        [TestMethod]
        public void AggregateExpressionsWithoutGrouping()
        {
            var query = "SELECT count(DISTINCT firstname + ' ' + lastname) AS distinctnames FROM contact";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            AssertFetchXml(queries, @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='firstname' />
                        <attribute name='lastname' />
                    </entity>
                </fetch>
            ");

            var guid1 = Guid.NewGuid();
            var guid2 = Guid.NewGuid();
            var guid3 = Guid.NewGuid();

            _context.Data["contact"] = new Dictionary<Guid, Entity>
            {
                [guid1] = new Entity("contact", guid1)
                {
                    ["lastname"] = "Carrington",
                    ["firstname"] = "Mark",
                    ["contactid"] = guid1
                },
                [guid2] = new Entity("contact", guid2)
                {
                    ["lastname"] = "CARRINGTON",
                    ["firstname"] = "Mark",
                    ["contactid"] = guid2
                },
                [guid3] = new Entity("contact", guid3)
                {
                    ["lastname"] = "Bloggs",
                    ["firstname"] = "Joe",
                    ["contactid"] = guid3
                }
            };

            var dataReader = ((SelectNode)queries[0]).Execute(new NodeExecutionContext(GetDataSources(_context), this, new Dictionary<string, DataTypeReference>(), new Dictionary<string, object>(), null), CommandBehavior.Default);
            var dataTable = new DataTable();
            dataTable.Load(dataReader);

            Assert.AreEqual(1, dataTable.Rows.Count);

            Assert.AreEqual(2, dataTable.Rows[0]["distinctnames"]);
        }

        [TestMethod]
        public void AggregateQueryProducesAlternative()
        {
            var query = "SELECT name, count(*) FROM account GROUP BY name ORDER BY 2 DESC";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            var select = (SelectNode)queries[0];
            var source = ((TryCatchNode)select.Source).CatchSource;

            var alternativeQuery = new SelectNode { Source = source };
            alternativeQuery.ColumnSet.AddRange(select.ColumnSet);

            AssertFetchXml(new[] { alternativeQuery }, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='name' />
                        <order attribute='name' />
                    </entity>
                </fetch>
            ");

            var guid1 = Guid.NewGuid();
            var guid2 = Guid.NewGuid();
            var guid3 = Guid.NewGuid();

            _context.Data["account"] = new Dictionary<Guid, Entity>
            {
                [guid1] = new Entity("account", guid1)
                {
                    ["name"] = "Data8",
                    ["accountid"] = guid1
                },
                [guid2] = new Entity("account", guid2)
                {
                    ["name"] = "Data8",
                    ["accountid"] = guid2
                },
                [guid3] = new Entity("account", guid3)
                {
                    ["name"] = "Microsoft",
                    ["accountid"] = guid3
                }
            };

            var dataReader = alternativeQuery.Execute(new NodeExecutionContext(GetDataSources(_context), this, new Dictionary<string, DataTypeReference>(), new Dictionary<string, object>(), null), CommandBehavior.Default);
            var dataTable = new DataTable();
            dataTable.Load(dataReader);

            Assert.AreEqual(2, dataTable.Rows.Count);

            Assert.AreEqual("Data8", dataTable.Rows[0]["name"]);
            Assert.AreEqual(2, dataTable.Rows[0][1]);
        }

        [TestMethod]
        public void GuidEntityReferenceInequality()
        {
            var query = "SELECT a.name FROM account a INNER JOIN contact c ON a.primarycontactid = c.contactid WHERE (c.parentcustomerid is null or a.accountid <> c.parentcustomerid)";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            var select = (SelectNode)queries[0];

            var account1 = Guid.NewGuid();
            var account2 = Guid.NewGuid();
            var contact1 = Guid.NewGuid();
            var contact2 = Guid.NewGuid();

            _context.Data["account"] = new Dictionary<Guid, Entity>
            {
                [account1] = new Entity("account", account1)
                {
                    ["name"] = "Data8",
                    ["accountid"] = account1,
                    ["primarycontactid"] = new EntityReference("contact", contact1)
                },
                [account2] = new Entity("account", account2)
                {
                    ["name"] = "Microsoft",
                    ["accountid"] = account2,
                    ["primarycontactid"] = new EntityReference("contact", contact2)
                }
            };
            _context.Data["contact"] = new Dictionary<Guid, Entity>
            {
                [contact1] = new Entity("contact", contact1)
                {
                    ["parentcustomerid"] = new EntityReference("account", account2),
                    ["contactid"] = contact1
                },
                [contact2] = new Entity("contact", contact2)
                {
                    ["parentcustomerid"] = new EntityReference("account", account2),
                    ["contactid"] = contact2
                }
            };

            var dataReader = select.Execute(new NodeExecutionContext(GetDataSources(_context), this, new Dictionary<string, DataTypeReference>(), new Dictionary<string, object>(), null), CommandBehavior.Default);
            var dataTable = new DataTable();
            dataTable.Load(dataReader);

            Assert.AreEqual(1, dataTable.Rows.Count);

            Assert.AreEqual("Data8", dataTable.Rows[0]["name"]);
        }

        [TestMethod]
        public void UpdateGuidToEntityReference()
        {
            var query = "UPDATE a SET primarycontactid = c.contactid FROM account AS a INNER JOIN contact AS c ON a.accountid = c.parentcustomerid";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            var update = (UpdateNode)queries[0];

            var account1 = Guid.NewGuid();
            var account2 = Guid.NewGuid();
            var contact1 = Guid.NewGuid();
            var contact2 = Guid.NewGuid();

            _context.Data["account"] = new Dictionary<Guid, Entity>
            {
                [account1] = new Entity("account", account1)
                {
                    ["name"] = "Data8",
                    ["accountid"] = account1
                },
                [account2] = new Entity("account", account2)
                {
                    ["name"] = "Microsoft",
                    ["accountid"] = account2
                }
            };
            _context.Data["contact"] = new Dictionary<Guid, Entity>
            {
                [contact1] = new Entity("contact", contact1)
                {
                    ["parentcustomerid"] = new EntityReference("account", account1),
                    ["contactid"] = contact1
                },
                [contact2] = new Entity("contact", contact2)
                {
                    ["parentcustomerid"] = new EntityReference("account", account2),
                    ["contactid"] = contact2
                }
            };

            update.Execute(new NodeExecutionContext(GetDataSources(_context), this, new Dictionary<string, DataTypeReference>(), new Dictionary<string, object>(), null), out _, out _);

            Assert.AreEqual(new EntityReference("contact", contact1), _context.Data["account"][account1].GetAttributeValue<EntityReference>("primarycontactid"));
            Assert.AreEqual(new EntityReference("contact", contact2), _context.Data["account"][account2].GetAttributeValue<EntityReference>("primarycontactid"));
        }

        [TestMethod]
        public void CompareDateFields()
        {
            var query = "DELETE c2 FROM contact c1 INNER JOIN contact c2 ON c1.parentcustomerid = c2.parentcustomerid AND c2.createdon > c1.createdon";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            AssertFetchXml(queries, @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='createdon' />
                        <attribute name='contactid' />
                        <link-entity name='contact' to='parentcustomerid' from='parentcustomerid' alias='c2' link-type='inner'>
                            <attribute name='contactid' />
                            <attribute name='createdon' />
                            <order attribute='contactid' />
                        </link-entity>
                        <order attribute='contactid' />
                    </entity>
                </fetch>");

            var delete = (DeleteNode)queries[0];
            Assert.IsNotInstanceOfType(delete.Source, typeof(FetchXmlScan));
        }

        [TestMethod]
        public void ColumnComparison()
        {
            var query = "SELECT firstname, lastname FROM contact WHERE firstname = lastname";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            AssertFetchXml(queries, @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='firstname' />
                        <attribute name='lastname' />
                        <filter>
                            <condition attribute='firstname' operator='eq' valueof='lastname' />
                        </filter>
                    </entity>
                </fetch>
            ");
        }

        [TestMethod]
        public void QuotedIdentifierError()
        {
            var query = "SELECT firstname, lastname FROM contact WHERE firstname = \"mark\"";

            try
            {
                var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, new OptionsWrapper(this) { QuotedIdentifiers = true });
                var queries = planBuilder.Build(query, null, out _);

                Assert.Fail("Expected exception");
            }
            catch (NotSupportedQueryFragmentException ex)
            {
                Assert.IsTrue(ex.Suggestion == "Did you mean 'mark'?");
            }
        }

        [TestMethod]
        public void FilterExpressionConstantValueToFetchXml()
        {
            var query = "SELECT firstname, lastname FROM contact WHERE firstname = 'Ma' + 'rk'";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            AssertFetchXml(queries, $@"
                <fetch>
                    <entity name='contact'>
                        <attribute name='firstname' />
                        <attribute name='lastname' />
                        <filter>
                            <condition attribute='firstname' operator='eq' value='Mark' />
                        </filter>
                    </entity>
                </fetch>
            ");
        }

        [TestMethod]
        public void Count1ConvertedToCountStar()
        {
            var query = "SELECT COUNT(1) FROM contact OPTION(USE HINT('RETRIEVE_TOTAL_RECORD_COUNT'))";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            var selectNode = (SelectNode)queries[0];
            var computeScalarNode = (ComputeScalarNode)selectNode.Source;
            var count = (RetrieveTotalRecordCountNode)computeScalarNode.Source;
        }

        [TestMethod]
        public void CaseInsensitive()
        {
            var query = "Select Name From Account";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            AssertFetchXml(queries, $@"
                <fetch>
                    <entity name='account'>
                        <attribute name='name' />
                    </entity>
                </fetch>
            ");
        }

        [TestMethod]
        public void ContainsValues1()
        {
            var query = "SELECT new_name FROM new_customentity WHERE CONTAINS(new_optionsetvaluecollection, '1')";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            AssertFetchXml(queries, $@"
                <fetch>
                    <entity name='new_customentity'>
                        <attribute name='new_name' />
                        <filter>
                            <condition attribute='new_optionsetvaluecollection' operator='contain-values'>
                                <value>1</value>
                            </condition>
                        </filter>
                    </entity>
                </fetch>
            ");
        }

        [TestMethod]
        public void ContainsValuesFunction1()
        {
            var query = "SELECT new_name FROM new_customentity WHERE new_optionsetvaluecollection = containvalues(1)";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            AssertFetchXml(queries, $@"
                <fetch>
                    <entity name='new_customentity'>
                        <attribute name='new_name' />
                        <filter>
                            <condition attribute='new_optionsetvaluecollection' operator='contain-values'>
                                <value>1</value>
                            </condition>
                        </filter>
                    </entity>
                </fetch>
            ");
        }

        [TestMethod]
        public void ContainsValues()
        {
            var query = "SELECT new_name FROM new_customentity WHERE CONTAINS(new_optionsetvaluecollection, '1 OR 2')";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            AssertFetchXml(queries, $@"
                <fetch>
                    <entity name='new_customentity'>
                        <attribute name='new_name' />
                        <filter>
                            <condition attribute='new_optionsetvaluecollection' operator='contain-values'>
                                <value>1</value>
                                <value>2</value>
                            </condition>
                        </filter>
                    </entity>
                </fetch>
            ");
        }

        [TestMethod]
        public void ContainsValuesFunction()
        {
            var query = "SELECT new_name FROM new_customentity WHERE new_optionsetvaluecollection = containvalues(1, 2)";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            AssertFetchXml(queries, $@"
                <fetch>
                    <entity name='new_customentity'>
                        <attribute name='new_name' />
                        <filter>
                            <condition attribute='new_optionsetvaluecollection' operator='contain-values'>
                                <value>1</value>
                                <value>2</value>
                            </condition>
                        </filter>
                    </entity>
                </fetch>
            ");
        }

        [TestMethod]
        public void NotContainsValues()
        {
            var query = "SELECT new_name FROM new_customentity WHERE NOT CONTAINS(new_optionsetvaluecollection, '1 OR 2')";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            AssertFetchXml(queries, $@"
                <fetch>
                    <entity name='new_customentity'>
                        <attribute name='new_name' />
                        <filter>
                            <condition attribute='new_optionsetvaluecollection' operator='not-contain-values'>
                                <value>1</value>
                                <value>2</value>
                            </condition>
                        </filter>
                    </entity>
                </fetch>
            ");
        }

        [TestMethod]
        public void TSqlAggregates()
        {
            var query = "SELECT COUNT(*) AS count FROM account WHERE name IS NULL";

            BuildTDSQuery(planBuilder =>
            {
                var queries = planBuilder.Build(query, null, out var useTDSEndpointDirectly);
                Assert.IsTrue(useTDSEndpointDirectly);
                Assert.AreEqual(1, queries.Length);
                Assert.IsInstanceOfType(queries[0], typeof(SqlNode));
            });
        }

        [TestMethod]
        public void ImplicitTypeConversion()
        {
            var query = "SELECT employees / 2.0 AS half FROM account";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            var account1 = Guid.NewGuid();
            var account2 = Guid.NewGuid();

            _context.Data["account"] = new Dictionary<Guid, Entity>
            {
                [account1] = new Entity("account", account1)
                {
                    ["employees"] = null,
                    ["accountid"] = account1
                },
                [account2] = new Entity("account", account2)
                {
                    ["employees"] = 2,
                    ["accountid"] = account2
                }
            };

            var dataReader = ((SelectNode)queries[0]).Execute(new NodeExecutionContext(GetDataSources(_context), this, new Dictionary<string, DataTypeReference>(), new Dictionary<string, object>(), null), CommandBehavior.Default);
            var dataTable = new DataTable();
            dataTable.Load(dataReader);

            Assert.AreEqual(DBNull.Value, dataTable.Rows[0]["half"]);
            Assert.AreEqual(1M, dataTable.Rows[1]["half"]);
        }

        [TestMethod]
        public void ImplicitTypeConversionComparison()
        {
            var query = "SELECT accountid FROM account WHERE turnover / 2 > 10";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            var account1 = Guid.NewGuid();
            var account2 = Guid.NewGuid();

            _context.Data["account"] = new Dictionary<Guid, Entity>
            {
                [account1] = new Entity("account", account1)
                {
                    ["turnover"] = null,
                    ["accountid"] = account1
                },
                [account2] = new Entity("account", account2)
                {
                    ["turnover"] = new Money(21),
                    ["accountid"] = account2
                }
            };

            var dataReader = ((SelectNode)queries[0]).Execute(new NodeExecutionContext(GetDataSources(_context), this, new Dictionary<string, DataTypeReference>(), new Dictionary<string, object>(), null), CommandBehavior.Default);
            var dataTable = new DataTable();
            dataTable.Load(dataReader);

            Assert.AreEqual(account2, ((SqlEntityReference)dataTable.Rows[0]["accountid"]).Id);
        }

        [TestMethod]
        public void GlobalOptionSet()
        {
            var query = "SELECT displayname FROM metadata.globaloptionset WHERE name = 'test'";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            Assert.IsInstanceOfType(queries.Single(), typeof(SelectNode));

            var selectNode = (SelectNode)queries[0];
            Assert.AreEqual(1, selectNode.ColumnSet.Count);
            Assert.AreEqual("globaloptionset.displayname", selectNode.ColumnSet[0].SourceColumn);
            var filterNode = (FilterNode)selectNode.Source;
            Assert.AreEqual("name = 'test'", filterNode.Filter.ToSql());
            var optionsetNode = (GlobalOptionSetQueryNode)filterNode.Source;

            var dataReader = selectNode.Execute(new NodeExecutionContext(GetDataSources(_context), this, new Dictionary<string, DataTypeReference>(), new Dictionary<string, object>(), null), CommandBehavior.Default);
            var dataTable = new DataTable();
            dataTable.Load(dataReader);

            Assert.AreEqual(1, dataTable.Rows.Count);
        }

        [TestMethod]
        public void EntityDetails()
        {
            var query = "SELECT logicalname FROM metadata.entity ORDER BY 1";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            Assert.IsInstanceOfType(queries.Single(), typeof(SelectNode));

            var selectNode = (SelectNode)queries[0];
            var sortNode = (SortNode)selectNode.Source;
            var metadataNode = (MetadataQueryNode)sortNode.Source;

            var dataReader = selectNode.Execute(new NodeExecutionContext(GetDataSources(_context), this, new Dictionary<string, DataTypeReference>(), new Dictionary<string, object>(), null), CommandBehavior.Default);
            var dataTable = new DataTable();
            dataTable.Load(dataReader);

            Assert.AreEqual(3, dataTable.Rows.Count);
            Assert.AreEqual("account", dataTable.Rows[0]["logicalname"]);
            Assert.AreEqual("contact", dataTable.Rows[1]["logicalname"]);
            Assert.AreEqual("new_customentity", dataTable.Rows[2]["logicalname"]);
        }

        [TestMethod]
        public void AttributeDetails()
        {
            var query = "SELECT e.logicalname, a.logicalname FROM metadata.entity e INNER JOIN metadata.attribute a ON e.logicalname = a.entitylogicalname WHERE e.logicalname = 'new_customentity' ORDER BY 1, 2";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            var dataReader = ((SelectNode)queries[0]).Execute(new NodeExecutionContext(GetDataSources(_context), this, new Dictionary<string, DataTypeReference>(), new Dictionary<string, object>(), null), CommandBehavior.Default);
            var dataTable = new DataTable();
            dataTable.Load(dataReader);

            Assert.AreEqual(9, dataTable.Rows.Count);
            var row = 0;
            Assert.AreEqual("new_boolprop", dataTable.Rows[row++]["logicalname1"]);
            Assert.AreEqual("new_customentityid", dataTable.Rows[row++]["logicalname1"]);
            Assert.AreEqual("new_decimalprop", dataTable.Rows[row++]["logicalname1"]);
            Assert.AreEqual("new_doubleprop", dataTable.Rows[row++]["logicalname1"]);
            Assert.AreEqual("new_name", dataTable.Rows[row++]["logicalname1"]);
            Assert.AreEqual("new_optionsetvalue", dataTable.Rows[row++]["logicalname1"]);
            Assert.AreEqual("new_optionsetvaluecollection", dataTable.Rows[row++]["logicalname1"]);
            Assert.AreEqual("new_optionsetvaluename", dataTable.Rows[row++]["logicalname1"]);
            Assert.AreEqual("new_parentid", dataTable.Rows[row++]["logicalname1"]);
        }

        [TestMethod]
        public void OptionSetNameSelect()
        {
            var query = "SELECT new_optionsetvalue, new_optionsetvaluename FROM new_customentity ORDER BY new_optionsetvaluename";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            var record1 = Guid.NewGuid();
            var record2 = Guid.NewGuid();

            _context.Data["new_customentity"] = new Dictionary<Guid, Entity>
            {
                [record1] = new Entity("new_customentity", record1)
                {
                    ["new_optionsetvalue"] = null,
                    ["new_customentityid"] = record1
                },
                [record2] = new Entity("new_customentity", record2)
                {
                    ["new_optionsetvalue"] = new OptionSetValue((int) Metadata.New_OptionSet.Value1),
                    ["new_customentityid"] = record2,
                    FormattedValues =
                    {
                        ["new_optionsetvalue"] = Metadata.New_OptionSet.Value1.ToString()
                    }
                }
            };

            var select = (SelectNode)queries[0];

            AssertFetchXml(queries, @"
                <fetch>
                    <entity name='new_customentity'>
                        <attribute name='new_optionsetvalue' />
                        <order attribute='new_optionsetvalue' />
                    </entity>
                </fetch>");

            CollectionAssert.AreEqual(new[] { "new_optionsetvalue", "new_optionsetvaluename" }, select.ColumnSet.Select(c => c.OutputColumn).ToList());

            var dataReader = select.Execute(new NodeExecutionContext(GetDataSources(_context), this, new Dictionary<string, DataTypeReference>(), new Dictionary<string, object>(), null), CommandBehavior.Default);
            var dataTable = new DataTable();
            dataTable.Load(dataReader);

            Assert.AreEqual(DBNull.Value, dataTable.Rows[0]["new_optionsetvalue"]);
            Assert.AreEqual(DBNull.Value, dataTable.Rows[0]["new_optionsetvaluename"]);
            Assert.AreEqual((int)Metadata.New_OptionSet.Value1, dataTable.Rows[1]["new_optionsetvalue"]);
            Assert.AreEqual(Metadata.New_OptionSet.Value1.ToString(), dataTable.Rows[1]["new_optionsetvaluename"]);
        }

        [TestMethod]
        public void OptionSetNameFilter()
        {
            var query = "SELECT new_customentityid FROM new_customentity WHERE new_optionsetvaluename = 'Value1'";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            AssertFetchXml(queries, @"
                <fetch>
                    <entity name='new_customentity'>
                        <attribute name='new_customentityid' />
                        <filter>
                            <condition attribute='new_optionsetvalue' operator='eq' value='100001' />
                        </filter>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void EntityReferenceNameSelect()
        {
            var query = "SELECT primarycontactid, primarycontactidname FROM account ORDER BY primarycontactidname";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            AssertFetchXml(queries, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='primarycontactid' />
                        <order attribute='primarycontactid' />
                    </entity>
                </fetch>");

            var select = (SelectNode)queries[0];

            CollectionAssert.AreEqual(new[] { "primarycontactid", "primarycontactidname" }, select.ColumnSet.Select(c => c.OutputColumn).ToList());
        }

        [TestMethod]
        public void EntityReferenceNameFilter()
        {
            var query = "SELECT accountid FROM account WHERE primarycontactidname = 'Mark Carrington'";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            AssertFetchXml(queries, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='accountid' />
                        <filter>
                            <condition attribute='primarycontactidname' operator='eq' value='Mark Carrington' />
                        </filter>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void UpdateMissingAlias()
        {
            var query = "UPDATE account SET primarycontactid = c.contactid FROM account AS a INNER JOIN contact AS c ON a.name = c.fullname";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);
        }

        [TestMethod]
        public void UpdateMissingAliasAmbiguous()
        {
            var query = "UPDATE account SET primarycontactid = other.primarycontactid FROM account AS main INNER JOIN account AS other on main.name = other.name";

            try
            {
                var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
                var queries = planBuilder.Build(query, null, out _);
                Assert.Fail("Expected exception");
            }
            catch (NotSupportedQueryFragmentException ex)
            {
                Assert.AreEqual("The table 'account' is ambiguous", ex.Message);
            }
        }

        [TestMethod]
        public void ConvertIntToBool()
        {
            var query = "UPDATE new_customentity SET new_boolprop = CASE WHEN new_name = 'True' THEN 1 ELSE 0 END";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);
        }

        [TestMethod]
        public void ImpersonateRevert()
        {
            var query = @"
                EXECUTE AS LOGIN = 'test1'
                REVERT";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            Assert.IsInstanceOfType(queries[0], typeof(ExecuteAsNode));
            Assert.IsInstanceOfType(queries[1], typeof(RevertNode));

            AssertFetchXml(new[] { queries[0] }, @"
                <fetch>
                    <entity name='systemuser'>
                        <attribute name='systemuserid' />
                        <filter>
                            <condition attribute='domainname' operator='eq' value='test1' />
                        </filter>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void OrderByOuterEntityWithLinkEntityWithNoAttributes()
        {
            var query = @"
                SELECT contact.fullname
                FROM contact INNER JOIN account ON contact.contactid = account.primarycontactid INNER JOIN new_customentity ON contact.parentcustomerid = new_customentity.new_parentid
                ORDER BY account.employees, contact.fullname";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            AssertFetchXml(new[] { queries[0] }, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='employees' />
                        <attribute name='accountid' />
                        <link-entity name='contact' from='contactid' to='primarycontactid' alias='contact' link-type='inner'>
                            <attribute name='fullname' />
                            <attribute name='contactid' />
                            <link-entity name='new_customentity' from='new_parentid' to='parentcustomerid' alias='new_customentity' link-type='inner'>
                                <attribute name='new_customentityid' />
                                <order attribute='new_customentityid' />
                            </link-entity>
                            <order attribute='contactid' />
                        </link-entity>
                        <order attribute='accountid' />
                    </entity>
                </fetch>");

            Assert.IsNotInstanceOfType(((SelectNode)queries[0]).Source, typeof(FetchXmlScan));
        }

        [TestMethod]
        public void UpdateUsingTSql()
        {
            var query = @"
                UPDATE c
                SET parentcustomerid = account.accountid, parentcustomeridtype = 'account'
                FROM contact AS c INNER JOIN account ON c.parentcustomerid = account.accountid INNER JOIN new_customentity ON c.parentcustomerid = new_customentity.new_parentid
                WHERE c.fullname IN (SELECT fullname FROM contact WHERE firstname = 'Mark')";

            BuildTDSQuery(planBuilder =>
            {
                var queries = planBuilder.Build(query, null, out _);

                var tds = (SqlNode)((UpdateNode)queries[0]).Source;

                Assert.AreEqual(Regex.Replace(@"
                SELECT DISTINCT
                    c.contactid AS contactid,
                    account.accountid AS new_parentcustomerid,
                    'account' AS new_parentcustomeridtype
                FROM
                    contact AS c
                    INNER JOIN account
                        ON c.parentcustomerid = account.accountid
                    INNER JOIN new_customentity
                        ON c.parentcustomerid = new_customentity.new_parentid
                WHERE
                    c.fullname IN (SELECT fullname FROM contact WHERE firstname = 'Mark')", @"\s+", " ").Trim(), Regex.Replace(tds.Sql, @"\s+", " ").Trim());
            });
        }

        [TestMethod]
        public void DeleteUsingTSql()
        {
            var query = @"
                DELETE c
                FROM contact AS c INNER JOIN account ON c.parentcustomerid = account.accountid INNER JOIN new_customentity ON c.parentcustomerid = new_customentity.new_parentid
                WHERE c.fullname IN (SELECT fullname FROM contact WHERE firstname = 'Mark')";

            BuildTDSQuery(planBuilder =>
            {
                var queries = planBuilder.Build(query, null, out _);

                var tds = (SqlNode)((DeleteNode)queries[0]).Source;

                Assert.AreEqual(Regex.Replace(@"
                SELECT DISTINCT
                    c.contactid AS contactid
                FROM
                    contact AS c
                    INNER JOIN account
                        ON c.parentcustomerid = account.accountid
                    INNER JOIN new_customentity
                        ON c.parentcustomerid = new_customentity.new_parentid
                WHERE
                    c.fullname IN (SELECT fullname FROM contact WHERE firstname = 'Mark')", @"\s+", " ").Trim(), Regex.Replace(tds.Sql, @"\s+", " ").Trim());
            });
        }

        private void BuildTDSQuery(Action<ExecutionPlanBuilder> action)
        {
            var ds = _localDataSource["local"];
            var con = ds.Connection;
            ds.Connection = null;
            try
            {
                var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, new OptionsWrapper(this) { UseTDSEndpoint = true });
                action(planBuilder);
            }
            finally
            {
                ds.Connection = con;
            }
        }

        [TestMethod]
        public void OrderByAggregateByIndex()
        {
            var query = "SELECT firstname, count(*) FROM contact GROUP BY firstname ORDER BY 2";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            AssertFetchXml(queries, @"
                <fetch aggregate='true'>
                    <entity name='contact'>
                        <attribute name='firstname' groupby='true' alias='firstname' />
                        <attribute name='contactid' aggregate='count' alias='count' />
                        <order alias='count' />
                    </entity>
                </fetch>
            ");
        }

        [TestMethod]
        public void OrderByAggregateJoinByIndex()
        {
            var query = "SELECT firstname, count(*) FROM contact INNER JOIN account ON contact.parentcustomerid = account.accountid GROUP BY firstname ORDER BY 2";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            AssertFetchXml(queries, @"
                <fetch aggregate='true'>
                    <entity name='contact'>
                        <attribute name='firstname' groupby='true' alias='firstname' />
                        <attribute name='contactid' aggregate='count' alias='count' />
                        <link-entity name='account' from='accountid' to='parentcustomerid' link-type='inner' alias='account'>
                        </link-entity>
                        <order alias='count' />
                    </entity>
                </fetch>
            ");
        }

        [TestMethod]
        public void AggregateAlternativeDoesNotOrderByLinkEntity()
        {
            var query = "SELECT name, count(*) FROM contact INNER JOIN account ON contact.parentcustomerid = account.accountid GROUP BY name";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            var select = (SelectNode)queries[0];
            var source = select.Source;

            while (!(source is IFetchXmlExecutionPlanNode))
            {
                if (source is TryCatchNode tryCatch)
                    source = tryCatch.CatchSource;
                else
                    source = (IDataExecutionPlanNodeInternal) source.GetSources().First();
            }

            AssertFetchXml(new[] { new SelectNode { Source = source } }, @"
                <fetch>
                    <entity name='contact'>
                        <link-entity name='account' from='accountid' to='parentcustomerid' link-type='inner' alias='account'>
                            <attribute name='name'/>
                        </link-entity>
                    </entity>
                </fetch>
            ");
        }

        [TestMethod]
        public void CharIndex()
        {
            var query = "SELECT CHARINDEX('a', fullname) AS ci0, CHARINDEX('a', fullname, 1) AS ci1, CHARINDEX('a', fullname, 2) AS ci2, CHARINDEX('a', fullname, 3) AS ci3, CHARINDEX('a', fullname, 8) AS ci8 FROM contact";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            var contact1 = Guid.NewGuid();

            _context.Data["contact"] = new Dictionary<Guid, Entity>
            {
                [contact1] = new Entity("contact", contact1)
                {
                    ["fullname"] = "Mark Carrington",
                    ["contactid"] = contact1
                }
            };

            var dataReader = ((SelectNode)queries[0]).Execute(new NodeExecutionContext(GetDataSources(_context), this, new Dictionary<string, DataTypeReference>(), new Dictionary<string, object>(), null), CommandBehavior.Default);
            var dataTable = new DataTable();
            dataTable.Load(dataReader);

            Assert.AreEqual(2, dataTable.Rows[0]["ci0"]);
            Assert.AreEqual(2, dataTable.Rows[0]["ci1"]);
            Assert.AreEqual(2, dataTable.Rows[0]["ci2"]);
            Assert.AreEqual(7, dataTable.Rows[0]["ci3"]);
            Assert.AreEqual(0, dataTable.Rows[0]["ci8"]);
        }

        [TestMethod]
        public void CastDateTimeToDate()
        {
            var query = "SELECT CAST(createdon AS date) AS converted FROM contact";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            var contact1 = Guid.NewGuid();

            _context.Data["contact"] = new Dictionary<Guid, Entity>
            {
                [contact1] = new Entity("contact", contact1)
                {
                    ["createdon"] = new DateTime(2000, 1, 1, 12, 34, 56),
                    ["contactid"] = contact1
                }
            };

            var dataReader = ((SelectNode)queries[0]).Execute(new NodeExecutionContext(GetDataSources(_context), this, new Dictionary<string, DataTypeReference>(), new Dictionary<string, object>(), null), CommandBehavior.Default);
            var dataTable = new DataTable();
            dataTable.Load(dataReader);

            Assert.AreEqual(new DateTime(2000, 1, 1), dataTable.Rows[0]["converted"]);
        }

        [TestMethod]
        public void GroupByPrimaryFunction()
        {
            var query = "SELECT left(firstname, 1) AS initial, count(*) AS count FROM contact GROUP BY left(firstname, 1) ORDER BY 2 DESC";

            var planBuilder = new ExecutionPlanBuilder(_localDataSource.Values, this);
            var queries = planBuilder.Build(query, null, out _);

            var contact1 = Guid.NewGuid();
            var contact2 = Guid.NewGuid();
            var contact3 = Guid.NewGuid();

            _context.Data["contact"] = new Dictionary<Guid, Entity>
            {
                [contact1] = new Entity("contact", contact1)
                {
                    ["firstname"] = "Mark",
                    ["contactid"] = contact1
                },
                [contact2] = new Entity("contact", contact2)
                {
                    ["firstname"] = "Matt",
                    ["contactid"] = contact2
                },
                [contact3] = new Entity("contact", contact3)
                {
                    ["firstname"] = "Rich",
                    ["contactid"] = contact3
                }
            };

            var dataReader = ((SelectNode)queries[0]).Execute(new NodeExecutionContext(GetDataSources(_context), this, new Dictionary<string, DataTypeReference>(), new Dictionary<string, object>(), null), CommandBehavior.Default);
            var dataTable = new DataTable();
            dataTable.Load(dataReader);

            Assert.AreEqual("M", dataTable.Rows[0]["initial"]);
            Assert.AreEqual(2, dataTable.Rows[0]["count"]);
            Assert.AreEqual("R", dataTable.Rows[1]["initial"]);
            Assert.AreEqual(1, dataTable.Rows[1]["count"]);
        }

        private void AssertFetchXml(IRootExecutionPlanNode[] queries, string fetchXml)
        {
            Assert.AreEqual(1, queries.Length);

            var query = queries[0].GetSources().First() as IDataExecutionPlanNode;
            while (query != null)
            {
                if (query is IFetchXmlExecutionPlanNode)
                    break;

                var source = query.GetSources().FirstOrDefault();

                if (source == null)
                    Assert.Fail("No FetchXML source found");

                query = source as IDataExecutionPlanNode;
            }

            try
            {
                var serializer = new XmlSerializer(typeof(FetchXml.FetchType));
                FetchXml.FetchType expectedFetch;
                FetchXml.FetchType actualFetch;

                using (var reader = new StringReader(fetchXml))
                {
                    expectedFetch = (FetchXml.FetchType)serializer.Deserialize(reader);
                }

                using (var reader = new StringReader(((IFetchXmlExecutionPlanNode)query).FetchXmlString))
                {
                    actualFetch = (FetchXml.FetchType)serializer.Deserialize(reader);
                }

                PropertyEqualityAssert.Equals(expectedFetch, actualFetch);
            }
            catch (AssertFailedException ex)
            {
                Assert.Fail($"Expected:\r\n{fetchXml}\r\n\r\nActual:\r\n{((IFetchXmlExecutionPlanNode)query).FetchXmlString}\r\n\r\n{ex.Message}");
            }
        }

        void IQueryExecutionOptions.Progress(double? progress, string message)
        {
        }

        bool IQueryExecutionOptions.ContinueRetrieve(int count)
        {
            return true;
        }

        void IQueryExecutionOptions.ConfirmInsert(ConfirmDmlStatementEventArgs e)
        {
        }

        void IQueryExecutionOptions.ConfirmUpdate(ConfirmDmlStatementEventArgs e)
        {
        }

        void IQueryExecutionOptions.ConfirmDelete(ConfirmDmlStatementEventArgs e)
        {
        }
    }
}
