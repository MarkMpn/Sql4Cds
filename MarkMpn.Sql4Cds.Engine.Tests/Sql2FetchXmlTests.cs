using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.RegularExpressions;
using System.Xml.Serialization;
using FakeXrmEasy;
using FakeXrmEasy.FakeMessageExecutors;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Metadata;
using Microsoft.Xrm.Sdk.Metadata.Query;
using Microsoft.Xrm.Sdk.Query;

namespace MarkMpn.Sql4Cds.Engine.Tests
{
    [TestClass]
    public class Sql2FetchXmlTests : IQueryExecutionOptions
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

        bool IQueryExecutionOptions.UseLocalTimeZone => false;

        List<JoinOperator> IQueryExecutionOptions.JoinOperatorsAvailable => new List<JoinOperator> { JoinOperator.Inner, JoinOperator.LeftOuter };

        bool IQueryExecutionOptions.BypassCustomPlugins => false;

        void IQueryExecutionOptions.RetrievingNextPage()
        {
        }

        string IQueryExecutionOptions.PrimaryDataSource => "local";

        Guid IQueryExecutionOptions.UserId => Guid.NewGuid();

        [TestMethod]
        public void SimpleSelect()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = "SELECT accountid, name FROM account";

            var queries = sql2FetchXml.Convert(query);

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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = "SELECT accountid, name, name FROM account";

            var queries = sql2FetchXml.Convert(query);

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
            }, ((SelectQuery)queries[0]).ColumnSet);
        }

        [TestMethod]
        public void SelectStar()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = "SELECT * FROM account";

            var queries = sql2FetchXml.Convert(query);

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
                "primarycontactid",
                "primarycontactidname",
                "turnover"
            }, ((SelectQuery)queries[0]).ColumnSet);
        }

        [TestMethod]
        public void SelectStarAndField()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = "SELECT *, name FROM account";

            var queries = sql2FetchXml.Convert(query);

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
                "primarycontactid",
                "primarycontactidname",
                "turnover",
                "name"
            }, ((SelectQuery)queries[0]).ColumnSet);
        }

        [TestMethod]
        public void SimpleFilter()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = "SELECT accountid, name FROM account WHERE name = 'test'";

            var queries = sql2FetchXml.Convert(query);

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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = "SELECT accountid, name FROM account WHERE employees BETWEEN 1 AND 10 AND turnover NOT BETWEEN 2 AND 20";

            var queries = sql2FetchXml.Convert(query);

            AssertFetchXml(queries, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='accountid' />
                        <attribute name='name' />
                        <filter>
                            <filter>
                                <condition attribute='employees' operator='ge' value='1' />
                                <condition attribute='employees' operator='le' value='10' />
                            </filter>
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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = "SELECT contactid, firstname FROM contact WHERE createdon = lastxdays(7)";

            var queries = sql2FetchXml.Convert(query);

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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = "SELECT accountid, name FROM account WHERE name = 'test' OR (accountid is not null and name like 'foo%')";

            var queries = sql2FetchXml.Convert(query);

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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = "SELECT accountid, name FROM account ORDER BY name DESC, accountid";

            var queries = sql2FetchXml.Convert(query);

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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = "SELECT accountid, name FROM account ORDER BY 2 DESC, 1";

            var queries = sql2FetchXml.Convert(query);

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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = "SELECT accountid, name as accountname FROM account ORDER BY name";

            var queries = sql2FetchXml.Convert(query);

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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = "SELECT TOP 10 accountid, name FROM account";

            var queries = sql2FetchXml.Convert(query);

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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = "SELECT TOP (10) accountid, name FROM account";

            var queries = sql2FetchXml.Convert(query);

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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = "SELECT TOP 10000 accountid, name FROM account";

            var queries = sql2FetchXml.Convert(query);

            AssertFetchXml(queries, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='accountid' />
                        <attribute name='name' />
                    </entity>
                </fetch>
            ");

            var converted = (SelectQuery)queries[0];

            Assert.AreEqual(1, converted.Extensions.Count);
        }

        [TestMethod]
        public void NoLock()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = "SELECT accountid, name FROM account (NOLOCK)";

            var queries = sql2FetchXml.Convert(query);

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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = "SELECT DISTINCT name FROM account";

            var queries = sql2FetchXml.Convert(query);

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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = "SELECT accountid, name FROM account ORDER BY name OFFSET 100 ROWS FETCH NEXT 50 ROWS ONLY";

            var queries = sql2FetchXml.Convert(query);

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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = "SELECT accountid, name FROM account INNER JOIN contact ON primarycontactid = contactid";

            var queries = sql2FetchXml.Convert(query);

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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = "SELECT contact.contactid, contact.firstname, manager.firstname FROM contact LEFT OUTER JOIN contact AS manager ON contact.parentcustomerid = manager.contactid";

            var queries = sql2FetchXml.Convert(query);

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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = "SELECT accountid, name FROM account INNER JOIN contact ON accountid = parentcustomerid AND (firstname = 'Mark' OR lastname = 'Carrington')";

            var queries = sql2FetchXml.Convert(query);

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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = "SELECT accountid, name FROM account INNER JOIN contact ON accountid = parentcustomerid OR (firstname = 'Mark' AND lastname = 'Carrington')";

            var queries = sql2FetchXml.Convert(query);

            Assert.AreNotEqual(0, ((SelectQuery)queries[0]).Extensions.Count);
        }

        [TestMethod]
        public void SortOnLinkEntity()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = "SELECT TOP 100 accountid, name FROM account INNER JOIN contact ON primarycontactid = contactid ORDER BY name, firstname";

            var queries = sql2FetchXml.Convert(query);

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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = "SELECT TOP 100 accountid, name FROM account INNER JOIN contact ON accountid = parentcustomerid ORDER BY name, firstname";

            var queries = sql2FetchXml.Convert(query);

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

            Assert.AreEqual(2, ((FetchXmlQuery)queries[0]).Extensions.Count);
        }

        [TestMethod]
        public void SimpleAggregate()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = "SELECT count(*), count(name), count(DISTINCT name), max(name), min(name), avg(name) FROM account";

            var queries = sql2FetchXml.Convert(query);

            AssertFetchXml(queries, @"
                <fetch aggregate='true'>
                    <entity name='account'>
                        <attribute name='accountid' aggregate='count' alias='count' />
                        <attribute name='name' aggregate='countcolumn' alias='name_count' />
                        <attribute name='name' aggregate='countcolumn' distinct='true' alias='name_count_distinct' />
                        <attribute name='name' aggregate='max' alias='name_max' />
                        <attribute name='name' aggregate='min' alias='name_min' />
                        <attribute name='name' aggregate='avg' alias='name_avg' />
                    </entity>
                </fetch>
            ");
        }

        [TestMethod]
        public void GroupBy()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = "SELECT name, count(*) FROM account GROUP BY name";

            var queries = sql2FetchXml.Convert(query);

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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = "SELECT name, count(*) FROM account GROUP BY name ORDER BY name, count(*)";

            var queries = sql2FetchXml.Convert(query);

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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = "SELECT name, firstname, count(*) FROM account INNER JOIN contact ON parentcustomerid = account.accountid GROUP BY name, firstname ORDER BY firstname, name";

            var queries = sql2FetchXml.Convert(query);

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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = "SELECT name, firstname, count(*) as count FROM account INNER JOIN contact ON parentcustomerid = account.accountid GROUP BY name, firstname ORDER BY count";

            var queries = sql2FetchXml.Convert(query);

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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = "UPDATE contact SET firstname = 'Mark'";

            var queries = sql2FetchXml.Convert(query);

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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = "SELECT employees + 1 AS a, employees * 2 AS b, turnover / 3 AS c, turnover - 4 AS d, turnover / employees AS e FROM account";

            var queries = sql2FetchXml.Convert(query);

            AssertFetchXml(queries, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='employees' />
                        <attribute name='turnover' />
                    </entity>
                </fetch>
            ");

            var id = Guid.NewGuid();
            context.Data["account"] = new Dictionary<Guid, Entity>
            {
                [id] = new Entity("account", id)
                {
                    ["accountid"] = id,
                    ["employees"] = 2,
                    ["turnover"] = new Money(9)
                }
            };

            queries[0].Execute(GetDataSources(context), this);

            Assert.AreEqual(1, ((EntityCollection)queries[0].Result).Entities.Count);
            Assert.AreEqual(3, ((EntityCollection)queries[0].Result).Entities[0]["a"]);
            Assert.AreEqual(4, ((EntityCollection)queries[0].Result).Entities[0]["b"]);
            Assert.AreEqual(3M, ((EntityCollection)queries[0].Result).Entities[0]["c"]);
            Assert.AreEqual(5M, ((EntityCollection)queries[0].Result).Entities[0]["d"]);
            Assert.AreEqual(4.5M, ((EntityCollection)queries[0].Result).Entities[0]["e"]);
        }

        [TestMethod]
        public void WhereComparingTwoFields()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = "SELECT contactid FROM contact WHERE firstname = lastname";

            var queries = sql2FetchXml.Convert(query);

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
            context.Data["contact"] = new Dictionary<Guid, Entity>
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

            queries[0].Execute(GetDataSources(context), this);

            Assert.AreEqual(1, ((EntityCollection)queries[0].Result).Entities.Count);
            Assert.AreEqual(guid2, ((EntityCollection)queries[0].Result).Entities[0]["contactid"]);
        }

        [TestMethod]
        public void WhereComparingExpression()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = "SELECT contactid FROM contact WHERE lastname = firstname + 'rington'";

            var queries = sql2FetchXml.Convert(query);

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
            context.Data["contact"] = new Dictionary<Guid, Entity>
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

            queries[0].Execute(GetDataSources(context), this);

            Assert.AreEqual(1, ((EntityCollection)queries[0].Result).Entities.Count);
            Assert.AreEqual(guid1, ((EntityCollection)queries[0].Result).Entities[0]["contactid"]);
        }

        [TestMethod]
        public void BackToFrontLikeExpression()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = "SELECT contactid FROM contact WHERE 'Mark' like firstname";

            var queries = sql2FetchXml.Convert(query);

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
            context.Data["contact"] = new Dictionary<Guid, Entity>
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

            queries[0].Execute(GetDataSources(context), this);

            Assert.AreEqual(1, ((EntityCollection)queries[0].Result).Entities.Count);
            Assert.AreEqual(guid2, ((EntityCollection)queries[0].Result).Entities[0]["contactid"]);
        }

        [TestMethod]
        public void UpdateFieldToField()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = "UPDATE contact SET firstname = lastname";

            var queries = sql2FetchXml.Convert(query);

            AssertFetchXml(queries, @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='contactid' />
                        <attribute name='lastname' />
                    </entity>
                </fetch>
            ");

            var guid = Guid.NewGuid();
            context.Data["contact"] = new Dictionary<Guid, Entity>
            {
                [guid] = new Entity("contact", guid)
                {
                    ["contactid"] = guid,
                    ["lastname"] = "Carrington"
                }
            };

            queries[0].Execute(GetDataSources(context), this);

            Assert.AreEqual("Carrington", context.Data["contact"][guid]["firstname"]);
        }

        [TestMethod]
        public void UpdateFieldToExpression()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = "UPDATE contact SET firstname = 'Hello ' + lastname";

            var queries = sql2FetchXml.Convert(query);

            AssertFetchXml(queries, @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='contactid' />
                        <attribute name='lastname' />
                    </entity>
                </fetch>
            ");

            var guid = Guid.NewGuid();
            context.Data["contact"] = new Dictionary<Guid, Entity>
            {
                [guid] = new Entity("contact", guid)
                {
                    ["contactid"] = guid,
                    ["lastname"] = "Carrington"
                }
            };

            queries[0].Execute(GetDataSources(context), this);

            Assert.AreEqual("Hello Carrington", context.Data["contact"][guid]["firstname"]);
        }

        [TestMethod]
        public void UpdateReplace()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = "UPDATE contact SET firstname = REPLACE(firstname, 'Dataflex Pro', 'CDS') WHERE lastname = 'Carrington'";

            var queries = sql2FetchXml.Convert(query);

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
            context.Data["contact"] = new Dictionary<Guid, Entity>
            {
                [guid] = new Entity("contact", guid)
                {
                    ["contactid"] = guid,
                    ["firstname"] = "--Dataflex Pro--",
                    ["lastname"] = "Carrington"
                }
            };

            queries[0].Execute(GetDataSources(context), this);

            Assert.AreEqual("--CDS--", context.Data["contact"][guid]["firstname"]);
        }

        [TestMethod]
        public void StringFunctions()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = "SELECT trim(firstname) as trim, ltrim(firstname) as ltrim, rtrim(firstname) as rtrim, substring(firstname, 2, 3) as substring23, len(firstname) as len FROM contact";

            var queries = sql2FetchXml.Convert(query);

            AssertFetchXml(queries, @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='firstname' />
                    </entity>
                </fetch>
            ");

            var guid1 = Guid.NewGuid();
            context.Data["contact"] = new Dictionary<Guid, Entity>
            {
                [guid1] = new Entity("contact", guid1)
                {
                    ["contactid"] = guid1,
                    ["firstname"] = " Mark "
                }
            };

            queries[0].Execute(GetDataSources(context), this);

            Assert.AreEqual(1, ((EntityCollection)queries[0].Result).Entities.Count);

            var entity = ((EntityCollection)queries[0].Result).Entities[0];
            Assert.AreEqual("Mark", entity.GetAttributeValue<string>("trim"));
            Assert.AreEqual("Mark ", entity.GetAttributeValue<string>("ltrim"));
            Assert.AreEqual(" Mark", entity.GetAttributeValue<string>("rtrim"));
            Assert.AreEqual("Mar", entity.GetAttributeValue<string>("substring23"));
            Assert.AreEqual(5, entity.GetAttributeValue<int>("len"));
        }

        [TestMethod]
        public void SelectExpression()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = "SELECT firstname, 'Hello ' + firstname AS greeting FROM contact";

            var queries = sql2FetchXml.Convert(query);

            AssertFetchXml(queries, @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='firstname' />
                    </entity>
                </fetch>
            ");

            var guid = Guid.NewGuid();
            context.Data["contact"] = new Dictionary<Guid, Entity>
            {
                [guid] = new Entity("contact", guid)
                {
                    ["contactid"] = guid,
                    ["firstname"] = "Mark"
                }
            };

            queries[0].Execute(GetDataSources(context), this);

            Assert.AreEqual(1, ((EntityCollection)queries[0].Result).Entities.Count);
            Assert.AreEqual("Mark", ((EntityCollection)queries[0].Result).Entities[0].GetAttributeValue<string>("firstname"));
            Assert.AreEqual("Hello Mark", ((EntityCollection)queries[0].Result).Entities[0].GetAttributeValue<string>("greeting"));
        }

        private IDictionary<string, DataSource> GetDataSources(XrmFakedContext context)
        {
            var dataSource = new DataSource
            {
                Name = "local",
                Connection = context.GetOrganizationService(),
                Metadata = new AttributeMetadataCache(context.GetOrganizationService()),
                TableSizeCache = new StubTableSizeCache()
            };

            return new Dictionary<string, DataSource> { ["local"] = dataSource };
        }

        [TestMethod]
        public void SelectExpressionNullValues()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = "SELECT firstname, 'Hello ' + firstname AS greeting, case when createdon > '2020-01-01' then 'new' else 'old' end AS age FROM contact";

            var queries = sql2FetchXml.Convert(query);

            AssertFetchXml(queries, @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='firstname' />
                        <attribute name='createdon' />
                    </entity>
                </fetch>
            ");

            var guid = Guid.NewGuid();
            context.Data["contact"] = new Dictionary<Guid, Entity>
            {
                [guid] = new Entity("contact", guid)
                {
                    ["contactid"] = guid
                }
            };

            queries[0].Execute(GetDataSources(context), this);

            Assert.AreEqual(1, ((EntityCollection)queries[0].Result).Entities.Count);
            Assert.IsNull(((EntityCollection)queries[0].Result).Entities[0].GetAttributeValue<string>("firstname"));
            Assert.IsNull(((EntityCollection)queries[0].Result).Entities[0].GetAttributeValue<string>("greeting"));
            Assert.AreEqual("old", ((EntityCollection)queries[0].Result).Entities[0].GetAttributeValue<string>("age"));
        }

        [TestMethod]
        public void OrderByExpression()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = "SELECT firstname, lastname FROM contact ORDER BY lastname + ', ' + firstname";

            var queries = sql2FetchXml.Convert(query);

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
            context.Data["contact"] = new Dictionary<Guid, Entity>
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

            queries[0].Execute(GetDataSources(context), this);

            Assert.AreEqual(2, ((EntityCollection)queries[0].Result).Entities.Count);
            Assert.AreEqual("Data", ((EntityCollection)queries[0].Result).Entities[0]["firstname"]);
            Assert.AreEqual("Mark", ((EntityCollection)queries[0].Result).Entities[1]["firstname"]);
        }

        [TestMethod]
        public void OrderByAliasedField()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = "SELECT firstname, lastname AS surname FROM contact ORDER BY surname";

            var queries = sql2FetchXml.Convert(query);

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
            context.Data["contact"] = new Dictionary<Guid, Entity>
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

            queries[0].Execute(GetDataSources(context), this);

            Assert.AreEqual(2, ((EntityCollection)queries[0].Result).Entities.Count);
            Assert.AreEqual("8", ((EntityCollection)queries[0].Result).Entities[0]["surname"]);
            Assert.AreEqual("Carrington", ((EntityCollection)queries[0].Result).Entities[1]["surname"]);
        }

        [TestMethod]
        public void OrderByCalculatedField()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = "SELECT firstname, lastname, lastname + ', ' + firstname AS fullname FROM contact ORDER BY fullname";

            var queries = sql2FetchXml.Convert(query);

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
            context.Data["contact"] = new Dictionary<Guid, Entity>
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

            queries[0].Execute(GetDataSources(context), this);

            Assert.AreEqual(2, ((EntityCollection)queries[0].Result).Entities.Count);
            Assert.AreEqual("8, Data", ((EntityCollection)queries[0].Result).Entities[0]["fullname"]);
            Assert.AreEqual("Carrington, Mark", ((EntityCollection)queries[0].Result).Entities[1]["fullname"]);
        }

        [TestMethod]
        public void OrderByCalculatedFieldByIndex()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = "SELECT firstname, lastname, lastname + ', ' + firstname AS fullname FROM contact ORDER BY 3";

            var queries = sql2FetchXml.Convert(query);

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
            context.Data["contact"] = new Dictionary<Guid, Entity>
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

            queries[0].Execute(GetDataSources(context), this);

            Assert.AreEqual(2, ((EntityCollection)queries[0].Result).Entities.Count);
            Assert.AreEqual("8, Data", ((EntityCollection)queries[0].Result).Entities[0]["fullname"]);
            Assert.AreEqual("Carrington, Mark", ((EntityCollection)queries[0].Result).Entities[1]["fullname"]);
        }

        [TestMethod]
        public void DateCalculations()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = "SELECT contactid, DATEADD(day, 1, createdon) AS nextday, DATEPART(minute, createdon) AS minute FROM contact WHERE DATEDIFF(hour, '2020-01-01', createdon) < 1";

            var queries = sql2FetchXml.Convert(query);

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
            context.Data["contact"] = new Dictionary<Guid, Entity>
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

            queries[0].Execute(GetDataSources(context), this);

            Assert.AreEqual(1, ((EntityCollection)queries[0].Result).Entities.Count);
            Assert.AreEqual(guid2, ((EntityCollection)queries[0].Result).Entities[0]["contactid"]);
            Assert.AreEqual(new DateTime(2020, 1, 2, 0, 30, 0), ((EntityCollection)queries[0].Result).Entities[0].GetAttributeValue<DateTime>("nextday"));
            Assert.AreEqual(30, ((EntityCollection)queries[0].Result).Entities[0].GetAttributeValue<int>("minute"));
        }

        [TestMethod]
        public void TopAppliedAfterCustomFilter()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = "SELECT TOP 10 contactid FROM contact WHERE firstname = lastname";

            var queries = sql2FetchXml.Convert(query);

            AssertFetchXml(queries, @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='contactid' />
                        <attribute name='firstname' />
                        <attribute name='lastname' />
                    </entity>
                </fetch>
            ");

            Assert.AreEqual(2, ((FetchXmlQuery)queries[0]).Extensions.Count);
        }

        [TestMethod]
        public void CustomFilterAggregateHavingProjectionSortAndTop()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = "SELECT TOP 10 lastname, SUM(CASE WHEN firstname = 'Mark' THEN 1 ELSE 0 END) as nummarks, LEFT(lastname, 1) AS lastinitial FROM contact WHERE DATEDIFF(day, '2020-01-01', createdon) > 10 GROUP BY lastname HAVING count(*) > 1 ORDER BY 2 DESC";

            var queries = sql2FetchXml.Convert(query);

            AssertFetchXml(queries, @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='lastname' />
                        <attribute name='firstname' />
                        <attribute name='createdon' />
                    </entity>
                </fetch>
            ");

            Assert.AreEqual(6, ((FetchXmlQuery)queries[0]).Extensions.Count);

            context.Data["contact"] = new Dictionary<Guid, Entity>
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

            queries[0].Execute(GetDataSources(context), this);
            var results = (EntityCollection)queries[0].Result;
            Assert.AreEqual(2, results.Entities.Count);

            Assert.AreEqual("Doe", results.Entities[0].GetAttributeValue<string>("lastname"));
            Assert.AreEqual(2, results.Entities[0].GetAttributeValue<int>("nummarks"));
            Assert.AreEqual("D", results.Entities[0].GetAttributeValue<string>("lastinitial"));

            Assert.AreEqual("Carrington", results.Entities[1].GetAttributeValue<string>("lastname"));
            Assert.AreEqual(1, results.Entities[1].GetAttributeValue<int>("nummarks"));
            Assert.AreEqual("C", results.Entities[1].GetAttributeValue<string>("lastinitial"));
        }

        [TestMethod]
        public void FilterCaseInsensitive()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = "SELECT contactid FROM contact WHERE DATEDIFF(day, '2020-01-01', createdon) < 10 OR lastname = 'Carrington' ORDER BY createdon";

            var queries = sql2FetchXml.Convert(query);

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

            context.Data["contact"] = new Dictionary<Guid, Entity>
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

            queries[0].Execute(GetDataSources(context), this);
            var results = (EntityCollection)queries[0].Result;
            Assert.AreEqual(2, results.Entities.Count);

            Assert.AreEqual(guid1, results.Entities[0]["contactid"]);
            Assert.AreEqual(guid2, results.Entities[1]["contactid"]);
        }

        [TestMethod]
        public void GroupCaseInsensitive()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = "SELECT lastname, count(*) FROM contact WHERE DATEDIFF(day, '2020-01-01', createdon) > 10 GROUP BY lastname ORDER BY 2 DESC";

            var queries = sql2FetchXml.Convert(query);

            AssertFetchXml(queries, @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='lastname' />
                        <attribute name='createdon' />
                    </entity>
                </fetch>
            ");

            var guid1 = Guid.NewGuid();
            var guid2 = Guid.NewGuid();
            var guid3 = Guid.NewGuid();

            context.Data["contact"] = new Dictionary<Guid, Entity>
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

            queries[0].Execute(GetDataSources(context), this);
            var results = (EntityCollection)queries[0].Result;
            Assert.AreEqual(2, results.Entities.Count);

            Assert.AreEqual("Carrington", results.Entities[0].GetAttributeValue<string>("lastname"), true);
            Assert.AreEqual("BLoggs", results.Entities[1].GetAttributeValue<string>("lastname"), true);
        }

        [TestMethod]
        public void AggregateExpressionsWithoutGrouping()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = "SELECT count(DISTINCT firstname + ' ' + lastname) AS distinctnames FROM contact";

            var queries = sql2FetchXml.Convert(query);

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

            context.Data["contact"] = new Dictionary<Guid, Entity>
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

            queries[0].Execute(GetDataSources(context), this);
            var results = (EntityCollection)queries[0].Result;
            Assert.AreEqual(1, results.Entities.Count);

            Assert.AreEqual(2, results.Entities[0].GetAttributeValue<int>("distinctnames"));
        }

        [TestMethod]
        public void AggregateQueryProducesAlternative()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = "SELECT name, count(*) FROM account GROUP BY name ORDER BY 2 DESC";

            var queries = sql2FetchXml.Convert(query);

            var simpleAggregate = (SelectQuery)queries[0];
            var alterativeQuery = (SelectQuery)simpleAggregate.AggregateAlternative;

            AssertFetchXml(new[] { alterativeQuery }, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='name' />
                    </entity>
                </fetch>
            ");

            CollectionAssert.AreEqual(simpleAggregate.ColumnSet, alterativeQuery.ColumnSet);

            var guid1 = Guid.NewGuid();
            var guid2 = Guid.NewGuid();
            var guid3 = Guid.NewGuid();

            context.Data["account"] = new Dictionary<Guid, Entity>
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

            alterativeQuery.Execute(GetDataSources(context), this);
            var results = (EntityCollection)alterativeQuery.Result;
            Assert.AreEqual(2, results.Entities.Count);

            Assert.AreEqual("Data8", results.Entities[0].GetAttributeValue<string>("name"));
            Assert.AreEqual(2, results.Entities[0].GetAttributeValue<int>(simpleAggregate.ColumnSet[1]));
        }

        [TestMethod]
        public void GuidEntityReferenceInequality()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = "SELECT a.name FROM account a INNER JOIN contact c ON a.primarycontactid = c.contactid WHERE (c.parentcustomerid is null or a.accountid <> c.parentcustomerid)";

            var queries = sql2FetchXml.Convert(query);

            var select = (SelectQuery)queries[0];

            var account1 = Guid.NewGuid();
            var account2 = Guid.NewGuid();
            var contact1 = Guid.NewGuid();
            var contact2 = Guid.NewGuid();

            context.Data["account"] = new Dictionary<Guid, Entity>
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
            context.Data["contact"] = new Dictionary<Guid, Entity>
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

            select.Execute(GetDataSources(context), this);
            var results = (EntityCollection)select.Result;
            Assert.AreEqual(1, results.Entities.Count);

            Assert.AreEqual("Data8", results.Entities[0].GetAttributeValue<string>("name"));
        }

        [TestMethod]
        public void UpdateGuidToEntityReference()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var lookup = (LookupAttributeMetadata)metadata["account"].Attributes.Single(a => a.LogicalName == "primarycontactid");
            lookup.Targets = new[] { "contact" };
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = "UPDATE a SET primarycontactid = c.contactid FROM account AS a INNER JOIN contact AS c ON a.accountid = c.parentcustomerid";

            var queries = sql2FetchXml.Convert(query);

            var update = (UpdateQuery)queries[0];

            var account1 = Guid.NewGuid();
            var account2 = Guid.NewGuid();
            var contact1 = Guid.NewGuid();
            var contact2 = Guid.NewGuid();

            context.Data["account"] = new Dictionary<Guid, Entity>
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
            context.Data["contact"] = new Dictionary<Guid, Entity>
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

            var dataSources = new Dictionary<string, DataSource>
            {
                ["local"] = new DataSource
                {
                    Connection = org,
                    Metadata = metadata,
                    TableSizeCache = new StubTableSizeCache(),
                    Name = "local"
                }
            };
            update.Execute(dataSources, this);

            Assert.AreEqual(new EntityReference("contact", contact1), context.Data["account"][account1].GetAttributeValue<EntityReference>("primarycontactid"));
            Assert.AreEqual(new EntityReference("contact", contact2), context.Data["account"][account2].GetAttributeValue<EntityReference>("primarycontactid"));
        }

        [TestMethod]
        public void CompareDateFields()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = "DELETE c2 FROM contact c1 INNER JOIN contact c2 ON c1.parentcustomerid = c2.parentcustomerid AND c2.createdon > c1.createdon";

            var queries = sql2FetchXml.Convert(query);

            AssertFetchXml(queries, @"
                <fetch>
                    <entity name='contact'>
                        <attribute name='createdon' />
                        <link-entity name='contact' to='parentcustomerid' from='parentcustomerid' alias='c2' link-type='inner'>
                            <attribute name='contactid' />
                            <attribute name='createdon' />
                        </link-entity>
                    </entity>
                </fetch>");

            var select = (DeleteQuery)queries[0];
            Assert.AreEqual(1, select.Extensions.Count);
        }

        [TestMethod]
        public void ColumnComparison()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);
            sql2FetchXml.ColumnComparisonAvailable = true;

            var query = "SELECT firstname, lastname FROM contact WHERE firstname = lastname";

            var queries = sql2FetchXml.Convert(query);

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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = "SELECT firstname, lastname FROM contact WHERE firstname = \"mark\"";

            try
            {
                sql2FetchXml.Convert(query);
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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = "SELECT firstname, lastname FROM contact WHERE firstname = 'Ma' + 'rk'";

            var queries = sql2FetchXml.Convert(query);

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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = "SELECT COUNT(1) FROM contact";

            var queries = sql2FetchXml.Convert(query);

            var selectQuery = (SelectQuery)queries[0];
            var selectNode = (SelectNode)selectQuery.Node;
            var computeScalarNode = (ComputeScalarNode)selectNode.Source;
            var count = (RetrieveTotalRecordCountNode)computeScalarNode.Source;
        }

        [TestMethod]
        public void CaseInsensitive()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = "Select Name From Account";

            var queries = sql2FetchXml.Convert(query);

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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = "SELECT new_name FROM new_customentity WHERE CONTAINS(new_optionsetvaluecollection, '1')";

            var queries = sql2FetchXml.Convert(query);

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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = "SELECT new_name FROM new_customentity WHERE new_optionsetvaluecollection = containvalues(1)";

            var queries = sql2FetchXml.Convert(query);

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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = "SELECT new_name FROM new_customentity WHERE CONTAINS(new_optionsetvaluecollection, '1 OR 2')";

            var queries = sql2FetchXml.Convert(query);

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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = "SELECT new_name FROM new_customentity WHERE new_optionsetvaluecollection = containvalues(1, 2)";

            var queries = sql2FetchXml.Convert(query);

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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = "SELECT new_name FROM new_customentity WHERE NOT CONTAINS(new_optionsetvaluecollection, '1 OR 2')";

            var queries = sql2FetchXml.Convert(query);

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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);
            sql2FetchXml.TDSEndpointAvailable = true;
            sql2FetchXml.ForceTDSEndpoint = true;

            var query = "SELECT COUNT(*) AS count FROM account WHERE name IS NULL";

            var queries = sql2FetchXml.Convert(query);

            Assert.AreEqual("SELECT COUNT(*) AS count FROM account WHERE name IS NULL", Regex.Replace(queries[0].TSql, "\\s+", " "));
        }

        [TestMethod]
        public void ImplicitTypeConversion()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = "SELECT employees / 2.0 AS half FROM account";

            var queries = sql2FetchXml.Convert(query);

            var account1 = Guid.NewGuid();
            var account2 = Guid.NewGuid();

            context.Data["account"] = new Dictionary<Guid, Entity>
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

            var select = queries[0];
            select.Execute(GetDataSources(context), this);
            var result = (EntityCollection)select.Result;
            Assert.AreEqual(null, result.Entities[0]["half"]);
            Assert.AreEqual(1M, result.Entities[1]["half"]);
        }

        [TestMethod]
        public void ImplicitTypeConversionComparison()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = "SELECT * FROM account WHERE turnover / 2 > 10";

            var queries = sql2FetchXml.Convert(query);

            var account1 = Guid.NewGuid();
            var account2 = Guid.NewGuid();

            context.Data["account"] = new Dictionary<Guid, Entity>
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

            var select = queries[0];
            select.Execute(GetDataSources(context), this);
            var result = (EntityCollection)select.Result;
            Assert.AreEqual(account2, result.Entities[0]["accountid"]);
        }

        [TestMethod]
        public void GlobalOptionSet()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());
            context.AddFakeMessageExecutor<RetrieveAllOptionSetsRequest>(new RetrieveAllOptionSetsHandler());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = "SELECT displayname FROM metadata.globaloptionset WHERE name = 'test'";

            var queries = sql2FetchXml.Convert(query);

            Assert.IsInstanceOfType(queries.Single(), typeof(SelectQuery));

            var selectQuery = (SelectQuery)queries[0];
            var selectNode = (SelectNode)selectQuery.Node;
            Assert.AreEqual(1, selectNode.ColumnSet.Count);
            Assert.AreEqual("globaloptionset.displayname", selectNode.ColumnSet[0].SourceColumn);
            var filterNode = (FilterNode)selectNode.Source;
            Assert.AreEqual("name = 'test'", filterNode.Filter.ToSql());
            var optionsetNode = (GlobalOptionSetQueryNode)filterNode.Source;

            queries[0].Execute(GetDataSources(context), this);

            var result = (EntityCollection)queries[0].Result;

            Assert.AreEqual(1, result.Entities.Count);
        }

        [TestMethod]
        public void EntityDetails()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);
            context.AddFakeMessageExecutor<RetrieveMetadataChangesRequest>(new RetrieveMetadataChangesHandler(metadata));

            var query = "SELECT logicalname FROM metadata.entity ORDER BY 1";

            var queries = sql2FetchXml.Convert(query);

            Assert.IsInstanceOfType(queries.Single(), typeof(SelectQuery));

            var selectQuery = (SelectQuery)queries[0];
            var selectNode = (SelectNode)selectQuery.Node;
            var sortNode = (SortNode)selectNode.Source;
            var metadataNode = (MetadataQueryNode)sortNode.Source;

            queries[0].Execute(GetDataSources(context), this);

            var result = (EntityCollection)queries[0].Result;

            Assert.AreEqual(3, result.Entities.Count);
            Assert.AreEqual("account", result.Entities[0]["logicalname"]);
            Assert.AreEqual("contact", result.Entities[1]["logicalname"]);
            Assert.AreEqual("new_customentity", result.Entities[2]["logicalname"]);
        }

        [TestMethod]
        public void AttributeDetails()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);
            context.AddFakeMessageExecutor<RetrieveMetadataChangesRequest>(new RetrieveMetadataChangesHandler(metadata));

            var query = "SELECT e.logicalname, a.logicalname FROM metadata.entity e INNER JOIN metadata.attribute a ON e.logicalname = a.entitylogicalname WHERE e.logicalname = 'new_customentity' ORDER BY 1, 2";

            var queries = sql2FetchXml.Convert(query);

            queries[0].Execute(GetDataSources(context), this);

            var result = (EntityCollection)queries[0].Result;

            Assert.AreEqual(7, result.Entities.Count);
            var row = 0;
            Assert.AreEqual("new_boolprop", result.Entities[row++]["logicalname1"]);
            Assert.AreEqual("new_customentityid", result.Entities[row++]["logicalname1"]);
            Assert.AreEqual("new_name", result.Entities[row++]["logicalname1"]);
            Assert.AreEqual("new_optionsetvalue", result.Entities[row++]["logicalname1"]);
            Assert.AreEqual("new_optionsetvaluecollection", result.Entities[row++]["logicalname1"]);
            Assert.AreEqual("new_optionsetvaluename", result.Entities[row++]["logicalname1"]);
            Assert.AreEqual("new_parentid", result.Entities[row++]["logicalname1"]);
        }

        [TestMethod]
        public void OptionSetNameSelect()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            // Add metadata for new_optionsetvaluename virtual attribute
            var attr = metadata["new_customentity"].Attributes.Single(a => a.LogicalName == "new_optionsetvaluename");
            attr.GetType().GetProperty(nameof(AttributeMetadata.AttributeOf)).SetValue(attr, "new_optionsetvalue");

            var query = "SELECT new_optionsetvalue, new_optionsetvaluename FROM new_customentity ORDER BY new_optionsetvaluename";

            var queries = sql2FetchXml.Convert(query);

            var record1 = Guid.NewGuid();
            var record2 = Guid.NewGuid();

            context.Data["new_customentity"] = new Dictionary<Guid, Entity>
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

            var select = (SelectQuery)queries[0];

            AssertFetchXml(queries, @"
                <fetch>
                    <entity name='new_customentity'>
                        <attribute name='new_optionsetvalue' />
                        <order attribute='new_optionsetvalue' />
                    </entity>
                </fetch>");

            CollectionAssert.AreEqual(new[] { "new_optionsetvalue", "new_optionsetvaluename" }, select.ColumnSet);

            queries[0].Execute(GetDataSources(context), this);

            select.Execute(GetDataSources(context), this);
            var result = (EntityCollection)select.Result;
            Assert.IsNull(result.Entities[0].GetAttributeValue<OptionSetValue>("new_optionsetvalue"));
            Assert.IsNull(result.Entities[0].GetAttributeValue<string>("new_optionsetvaluename"));
            Assert.AreEqual(Metadata.New_OptionSet.Value1, result.Entities[1].GetAttributeValue<Metadata.New_OptionSet>("new_optionsetvalue"));
            Assert.AreEqual(Metadata.New_OptionSet.Value1.ToString(), result.Entities[1].GetAttributeValue<string>("new_optionsetvaluename"));
        }

        [TestMethod]
        public void OptionSetNameFilter()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            // Add metadata for new_optionsetvaluename virtual attribute
            var nameAttr = metadata["new_customentity"].Attributes.Single(a => a.LogicalName == "new_optionsetvaluename");
            nameAttr.GetType().GetProperty(nameof(AttributeMetadata.AttributeOf)).SetValue(nameAttr, "new_optionsetvalue");
            var valueAttr = (EnumAttributeMetadata)metadata["new_customentity"].Attributes.Single(a => a.LogicalName == "new_optionsetvalue");
            valueAttr.OptionSet = new OptionSetMetadata
            {
                Options =
                {
                    new OptionMetadata(new Label { UserLocalizedLabel = new LocalizedLabel(Metadata.New_OptionSet.Value1.ToString(), 1033) }, (int) Metadata.New_OptionSet.Value1),
                    new OptionMetadata(new Label { UserLocalizedLabel = new LocalizedLabel(Metadata.New_OptionSet.Value2.ToString(), 1033) }, (int) Metadata.New_OptionSet.Value2),
                    new OptionMetadata(new Label { UserLocalizedLabel = new LocalizedLabel(Metadata.New_OptionSet.Value3.ToString(), 1033) }, (int) Metadata.New_OptionSet.Value3)
                }
            };

            var query = "SELECT new_customentityid FROM new_customentity WHERE new_optionsetvaluename = 'Value1'";

            var queries = sql2FetchXml.Convert(query);

            var record1 = Guid.NewGuid();
            var record2 = Guid.NewGuid();

            context.Data["new_customentity"] = new Dictionary<Guid, Entity>
            {
                [record1] = new Entity("new_customentity", record1)
                {
                    ["new_optionsetvalue"] = null,
                    ["new_customentityid"] = record1
                },
                [record2] = new Entity("new_customentity", record2)
                {
                    ["new_optionsetvalue"] = new OptionSetValue((int)Metadata.New_OptionSet.Value1),
                    ["new_customentityid"] = record2
                }
            };

            var select = (SelectQuery)queries[0];

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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            // Add metadata for primarycontactidname virtual attribute
            var attr = metadata["account"].Attributes.Single(a => a.LogicalName == "primarycontactidname");
            attr.GetType().GetProperty(nameof(AttributeMetadata.AttributeOf)).SetValue(attr, "primarycontactid");

            var query = "SELECT primarycontactid, primarycontactidname FROM account ORDER BY primarycontactidname";

            var queries = sql2FetchXml.Convert(query);

            AssertFetchXml(queries, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='primarycontactid' />
                        <order attribute='primarycontactid' />
                    </entity>
                </fetch>");

            var select = (SelectQuery)queries[0];

            CollectionAssert.AreEqual(new[] { "primarycontactid", "primarycontactidname" }, select.ColumnSet);
        }

        [TestMethod]
        public void EntityReferenceNameFilter()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            // Add metadata for primarycontactidname virtual attribute
            var nameAttr = metadata["account"].Attributes.Single(a => a.LogicalName == "primarycontactidname");
            nameAttr.GetType().GetProperty(nameof(AttributeMetadata.AttributeOf)).SetValue(nameAttr, "primarycontactid");

            var idAttr = (LookupAttributeMetadata) metadata["account"].Attributes.Single(a => a.LogicalName == "primarycontactid");
            idAttr.Targets = new[] { "contact" };

            // Set the primary name attribute on contact
            typeof(EntityMetadata).GetProperty(nameof(EntityMetadata.PrimaryNameAttribute)).SetValue(metadata["contact"], "fullname");

            var query = "SELECT accountid FROM account WHERE primarycontactidname = 'Mark Carrington'";

            var queries = sql2FetchXml.Convert(query);

            AssertFetchXml(queries, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='accountid' />
                        <link-entity name='contact' from='contactid' to='primarycontactid' alias='account_primarycontactid' link-type='outer'>
                        </link-entity>
                        <filter>
                            <condition entityname='account_primarycontactid' attribute='fullname' operator='eq' value='Mark Carrington' />
                        </filter>
                    </entity>
                </fetch>");
        }

        [TestMethod]
        public void UpdateMissingAlias()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var lookup = (LookupAttributeMetadata)metadata["account"].Attributes.Single(a => a.LogicalName == "primarycontactid");
            lookup.Targets = new[] { "contact" };
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = "UPDATE account SET primarycontactid = c.contactid FROM account AS a INNER JOIN contact AS c ON a.name = c.fullname";

            sql2FetchXml.Convert(query);
        }

        [TestMethod]
        public void UpdateMissingAliasAmbiguous()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = "UPDATE account SET primarycontactid = other.primarycontactid FROM account AS main INNER JOIN account AS other on main.name = other.name";

            try
            {
                sql2FetchXml.Convert(query);
                Assert.Fail("Expected exception");
            }
            catch (NotSupportedQueryFragmentException ex)
            {
                Assert.AreEqual("Target table name is ambiguous", ex.Error);
            }
        }

        [TestMethod]
        public void ConvertIntToBool()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = "UPDATE new_customentity SET new_boolprop = CASE WHEN new_name = 'True' THEN 1 ELSE 0 END";

            sql2FetchXml.Convert(query);
        }

        [TestMethod]
        public void ImpersonateRevert()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = @"
                EXECUTE AS LOGIN = 'test1'
                REVERT";

            var queries = sql2FetchXml.Convert(query);
            Assert.IsInstanceOfType(queries[0], typeof(ImpersonateQuery));
            Assert.IsInstanceOfType(queries[1], typeof(RevertQuery));

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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = @"
                SELECT contact.fullname
                FROM contact INNER JOIN account ON contact.contactid = account.primarycontactid INNER JOIN new_customentity ON contact.parentcustomerid = new_customentity.new_parentid
                ORDER BY account.employees, contact.fullname";

            var queries = sql2FetchXml.Convert(query);

            AssertFetchXml(new[] { queries[0] }, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='employees' />
                        <link-entity name='contact' from='contactid' to='primarycontactid' alias='contact' link-type='inner'>
                            <attribute name='fullname' />
                            <link-entity name='new_customentity' from='new_parentid' to='parentcustomerid' alias='new_customentity' link-type='inner'>
                            </link-entity>
                        </link-entity>
                        <order attribute='employees' />
                    </entity>
                </fetch>");

            Assert.AreEqual(1, ((SelectQuery)queries[0]).Extensions.Count);
        }

        [TestMethod]
        public void UpdateUsingTSql()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var lookup = (LookupAttributeMetadata)metadata["contact"].Attributes.Single(a => a.LogicalName == "parentcustomerid");
            lookup.Targets = new[] { "contact", "account" };
            var parentcustomeridtype = new StringAttributeMetadata { LogicalName = "parentcustomeridtype" };
            typeof(EntityMetadata).GetProperty(nameof(EntityMetadata.Attributes)).SetValue(metadata["contact"], metadata["contact"].Attributes.Concat(new[] { parentcustomeridtype }).ToArray());
            var sql2FetchXml = new Sql2FetchXml(metadata, true);
            sql2FetchXml.TDSEndpointAvailable = true;
            sql2FetchXml.ForceTDSEndpoint = true;

            var query = @"
                UPDATE c
                SET parentcustomerid = account.accountid, parentcustomeridtype = 'account'
                FROM contact AS c INNER JOIN account ON c.parentcustomerid = account.accountid INNER JOIN new_customentity ON c.parentcustomerid = new_customentity.new_parentid
                WHERE c.fullname IN (SELECT fullname FROM contact WHERE firstname = 'Mark')";

            var queries = sql2FetchXml.Convert(query);

            Assert.AreEqual(Regex.Replace(@"
                SELECT DISTINCT
                    c.contactid AS contactid,
                    account.accountid AS parentcustomerid,
                    'account' AS parentcustomeridtype
                FROM
                    contact AS c
                    INNER JOIN account
                        ON c.parentcustomerid = account.accountid
                    INNER JOIN new_customentity
                        ON c.parentcustomerid = new_customentity.new_parentid
                WHERE
                    c.fullname IN (SELECT fullname FROM contact WHERE firstname = 'Mark')", @"\s+", " ").Trim(), Regex.Replace(queries[0].TSql, @"\s+", " ").Trim());
        }

        [TestMethod]
        public void DeleteUsingTSql()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);
            sql2FetchXml.TDSEndpointAvailable = true;
            sql2FetchXml.ForceTDSEndpoint = true;

            var query = @"
                DELETE c
                FROM contact AS c INNER JOIN account ON c.parentcustomerid = account.accountid INNER JOIN new_customentity ON c.parentcustomerid = new_customentity.new_parentid
                WHERE c.fullname IN (SELECT fullname FROM contact WHERE firstname = 'Mark')";

            var queries = sql2FetchXml.Convert(query);

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
                    c.fullname IN (SELECT fullname FROM contact WHERE firstname = 'Mark')", @"\s+", " ").Trim(), Regex.Replace(queries[0].TSql, @"\s+", " ").Trim());
        }

        [TestMethod]
        public void OrderByAggregateByIndex()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = "SELECT firstname, count(*) FROM contact GROUP BY firstname ORDER BY 2";

            var queries = sql2FetchXml.Convert(query);

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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = "SELECT firstname, count(*) FROM contact INNER JOIN account ON contact.parentcustomerid = account.accountid GROUP BY firstname ORDER BY 2";

            var queries = sql2FetchXml.Convert(query);

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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = "SELECT name, count(*) FROM contact INNER JOIN account ON contact.parentcustomerid = account.accountid GROUP BY name";

            var queries = sql2FetchXml.Convert(query);
            var select = (SelectQuery)queries[0];
            AssertFetchXml(new[] { select.AggregateAlternative }, @"
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
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = "SELECT CHARINDEX('a', fullname) AS ci0, CHARINDEX('a', fullname, 1) AS ci1, CHARINDEX('a', fullname, 2) AS ci2, CHARINDEX('a', fullname, 3) AS ci3, CHARINDEX('a', fullname, 8) AS ci8 FROM contact";

            var queries = sql2FetchXml.Convert(query);

            var contact1 = Guid.NewGuid();

            context.Data["contact"] = new Dictionary<Guid, Entity>
            {
                [contact1] = new Entity("contact", contact1)
                {
                    ["fullname"] = "Mark Carrington",
                    ["contactid"] = contact1
                }
            };

            var select = queries[0];
            select.Execute(GetDataSources(context), this);
            var result = (EntityCollection)select.Result;
            Assert.AreEqual(2, result.Entities[0].GetAttributeValue<int>("ci0"));
            Assert.AreEqual(2, result.Entities[0].GetAttributeValue<int>("ci1"));
            Assert.AreEqual(2, result.Entities[0].GetAttributeValue<int>("ci2"));
            Assert.AreEqual(7, result.Entities[0].GetAttributeValue<int>("ci3"));
            Assert.AreEqual(0, result.Entities[0].GetAttributeValue<int>("ci8"));
        }

        [TestMethod]
        public void CastDateTimeToDate()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = "SELECT CAST(createdon AS date) AS converted FROM contact";

            var queries = sql2FetchXml.Convert(query);

            var contact1 = Guid.NewGuid();

            context.Data["contact"] = new Dictionary<Guid, Entity>
            {
                [contact1] = new Entity("contact", contact1)
                {
                    ["createdon"] = new DateTime(2000, 1, 1, 12, 34, 56),
                    ["contactid"] = contact1
                }
            };

            var select = queries[0];
            select.Execute(GetDataSources(context), this);
            var result = (EntityCollection)select.Result;
            Assert.AreEqual(new DateTime(2000, 1, 1), result.Entities[0].GetAttributeValue<DateTime>("converted"));
        }

        [TestMethod]
        public void GroupByPrimaryFunction()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = "SELECT left(firstname, 1) AS initial, count(*) AS count FROM contact GROUP BY left(firstname, 1) ORDER BY 2 DESC";

            var queries = sql2FetchXml.Convert(query);

            var contact1 = Guid.NewGuid();
            var contact2 = Guid.NewGuid();
            var contact3 = Guid.NewGuid();

            context.Data["contact"] = new Dictionary<Guid, Entity>
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

            var select = queries[0];
            select.Execute(GetDataSources(context), this);
            var result = (EntityCollection)select.Result;
            Assert.AreEqual("M", result.Entities[0].GetAttributeValue<string>("initial"));
            Assert.AreEqual(2, result.Entities[0].GetAttributeValue<int>("count"));
            Assert.AreEqual("R", result.Entities[1].GetAttributeValue<string>("initial"));
            Assert.AreEqual(1, result.Entities[1].GetAttributeValue<int>("count"));
        }

        private void AssertFetchXml(Query[] queries, string fetchXml)
        {
            Assert.AreEqual(1, queries.Length);
            Assert.IsInstanceOfType(queries[0], typeof(FetchXmlQuery));

            try
            {
                var serializer = new XmlSerializer(typeof(FetchXml.FetchType));
                using (var reader = new StringReader(fetchXml))
                {
                    var fetch = (FetchXml.FetchType)serializer.Deserialize(reader);
                    PropertyEqualityAssert.Equals(fetch, ((FetchXmlQuery)queries[0]).FetchXml);
                }
            }
            catch (AssertFailedException ex)
            {
                Assert.Fail($"Expected:\r\n{fetchXml}\r\n\r\nActual:\r\n{((FetchXmlQuery)queries[0]).FetchXmlString}\r\n\r\n{ex.Message}");
            }
        }

        void IQueryExecutionOptions.Progress(double? progress, string message)
        {
        }

        bool IQueryExecutionOptions.ContinueRetrieve(int count)
        {
            return true;
        }

        bool IQueryExecutionOptions.ConfirmInsert(int count, EntityMetadata meta)
        {
            return true;
        }

        bool IQueryExecutionOptions.ConfirmUpdate(int count, EntityMetadata meta)
        {
            return true;
        }

        bool IQueryExecutionOptions.ConfirmDelete(int count, EntityMetadata meta)
        {
            return true;
        }

        private class RetrieveMetadataChangesHandler : IFakeMessageExecutor
        {
            private readonly IAttributeMetadataCache _metadata;

            public RetrieveMetadataChangesHandler(IAttributeMetadataCache metadata)
            {
                _metadata = metadata;
            }

            public bool CanExecute(OrganizationRequest request)
            {
                return request is RetrieveMetadataChangesRequest;
            }

            public OrganizationResponse Execute(OrganizationRequest request, XrmFakedContext ctx)
            {
                var metadata = new EntityMetadataCollection
                {
                    _metadata["account"],
                    _metadata["contact"],
                    _metadata["new_customentity"]
                };

                foreach (var entity in metadata)
                {
                    if (entity.MetadataId == null)
                        entity.MetadataId = Guid.NewGuid();

                    foreach (var attribute in entity.Attributes)
                    {
                        if (attribute.MetadataId == null)
                            attribute.MetadataId = Guid.NewGuid();
                    }
                }

                var req = (RetrieveMetadataChangesRequest)request;
                var metadataParam = Expression.Parameter(typeof(EntityMetadata));
                var filter = ToExpression(req.Query.Criteria, metadataParam);
                var filterFunc = (Func<EntityMetadata,bool>) Expression.Lambda(filter, metadataParam).Compile();

                var result = new EntityMetadataCollection();

                foreach (var match in metadata.Where(e => filterFunc(e)))
                    result.Add(match);

                var response = new RetrieveMetadataChangesResponse
                {
                    Results = new ParameterCollection
                    {
                        ["EntityMetadata"] = result
                    }
                };

                return response;
            }

            private Expression ToExpression(MetadataFilterExpression filter, ParameterExpression param)
            {
                if (filter == null)
                    return Expression.Constant(true);

                Expression expr = null;

                foreach (var condition in filter.Conditions)
                {
                    var conditionExpr = ToExpression(condition, param);

                    if (expr == null)
                        expr = conditionExpr;
                    else if (filter.FilterOperator == LogicalOperator.And)
                        expr = Expression.AndAlso(expr, conditionExpr);
                    else
                        expr = Expression.OrElse(expr, conditionExpr);
                }

                foreach (var subFilter in filter.Filters)
                {
                    var filterExpr = ToExpression(subFilter, param);

                    if (expr == null)
                        expr = filterExpr;
                    else if (filter.FilterOperator == LogicalOperator.And)
                        expr = Expression.AndAlso(expr, filterExpr);
                    else
                        expr = Expression.OrElse(expr, filterExpr);
                }

                return expr ?? Expression.Constant(true);
            }

            private Expression ToExpression(MetadataConditionExpression condition, ParameterExpression param)
            {
                var value = Expression.PropertyOrField(param, condition.PropertyName);
                var targetValue = Expression.Constant(condition.Value);

                switch (condition.ConditionOperator)
                {
                    case MetadataConditionOperator.Equals:
                        return Expression.Equal(value, targetValue);

                    case MetadataConditionOperator.NotEquals:
                        return Expression.NotEqual(value, targetValue);

                    case MetadataConditionOperator.LessThan:
                        return Expression.LessThan(value, targetValue);

                    case MetadataConditionOperator.GreaterThan:
                        return Expression.GreaterThan(value, targetValue);

                    default:
                        throw new NotImplementedException();
                }
            }

            public Type GetResponsibleRequestType()
            {
                return typeof(RetrieveMetadataChangesRequest);
            }
        }

        private class RetrieveAllOptionSetsHandler : IFakeMessageExecutor
        {
            public bool CanExecute(OrganizationRequest request)
            {
                return request is RetrieveAllOptionSetsRequest;
            }

            public OrganizationResponse Execute(OrganizationRequest request, XrmFakedContext ctx)
            {
                var labels = new[]
                {
                    new LocalizedLabel("TestGlobalOptionSet", 1033) { MetadataId = Guid.NewGuid() },
                    new LocalizedLabel("TranslatedDisplayName-Test", 9999) { MetadataId = Guid.NewGuid() },
                    new LocalizedLabel("FooGlobalOptionSet", 1033) { MetadataId = Guid.NewGuid() },
                    new LocalizedLabel("TranslatedDisplayName-Foo", 9999) { MetadataId = Guid.NewGuid() },
                    new LocalizedLabel("BarGlobalOptionSet", 1033) { MetadataId = Guid.NewGuid() },
                    new LocalizedLabel("TranslatedDisplayName-Bar", 9999) { MetadataId = Guid.NewGuid() },
                    new LocalizedLabel("Value1", 1033) { MetadataId = Guid.NewGuid() },
                    new LocalizedLabel("Value2", 1033) { MetadataId = Guid.NewGuid() }
                };

                return new RetrieveAllOptionSetsResponse
                {
                    Results = new ParameterCollection
                    {
                        ["OptionSetMetadata"] = new OptionSetMetadataBase[]
                        {
                            new OptionSetMetadata(new OptionMetadataCollection(new[]
                            {
                                new OptionMetadata(1) { MetadataId = Guid.NewGuid(), Label = new Label { UserLocalizedLabel = labels[6] } },
                                new OptionMetadata(2) { MetadataId = Guid.NewGuid(), Label = new Label { UserLocalizedLabel = labels[7] } }
                            }))
                            {
                                MetadataId = Guid.NewGuid(),
                                Name = "test",
                                DisplayName = new Label(
                                    labels[0],
                                    new[]
                                    {
                                        labels[0],
                                        labels[1]
                                    })
                            },
                            new OptionSetMetadata(new OptionMetadataCollection(new[]
                            {
                                new OptionMetadata(1) { MetadataId = Guid.NewGuid(), Label = new Label { UserLocalizedLabel = labels[6] } },
                                new OptionMetadata(2) { MetadataId = Guid.NewGuid(), Label = new Label { UserLocalizedLabel = labels[7] } }
                            }))
                            {
                                MetadataId = Guid.NewGuid(),
                                Name = "foo",
                                DisplayName = new Label(
                                    labels[2],
                                    new[]
                                    {
                                        labels[2],
                                        labels[3]
                                    })
                            },
                            new OptionSetMetadata(new OptionMetadataCollection(new[]
                            {
                                new OptionMetadata(1) { MetadataId = Guid.NewGuid(), Label = new Label { UserLocalizedLabel = labels[6] } },
                                new OptionMetadata(2) { MetadataId = Guid.NewGuid(), Label = new Label { UserLocalizedLabel = labels[7] } }
                            }))
                            {
                                MetadataId = Guid.NewGuid(),
                                Name = "bar",
                                DisplayName = new Label(
                                    labels[4],
                                    new[]
                                    {
                                        labels[4],
                                        labels[5]
                                    })
                            }
                        }
                    }
                };
            }

            public Type GetResponsibleRequestType()
            {
                return typeof(RetrieveAllOptionSetsRequest);
            }
        }
    }
}
