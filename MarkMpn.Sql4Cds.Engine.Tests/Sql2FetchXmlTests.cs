using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Xml.Serialization;
using FakeXrmEasy;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Metadata;

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
                "name"
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
                "name",
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
                        <attribute name='name' alias='accountname' />
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

            var query = "SELECT DISTINCT accountid, name FROM account";

            var queries = sql2FetchXml.Convert(query);

            AssertFetchXml(queries, @"
                <fetch distinct='true'>
                    <entity name='account'>
                        <attribute name='accountid' />
                        <attribute name='name' />
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

            var query = "SELECT accountid, name FROM account INNER JOIN contact ON accountid = parentcustomerid";

            var queries = sql2FetchXml.Convert(query);

            AssertFetchXml(queries, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='accountid' />
                        <attribute name='name' />
                        <link-entity name='contact' from='parentcustomerid' to='accountid' link-type='inner' alias='contact'>
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
                    <entity name='account'>
                        <attribute name='accountid' />
                        <attribute name='name' />
                        <link-entity name='contact' from='parentcustomerid' to='accountid' link-type='inner' alias='contact'>
                            <filter type='or'>
                                <condition attribute='firstname' operator='eq' value='Mark' />
                                <condition attribute='lastname' operator='eq' value='Carrington' />
                            </filter>
                        </link-entity>
                    </entity>
                </fetch>
            ");
        }

        [TestMethod]
        [ExpectedException(typeof(NotSupportedQueryFragmentException))]
        public void InvalidAdditionalJoinCriteria()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = "SELECT accountid, name FROM account INNER JOIN contact ON accountid = parentcustomerid OR (firstname = 'Mark' AND lastname = 'Carrington')";

            sql2FetchXml.Convert(query);
        }

        [TestMethod]
        public void SortOnLinkEntity()
        {
            var context = new XrmFakedContext();
            context.InitializeMetadata(Assembly.GetExecutingAssembly());

            var org = context.GetOrganizationService();
            var metadata = new AttributeMetadataCache(org);
            var sql2FetchXml = new Sql2FetchXml(metadata, true);

            var query = "SELECT accountid, name FROM account INNER JOIN contact ON accountid = parentcustomerid ORDER BY name, firstname";

            var queries = sql2FetchXml.Convert(query);

            AssertFetchXml(queries, @"
                <fetch>
                    <entity name='account'>
                        <attribute name='accountid' />
                        <attribute name='name' />
                        <link-entity name='contact' from='parentcustomerid' to='accountid' link-type='inner' alias='contact'>
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

            var query = "SELECT accountid, name FROM account INNER JOIN contact ON accountid = parentcustomerid ORDER BY firstname, name";

            var queries = sql2FetchXml.Convert(query);

            Assert.AreEqual(2, ((FetchXmlQuery)queries[0]).PostSorts.Length);
            Assert.AreEqual(true, ((FetchXmlQuery)queries[0]).PostSorts[0].FetchXmlSorted);
            Assert.AreEqual(false, ((FetchXmlQuery)queries[0]).PostSorts[1].FetchXmlSorted);
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
                        <attribute name='accountid' aggregate='count' alias='accountid_count' />
                        <attribute name='name' aggregate='countcolumn' alias='name_countcolumn' />
                        <attribute name='name' aggregate='countcolumn' distinct='true' alias='name_countcolumn_2' />
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
                        <attribute name='accountid' aggregate='count' alias='accountid_count' />
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
                        <attribute name='accountid' aggregate='count' alias='accountid_count' />
                        <order alias='name' />
                        <order alias='accountid_count' />
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

            var query = "SELECT name, firstname, count(*) FROM account INNER JOIN contact ON parentcustomerid = account.accountid GROUP BY name, firstname ORDER BY name, firstname";

            var queries = sql2FetchXml.Convert(query);

            AssertFetchXml(queries, @"
                <fetch aggregate='true'>
                    <entity name='account'>
                        <attribute name='name' groupby='true' alias='name' />
                        <attribute name='accountid' aggregate='count' alias='accountid_count' />
                        <link-entity name='contact' from='parentcustomerid' to='accountid' link-type='inner' alias='contact'>
                            <attribute name='firstname' groupby='true' alias='firstname' />
                            <order alias='firstname' />
                        </link-entity>
                        <order alias='name' />
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
                <fetch distinct='true'>
                    <entity name='contact'>
                        <attribute name='contactid' />
                    </entity>
                </fetch>
            ");
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

            queries[0].Execute(context.GetOrganizationService(), new AttributeMetadataCache(context.GetOrganizationService()), this);

            Assert.AreEqual(1, ((EntityCollection)queries[0].Result).Entities.Count);
            Assert.AreEqual(guid2, ((EntityCollection)queries[0].Result).Entities[0].Id);
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

            queries[0].Execute(context.GetOrganizationService(), new AttributeMetadataCache(context.GetOrganizationService()), this);

            Assert.AreEqual(1, ((EntityCollection)queries[0].Result).Entities.Count);
            Assert.AreEqual(guid1, ((EntityCollection)queries[0].Result).Entities[0].Id);
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
                <fetch distinct='true'>
                    <entity name='contact'>
                        <attribute name='lastname' />
                        <attribute name='contactid' />
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

            queries[0].Execute(context.GetOrganizationService(), new AttributeMetadataCache(context.GetOrganizationService()), this);

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
                <fetch distinct='true'>
                    <entity name='contact'>
                        <attribute name='lastname' />
                        <attribute name='contactid' />
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

            queries[0].Execute(context.GetOrganizationService(), new AttributeMetadataCache(context.GetOrganizationService()), this);

            Assert.AreEqual("Hello Carrington", context.Data["contact"][guid]["firstname"]);
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

            queries[0].Execute(context.GetOrganizationService(), new AttributeMetadataCache(context.GetOrganizationService()), this);

            Assert.AreEqual(1, ((EntityCollection)queries[0].Result).Entities.Count);
            Assert.AreEqual("Mark", ((EntityCollection)queries[0].Result).Entities[0].GetAttributeValue<string>("firstname"));
            Assert.AreEqual("Hello Mark", ((EntityCollection)queries[0].Result).Entities[0].GetAttributeValue<string>("greeting"));
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

            queries[0].Execute(context.GetOrganizationService(), new AttributeMetadataCache(context.GetOrganizationService()), this);

            Assert.AreEqual(2, ((EntityCollection)queries[0].Result).Entities.Count);
            Assert.AreEqual(guid2, ((EntityCollection)queries[0].Result).Entities[0].Id);
            Assert.AreEqual(guid1, ((EntityCollection)queries[0].Result).Entities[1].Id);
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

            queries[0].Execute(context.GetOrganizationService(), new AttributeMetadataCache(context.GetOrganizationService()), this);

            Assert.AreEqual(2, ((EntityCollection)queries[0].Result).Entities.Count);
            Assert.AreEqual(guid2, ((EntityCollection)queries[0].Result).Entities[0].Id);
            Assert.AreEqual(guid1, ((EntityCollection)queries[0].Result).Entities[1].Id);
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

            queries[0].Execute(context.GetOrganizationService(), new AttributeMetadataCache(context.GetOrganizationService()), this);

            Assert.AreEqual(2, ((EntityCollection)queries[0].Result).Entities.Count);
            Assert.AreEqual(guid2, ((EntityCollection)queries[0].Result).Entities[0].Id);
            Assert.AreEqual(guid1, ((EntityCollection)queries[0].Result).Entities[1].Id);
        }

        private void AssertFetchXml(Query[] queries, string fetchXml)
        {
            Assert.AreEqual(1, queries.Length);
            Assert.IsInstanceOfType(queries[0], typeof(FetchXmlQuery));

            var serializer = new XmlSerializer(typeof(FetchXml.FetchType));
            using (var reader = new StringReader(fetchXml))
            {
                var fetch = (FetchXml.FetchType)serializer.Deserialize(reader);
                PropertyEqualityAssert.Equals(fetch, ((FetchXmlQuery)queries[0]).FetchXml);
            }
        }

        void IQueryExecutionOptions.Progress(string message)
        {
        }

        bool IQueryExecutionOptions.ContinueRetrieve(int count)
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
    }
}
