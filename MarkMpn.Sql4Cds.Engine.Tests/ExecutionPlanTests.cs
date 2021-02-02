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
            var planBuilder = new ExecutionPlanBuilder(metadata, true);

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
            var planBuilder = new ExecutionPlanBuilder(metadata, true);

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
            var planBuilder = new ExecutionPlanBuilder(metadata, true);

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
            var planBuilder = new ExecutionPlanBuilder(metadata, true);

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
            var planBuilder = new ExecutionPlanBuilder(metadata, true);

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
            var planBuilder = new ExecutionPlanBuilder(metadata, true);

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
            var planBuilder = new ExecutionPlanBuilder(metadata, true);

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
            var planBuilder = new ExecutionPlanBuilder(metadata, true);

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
            var fetch = AssertNode<FetchXmlScan>(select.Source);
            AssertFetchXml(fetch, @"
                <fetch aggregate='true'>
                    <entity name='account'>
                        <attribute name='name' groupby='true' alias='name' />
                        <attribute name='accountid' aggregate='count' alias='count' />
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
