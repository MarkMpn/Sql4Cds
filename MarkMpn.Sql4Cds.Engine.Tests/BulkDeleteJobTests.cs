using System;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using Microsoft.Crm.Sdk.Messages;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Query;

namespace MarkMpn.Sql4Cds.Engine.Tests
{
    [TestClass]
    public class BulkDeleteJobTests : FakeXrmEasyTestsBase
    {
        [DataTestMethod]
        [DataRow(false)]
        [DataRow(true)]
        public void BulkDeleteConfirmsBeforeSubmittingAndReturnsJobIdentity(bool cancel)
        {
            var service = new JobService(_service);
            _localDataSource.Connection = service;
            var confirmations = 0;
            using (var connection = new Sql4CdsConnection(_localDataSources))
            using (var command = connection.CreateCommand())
            {
                connection.UseBulkDelete = true;
                connection.PreDelete += (sender, args) =>
                {
                    confirmations++;
                    Assert.AreEqual(0, service.Submissions);
                    args.Cancel = cancel;
                };
                command.CommandText = "DELETE FROM account WHERE name = 'test'";
                if (cancel)
                {
                    Assert.ThrowsException<Sql4CdsException>(() => command.ExecuteNonQuery());
                    Assert.AreEqual(0, service.Submissions);
                }
                else
                {
                    Assert.AreEqual(1, command.ExecuteNonQuery());
                    Assert.AreEqual(1, service.Submissions);
                    command.CommandText = "SELECT @@IDENTITY";
                    var identity = (SqlEntityReference)command.ExecuteScalar();
                    Assert.AreEqual(service.JobId, identity.Id);
                    Assert.AreEqual("asyncoperation", identity.LogicalName);
                    Assert.AreEqual("local", identity.DataSource);
                }
            }
            Assert.AreEqual(1, confirmations);
            StringAssert.Contains(service.FetchXml, "value=\"test\"");
        }

        private sealed class JobService : IOrganizationService
        {
            private readonly IOrganizationService _inner;
            public readonly Guid JobId = Guid.NewGuid();
            public int Submissions;
            public string FetchXml;
            public JobService(IOrganizationService inner) => _inner = inner;
            public OrganizationResponse Execute(OrganizationRequest request)
            {
                if (request is FetchXmlToQueryExpressionRequest conversion)
                {
                    FetchXml = conversion.FetchXml;
                    var query = new QueryExpression("account");
                    query.Criteria.AddCondition("name", ConditionOperator.Equal, "test");
                    var result = new FetchXmlToQueryExpressionResponse();
                    result.Results["Query"] = query;
                    return result;
                }
                if (request is BulkDeleteRequest bulk)
                {
                    Submissions++;
                    Assert.AreEqual("account", bulk.QuerySet[0].EntityName);
                    Assert.AreEqual(false, bulk.SendEmailNotification);
                    Assert.AreEqual("", bulk.RecurrencePattern);
                    var result = new BulkDeleteResponse();
                    result.Results["JobId"] = JobId;
                    return result;
                }
                return _inner.Execute(request);
            }
            public Guid Create(Entity entity) => _inner.Create(entity);
            public void Update(Entity entity) => _inner.Update(entity);
            public void Delete(string entityName, Guid id) => _inner.Delete(entityName, id);
            public Entity Retrieve(string entityName, Guid id, ColumnSet columnSet) => _inner.Retrieve(entityName, id, columnSet);
            public EntityCollection RetrieveMultiple(QueryBase query) => _inner.RetrieveMultiple(query);
            public void Associate(string entityName, Guid id, Relationship relationship, EntityReferenceCollection relatedEntities) => _inner.Associate(entityName, id, relationship, relatedEntities);
            public void Disassociate(string entityName, Guid id, Relationship relationship, EntityReferenceCollection relatedEntities) => _inner.Disassociate(entityName, id, relationship, relatedEntities);
        }
    }
}
