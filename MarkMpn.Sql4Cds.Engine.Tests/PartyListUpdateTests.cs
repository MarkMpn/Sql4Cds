using System;
using System.Linq;
using FakeXrmEasy.Extensions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Metadata;
using Microsoft.Xrm.Sdk.Query;

namespace MarkMpn.Sql4Cds.Engine.Tests
{
    [TestClass]
    public class PartyListUpdateTests : FakeXrmEasyTestsBase
    {
        [DataTestMethod]
        [DataRow(true, false, false, false)]
        [DataRow(false, false, false, false)]
        [DataRow(true, true, false, false)]
        [DataRow(true, false, true, false)]
        [DataRow(true, false, false, true)]
        public void OnlyWebApiUpdatesIncludeUnchangedPartyLists(bool webApi, bool clear, bool multiple, bool alias)
        {
            var metadata = _localDataSource.Metadata["account"];
            var parties = new[] { "from", "to", "cc" }.Select(name =>
            {
                var attribute = new LookupAttributeMetadata { LogicalName = name, Targets = new[] { "contact" } };
                attribute.SetSealedPropertyValue(nameof(attribute.AttributeType), AttributeTypeCode.PartyList);
                attribute.SetSealedPropertyValue(nameof(attribute.IsValidForUpdate), true);
                attribute.SetSealedPropertyValue(nameof(attribute.IsValidForRead), true);
                attribute.SetSealedPropertyValue(nameof(attribute.ColumnNumber), 100);
                return attribute;
            }).ToArray();
            metadata.SetSealedPropertyValue(nameof(metadata.Attributes), metadata.Attributes.Concat(parties).ToArray());
            var service = new PartyService(_service, webApi);
            _localDataSource.Connection = service;
            using (var connection = new Sql4CdsConnection(_localDataSources))
            using (var command = connection.CreateCommand())
            {
                command.CommandText = "UPDATE " + (alias ? "a" : "account") + " SET [to] = " + (clear ? "NULL" : "'new@example.invalid'") +
                    (multiple ? ", [from] = 'explicit@example.invalid'" : "") +
                    (alias ? " FROM account a" : "") + " WHERE accountid = '" + service.Id + "'";
                Assert.AreEqual(1, command.ExecuteNonQuery());
                if (webApi && !multiple)
                {
                    var submitted = service.UpdateTarget;
                    command.CommandText = "SELECT [from] FROM account WHERE accountid = '" + service.Id + "'";
                    var text = (string)command.ExecuteScalar();
                    StringAssert.StartsWith(text, "{");
                    command.CommandText = "UPDATE account SET [to] = @parties WHERE accountid = '" + service.Id + "'";
                    var parameter = command.CreateParameter();
                    parameter.ParameterName = "@parties";
                    parameter.Value = text;
                    command.Parameters.Add(parameter);
                    Assert.AreEqual(1, command.ExecuteNonQuery());
                    var roundTripped = service.UpdateTarget.GetAttributeValue<EntityCollection>("to").Entities.Single();
                    Assert.AreEqual(service.PartyId, roundTripped.GetAttributeValue<EntityReference>("partyid").Id);
                    Assert.AreEqual("unchanged@example.invalid", roundTripped.GetAttributeValue<string>("addressused"));
                    foreach (var invalid in new[] { "{}", "{\"entities\":[] } trailing", "{\"entities\":[null]}" })
                    {
                        parameter.Value = invalid;
                        Assert.ThrowsException<Sql4CdsException>(() => command.ExecuteNonQuery());
                    }
                    service.UpdateTarget = submitted;
                }
            }

            Assert.IsNotNull(service.UpdateTarget);
            Assert.AreEqual(webApi, service.UpdateTarget.Contains("from"));
            Assert.AreEqual(webApi, service.UpdateTarget.Contains("cc"));
            if (webApi)
            {
                Assert.AreEqual(multiple ? "explicit@example.invalid" : "unchanged@example.invalid", service.UpdateTarget.GetAttributeValue<EntityCollection>("from").Entities.Single().GetAttributeValue<string>("addressused"));
                if (!multiple)
                {
                    Assert.AreEqual(service.PartyId, service.UpdateTarget.GetAttributeValue<EntityCollection>("from").Entities.Single().GetAttributeValue<EntityReference>("partyid").Id);
                    Assert.AreEqual(service.ActivityPartyId, service.UpdateTarget.GetAttributeValue<EntityCollection>("from").Entities.Single().Id);
                    var party = service.UpdateTarget.GetAttributeValue<EntityCollection>("from").Entities.Single();
                    Assert.AreEqual(2, party.GetAttributeValue<int>("addressusedemailcolumnnumber"));
                    Assert.AreEqual(1, party.GetAttributeValue<OptionSetValue>("participationtypemask").Value);
                    Assert.IsFalse(party.GetAttributeValue<bool>("donotemail"));
                    Assert.IsNull(party["externalid"]);
                    Assert.AreEqual(service.PartyId, party.GetAttributeValue<Guid>("activitypartyid"));
                    Assert.AreEqual(12.34m, party.GetAttributeValue<decimal>("effort"));
                    Assert.AreEqual(new DateTime(2026, 10, 14, 12, 0, 0, DateTimeKind.Utc), party.GetAttributeValue<DateTime>("scheduledstart"));
                }
            }
            if (clear)
                Assert.IsNull(service.UpdateTarget["to"]);
            else
                Assert.AreEqual("new@example.invalid", service.UpdateTarget.GetAttributeValue<EntityCollection>("to").Entities.Single().GetAttributeValue<string>("addressused"));
        }

        [TestMethod]
        public void OrdinaryWebApiUpdatesDoNotReadOrIncludePartyLists()
        {
            var service = new PartyService(_service, true);
            _localDataSource.Connection = service;
            using (var connection = new Sql4CdsConnection(_localDataSources))
            using (var command = connection.CreateCommand())
            {
                command.CommandText = "UPDATE account SET name = 'changed' WHERE accountid = '" + service.Id + "'";
                Assert.AreEqual(1, command.ExecuteNonQuery());
            }
            Assert.AreEqual(0, service.Reads);
            Assert.IsFalse(service.UpdateTarget.Contains("from"));
        }

        private sealed class PartyService : IOrganizationService, IServiceClient
        {
            private readonly IOrganizationService _inner;
            public PartyService(IOrganizationService inner, bool webApi) { _inner = inner; UsesWebAPI = webApi; }
            public Guid Id { get; } = Guid.NewGuid();
            public Guid PartyId { get; } = Guid.NewGuid();
            public Guid ActivityPartyId { get; } = Guid.NewGuid();
            public Entity UpdateTarget { get; set; }
            public int Reads { get; private set; }
            public Version Version => new Version(9, 2);
            public string Name => "local";
            public Uri Uri => new Uri("https://example.invalid");
            public bool UsesWebAPI { get; }
            public Guid CallerId { get; set; }
            public bool CanParallelise => false;
            public int RecommendedDegreesOfParallelism => 1;
            public IOrganizationService Clone() => throw new NotSupportedException();
            public OrganizationResponse Execute(OrganizationRequest request)
            {
                if (request is RetrieveMultipleRequest retrieve && retrieve.Query is FetchExpression)
                {
                    Reads++;
                    var result = new RetrieveMultipleResponse();
                    result.Results["EntityCollection"] = new EntityCollection(new[]
                    {
                        new Entity("account", Id)
                        {
                            ["accountid"] = Id,
                            ["from"] = new EntityCollection(new[] { new Entity("activityparty", ActivityPartyId)
                            {
                                ["partyid"] = new EntityReference("contact", PartyId),
                                ["addressused"] = "unchanged@example.invalid",
                                ["addressusedemailcolumnnumber"] = 2,
                                ["participationtypemask"] = new OptionSetValue(1),
                                ["donotemail"] = false,
                                ["externalid"] = null,
                                ["activitypartyid"] = PartyId,
                                ["effort"] = 12.34m,
                                ["scheduledstart"] = new DateTime(2026, 10, 14, 12, 0, 0, DateTimeKind.Utc)
                            } }),
                            ["to"] = new EntityCollection(new[] { new Entity("activityparty") { ["addressused"] = "old@example.invalid" } }),
                            ["cc"] = new EntityCollection()
                        }
                    });
                    return result;
                }
                if (request is UpdateRequest update)
                {
                    UpdateTarget = update.Target;
                    return new UpdateResponse();
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
