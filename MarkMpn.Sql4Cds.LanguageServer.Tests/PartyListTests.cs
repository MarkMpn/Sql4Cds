using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.Engine;
using MarkMpn.Sql4Cds.LanguageServer.Pptb;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Metadata;
using Microsoft.Xrm.Sdk.Query;
using Newtonsoft.Json.Linq;

namespace MarkMpn.Sql4Cds.LanguageServer.Tests
{
    [TestClass]
    public class PartyListTests
    {
        private static IAttributeMetadataCache ActivityMetadata()
        {
            return MetadataTestHelper.Cache((JObject)ActivityMetadataResponse().Json);
        }

        private static WebApiResponse ActivityMetadataResponse()
        {
            return new WebApiResponse
            {
                Status = 200,
                Body = new JObject
                {
                    ["LogicalName"] = "phonecall",
                    ["EntitySetName"] = "phonecalls",
                    ["PrimaryIdAttribute"] = "activityid",
                    ["Attributes"] = new JArray(
                        new JObject { ["LogicalName"] = "activityid", ["AttributeType"] = "Uniqueidentifier" },
                        new JObject { ["LogicalName"] = "versionnumber", ["AttributeType"] = "BigInt" },
                        new JObject { ["LogicalName"] = "from", ["AttributeType"] = "PartyList" },
                        new JObject { ["LogicalName"] = "to", ["AttributeType"] = "PartyList" },
                        new JObject { ["LogicalName"] = "cc", ["AttributeType"] = "PartyList" })
                }.ToString()
            };
        }

        [TestMethod]
        public void RetrieveAndRetrieveMultipleExposeIndividualPartyColumns()
        {
            var transport = new ActivityTransport();
            var service = new PptbOrganizationService(transport, "https://example.invalid", "test");
            var retrieved = (RetrieveResponse)service.Execute(new RetrieveRequest
            {
                Target = new EntityReference("phonecall", transport.Id),
                ColumnSet = new ColumnSet("from", "to")
            });
            var multiple = (RetrieveMultipleResponse)service.Execute(new RetrieveMultipleRequest
            {
                Query = new QueryExpression("phonecall") { ColumnSet = new ColumnSet("from", "to") }
            });
            foreach (var entity in new[] { retrieved.Entity, multiple.EntityCollection.Entities.Single() })
            {
                Assert.AreEqual(transport.Id, entity.Id);
                Assert.AreEqual("caller@example.invalid", entity.GetAttributeValue<EntityCollection>("from").Entities.Single().GetAttributeValue<string>("addressused"));
                Assert.AreEqual(0, entity.GetAttributeValue<EntityCollection>("to").Entities.Count);
            }
        }

        private sealed class ActivityTransport : IWebApiTransport
        {
            public Guid Id { get; } = Guid.NewGuid();

            public Task<IReadOnlyList<WebApiResponse>> ExecuteBatchAsync(IReadOnlyList<WebApiRequest> requests, bool transaction, bool continueOnError, IReadOnlyDictionary<string, string> headers, CancellationToken cancellationToken)
            {
                return Task.FromResult<IReadOnlyList<WebApiResponse>>(requests.Select(request =>
                {
                    if (request.Url.StartsWith("RetrieveMetadataChanges(", StringComparison.Ordinal))
                        return MetadataTestHelper.Response((JObject)ActivityMetadataResponse().Json);
                    StringAssert.StartsWith(request.Url, "phonecalls?fetchXml=");
                    return new WebApiResponse
                    {
                        Status = 200,
                        Body = new JObject
                        {
                            ["value"] = new JArray(new JObject
                            {
                                ["activityid"] = Id.ToString(),
                                ["phonecall_activity_parties"] = new JArray(new JObject
                                {
                                    ["participationtypemask"] = 1,
                                    ["addressused"] = "caller@example.invalid"
                                })
                            })
                        }.ToString()
                    };
                }).ToArray());
            }
        }

        [TestMethod]
        public void CombinedActivityPartiesAreSplitIntoRolesAndAliases()
        {
            var parser = new FetchXmlResultParser("<fetch><entity name='phonecall'><attribute name='from' alias='sender'/><attribute name='to'/><attribute name='cc'/></entity></fetch>", ActivityMetadata());
            var id = Guid.NewGuid();
            var partyId = Guid.NewGuid();
            var record = new JObject
            {
                ["activityid"] = id.ToString(),
                ["phonecall_activity_parties"] = new JArray(
                    new JObject { ["participationtypemask"] = 1, ["activitypartyid"] = partyId.ToString(),
                        ["_partyid_value"] = id.ToString(), ["_partyid_value" + WebApiJson.LookupLogicalNameAnnotation] = "contact",
                        ["addressused"] = "chosen@example.invalid" },
                    new JObject { ["participationtypemask"] = 2, ["addressused"] = "first@example.invalid" },
                    new JObject { ["participationtypemask"] = 2, ["addressused"] = "second@example.invalid" },
                    new JObject { ["participationtypemask"] = 9, ["addressused"] = "owner@example.invalid" })
            };
            var entity = parser.Parse(new JObject { ["value"] = new JArray(record) }).Entities.Single();
            Assert.AreEqual(id, entity.Id);
            Assert.IsFalse(entity.Contains("phonecall_activity_parties"));
            var sender = entity.GetAttributeValue<EntityCollection>("from").Entities.Single();
            Assert.AreEqual(partyId, sender.Id);
            Assert.AreEqual("activityparty", sender.LogicalName);
            Assert.AreEqual(id, sender.GetAttributeValue<EntityReference>("partyid").Id);
            Assert.AreEqual("chosen@example.invalid", sender.GetAttributeValue<string>("addressused"));
            Assert.AreSame(entity["from"], entity.GetAttributeValue<AliasedValue>("sender").Value);
            Assert.AreEqual(2, entity.GetAttributeValue<EntityCollection>("to").Entities.Count);
            Assert.AreEqual(0, entity.GetAttributeValue<EntityCollection>("cc").Entities.Count);
        }

        [TestMethod]
        public void LinkedCollectionsAreSplitWithoutMixingAliases()
        {
            var parser = new FetchXmlResultParser("<fetch><entity name='phonecall'><link-entity name='phonecall' alias='a'><attribute name='to' alias='first'/></link-entity><link-entity name='phonecall' alias='b'><attribute name='to' alias='second'/></link-entity></entity></fetch>", ActivityMetadata());
            var entity = parser.Parse(new JObject
            {
                ["value"] = new JArray(new JObject
                {
                    ["a.phonecall_activity_parties"] = new JArray(new JObject { ["participationtypemask"] = 2, ["addressused"] = "first@example.invalid" }),
                    ["b.phonecall_activity_parties"] = new JArray()
                })
            }).Entities.Single();
            Assert.AreEqual("first@example.invalid", ((EntityCollection)entity.GetAttributeValue<AliasedValue>("first").Value).Entities.Single().GetAttributeValue<string>("addressused"));
            Assert.AreEqual(0, ((EntityCollection)entity.GetAttributeValue<AliasedValue>("second").Value).Entities.Count);
            Assert.IsInstanceOfType<EntityCollection>(entity.GetAttributeValue<AliasedValue>("a.to").Value);
            Assert.IsFalse(entity.Contains("to"));
        }

        [TestMethod]
        public void EmptyAndInvalidCombinedCollectionsAreHandledExplicitly()
        {
            var parser = new FetchXmlResultParser("<fetch><entity name='phonecall'><all-attributes/></entity></fetch>", ActivityMetadata());
            var record = new JObject { ["phonecall_activity_parties"] = new JArray() };
            var entity = parser.Parse(new JObject { ["value"] = new JArray(record) }).Entities.Single();
            Assert.AreEqual(0, entity.GetAttributeValue<EntityCollection>("from").Entities.Count);
            Assert.AreEqual(0, entity.GetAttributeValue<EntityCollection>("to").Entities.Count);
            foreach (var invalid in new JToken[] { JValue.CreateNull(), new JValue("invalid"),
                new JArray(new JObject()), new JArray(new JObject { ["participationtypemask"] = "invalid" }) })
            {
                record["phonecall_activity_parties"] = invalid;
                Assert.ThrowsException<InvalidOperationException>(() => parser.Parse(new JObject { ["value"] = new JArray(record) }));
            }
        }

        [TestMethod]
        public void RetrievedPartyListsHaveSdkTypesIncludingEmptyLists()
        {
            var attribute = ActivityMetadata()["phonecall"].Attributes.First(a => a.AttributeType == AttributeTypeCode.PartyList);
            var id = Guid.NewGuid();
            var json = new JArray(new JObject
            {
                ["_partyid_value"] = id.ToString(),
                ["_partyid_value" + WebApiJson.LookupLogicalNameAnnotation] = "contact",
                ["addressused"] = "existing@example.invalid"
            });
            var parties = (EntityCollection)FetchXmlResultParser.ConvertValue(json, attribute, null, null);
            Assert.AreEqual("activityparty", parties.Entities.Single().LogicalName);
            Assert.AreEqual(id, parties.Entities.Single().GetAttributeValue<EntityReference>("partyid").Id);
            Assert.AreEqual("existing@example.invalid", parties.Entities.Single().GetAttributeValue<string>("addressused"));
            Assert.AreEqual(0, ((EntityCollection)FetchXmlResultParser.ConvertValue(new JArray(), attribute, null, null)).Entities.Count);
            Assert.ThrowsException<InvalidOperationException>(() => FetchXmlResultParser.ConvertValue(new JValue("invalid"), attribute, null, null));
        }

        [TestMethod]
        public void UpdatesCombineRolesAndSupportClearingWithoutChangingCreate()
        {
            var metadata = MetadataTestHelper.Cache(new[] { "phonecall", "contact", "activityparty" }
                .Select(name => new JObject
                    {
                        ["LogicalName"] = name,
                        ["EntitySetName"] = name == "activityparty" ? "activityparties" : name + "s",
                        ["PrimaryIdAttribute"] = name + "id",
                        ["Attributes"] = new JArray(new JObject { ["LogicalName"] = "to", ["AttributeType"] = "PartyList" },
                            new JObject { ["LogicalName"] = "from", ["AttributeType"] = "PartyList" })
                    }).ToArray());
            var serializer = new WebApiEntitySerializer(metadata);
            var id = Guid.NewGuid();
            var entity = new Entity("phonecall", Guid.NewGuid())
            {
                ["from"] = new EntityCollection(new[] { new Entity("activityparty") { ["partyid"] = new EntityReference("contact", id) } }),
                ["to"] = new EntityCollection(new[] { new Entity("activityparty") { ["addressused"] = "test@example.invalid" } })
            };
            var update = serializer.Serialize(entity, false);
            var parties = (JArray)update["phonecall_activity_parties"];
            Assert.AreEqual(2, parties.Count);
            Assert.AreEqual(1, parties[0].Value<int>("participationtypemask"));
            Assert.AreEqual("/contacts(" + id.ToString("D") + ")", parties[0].Value<string>("partyid_contact@odata.bind"));
            Assert.AreEqual(2, parties[1].Value<int>("participationtypemask"));
            Assert.AreEqual("test@example.invalid", parties[1].Value<string>("addressused"));
            Assert.IsTrue(JToken.DeepEquals(update["phonecall_activity_parties"], serializer.Serialize(entity, true)["phonecall_activity_parties"]));
            var existingId = Guid.NewGuid();
            entity.GetAttributeValue<EntityCollection>("from").Entities[0].Id = existingId;
            Assert.AreEqual(existingId.ToString("D"), serializer.Serialize(entity, false)["phonecall_activity_parties"][0].Value<string>("activitypartyid"));
            Assert.IsNull(serializer.Serialize(entity, true)["phonecall_activity_parties"][0]["activitypartyid"]);
            entity["to"] = null;
            Assert.AreEqual(1, ((JArray)serializer.Serialize(entity, false)["phonecall_activity_parties"]).Count);
            entity["from"] = new EntityCollection();
            Assert.AreEqual(0, ((JArray)serializer.Serialize(entity, false)["phonecall_activity_parties"]).Count);
            Assert.IsNull(serializer.Serialize(entity, false)["to"]);
        }
    }
}
