using System;
using System.Linq;
using System.ServiceModel;
using MarkMpn.Sql4Cds.LanguageServer.Pptb;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Query;
using Newtonsoft.Json.Linq;

namespace MarkMpn.Sql4Cds.LanguageServer.Tests
{
    [TestClass]
    public class RetrieveTests
    {
        private static PptbOrganizationService Service()
        {
            var service = new PptbOrganizationService(new MetadataTestHelper.MetadataTransport(Array.Empty<JObject>()), "https://example.invalid", "test");
            service.SetMetadataCache(MetadataTestHelper.Cache(
                new JObject
                {
                    ["LogicalName"] = "account", ["EntitySetName"] = "accounts", ["PrimaryIdAttribute"] = "accountid",
                    ["Attributes"] = new JArray(
                        new JObject { ["LogicalName"] = "accountid", ["AttributeType"] = "Uniqueidentifier" },
                        new JObject { ["LogicalName"] = "name", ["AttributeType"] = "String" },
                        new JObject { ["LogicalName"] = "primarycontactid", ["AttributeType"] = "Lookup", ["Targets"] = new JArray("contact") },
                        new JObject { ["LogicalName"] = "revenue", ["AttributeType"] = "Money" }),
                    ["OneToManyRelationships"] = new JArray(new JObject
                    {
                        ["SchemaName"] = "account_contacts", ["ReferencedEntity"] = "account", ["ReferencingEntity"] = "contact",
                        ["ReferencedEntityNavigationPropertyName"] = "account_contacts", ["ReferencingEntityNavigationPropertyName"] = "parentcustomerid_account"
                    }),
                    ["ManyToOneRelationships"] = new JArray(new JObject
                    {
                        ["SchemaName"] = "account_primary_contact", ["ReferencedEntity"] = "contact", ["ReferencingEntity"] = "account",
                        ["ReferencedEntityNavigationPropertyName"] = "primary_accounts", ["ReferencingEntityNavigationPropertyName"] = "primarycontactid"
                    }),
                    ["ManyToManyRelationships"] = new JArray(new JObject
                    {
                        ["SchemaName"] = "account_peers", ["Entity1LogicalName"] = "account", ["Entity2LogicalName"] = "account",
                        ["Entity1NavigationPropertyName"] = "peers", ["Entity2NavigationPropertyName"] = "reverse_peers"
                    })
                },
                new JObject
                {
                    ["LogicalName"] = "contact", ["EntitySetName"] = "contacts", ["PrimaryIdAttribute"] = "contactid",
                    ["Attributes"] = new JArray(
                        new JObject { ["LogicalName"] = "contactid", ["AttributeType"] = "Uniqueidentifier" },
                        new JObject { ["LogicalName"] = "fullname", ["AttributeType"] = "String" },
                        new JObject { ["LogicalName"] = "statecode", ["AttributeType"] = "State" })
                }));
            return service;
        }

        [TestMethod]
        public void RetrieveUsesRecordUrlSelectAndTypedResponse()
        {
            var id = Guid.NewGuid();
            var contact = Guid.NewGuid();
            var operation = Service().Prepare(new RetrieveRequest
            {
                Target = new EntityReference("account", id),
                ColumnSet = new ColumnSet("name", "primarycontactid", "revenue")
            });
            var request = operation.Requests.Single();
            Assert.AreEqual("GET", request.Method);
            Assert.AreEqual($"accounts({id:D})?$select=name,_primarycontactid_value,revenue,accountid", Uri.UnescapeDataString(request.Url));
            Assert.AreEqual("odata.include-annotations=\"*\"", request.Headers["Prefer"]);
            var result = (RetrieveResponse)operation.MapResponses(new[] { Response(new JObject
            {
                ["accountid"] = id.ToString(), ["name"] = "Account", ["revenue"] = 123.45m,
                ["_primarycontactid_value"] = contact.ToString(),
                ["_primarycontactid_value@Microsoft.Dynamics.CRM.lookuplogicalname"] = "contact",
                ["_primarycontactid_value@OData.Community.Display.V1.FormattedValue"] = "Contact",
                ["revenue@OData.Community.Display.V1.FormattedValue"] = "$123.45",
                ["@odata.etag"] = "W/\"123\""
            }) });
            Assert.AreEqual(id, result.Entity.Id);
            Assert.AreEqual("123", result.Entity.RowVersion);
            Assert.AreEqual(123.45m, result.Entity.GetAttributeValue<Money>("revenue").Value);
            Assert.AreEqual("$123.45", result.Entity.FormattedValues["revenue"]);
            var lookup = result.Entity.GetAttributeValue<EntityReference>("primarycontactid");
            Assert.AreEqual(contact, lookup.Id);
            Assert.AreEqual("contact", lookup.LogicalName);
            Assert.AreEqual("Contact", lookup.Name);
        }

        [TestMethod]
        public void RetrieveSupportsAlternateKeysAndEmptyAndAllColumnSets()
        {
            var service = Service();
            var target = new EntityReference("account", "name", "O'Brien & Sons");
            var empty = service.Prepare(new RetrieveRequest { Target = target }).Requests.Single();
            Assert.AreEqual(service.Serializer.GetRecordUrl(target) + "?$select=accountid", Uri.UnescapeDataString(empty.Url));
            var all = service.Prepare(new RetrieveRequest { Target = target, ColumnSet = new ColumnSet(true) }).Requests.Single();
            Assert.AreEqual(service.Serializer.GetRecordUrl(target), all.Url);
        }

        [TestMethod]
        public void RetrieveExpandsCollectionAndSingleRelationships()
        {
            var id = Guid.NewGuid();
            var contact = Guid.NewGuid();
            var related = new Relationship("account_contacts");
            var primary = new Relationship("account_primary_contact");
            var query = new QueryExpression("contact") { ColumnSet = new ColumnSet("fullname"), TopCount = 5 };
            query.Criteria.AddCondition("statecode", ConditionOperator.Equal, 0);
            query.AddOrder("fullname", OrderType.Descending);
            var request = new RetrieveRequest
            {
                Target = new EntityReference("account", id),
                ColumnSet = new ColumnSet("name"),
                RelatedEntitiesQuery = new RelationshipQueryCollection
                {
                    { related, query },
                    { primary, new QueryExpression("contact") { ColumnSet = new ColumnSet("fullname") } }
                }
            };
            var operation = Service().Prepare(request);
            var url = Uri.UnescapeDataString(operation.Requests.Single().Url);
            StringAssert.Contains(url, "$expand=account_contacts($select=fullname,contactid;$filter=(statecode eq 0);$orderby=fullname desc;$top=5),primarycontactid($select=fullname,contactid)");
            var entity = ((RetrieveResponse)operation.MapResponses(new[] { Response(new JObject
            {
                ["accountid"] = id.ToString(), ["name"] = "Account",
                ["account_contacts"] = new JArray(new JObject { ["contactid"] = contact.ToString(), ["fullname"] = "Contact" }),
                ["primarycontactid"] = new JObject { ["contactid"] = contact.ToString(), ["fullname"] = "Primary" }
            }) })).Entity;
            Assert.AreEqual(contact, entity.RelatedEntities[related].Entities.Single().Id);
            Assert.AreEqual("contact", entity.RelatedEntities[related].EntityName);
            Assert.AreEqual("Primary", entity.RelatedEntities[primary].Entities.Single().GetAttributeValue<string>("fullname"));
            Assert.IsFalse(entity.Contains("account_contacts"));
            Assert.IsFalse(entity.Contains("primarycontactid"));
        }

        [TestMethod]
        public void RetrieveHandlesNullAndEmptyExpandedRelationships()
        {
            var related = new Relationship("account_contacts");
            var primary = new Relationship("account_primary_contact");
            var operation = Service().Prepare(new RetrieveRequest
            {
                Target = new EntityReference("account", Guid.NewGuid()),
                RelatedEntitiesQuery = new RelationshipQueryCollection
                {
                    { related, new QueryExpression("contact") },
                    { primary, new QueryExpression("contact") }
                }
            });
            var entity = ((RetrieveResponse)operation.MapResponses(new[] { Response(new JObject
            {
                ["account_contacts"] = new JArray(),
                ["primarycontactid"] = JValue.CreateNull()
            }) })).Entity;
            Assert.AreEqual(0, entity.RelatedEntities[related].Entities.Count);
            Assert.AreEqual(0, entity.RelatedEntities[primary].Entities.Count);
        }

        [TestMethod]
        public void UnsupportedRelatedQueryFailsDuringPreparation()
        {
            var query = new QueryExpression("contact") { Distinct = true };
            Assert.ThrowsException<NotSupportedException>(() => Service().Prepare(new RetrieveRequest
            {
                Target = new EntityReference("account", Guid.NewGuid()),
                RelatedEntitiesQuery = new RelationshipQueryCollection { { new Relationship("account_contacts"), query } }
            }));
        }

        [DataTestMethod]
        [DataRow(EntityRole.Referenced, "reverse_peers")]
        [DataRow(EntityRole.Referencing, "peers")]
        public void RetrieveResolvesSelfReferencingManyToManyDirection(EntityRole role, string navigation)
        {
            var relationship = new Relationship("account_peers") { PrimaryEntityRole = role };
            var operation = Service().Prepare(new RetrieveRequest
            {
                Target = new EntityReference("account", Guid.NewGuid()),
                RelatedEntitiesQuery = new RelationshipQueryCollection
                {
                    { relationship, new QueryExpression("account") { ColumnSet = new ColumnSet("name") } }
                }
            });
            StringAssert.Contains(Uri.UnescapeDataString(operation.Requests.Single().Url), "$expand=" + navigation + "($select=name,accountid)");
            var entity = ((RetrieveResponse)operation.MapResponses(new[] { Response(new JObject { [navigation] = new JArray() }) })).Entity;
            Assert.AreEqual(0, entity.RelatedEntities[relationship].Entities.Count);
        }

        [TestMethod]
        public void RelatedFiltersEscapeLiteralsAndPreserveNestedLogic()
        {
            var query = new QueryExpression("contact");
            query.Criteria.AddCondition("statecode", ConditionOperator.Equal, new OptionSetValue(0));
            var nested = query.Criteria.AddFilter(LogicalOperator.Or);
            nested.AddCondition("fullname", ConditionOperator.Equal, "O'Brien & Sons");
            nested.AddCondition("fullname", ConditionOperator.Null);
            var operation = Service().Prepare(new RetrieveRequest
            {
                Target = new EntityReference("account", Guid.NewGuid()),
                RelatedEntitiesQuery = new RelationshipQueryCollection { { new Relationship("account_contacts"), query } }
            });
            var url = operation.Requests.Single().Url;
            StringAssert.Contains(url, "%26");
            StringAssert.Contains(Uri.UnescapeDataString(url), "$filter=((fullname eq 'O''Brien & Sons' or fullname eq null) and statecode eq 0)");
        }

        [TestMethod]
        public void RelatedQueryCannotUseWrongEntityOrUnsupportedFilter()
        {
            var service = Service();
            var query = new QueryExpression("account");
            var request = new RetrieveRequest
            {
                Target = new EntityReference("account", Guid.NewGuid()),
                RelatedEntitiesQuery = new RelationshipQueryCollection { { new Relationship("account_contacts"), query } }
            };
            Assert.ThrowsException<NotSupportedException>(() => service.Prepare(request));
            query.EntityName = "contact";
            query.Criteria.AddCondition("fullname", ConditionOperator.Like, "%Smith%");
            Assert.ThrowsException<NotSupportedException>(() => service.Prepare(request));
        }

        [TestMethod]
        public void MissingExpandedResponseIsNotTreatedAsEmpty()
        {
            var operation = Service().Prepare(new RetrieveRequest
            {
                Target = new EntityReference("account", Guid.NewGuid()),
                RelatedEntitiesQuery = new RelationshipQueryCollection
                {
                    { new Relationship("account_contacts"), new QueryExpression("contact") }
                }
            });
            Assert.ThrowsException<InvalidOperationException>(() => operation.MapResponses(new[] { Response(new JObject()) }));
        }

        [TestMethod]
        public void RetrievePreservesServerNotFoundFault()
        {
            var operation = Service().Prepare(new RetrieveRequest { Target = new EntityReference("account", Guid.NewGuid()) });
            var exception = Assert.ThrowsException<FaultException<OrganizationServiceFault>>(() => operation.MapResponses(new[]
            {
                new WebApiResponse { Status = 404, Body = "{\"error\":{\"code\":\"0x80040217\",\"message\":\"Record not found\"}}" }
            }));
            Assert.AreEqual(unchecked((int)0x80040217), exception.Detail.ErrorCode);
            Assert.AreEqual("Record not found", exception.Detail.Message);
        }

        private static WebApiResponse Response(JObject body) => new WebApiResponse { Status = 200, Body = body.ToString() };
    }
}
