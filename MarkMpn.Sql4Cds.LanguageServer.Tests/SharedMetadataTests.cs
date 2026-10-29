using System;
using System.IO;
using System.Linq;
using MarkMpn.Sql4Cds.LanguageServer.Pptb;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Newtonsoft.Json.Linq;
using StreamJsonRpc;

namespace MarkMpn.Sql4Cds.LanguageServer.Tests
{
    [TestClass]
    public class SharedMetadataTests
    {
        private static JObject AccountMetadata()
        {
            return JObject.Parse(@"{
                'LogicalName': 'account', 'EntitySetName': 'accounts', 'ObjectTypeCode': 1,
                'PrimaryIdAttribute': 'accountid',
                'Attributes': [
                    { '@odata.type': '#Microsoft.Dynamics.CRM.UniqueIdentifierAttributeMetadata', 'LogicalName': 'accountid', 'AttributeType': 'Uniqueidentifier' },
                    { '@odata.type': '#Microsoft.Dynamics.CRM.LookupAttributeMetadata', 'LogicalName': 'primarycontactid', 'AttributeType': 'Lookup' },
                    { '@odata.type': '#Microsoft.Dynamics.CRM.DateTimeAttributeMetadata', 'LogicalName': 'anniversary', 'AttributeType': 'DateTime', 'Format': 'DateOnly' },
                    { '@odata.type': '#Microsoft.Dynamics.CRM.MultiSelectPicklistAttributeMetadata', 'LogicalName': 'choices', 'AttributeType': 'Virtual', 'AttributeTypeName': { 'Value': 'MultiSelectPicklistType' } },
                    { '@odata.type': '#Microsoft.Dynamics.CRM.ImageAttributeMetadata', 'LogicalName': 'image', 'AttributeType': 'Virtual', 'AttributeTypeName': { 'Value': 'ImageType' } },
                    { '@odata.type': '#Microsoft.Dynamics.CRM.MoneyAttributeMetadata', 'LogicalName': 'revenue', 'AttributeType': 'Money' },
                    { '@odata.type': '#Microsoft.Dynamics.CRM.StringAttributeMetadata', 'LogicalName': 'derived', 'AttributeType': 'String', 'AttributeOf': 'name' }
                ],
                'ManyToOneRelationships': [
                    { 'SchemaName': 'account_primary_contact', 'ReferencedEntity': 'contact', 'ReferencingEntity': 'account', 'ReferencingAttribute': 'primarycontactid',
                      'ReferencedEntityNavigationPropertyName': 'primary_accounts', 'ReferencingEntityNavigationPropertyName': 'primarycontactid' }
                ],
                'OneToManyRelationships': [
                    { 'SchemaName': 'account_contacts', 'ReferencedEntity': 'account', 'ReferencingEntity': 'contact',
                      'ReferencedEntityNavigationPropertyName': 'contacts', 'ReferencingEntityNavigationPropertyName': 'parentcustomerid_account' },
                    { 'SchemaName': 'account_parent', 'ReferencedEntity': 'account', 'ReferencingEntity': 'account',
                      'ReferencedEntityNavigationPropertyName': 'children', 'ReferencingEntityNavigationPropertyName': 'parent' }
                ],
                'ManyToManyRelationships': [
                    { 'SchemaName': 'account_contact', 'Entity1LogicalName': 'account', 'Entity2LogicalName': 'contact',
                      'Entity1NavigationPropertyName': 'account_contacts', 'Entity2NavigationPropertyName': 'contact_accounts' },
                    { 'SchemaName': 'account_peers', 'Entity1LogicalName': 'account', 'Entity2LogicalName': 'account',
                      'Entity1NavigationPropertyName': 'peers1', 'Entity2NavigationPropertyName': 'peers2' }
                ]
            }");
        }

        private static JObject ContactMetadata()
        {
            return new JObject { ["LogicalName"] = "contact", ["EntitySetName"] = "contacts", ["PrimaryIdAttribute"] = "contactid", ["ObjectTypeCode"] = 2 };
        }

        [DataTestMethod]
        [DataRow(false)]
        [DataRow(true)]
        public void TranslationUsesSharedPersistentCacheWithoutSeparateDownloads(bool byObjectTypeCode)
        {
            var path = Path.Combine(Path.GetTempPath(), nameof(SharedMetadataTests), Guid.NewGuid().ToString("N"));
            var orgId = Guid.NewGuid();
            using var rpc = new JsonRpc(new HeaderDelimitedMessageHandler(Stream.Null, Stream.Null));
            try
            {
                for (var run = 0; run < 2; run++)
                {
                    var transport = new MetadataTestHelper.MetadataTransport(new[] { AccountMetadata(), ContactMetadata() });
                    var service = new PptbOrganizationService(transport, "https://example.invalid", "test");
                    var cache = MetadataTestHelper.PersistentCache(path, rpc, service, orgId);
                    service.SetMetadataCache(cache);
                    Assert.AreSame(cache, service.Metadata);
                    var clone = (PptbOrganizationService)service.Clone();
                    Assert.AreSame(cache, clone.Metadata);
                    Assert.AreSame(service.Serializer, clone.Serializer);
                    var metadata = byObjectTypeCode ? cache[1] : cache["account"];
                    Assert.AreSame(metadata, cache["account"]);
                    Assert.AreSame(metadata, cache[1]);

                    var contactId = Guid.NewGuid();
                    var json = clone.Serializer.Serialize(new Entity("account")
                    {
                        ["primarycontactid"] = new EntityReference("contact", contactId),
                        ["anniversary"] = new DateTime(2026, 10, 16),
                        ["choices"] = new OptionSetValueCollection { new OptionSetValue(1), new OptionSetValue(2) },
                        ["derived"] = "ignored"
                    }, true);
                    Assert.AreEqual("/contacts(" + contactId.ToString("D") + ")", json.Value<string>("primarycontactid@odata.bind"));
                    Assert.AreEqual("2026-10-16", json.Value<string>("anniversary"));
                    Assert.AreEqual("1,2", json.Value<string>("choices"));
                    Assert.IsNull(json["derived"]);
                    Assert.IsNull(clone.Serializer.Serialize(new Entity("account") { ["primarycontactid"] = null }, false)["primarycontactid@odata.bind"].Value<string>());
                    Assert.AreEqual("accounts(_primarycontactid_value=" + contactId.ToString("D") + ")",
                        clone.Serializer.GetRecordUrl(new EntityReference("account", new KeyAttributeCollection { ["primarycontactid"] = contactId })));

                    var parser = new FetchXmlResultParser("<fetch><entity name='account'><all-attributes/></entity></fetch>", cache);
                    var accountId = Guid.NewGuid();
                    var entity = parser.Parse(new JObject { ["value"] = new JArray(new JObject
                    {
                        ["accountid"] = accountId,
                        ["choices"] = "1,2",
                        ["image"] = "AQID",
                        ["revenue"] = 12.5m,
                        ["anniversary"] = "2026-10-16T00:00:00Z"
                    }) }).Entities.Single();
                    Assert.AreEqual(accountId, entity.Id);
                    CollectionAssert.AreEqual(new[] { 1, 2 }, entity.GetAttributeValue<OptionSetValueCollection>("choices").Select(o => o.Value).ToArray());
                    CollectionAssert.AreEqual(new byte[] { 1, 2, 3 }, entity.GetAttributeValue<byte[]>("image"));
                    Assert.AreEqual(12.5m, entity.GetAttributeValue<Money>("revenue").Value);
                    Assert.AreEqual(new DateTime(2026, 10, 16), entity.GetAttributeValue<DateTime>("anniversary"));
                    Assert.AreEqual(run == 0 ? 3 : 1, transport.Queries.Count);
                    Assert.AreEqual(1, transport.Queries.Count(q => q["Properties"]["PropertyNames"] is JArray properties &&
                        properties.Values<string>().SequenceEqual(new[] { "MetadataId" })));
                }
            }
            finally
            {
                if (Directory.Exists(path))
                    Directory.Delete(path, true);
            }
        }

        [DataTestMethod]
        [DataRow("account_contacts", null, "contacts", true)]
        [DataRow("account_primary_contact", null, "primarycontactid", false)]
        [DataRow("account_contact", null, "account_contacts", true)]
        [DataRow("account_parent", EntityRole.Referenced, "children", true)]
        [DataRow("account_parent", EntityRole.Referencing, "parent", false)]
        [DataRow("account_peers", EntityRole.Referenced, "peers2", true)]
        [DataRow("account_peers", EntityRole.Referencing, "peers1", true)]
        public void AssociationsUseRelationshipsFromSharedEntityMetadata(string schemaName, EntityRole? role, string navigationProperty, bool collection)
        {
            var service = new PptbOrganizationService(new MetadataTestHelper.MetadataTransport(Array.Empty<JObject>()), "https://example.invalid", "test");
            service.SetMetadataCache(MetadataTestHelper.Cache(AccountMetadata(), ContactMetadata()));
            var target = new EntityReference("account", Guid.NewGuid());
            var related = new EntityReference("contact", Guid.NewGuid());
            var relationship = new Relationship(schemaName) { PrimaryEntityRole = role };
            var associate = service.Prepare(new AssociateRequest
            {
                Target = target, Relationship = relationship, RelatedEntities = new EntityReferenceCollection { related }
            }).Requests.Single();
            Assert.AreEqual(collection ? "POST" : "PUT", associate.Method);
            Assert.AreEqual($"accounts({target.Id:D})/{navigationProperty}/$ref", associate.Url);
            var disassociate = service.Prepare(new DisassociateRequest
            {
                Target = target, Relationship = relationship, RelatedEntities = new EntityReferenceCollection { related }
            }).Requests.Single();
            Assert.AreEqual("DELETE", disassociate.Method);
            Assert.AreEqual(collection ? $"accounts({target.Id:D})/{navigationProperty}({related.Id:D})/$ref" :
                $"accounts({target.Id:D})/{navigationProperty}/$ref", disassociate.Url);
            Assert.ThrowsException<NotSupportedException>(() => service.Prepare(new AssociateRequest
            {
                Target = target, Relationship = new Relationship("missing"), RelatedEntities = new EntityReferenceCollection { related }
            }));
        }
    }
}
