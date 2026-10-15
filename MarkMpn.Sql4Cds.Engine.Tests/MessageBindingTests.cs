using System;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Metadata;
using Microsoft.Xrm.Sdk.Query;

namespace MarkMpn.Sql4Cds.Engine.Tests
{
    [TestClass]
    public class MessageBindingTests
    {
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public void CacheLoadsBindingAndCombinedParameterInformation(bool readOnly)
        {
            var service = new MessageMetadataService { IsReadOnly = readOnly };
            var cache = new MessageCache(service, new AttributeMetadataCache(service));
            Assert.IsTrue(cache.TryGetValue("MappedMessage", out var message));
            Assert.AreEqual("team", message.BoundEntityLogicalName);
            Assert.AreEqual(readOnly, message.IsReadOnly);
            Assert.IsTrue(message.InputParameters[0].IsBindingParameter);
            Assert.AreEqual(typeof(Guid), message.InputParameters[0].Type);
            Assert.AreEqual(typeof(Guid[]), message.InputParameters[1].Type);
            Assert.AreEqual(8, message.InputParameters[1].OTC);
            Assert.AreEqual("Members", message.InputParameters[1].EntityParameterName);
            Assert.AreEqual(typeof(Guid), message.OutputParameters[0].Type);
            Assert.AreEqual(4700, message.OutputParameters[0].OTC);
            Assert.AreEqual("AsyncOperation", message.OutputParameters[0].EntityParameterName);
            Assert.IsTrue(cache.TryGetValue("mappedmessage", out var same));
            Assert.AreSame(message, same);
            Assert.AreEqual(2, service.Reads);
        }

        [TestMethod]
        public void BindingInformationSupportsOrderingWhitespaceAndReset()
        {
            var parameter = new MessageParameter();
            parameter.SetBindingInformation("AsEntityParameter:Members, OTC:8, Bound:TRUE");
            Assert.AreEqual(8, parameter.OTC);
            Assert.AreEqual("Members", parameter.EntityParameterName);
            Assert.IsTrue(parameter.IsBindingParameter);
            parameter.SetBindingInformation("OTC:4700");
            Assert.AreEqual(4700, parameter.OTC);
            Assert.IsNull(parameter.EntityParameterName);
            Assert.IsFalse(parameter.IsBindingParameter);
            parameter.SetBindingInformation(null);
            Assert.IsNull(parameter.OTC);
            Assert.ThrowsException<FormatException>(() => parameter.SetBindingInformation("OTC:invalid"));
        }

        private sealed class MessageMetadataService : IOrganizationService
        {
            public int Reads;
            public bool IsReadOnly;
            public OrganizationResponse Execute(OrganizationRequest request)
            {
                var entity = new EntityMetadata { LogicalName = ((RetrieveEntityRequest)request).LogicalName };
                typeof(EntityMetadata).GetProperty(nameof(EntityMetadata.Attributes)).SetValue(entity,
                    new[] { "name", "primaryobjecttypecode", "clrparser", "clrformatter", "position", "optional", "parameterbindinginformation", "isreadonly" }
                        .Select(n => (AttributeMetadata)new StringAttributeMetadata { LogicalName = n }).ToArray());
                var response = new RetrieveEntityResponse();
                response.Results["EntityMetadata"] = entity;
                return response;
            }

            public EntityCollection RetrieveMultiple(QueryBase queryBase)
            {
                Reads++;
                var query = (QueryExpression)queryBase;
                if (query.EntityName == "sdkmessagerequest")
                {
                    CollectionAssert.Contains(query.ColumnSet.Columns.ToArray(), "primaryobjecttypecode");
                    var messageLink = query.LinkEntities.Single(l => l.LinkToEntityName == "sdkmessagepair").LinkEntities.Single();
                    Assert.AreEqual("sdkmessage", messageLink.LinkToEntityName);
                    CollectionAssert.Contains(messageLink.Columns.Columns.ToArray(), "isreadonly");
                    return new EntityCollection(new[]
                    {
                        RequestField("TeamId", typeof(Guid), 0, "Bound:TRUE"),
                        RequestField("MemberIds", typeof(Guid[]), 1, "OTC:8,AsEntityParameter:Members")
                    });
                }
                Assert.AreEqual("sdkmessageresponse", query.EntityName);
                return new EntityCollection(new[]
                {
                    new Entity("sdkmessageresponse")
                    {
                        ["sdkmessagerequest.name"] = new AliasedValue("sdkmessagerequest", "name", "MappedMessage"),
                        ["sdkmessageresponsefield.name"] = new AliasedValue("sdkmessageresponsefield", "name", "Id"),
                        ["sdkmessageresponsefield.clrformatter"] = new AliasedValue("sdkmessageresponsefield", "clrformatter", typeof(Guid).AssemblyQualifiedName),
                        ["sdkmessageresponsefield.position"] = new AliasedValue("sdkmessageresponsefield", "position", 0),
                        ["sdkmessageresponsefield.parameterbindinginformation"] = new AliasedValue("sdkmessageresponsefield", "parameterbindinginformation", "OTC:4700,AsEntityParameter:AsyncOperation")
                    }
                });
            }

            private Entity RequestField(string name, Type type, int position, string binding) => new Entity("sdkmessagerequest")
            {
                ["name"] = "MappedMessage",
                ["primaryobjecttypecode"] = "team",
                ["sdkmessage.isreadonly"] = new AliasedValue("sdkmessage", "isreadonly", IsReadOnly),
                ["sdkmessagerequestfield.name"] = new AliasedValue("sdkmessagerequestfield", "name", name),
                ["sdkmessagerequestfield.clrparser"] = new AliasedValue("sdkmessagerequestfield", "clrparser", type.AssemblyQualifiedName),
                ["sdkmessagerequestfield.position"] = new AliasedValue("sdkmessagerequestfield", "position", position),
                ["sdkmessagerequestfield.optional"] = new AliasedValue("sdkmessagerequestfield", "optional", false),
                ["sdkmessagerequestfield.parameterbindinginformation"] = new AliasedValue("sdkmessagerequestfield", "parameterbindinginformation", binding)
            };

            public Guid Create(Entity entity) => throw new NotSupportedException();
            public void Update(Entity entity) => throw new NotSupportedException();
            public void Delete(string entityName, Guid id) => throw new NotSupportedException();
            public Entity Retrieve(string entityName, Guid id, ColumnSet columnSet) => throw new NotSupportedException();
            public void Associate(string entityName, Guid id, Relationship relationship, EntityReferenceCollection relatedEntities) => throw new NotSupportedException();
            public void Disassociate(string entityName, Guid id, Relationship relationship, EntityReferenceCollection relatedEntities) => throw new NotSupportedException();
        }
    }
}
