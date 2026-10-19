using System;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using MarkMpn.Sql4Cds.Engine;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Metadata;
using Microsoft.Xrm.Sdk.Metadata.Query;
using Microsoft.Xrm.Sdk.Query;
using Newtonsoft.Json.Linq;
using StreamJsonRpc;

namespace MarkMpn.Sql4Cds.LanguageServer.Tests
{
    [TestClass]
    public class LazyMetadataCacheTests
    {
        [DataTestMethod]
        [DataRow("9.2.26094.142:2506169!10/16/2026 18:09:38", "9.2.26094.142:2506169!10/16/2026 19:09:38", false)]
        [DataRow("9.2.26094.142:2506169", "9.2.26094.142:2506169!10/16/2026 19:09:38", false)]
        [DataRow("9.2.26094.142:2506169!10/16/2026 18:09:38", "9.2.26094.142:2506169", false)]
        [DataRow("9.2.26094.142:2506169!10/16/2026 18:09:38", "9.2.26094.142:2506170!10/16/2026 19:09:38", true)]
        [DataRow("9.2.26094.142:2506169!10/16/2026 18:09:38", "9.2.26095.100:2506169!10/16/2026 19:09:38", true)]
        [DataRow("stamp", "stamp", false)]
        [DataRow("stamp", "changed", true)]
        public void CacheValidationIgnoresOnlyStampTimestamp(string savedStamp, string currentStamp, bool reload)
        {
            var path = Path.Combine(Path.GetTempPath(), nameof(LazyMetadataCacheTests), Guid.NewGuid().ToString("N"));
            var orgId = Guid.NewGuid();
            using var rpc = new JsonRpc(new HeaderDelimitedMessageHandler(Stream.Null, Stream.Null));
            try
            {
                var directory = Path.Combine(path, orgId + ".lazy");
                Directory.CreateDirectory(directory);
                var filename = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes("name:account"))) + ".json.gz";
                using (var stream = File.Create(Path.Combine(directory, filename)))
                using (var gzip = new GZipStream(stream, CompressionLevel.Optimal))
                using (var writer = new StreamWriter(gzip))
                {
                    writer.Write(new JObject
                    {
                        ["Version"] = "9.2:" + savedStamp,
                        ["QueryVersion"] = 2,
                        ["Entity"] = new JObject { ["LogicalName"] = "account", ["ObjectTypeCode"] = 1 }
                    }.ToString());
                }

                var service = new MetadataService { VersionStamp = currentStamp, AllowEntityLoads = reload };
                var cache = MetadataTestHelper.PersistentCache(path, rpc, service, orgId);
                Assert.AreEqual("account", cache["account"].LogicalName);
                Assert.AreEqual(reload ? 1 : 0, service.EntityLoads);
                var nextService = new MetadataService { VersionStamp = currentStamp, AllowEntityLoads = false };
                var nextCache = MetadataTestHelper.PersistentCache(path, rpc, nextService, orgId);
                Assert.AreEqual("account", nextCache["account"].LogicalName);
                Assert.AreEqual(0, nextService.EntityLoads);
            }
            finally
            {
                if (Directory.Exists(path))
                    Directory.Delete(path, true);
            }
        }

        [TestMethod]
        public void PreviousObjectTypeCodeEntryIsReloadedWithFullMetadata()
        {
            var path = Path.Combine(Path.GetTempPath(), nameof(LazyMetadataCacheTests), Guid.NewGuid().ToString("N"));
            var orgId = Guid.NewGuid();
            using var rpc = new JsonRpc(new HeaderDelimitedMessageHandler(Stream.Null, Stream.Null));
            try
            {
                var directory = Path.Combine(path, orgId + ".lazy");
                Directory.CreateDirectory(directory);
                var filename = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes("otc:1"))) + ".json.gz";
                using (var stream = File.Create(Path.Combine(directory, filename)))
                using (var gzip = new GZipStream(stream, CompressionLevel.Optimal))
                using (var writer = new StreamWriter(gzip))
                {
                    writer.Write(new JObject
                    {
                        ["Version"] = "9.2:stamp",
                        ["QueryVersion"] = 1,
                        ["Entity"] = new JObject { ["LogicalName"] = "account", ["ObjectTypeCode"] = 1 }
                    }.ToString());
                }

                var service = new MetadataService();
                var cache = MetadataTestHelper.PersistentCache(path, rpc, service, orgId);
                var metadata = cache[1];
                Assert.AreEqual(1, service.EntityLoads);
                Assert.AreEqual("name", metadata.Attributes.Single().LogicalName);
                Assert.AreEqual("contact", metadata.OneToManyRelationships.Single().ReferencingEntity);
                var nextService = new MetadataService { AllowEntityLoads = false };
                var nextCache = MetadataTestHelper.PersistentCache(path, rpc, nextService, orgId);
                Assert.AreEqual("name", nextCache[1].Attributes.Single().LogicalName);
                Assert.AreEqual(0, nextService.EntityLoads);
            }
            finally
            {
                if (Directory.Exists(path))
                    Directory.Delete(path, true);
            }
        }

        [DataTestMethod]
        [DataRow("name")]
        [DataRow("otc")]
        [DataRow("background")]
        [DataRow("minimal")]
        public void LoadedEntityIsPersistedAndReusedByNewCache(string lookup)
        {
            var path = Path.Combine(Path.GetTempPath(), nameof(LazyMetadataCacheTests), Guid.NewGuid().ToString("N"));
            var orgId = Guid.NewGuid();
            using var rpc = new JsonRpc(new HeaderDelimitedMessageHandler(Stream.Null, Stream.Null));
            try
            {
                var service = new MetadataService();
                var cache = MetadataTestHelper.PersistentCache(path, rpc, service, orgId);
                var entity = Load(cache, lookup);
                Assert.AreEqual(1, service.EntityLoads);
                Assert.AreEqual("account", entity.LogicalName);

                var key = lookup == "otc" ? "otc:1" : "name:account";
                var filename = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(key))) + ".json.gz";
                Assert.IsTrue(File.Exists(Path.Combine(path, orgId + ".lazy", filename)));
                Assert.AreEqual(0, Directory.GetFiles(path, "*.tmp", SearchOption.AllDirectories).Length);

                var nextService = new MetadataService { AllowEntityLoads = false };
                var nextCache = MetadataTestHelper.PersistentCache(path, rpc, nextService, orgId);
                var cached = Load(nextCache, lookup);
                Assert.AreEqual(0, nextService.EntityLoads);
                Assert.AreEqual("account", cached.LogicalName);
                Assert.AreEqual(1, cached.ObjectTypeCode);
                Assert.IsInstanceOfType<StringAttributeMetadata>(cached.Attributes.Single());
                Assert.AreEqual("name", cached.Attributes.Single().LogicalName);
                Assert.AreEqual("contact", cached.OneToManyRelationships.Single().ReferencingEntity);
            }
            finally
            {
                if (Directory.Exists(path))
                    Directory.Delete(path, true);
            }
        }

        private static EntityMetadata Load(IAttributeMetadataCache cache, string lookup)
        {
            if (lookup == "name")
                return cache["ACCOUNT"];
            if (lookup == "otc")
                return cache[1];

            EntityMetadata metadata = null;
            Assert.IsTrue(SpinWait.SpinUntil(() => lookup == "minimal"
                ? cache.TryGetMinimalData("account", out metadata)
                : cache.TryGetValue("account", out metadata), TimeSpan.FromSeconds(10)));
            return metadata;
        }

        private sealed class MetadataService : IOrganizationService
        {
            public int EntityLoads { get; private set; }
            public bool AllowEntityLoads { get; set; } = true;
            public string VersionStamp { get; set; } = "stamp";

            public OrganizationResponse Execute(OrganizationRequest request)
            {
                if (request is RetrieveMetadataChangesRequest changes &&
                    changes.Query.Properties.PropertyNames.SequenceEqual(new[] { nameof(EntityMetadata.MetadataId) }))
                {
                    var stamp = new RetrieveMetadataChangesResponse();
                    stamp.Results["ServerVersionStamp"] = VersionStamp;
                    return stamp;
                }

                if (!AllowEntityLoads)
                    throw new InvalidOperationException("The entity must be loaded from the persistent cache");
                EntityLoads++;

                var entity = new EntityMetadata { LogicalName = "account" };
                typeof(EntityMetadata).GetProperty(nameof(EntityMetadata.ObjectTypeCode)).SetValue(entity, 1);
                typeof(EntityMetadata).GetProperty(nameof(EntityMetadata.Attributes)).SetValue(entity,
                    new AttributeMetadata[] { new StringAttributeMetadata { LogicalName = "name" } });
                typeof(EntityMetadata).GetProperty(nameof(EntityMetadata.OneToManyRelationships)).SetValue(entity,
                    new[] { new OneToManyRelationshipMetadata { ReferencedEntity = "account", ReferencingEntity = "contact" } });

                if (request is RetrieveEntityRequest retrieve)
                {
                    Assert.AreEqual("account", retrieve.LogicalName);
                    var response = new RetrieveEntityResponse();
                    response.Results["EntityMetadata"] = entity;
                    return response;
                }

                if (request is RetrieveMetadataChangesRequest byCode)
                {
                    Assert.AreEqual(nameof(EntityMetadata.ObjectTypeCode), byCode.Query.Criteria.Conditions.Single().PropertyName);
                    Assert.IsTrue(byCode.Query.AttributeQuery.Properties.AllProperties);
                    Assert.IsTrue(byCode.Query.RelationshipQuery.Properties.AllProperties);
                    var response = new RetrieveMetadataChangesResponse();
                    response.Results["EntityMetadata"] = new EntityMetadataCollection { entity };
                    return response;
                }

                throw new NotSupportedException(request.RequestName);
            }

            public Guid Create(Entity entity) => throw new NotSupportedException();
            public void Update(Entity entity) => throw new NotSupportedException();
            public void Delete(string entityName, Guid id) => throw new NotSupportedException();
            public Entity Retrieve(string entityName, Guid id, ColumnSet columnSet) => throw new NotSupportedException();
            public EntityCollection RetrieveMultiple(QueryBase query) => throw new NotSupportedException();
            public void Associate(string entityName, Guid entityId, Relationship relationship, EntityReferenceCollection relatedEntities) => throw new NotSupportedException();
            public void Disassociate(string entityName, Guid entityId, Relationship relationship, EntityReferenceCollection relatedEntities) => throw new NotSupportedException();
        }
    }
}
