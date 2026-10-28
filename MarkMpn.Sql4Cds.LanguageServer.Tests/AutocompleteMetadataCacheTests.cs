using System;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.ServiceModel;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.Engine;
using MarkMpn.Sql4Cds.LanguageServer.Connection;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Metadata;
using Microsoft.Xrm.Sdk.Metadata.Query;
using Microsoft.Xrm.Sdk.Query;
using StreamJsonRpc;

namespace MarkMpn.Sql4Cds.LanguageServer.Tests
{
    [TestClass]
    public class AutocompleteMetadataCacheTests
    {
        private string _path;
        private Guid _orgId;
        private JsonRpc _rpc;

        [TestInitialize]
        public void Initialize()
        {
            _path = Path.Combine(Path.GetTempPath(), nameof(AutocompleteMetadataCacheTests), Guid.NewGuid().ToString("N"));
            _orgId = Guid.NewGuid();
            _rpc = new JsonRpc(new HeaderDelimitedMessageHandler(Stream.Null, Stream.Null));
        }

        [TestCleanup]
        public void Cleanup()
        {
            _rpc.Dispose();
            if (Directory.Exists(_path))
                Directory.Delete(_path, true);
        }

        private IAttributeMetadataCache Cache(MetadataService service)
        {
            return MetadataTestHelper.PersistentCache(_path, _rpc, service, _orgId);
        }

        private string CacheFile(string key)
        {
            return Path.Combine(_path, _orgId + ".lazy",
                Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(key))) + ".json.gz");
        }

        private static EntityMetadata[] WaitForEntities(IAttributeMetadataCache cache)
        {
            EntityMetadata[] entities = null;
            Assert.IsTrue(SpinWait.SpinUntil(() =>
            {
                entities = cache.GetAllEntities().ToArray();
                return entities.Length > 0;
            }, TimeSpan.FromSeconds(10)), "Autocomplete metadata did not finish loading");
            return entities;
        }

        private static string[] WaitForRecycleBin(IAttributeMetadataCache cache)
        {
            string[] entities = null;
            Assert.IsTrue(SpinWait.SpinUntil(() =>
            {
                entities = cache.TryGetRecycleBinEntities();
                return entities == null || entities.Length > 0;
            }, TimeSpan.FromSeconds(10)), "Recycle-bin metadata did not finish loading");
            return entities;
        }

        [TestMethod]
        public void AutocompleteEntitiesArePersistedAndReusedWithoutLoadingFullEntities()
        {
            for (var run = 0; run < 2; run++)
            {
                var service = new MetadataService
                {
                    Stamp = run == 0 ? "2506169!10/16/2026 18:09:38" : "2506169!10/16/2026 19:09:38",
                    AllowDownloads = run == 0
                };
                var cache = Cache(service);
                var metadata = (run == 0 ? WaitForEntities(cache) : cache.GetAllEntities().ToArray()).Single();
                Assert.AreEqual("account", metadata.LogicalName);
                Assert.AreEqual("Account", metadata.DisplayName.LocalizedLabels.Single().Label);
                Assert.AreEqual("Accounts description", metadata.Description.LocalizedLabels.Single().Label);
                Assert.AreEqual(1, metadata.ObjectTypeCode);
                Assert.AreEqual(MetadataService.ProviderId, metadata.DataProviderId);
                Assert.IsTrue(metadata.IsArchivalEnabled);
                Assert.IsFalse(metadata.IsRetentionEnabled);
                Assert.IsNull(metadata.Attributes);
                Assert.AreEqual(run == 0 ? 1 : 0, service.EntityListLoads);
                Assert.AreEqual(0, service.FullEntityLoads);
                Assert.AreEqual(1, service.StampLoads);
                Assert.IsTrue(File.Exists(CacheFile("autocomplete:entities")));
            }
        }

        private static Task EnsureAllEntitiesLoadedAsync(IAttributeMetadataCache cache)
        {
            return ((CachedMetadata)cache).EnsureAllEntitiesLoadedAsync();
        }

        [TestMethod]
        public async Task ObjectExplorerWaitsForSharedEntityListWithoutDuplicatingDownloads()
        {
            using var release = new ManualResetEventSlim();
            using var started = new ManualResetEventSlim();
            var service = new MetadataService { Release = release, ListStarted = started };
            var cache = Cache(service);
            cache.GetAllEntities();
            Assert.IsTrue(started.Wait(TimeSpan.FromSeconds(10)));
            var first = EnsureAllEntitiesLoadedAsync(cache);
            var second = EnsureAllEntitiesLoadedAsync(cache);
            try
            {
                Assert.IsFalse(first.IsCompleted);
                Assert.IsFalse(second.IsCompleted);
            }
            finally
            {
                release.Set();
            }
            await Task.WhenAll(first, second);
            Assert.AreEqual("account", cache.GetAllEntities().Single().LogicalName);
            Assert.AreEqual(1, service.EntityListLoads);
            Assert.AreEqual(0, service.FullEntityLoads);
            var diskService = new MetadataService { AllowDownloads = false };
            var diskCache = Cache(diskService);
            await EnsureAllEntitiesLoadedAsync(diskCache);
            Assert.IsTrue(diskCache.GetAllEntities().Single().IsArchivalEnabled);
            Assert.AreEqual(0, diskService.EntityListLoads);
        }

        [TestMethod]
        public async Task ObjectExplorerPreservesEmptyEntityListsAndReportsLoadFailures()
        {
            var empty = new MetadataService { EmptyEntityList = true };
            var emptyCache = Cache(empty);
            await EnsureAllEntitiesLoadedAsync(emptyCache);
            Assert.AreEqual(0, emptyCache.GetAllEntities().Count());
            await EnsureAllEntitiesLoadedAsync(emptyCache);
            Assert.AreEqual(1, empty.EntityListLoads);

            var failure = new MetadataService { AllowDownloads = false, Stamp = "changed!1" };
            await Assert.ThrowsExceptionAsync<InvalidOperationException>(() => EnsureAllEntitiesLoadedAsync(Cache(failure)));
        }

        [DataTestMethod]
        [DataRow(false)]
        [DataRow(true)]
        public void RecycleBinApisSharePersistedResults(bool asynchronousFirst)
        {
            for (var run = 0; run < 2; run++)
            {
                var service = new MetadataService { AllowDownloads = run == 0 };
                var cache = Cache(service);
                var entities = asynchronousFirst
                    ? (run == 0 ? WaitForRecycleBin(cache) : cache.TryGetRecycleBinEntities())
                    : cache.RecycleBinEntities;
                CollectionAssert.AreEqual(new[] { "account" }, entities);
                CollectionAssert.AreEqual(entities, cache.RecycleBinEntities);
                CollectionAssert.AreEqual(entities, cache.TryGetRecycleBinEntities());
                Assert.AreEqual(run == 0 ? 1 : 0, service.RecycleBinLoads);
                Assert.AreEqual(run == 0 ? 1 : 0, service.FullEntityLoads);
                Assert.AreEqual(1, service.StampLoads);
                Assert.IsTrue(File.Exists(CacheFile("autocomplete:recyclebin")));
            }
        }

        [DataTestMethod]
        [DataRow(false)]
        [DataRow(true)]
        public void EmptyAndUnavailableRecycleBinResultsArePersisted(bool unavailable)
        {
            for (var run = 0; run < 2; run++)
            {
                var service = new MetadataService
                {
                    AllowDownloads = run == 0,
                    EmptyRecycleBin = true,
                    RecycleBinUnavailable = unavailable
                };
                var cache = Cache(service);
                if (run != 0)
                {
                    var fromDisk = cache.TryGetRecycleBinEntities();
                    if (unavailable)
                        Assert.IsNull(fromDisk);
                    else
                        Assert.AreEqual(0, fromDisk.Length);
                    Assert.AreEqual(1, service.StampLoads);
                }
                if (unavailable)
                    Assert.IsNull(cache.RecycleBinEntities);
                else
                    Assert.AreEqual(0, cache.RecycleBinEntities.Length);
                Assert.AreSame(cache.RecycleBinEntities, cache.TryGetRecycleBinEntities());
                Assert.AreEqual(run == 0 && !unavailable ? 1 : 0, service.RecycleBinLoads);
                Assert.AreEqual(run == 0 ? 1 : 0, service.FullEntityLoads);
            }
        }

        [TestMethod]
        public void MinimalDataIsPersistedAndReused()
        {
            for (var run = 0; run < 2; run++)
            {
                var service = new MetadataService { AllowDownloads = run == 0 };
                var cache = Cache(service);
                EntityMetadata metadata = null;
                if (run == 0)
                    Assert.IsTrue(SpinWait.SpinUntil(() => cache.TryGetMinimalData("account", out metadata), TimeSpan.FromSeconds(10)));
                else
                    Assert.IsTrue(cache.TryGetMinimalData("account", out metadata));
                Assert.AreEqual("Account", metadata.DisplayName.LocalizedLabels.Single().Label);
                Assert.AreEqual("name", metadata.Attributes.Single().LogicalName);
                Assert.AreEqual(run == 0 ? 1 : 0, service.FullEntityLoads);
            }
        }

        [TestMethod]
        public void MetadataChangesInvalidateAutocompleteAndRecycleBinEntries()
        {
            var original = new MetadataService { Stamp = "1!first" };
            var cache = Cache(original);
            WaitForEntities(cache);
            _ = cache.RecycleBinEntities;

            var changed = new MetadataService { Stamp = "2!second" };
            var nextCache = Cache(changed);
            WaitForEntities(nextCache);
            _ = nextCache.RecycleBinEntities;
            Assert.AreEqual(1, changed.EntityListLoads);
            Assert.AreEqual(1, changed.RecycleBinLoads);
            Assert.AreEqual(1, changed.StampLoads);
        }

        [TestMethod]
        public void DiskMissDoesNotWaitForVersionStamp()
        {
            using var release = new ManualResetEventSlim();
            using var started = new ManualResetEventSlim();
            var service = new MetadataService { StampRelease = release, StampStarted = started };
            var cache = Cache(service);
            try
            {
                Assert.AreEqual(0, cache.GetAllEntities().Count());
                Assert.IsTrue(started.Wait(TimeSpan.FromSeconds(5)));
                Assert.AreEqual(0, cache.TryGetRecycleBinEntities().Length);
                Assert.IsFalse(cache.TryGetMinimalData("account", out var metadata));
                Assert.IsNull(metadata);
                Assert.AreEqual(0, service.FullEntityLoads);
                Assert.AreEqual(0, service.EntityListLoads);
                Assert.AreEqual(0, service.RecycleBinLoads);
            }
            finally
            {
                release.Set();
                WaitForEntities(cache);
                WaitForRecycleBin(cache);
                Assert.IsTrue(SpinWait.SpinUntil(() => cache.TryGetMinimalData("account", out _), TimeSpan.FromSeconds(10)));
            }
        }

        [TestMethod]
        public void StaleMinimalDataStartsBackgroundRefresh()
        {
            var cache = Cache(new MetadataService { Stamp = "1!first" });
            Assert.IsTrue(SpinWait.SpinUntil(() => cache.TryGetMinimalData("account", out _), TimeSpan.FromSeconds(10)));
            var changed = new MetadataService { Stamp = "2!second" };
            var nextCache = Cache(changed);
            Assert.IsFalse(nextCache.TryGetMinimalData("account", out _));
            Assert.AreEqual(1, changed.StampLoads);
            Assert.IsTrue(SpinWait.SpinUntil(() => nextCache.TryGetMinimalData("account", out _), TimeSpan.FromSeconds(10)));
            Assert.AreEqual(1, changed.FullEntityLoads);
        }

        [TestMethod]
        public void CorruptAutocompleteEntriesAreReplaced()
        {
            var cache = Cache(new MetadataService());
            WaitForEntities(cache);
            _ = cache.RecycleBinEntities;
            File.WriteAllText(CacheFile("autocomplete:entities"), "corrupt");
            File.WriteAllText(CacheFile("autocomplete:recyclebin"), "corrupt");

            var service = new MetadataService();
            var nextCache = Cache(service);
            WaitForEntities(nextCache);
            _ = nextCache.RecycleBinEntities;
            Assert.AreEqual(1, service.EntityListLoads);
            Assert.AreEqual(1, service.RecycleBinLoads);
            var persisted = Cache(new MetadataService { AllowDownloads = false });
            WaitForEntities(persisted);
            CollectionAssert.AreEqual(new[] { "account" }, persisted.RecycleBinEntities);
        }

        [TestMethod]
        public void CacheWriteFailuresDoNotDiscardDownloadedResults()
        {
            Directory.CreateDirectory(_path);
            File.WriteAllText(Path.Combine(_path, _orgId + ".lazy"), "not a directory");
            var service = new MetadataService();
            var cache = Cache(service);
            Assert.AreEqual("account", WaitForEntities(cache).Single().LogicalName);
            CollectionAssert.AreEqual(new[] { "account" }, cache.RecycleBinEntities);
            Assert.AreEqual(1, service.EntityListLoads);
            Assert.AreEqual(1, service.RecycleBinLoads);
            Assert.IsFalse(File.Exists(CacheFile("autocomplete:entities")));
            Assert.IsFalse(File.Exists(CacheFile("autocomplete:recyclebin")));
        }

        [TestMethod]
        public void BackgroundPollingDoesNotBlockOrStartDuplicateLoads()
        {
            using var release = new ManualResetEventSlim();
            using var listStarted = new ManualResetEventSlim();
            using var recycleStarted = new ManualResetEventSlim();
            var service = new MetadataService { Release = release, ListStarted = listStarted, RecycleStarted = recycleStarted };
            var cache = Cache(service);
            Task<string[]> synchronous = null;
            try
            {
                Parallel.For(0, 100, i =>
                {
                    Assert.AreEqual(0, cache.GetAllEntities().Count());
                    Assert.AreEqual(0, cache.TryGetRecycleBinEntities().Length);
                });
                Assert.IsTrue(listStarted.Wait(TimeSpan.FromSeconds(5)));
                Assert.IsTrue(recycleStarted.Wait(TimeSpan.FromSeconds(5)));
                synchronous = Task.Run(() => cache.RecycleBinEntities);
                Assert.AreEqual(1, service.EntityListLoads);
                Assert.AreEqual(1, service.RecycleBinLoads);
            }
            finally
            {
                release.Set();
                synchronous?.GetAwaiter().GetResult();
                WaitForEntities(cache);
                WaitForRecycleBin(cache);
            }
            Assert.AreEqual(1, service.EntityListLoads);
            Assert.AreEqual(1, service.RecycleBinLoads);
            Assert.AreEqual(1, service.StampLoads);
        }

        [TestMethod]
        public void FailedAutocompleteDownloadIsRetriedAndNotPersisted()
        {
            var service = new MetadataService { FailEntityListOnce = true };
            var cache = Cache(service);
            WaitForEntities(cache);
            Assert.AreEqual(2, service.EntityListLoads);
            WaitForEntities(Cache(new MetadataService { AllowDownloads = false }));
        }

        [DataTestMethod]
        [DataRow(false)]
        [DataRow(true)]
        public void FailedRecycleBinDownloadIsNotPersistedOrReturnedAsSuccess(bool metadataFailure)
        {
            var service = new MetadataService { FailRecycleBin = !metadataFailure, FailRecycleBinMetadata = metadataFailure };
            var cache = Cache(service);
            if (metadataFailure)
                Assert.ThrowsException<FaultException<OrganizationServiceFault>>(() => _ = cache.RecycleBinEntities);
            else
                Assert.ThrowsException<InvalidOperationException>(() => _ = cache.RecycleBinEntities);
            Assert.IsTrue(SpinWait.SpinUntil(() =>
            {
                try
                {
                    cache.TryGetRecycleBinEntities();
                    return false;
                }
                catch (InvalidOperationException)
                {
                    return true;
                }
                catch (FaultException<OrganizationServiceFault>)
                {
                    return true;
                }
            }, TimeSpan.FromSeconds(10)));
            Assert.IsFalse(File.Exists(CacheFile("autocomplete:recyclebin")));
            CollectionAssert.AreEqual(new[] { "account" }, Cache(new MetadataService()).RecycleBinEntities);
        }

        private sealed class MetadataService : IOrganizationService
        {
            public static readonly Guid ProviderId = Guid.NewGuid();
            public string Stamp = "2506169!10/16/2026 18:09:38";
            public bool AllowDownloads = true;
            public bool EmptyRecycleBin;
            public bool EmptyEntityList;
            public bool RecycleBinUnavailable;
            public bool FailEntityListOnce;
            public bool FailRecycleBin;
            public bool FailRecycleBinMetadata;
            public int EntityListLoads;
            public int RecycleBinLoads;
            public int FullEntityLoads;
            public int StampLoads;
            public ManualResetEventSlim Release;
            public ManualResetEventSlim ListStarted;
            public ManualResetEventSlim RecycleStarted;
            public ManualResetEventSlim StampRelease;
            public ManualResetEventSlim StampStarted;

            private static EntityMetadata Entity(bool full)
            {
                var entity = new EntityMetadata
                {
                    LogicalName = "account",
                    DisplayName = new Label("Account", 1033),
                    Description = new Label("Accounts description", 1033),
                    IsArchivalEnabled = true,
                    IsRetentionEnabled = false
                };
                typeof(EntityMetadata).GetProperty(nameof(EntityMetadata.ObjectTypeCode)).SetValue(entity, 1);
                typeof(EntityMetadata).GetProperty(nameof(EntityMetadata.DataProviderId)).SetValue(entity, ProviderId);
                if (full)
                    typeof(EntityMetadata).GetProperty(nameof(EntityMetadata.Attributes)).SetValue(entity,
                        new AttributeMetadata[] { new StringAttributeMetadata { LogicalName = "name" } });
                return entity;
            }

            public OrganizationResponse Execute(OrganizationRequest request)
            {
                if (request is RetrieveMetadataChangesRequest changes &&
                    changes.Query.Properties.PropertyNames.SequenceEqual(new[] { nameof(EntityMetadata.MetadataId) }))
                {
                    Interlocked.Increment(ref StampLoads);
                    StampStarted?.Set();
                    if (StampRelease != null && !StampRelease.Wait(TimeSpan.FromSeconds(10)))
                        throw new TimeoutException();
                    var response = new RetrieveMetadataChangesResponse();
                    response.Results["ServerVersionStamp"] = Stamp;
                    return response;
                }

                if (request is RetrieveEntityRequest retrieve)
                {
                    Interlocked.Increment(ref FullEntityLoads);
                    if (!AllowDownloads)
                        throw new InvalidOperationException("Entity metadata should have been read from disk");
                    if (retrieve.LogicalName == "recyclebinconfig" && FailRecycleBinMetadata)
                        throw new FaultException<OrganizationServiceFault>(new OrganizationServiceFault
                        {
                            ErrorCode = unchecked((int)0x80040220),
                            Message = "Metadata access denied"
                        });
                    if (retrieve.LogicalName == "recyclebinconfig" && RecycleBinUnavailable)
                        throw new FaultException<OrganizationServiceFault>(new OrganizationServiceFault
                        {
                            ErrorCode = unchecked((int)0x80040217),
                            Message = "Unknown entity recyclebinconfig"
                        });
                    var response = new RetrieveEntityResponse();
                    response.Results["EntityMetadata"] = Entity(true);
                    return response;
                }

                if (request is RetrieveMetadataChangesRequest list)
                {
                    var calls = Interlocked.Increment(ref EntityListLoads);
                    ListStarted?.Set();
                    if (Release != null && !Release.Wait(TimeSpan.FromSeconds(10)))
                        throw new TimeoutException();
                    if (!AllowDownloads || (FailEntityListOnce && calls == 1))
                        throw new InvalidOperationException("Entity list download is unavailable");
                    Assert.IsFalse(list.Query.Properties.AllProperties);
                    Assert.IsNull(list.Query.AttributeQuery);
                    Assert.IsNull(list.Query.RelationshipQuery);
                    CollectionAssert.Contains(list.Query.Properties.PropertyNames.ToArray(), nameof(EntityMetadata.DataProviderId));
                    CollectionAssert.Contains(list.Query.Properties.PropertyNames.ToArray(), nameof(EntityMetadata.IsArchivalEnabled));
                    CollectionAssert.Contains(list.Query.Properties.PropertyNames.ToArray(), nameof(EntityMetadata.IsRetentionEnabled));
                    var response = new RetrieveMetadataChangesResponse();
                    response.Results["EntityMetadata"] = EmptyEntityList
                        ? new EntityMetadataCollection()
                        : new EntityMetadataCollection { Entity(false) };
                    return response;
                }

                throw new NotSupportedException(request.RequestName);
            }

            public EntityCollection RetrieveMultiple(QueryBase query)
            {
                Interlocked.Increment(ref RecycleBinLoads);
                RecycleStarted?.Set();
                if (Release != null && !Release.Wait(TimeSpan.FromSeconds(10)))
                    throw new TimeoutException();
                if (!AllowDownloads || FailRecycleBin)
                    throw new InvalidOperationException("Recycle-bin download is unavailable");
                Assert.IsInstanceOfType<FetchExpression>(query);
                var results = new EntityCollection();
                if (!EmptyRecycleBin)
                    results.Entities.Add(new Entity("recyclebinconfig")
                    {
                        ["entity.logicalname"] = new AliasedValue("entity", "logicalname", "account")
                    });
                return results;
            }

            public Guid Create(Entity entity) => throw new NotSupportedException();
            public void Update(Entity entity) => throw new NotSupportedException();
            public void Delete(string entityName, Guid id) => throw new NotSupportedException();
            public Entity Retrieve(string entityName, Guid id, ColumnSet columnSet) => throw new NotSupportedException();
            public void Associate(string entityName, Guid entityId, Relationship relationship, EntityReferenceCollection relatedEntities) => throw new NotSupportedException();
            public void Disassociate(string entityName, Guid entityId, Relationship relationship, EntityReferenceCollection relatedEntities) => throw new NotSupportedException();
        }
    }
}
