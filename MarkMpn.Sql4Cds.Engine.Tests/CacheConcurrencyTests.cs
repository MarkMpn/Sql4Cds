using System;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Metadata;
using Microsoft.Xrm.Sdk.Query;

namespace MarkMpn.Sql4Cds.Engine.Tests
{
    [TestClass]
    public class CacheConcurrencyTests
    {
        [TestMethod]
        public void ConcurrentMetadataPollingStartsOnlyOneLoad()
        {
            using (var service = new BlockingService())
            {
                var cache = new AttributeMetadataCache(service);
                var loads = 0;
                Task load = null;
                cache.MetadataLoading += (sender, args) =>
                {
                    Interlocked.Increment(ref loads);
                    load = args.Task;
                };

                try
                {
                    Parallel.For(0, 1000, i => cache.TryGetValue("account", out _));
                    Assert.IsTrue(service.Started.Wait(TimeSpan.FromSeconds(5)));
                    Assert.AreEqual(1, loads);
                    Assert.AreEqual(1, service.MetadataCalls);
                }
                finally
                {
                    service.Release.Set();
                    load?.GetAwaiter().GetResult();
                }

                Assert.IsTrue(cache.TryGetValue("account", out var entity));
                Assert.AreEqual("account", entity.LogicalName);
            }
        }

        [TestMethod]
        public void LazyMessagePollingSurfacesLoadFailure()
        {
            using (var service = new BlockingService { FailRetrieval = true })
            {
                service.Release.Set();
                var cache = new MessageCache(service, new AttributeMetadataCache(service));
                var reported = SpinWait.SpinUntil(() =>
                {
                    try
                    {
                        cache.GetAllMessages(true).ToArray();
                        return false;
                    }
                    catch (InvalidOperationException ex) when (ex.Message == "Retrieval failed")
                    {
                        return true;
                    }
                }, TimeSpan.FromSeconds(5));

                Assert.IsTrue(reported, "A failed background load must not keep returning an empty success result.");
                Assert.AreEqual(1, service.RetrievalCalls);
            }
        }

        [TestMethod]
        public void LazyRecycleBinPollingSurfacesLoadFailure()
        {
            using (var service = new BlockingService { FailRetrieval = true })
            {
                service.Release.Set();
                var cache = new AttributeMetadataCache(service);
                var reported = SpinWait.SpinUntil(() =>
                {
                    try
                    {
                        cache.TryGetRecycleBinEntities();
                        return false;
                    }
                    catch (InvalidOperationException ex) when (ex.Message == "Retrieval failed")
                    {
                        return true;
                    }
                }, TimeSpan.FromSeconds(5));

                Assert.IsTrue(reported, "A failed background load must not keep returning an empty success result.");
                Assert.AreEqual(1, service.RetrievalCalls);
            }
        }

        private sealed class BlockingService : IOrganizationService, IDisposable
        {
            public readonly ManualResetEventSlim Started = new ManualResetEventSlim();
            public readonly ManualResetEventSlim Release = new ManualResetEventSlim();
            public int MetadataCalls;
            public int RetrievalCalls;
            public bool FailRetrieval;

            public OrganizationResponse Execute(OrganizationRequest request)
            {
                Interlocked.Increment(ref MetadataCalls);
                Started.Set();
                if (!Release.Wait(TimeSpan.FromSeconds(10)))
                    throw new TimeoutException("Test metadata load was not released.");

                var entity = new EntityMetadata { LogicalName = ((RetrieveEntityRequest)request).LogicalName };
                typeof(EntityMetadata).GetProperty(nameof(EntityMetadata.Attributes)).SetValue(entity, Array.Empty<AttributeMetadata>());
                var response = new RetrieveEntityResponse();
                response.Results["EntityMetadata"] = entity;
                return response;
            }

            public EntityCollection RetrieveMultiple(QueryBase query)
            {
                Interlocked.Increment(ref RetrievalCalls);
                if (FailRetrieval)
                    throw new InvalidOperationException("Retrieval failed");
                return new EntityCollection();
            }

            public void Dispose()
            {
                Started.Dispose();
                Release.Dispose();
            }

            public Guid Create(Entity entity) => throw new NotSupportedException();
            public void Update(Entity entity) => throw new NotSupportedException();
            public void Delete(string entityName, Guid id) => throw new NotSupportedException();
            public Entity Retrieve(string entityName, Guid id, ColumnSet columnSet) => throw new NotSupportedException();
            public void Associate(string entityName, Guid id, Relationship relationship, EntityReferenceCollection relatedEntities) => throw new NotSupportedException();
            public void Disassociate(string entityName, Guid id, Relationship relationship, EntityReferenceCollection relatedEntities) => throw new NotSupportedException();
        }
    }
}
