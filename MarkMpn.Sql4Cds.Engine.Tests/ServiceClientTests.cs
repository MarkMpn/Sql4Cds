using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Query;

namespace MarkMpn.Sql4Cds.Engine.Tests
{
    [TestClass]
    public class ServiceClientTests : FakeXrmEasyTestsBase
    {
        [DataTestMethod]
        [DataRow(false)]
        [DataRow(true)]
        public void CustomClientImpersonationVerifiesCallerAndRestoresOnFailure(bool verificationFails)
        {
            var id = Guid.NewGuid();
            _context.Initialize(new[] { new Entity("systemuser", id) { ["domainname"] = "test-user" } });
            var service = new ParallelService(_service) { FailImpersonation = verificationFails, CallerId = Guid.NewGuid() };
            var previousCallerId = service.CallerId;
            _localDataSource.Connection = service;
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                cmd.CommandText = "EXECUTE AS LOGIN = 'test-user'";
                if (verificationFails)
                {
                    Assert.ThrowsException<Sql4CdsException>(() => cmd.ExecuteNonQuery());
                    Assert.AreEqual(previousCallerId, service.CallerId);
                    return;
                }
                cmd.ExecuteNonQuery();
                Assert.AreEqual(id, service.CallerId);
                cmd.CommandText = "SELECT CURRENT_USER";
                Assert.AreEqual(id, ((SqlEntityReference)cmd.ExecuteScalar()).Id);
                cmd.CommandText = "REVERT";
                cmd.ExecuteNonQuery();
                Assert.AreEqual(Guid.Empty, service.CallerId);
            }
        }

        [TestMethod]
        public void CustomClientParallelismRespectsSettingsAndHints()
        {
            var service = new ParallelService(_service);
            _localDataSource.Connection = service;
            var options = new OptionsWrapper(new StubOptions()) { MaxDegreeOfParallelism = 0 };
            var context = new NodeCompilationContext(new SessionContext(_localDataSources, options), options, null, null);

            Assert.IsTrue(ParallelismHelper.CanParallelise(service));
            Assert.IsFalse(ParallelismHelper.CanParallelise(_service));
            Assert.AreEqual(2, ParallelismHelper.GetMaxDOP(_localDataSource, context, null));
            options.MaxDegreeOfParallelism = 4;
            Assert.AreEqual(4, ParallelismHelper.GetMaxDOP(_localDataSource, context, null));

            var hint = new LiteralOptimizerHint
            {
                HintKind = OptimizerHintKind.MaxDop,
                Value = new IntegerLiteral { Value = "3" }
            };
            Assert.AreEqual(3, ParallelismHelper.GetMaxDOP(_localDataSource, context, new[] { hint }));
            hint.Value = new IntegerLiteral { Value = "0" };
            Assert.AreEqual(2, ParallelismHelper.GetMaxDOP(_localDataSource, context, new[] { hint }));
            service.RecommendedDegreesOfParallelism = 0;
            Assert.AreEqual(1, ParallelismHelper.GetMaxDOP(_localDataSource, context, new[] { hint }));
            service.CanParallelise = false;
            Assert.IsFalse(ParallelismHelper.CanParallelise(service));
        }

        [TestMethod]
        public void CustomClientCanClonePartitionExecutionContext()
        {
            var service = new ParallelService(_service) { CallerId = Guid.NewGuid() };
            _localDataSource.Connection = service;
            var options = new StubOptions();
            var context = new NodeExecutionContext(new SessionContext(_localDataSources, options), options, null, null, null);
            var cloned = context.CreateClonedSession("local");
            Assert.AreNotSame(service, cloned.Session.DataSources["local"].Connection);
            Assert.AreSame(_localDataSource.Metadata, cloned.Session.DataSources["local"].Metadata);
            Assert.AreEqual(1, service.State.Clones);
            Assert.AreSame(service, context.Session.DataSources["local"].Connection);
            var clonedClient = (IServiceClient)cloned.Session.DataSources["local"].Connection;
            Assert.AreEqual(service.CallerId, clonedClient.CallerId);
            clonedClient.CallerId = Guid.Empty;
            Assert.AreNotEqual(Guid.Empty, service.CallerId);
        }

        [TestMethod]
        public void CustomClientExecutesParallelDml()
        {
            var service = new ParallelService(_service);
            _localDataSource.Connection = service;
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                con.MaxDegreeOfParallelism = 0;
                con.BatchSize = 1;
                cmd.CommandText = "INSERT INTO account (name) VALUES " +
                    String.Join(", ", Enumerable.Range(0, 80).Select(i => "('record" + i + "')"));
                Assert.AreEqual(80, cmd.ExecuteNonQuery());
            }

            Assert.AreEqual(80, service.State.Executed);
            Assert.AreEqual(2, service.State.PeakConcurrency, "DML must use, but not exceed, the client's recommendation.");
            Assert.IsTrue(service.State.Clones >= 2, "Parallel workers must each obtain a cloned connection.");
            Assert.AreEqual(service.State.Clones, service.State.Disposed);
        }

        [DataTestMethod]
        [DataRow(false, 4)]
        [DataRow(true, 1)]
        public void SerialDmlDoesNotClone(bool canParallelise, int maxDop)
        {
            var service = new ParallelService(_service) { CanParallelise = canParallelise };
            _localDataSource.Connection = service;
            using (var con = new Sql4CdsConnection(_localDataSources))
            using (var cmd = con.CreateCommand())
            {
                con.MaxDegreeOfParallelism = maxDop;
                cmd.CommandText = "INSERT INTO account (name) VALUES ('record')";
                Assert.AreEqual(1, cmd.ExecuteNonQuery());
            }

            Assert.AreEqual(1, service.State.PeakConcurrency);
            Assert.AreEqual(0, service.State.Clones);
            Assert.AreEqual(0, service.State.Disposed);
        }

        private sealed class ParallelService : IOrganizationService, IServiceClient, IDisposable
        {
            public sealed class SharedState
            {
                public int Clones;
                public int Disposed;
                public int Executed;
                public int Active;
                public int PeakConcurrency;
            }

            private readonly IOrganizationService _inner;
            public readonly SharedState State = new SharedState();
            public ParallelService(IOrganizationService inner) => _inner = inner;
            public Version Version => new Version(9, 2);
            public string Name => "local";
            public bool CanParallelise { get; set; } = true;
            public bool UsesWebAPI => false;
            public Guid CallerId { get; set; }
            public bool FailImpersonation { get; set; }
            public int RecommendedDegreesOfParallelism { get; set; } = 2;

            public IOrganizationService Clone()
            {
                Interlocked.Increment(ref State.Clones);
                return (ParallelService)MemberwiseClone();
            }

            public OrganizationResponse Execute(OrganizationRequest request)
            {
                if (request is RetrieveMultipleRequest retrieve && retrieve.Query is Microsoft.Xrm.Sdk.Query.QueryExpression query &&
                    query.EntityName == "systemuser" && query.Criteria.Conditions.Any(c => c.Operator == ConditionOperator.EqualUserId))
                {
                    var response = new RetrieveMultipleResponse();
                    response.Results["EntityCollection"] = new EntityCollection(new[]
                    {
                        new Entity("systemuser", FailImpersonation ? Guid.NewGuid() : CallerId)
                    });
                    return response;
                }

                if (!(request is CreateRequest))
                    return _inner.Execute(request);

                var active = Interlocked.Increment(ref State.Active);
                lock (State)
                    State.PeakConcurrency = Math.Max(State.PeakConcurrency, active);
                try
                {
                    Thread.Sleep(60);
                    Interlocked.Increment(ref State.Executed);
                    var response = new CreateResponse();
                    response.Results["id"] = Guid.NewGuid();
                    return response;
                }
                finally
                {
                    Interlocked.Decrement(ref State.Active);
                }
            }

            public void Dispose() => Interlocked.Increment(ref State.Disposed);
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
