// Deliberately share completion signals to control cross-thread authentication ordering.
#pragma warning disable VSTHRD003
using System.Collections.Concurrent;
using MarkMpn.Sql4Cds.LanguageServer.Configuration;
using MarkMpn.Sql4Cds.LanguageServer.Connection;
using MarkMpn.Sql4Cds.LanguageServer.Connection.Contracts;
using Microsoft.Crm.Sdk.Messages;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Query;
using Microsoft.VisualStudio.LanguageServer.Protocol;
using Newtonsoft.Json.Linq;
using StreamJsonRpc;

await CheckAsync("a late authentication cannot replace the retry's environment", async (handler, manager) =>
{
    var a = Request("A"); var b = Request("B");
    handler.HandleConnection(a); await manager.StartedAsync("A");
    handler.HandleConnection(b); await manager.StartedAsync("B");
    manager.Release("B"); await manager.FinishedAsync("B");
    manager.Release("A"); await manager.FinishedAsync("A");
    Assert(manager.Attached == "B", "The superseded environment replaced B");
});
await CheckAsync("cancellation prevents a pending authentication from attaching", async (handler, manager) =>
{
    handler.HandleConnection(Request("A")); await manager.StartedAsync("A");
    handler.HandleCancelConnect(new CancelConnectParams { OwnerUri = "editor", RequestId = "A" });
    manager.Release("A"); await manager.FinishedAsync("A");
    Assert(manager.Attached == null, "A cancelled attempt attached");
});
await CheckAsync("a late cancellation cannot cancel the replacement request", async (handler, manager) =>
{
    handler.HandleConnection(Request("A")); await manager.StartedAsync("A");
    handler.HandleConnection(Request("B")); await manager.StartedAsync("B");
    handler.HandleCancelConnect(new CancelConnectParams { OwnerUri = "editor", RequestId = "A" });
    manager.Release("A"); await manager.FinishedAsync("A");
    manager.Release("B"); await manager.FinishedAsync("B");
    Assert(manager.Attached == "B", "Cancelling A also cancelled B");
});
await CheckAsync("disconnect prevents a pending authentication from attaching", async (handler, manager) =>
{
    handler.HandleConnection(Request("A")); await manager.StartedAsync("A");
    handler.HandleDisconnect(new DisconnectParams { OwnerUri = "editor" });
    manager.Release("A"); await manager.FinishedAsync("A");
    Assert(manager.Attached == null, "Disconnect left a late attached connection");
});

// Exercise the actual cache identity policy without authenticating to Dataverse.
string Fingerprint(Dictionary<string, object> options) => ConnectionManager.GetConnectionIdentity(new ConnectionDetails { Options = options });
var first = new Dictionary<string, object> { ["connectionId"] = "profile-a", ["connectionName"] = "Shared name", ["clientsecret"] = "secret" };
var hash = Fingerprint(first);
Assert(hash == Fingerprint(first.Reverse().ToDictionary(pair => pair.Key, pair => pair.Value)), "Identity depends on option order");
Assert(hash != Fingerprint(new(first) { ["connectionId"] = "profile-b" }), "Distinct profiles reuse a cache identity");
Assert(hash != Fingerprint(new(first) { ["clientsecret"] = "changed" }), "Changed credentials reuse a cache identity");
Assert(Fingerprint(new() { ["connectionName"] = "Legacy ADS" }) == null, "Legacy ADS cache behavior changed");
Console.WriteLine("PASS: profile cache identity separates credentials and preserves legacy ADS compatibility");

var connections = new ConnectionManager(null, null, null);
connections._dataSources["test"] = new DataSourceWithInfo(new TestOrganizationService(), "https://example.crm.dynamics.com", null) { Name = "test" };
foreach (var owner in new[] { "first", "second", "third" }) connections._connectedDataSource[owner] = "test";
using var firstConnection = connections.GetConnection("first").Connection;
using var secondConnection = connections.GetConnection("second").Connection;
Assert(!firstConnection.IncludeDetailedErrorTelemetry && !secondConnection.IncludeDetailedErrorTelemetry,
    "Detailed error telemetry must be disabled before configuration arrives");
var configuration = new ConfigurationHandler(connections);
void ConfigureTelemetry(JObject settings) => configuration.HandleDidChangeConfiguration(new DidChangeConfigurationParams
{
    Settings = new JObject { ["SQL4CDS"] = settings }
});
ConfigureTelemetry(new JObject { ["includeDetailedErrorTelemetry"] = true });
Assert(firstConnection.IncludeDetailedErrorTelemetry && secondConnection.IncludeDetailedErrorTelemetry,
    "Enabling detailed error telemetry did not update existing connections");
using var thirdConnection = connections.GetConnection("third").Connection;
Assert(thirdConnection.IncludeDetailedErrorTelemetry, "New connections did not inherit detailed error telemetry");
connections.AssociateConnection("third", "associated");
Assert(ReferenceEquals(thirdConnection, connections.GetConnection("associated").Connection),
    "Associating a connection replaced it");
ConfigureTelemetry(new JObject { ["includeDetailedErrorTelemetry"] = false });
Assert(!firstConnection.IncludeDetailedErrorTelemetry && !secondConnection.IncludeDetailedErrorTelemetry &&
    !thirdConnection.IncludeDetailedErrorTelemetry, "Disabling detailed error telemetry did not update existing connections");
ConfigureTelemetry(new JObject { ["includeDetailedErrorTelemetry"] = true });
ConfigureTelemetry(new JObject());
Assert(!firstConnection.IncludeDetailedErrorTelemetry && !thirdConnection.IncludeDetailedErrorTelemetry,
    "Missing configuration must disable detailed error telemetry");
Console.WriteLine("PASS: detailed error telemetry defaults off and configuration updates existing, new and associated connections");

static ConnectParams Request(string id) => new() { OwnerUri = "editor", RequestId = id, Connection = new() { Options = new() { ["name"] = id } } };
static void Assert(bool condition, string message) { if (!condition) throw new Exception(message); }
static async Task CheckAsync(string name, Func<ConnectionHandler, TestConnectionManager, Task> test)
{
    using var rpc = new JsonRpc(Stream.Null, Stream.Null);
    var manager = new TestConnectionManager();
    await test(new ConnectionHandler(rpc, manager), manager).WaitAsync(TimeSpan.FromSeconds(10));
    Console.WriteLine("PASS: " + name);
}

namespace MarkMpn.Sql4Cds.LanguageServer.Connection
{
    // Replace authentication and connection storage, but exercise the compiled handler.
    class TestConnectionManager : ConnectionManager
    {
        public TestConnectionManager() : base(null, null, null) { }

        private sealed class Attempt
        {
            public readonly TaskCompletionSource Started = new(TaskCreationOptions.RunContinuationsAsynchronously);
            public readonly TaskCompletionSource Finished = new(TaskCreationOptions.RunContinuationsAsynchronously);
            public readonly ManualResetEventSlim Release = new();
        }
        private readonly ConcurrentDictionary<string, Attempt> _attempts = new();
        private readonly ConcurrentDictionary<string, string> _owners = new();
        public string Attached { get; private set; }
        private Attempt Get(string id) => _attempts.GetOrAdd(id, _ => new());
        public Task StartedAsync(string id) => Get(id).Started.Task;
        public Task FinishedAsync(string id) => Get(id).Finished.Task;
        public void Release(string id) => Get(id).Release.Set();
        public override Session Connect(ConnectionDetails details, string owner)
        {
            var id = (string)details.Options["name"];
            _owners[owner] = id;
            Get(id).Started.SetResult();
            if (!Get(id).Release.Wait(TimeSpan.FromSeconds(10))) throw new TimeoutException("Authentication was never released");
            return CreateSession(owner, id);
        }
        public override Session AssociateConnection(string pending, string owner)
        {
            Attached = _owners[pending];
            return CreateSession(owner, Attached);
        }
        public override void Disconnect(string owner)
        {
            if (owner == "editor") Attached = null;
            if (_owners.TryRemove(owner, out var id)) Get(id).Finished.TrySetResult();
        }

        private static Session CreateSession(string owner, string name) => new()
        {
            SessionId = owner,
            DataSource = new DataSourceWithInfo(new TestOrganizationService(), "https://example.crm.dynamics.com", null) { Name = name }
        };
    }

    class TestOrganizationService : IOrganizationService
    {
        public EntityCollection RetrieveMultiple(QueryBase query)
        {
            if (query is not QueryExpression { EntityName: "organization" })
                throw new NotSupportedException("Unexpected query: " + query);
            return new EntityCollection(new[] { new Entity("organization") { ["name"] = "Test organization" } });
        }

        public OrganizationResponse Execute(OrganizationRequest request)
        {
            switch (request)
            {
                case RetrieveVersionRequest:
                    return new RetrieveVersionResponse { Results = { ["Version"] = "9.2" } };
                case WhoAmIRequest:
                    return new WhoAmIResponse { Results = { ["OrganizationId"] = Guid.Empty } };
                case RetrieveMultipleRequest { Query: FetchExpression }:
                    return new RetrieveMultipleResponse
                    {
                        Results =
                        {
                            ["EntityCollection"] = new EntityCollection(new[]
                            {
                                new Entity("systemuser")
                                {
                                    ["domainname"] = "Test user",
                                    ["organization.localeid"] = new AliasedValue("organization", "localeid", 1033)
                                }
                            })
                        }
                    };
                default:
                    throw new NotSupportedException("Unexpected request: " + request.RequestName);
            }
        }

        public Guid Create(Entity entity) => throw new NotSupportedException();
        public void Update(Entity entity) => throw new NotSupportedException();
        public void Delete(string entityName, Guid id) => throw new NotSupportedException();
        public Entity Retrieve(string entityName, Guid id, ColumnSet columnSet) => throw new NotSupportedException();
        public void Associate(string entityName, Guid entityId, Relationship relationship, EntityReferenceCollection relatedEntities) => throw new NotSupportedException();
        public void Disassociate(string entityName, Guid entityId, Relationship relationship, EntityReferenceCollection relatedEntities) => throw new NotSupportedException();
    }
}
