// Deliberately share completion signals to control cross-thread authentication ordering.
#pragma warning disable VSTHRD003
using System.Collections.Concurrent;
using System.Reflection;
using MarkMpn.Sql4Cds.LanguageServer.Connection;
using MarkMpn.Sql4Cds.LanguageServer.Connection.Contracts;
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
var identity = typeof(ConnectParams).Assembly.GetType("MarkMpn.Sql4Cds.LanguageServer.Connection.ConnectionManager")!
    .GetMethod("GetConnectionIdentity", BindingFlags.Static | BindingFlags.NonPublic)!;
string Fingerprint(Dictionary<string, object> options) => (string)identity.Invoke(null, new[] { new ConnectionDetails { Options = options } });
var first = new Dictionary<string, object> { ["connectionId"] = "profile-a", ["connectionName"] = "Shared name", ["clientsecret"] = "secret" };
var hash = Fingerprint(first);
Assert(hash == Fingerprint(first.Reverse().ToDictionary(pair => pair.Key, pair => pair.Value)), "Identity depends on option order");
Assert(hash != Fingerprint(new(first) { ["connectionId"] = "profile-b" }), "Distinct profiles reuse a cache identity");
Assert(hash != Fingerprint(new(first) { ["clientsecret"] = "changed" }), "Changed credentials reuse a cache identity");
Assert(Fingerprint(new() { ["connectionName"] = "Legacy ADS" }) == null, "Legacy ADS cache behavior changed");
Console.WriteLine("PASS: profile cache identity separates credentials and preserves legacy ADS compatibility");

static ConnectParams Request(string id) => new() { OwnerUri = "editor", RequestId = id, Connection = new() { Options = new() { ["name"] = id } } };
static void Assert(bool condition, string message) { if (!condition) throw new Exception(message); }
static async Task CheckAsync(string name, Func<ConnectionHandler, ConnectionManager, Task> test)
{
    using var rpc = new JsonRpc(Stream.Null, Stream.Null);
    var manager = new ConnectionManager();
    await test(new ConnectionHandler(rpc, manager), manager).WaitAsync(TimeSpan.FromSeconds(10));
    Console.WriteLine("PASS: " + name);
}

namespace MarkMpn.Sql4Cds.LanguageServer.Connection
{
    // Only authentication and connection storage are replaced. ConnectionHandler is
    // linked directly from production so these tests control real scheduling races.
    class ConnectionManager
    {
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
        public TestSession Connect(ConnectionDetails details, string owner)
        {
            var id = (string)details.Options["name"];
            _owners[owner] = id;
            Get(id).Started.SetResult();
            if (!Get(id).Release.Wait(TimeSpan.FromSeconds(10))) throw new TimeoutException("Authentication was never released");
            return new() { SessionId = owner, DataSource = new() { Name = id } };
        }
        public TestSession AssociateConnection(string pending, string owner)
        {
            Attached = _owners[pending];
            return new() { SessionId = owner, DataSource = new() { Name = Attached } };
        }
        public void Disconnect(string owner)
        {
            if (owner == "editor") Attached = null;
            if (_owners.TryRemove(owner, out var id)) Get(id).Finished.TrySetResult();
        }
    }
    class TestSession { public string SessionId { get; set; } public TestDataSource DataSource { get; set; } }
    class TestDataSource
    {
        public string Name { get; set; }
        public string ServerName => "example.crm.dynamics.com";
        public string Version => "9.2";
        public string Username => "Test user";
    }
}
