using System.Collections.Generic;
using System.Text.Json;
using System.Runtime.InteropServices.JavaScript;
using System.Runtime.Versioning;

namespace MarkMpn.Sql4Cds.Engine.Wasm;

[SupportedOSPlatform("browser")]
public static partial class Sql4CdsExports
{
    [JSExport]
    public static int CreateLocalSession() => Sql4CdsExportRuntime.CreateLocalSession();

    [JSExport]
    public static int CreateSession(string callbackSetId, string dataSourceName = "js") => Sql4CdsExportRuntime.CreateSession(callbackSetId, dataSourceName);

    [JSExport]
    public static void DisposeSession(int sessionId) => Sql4CdsExportRuntime.DisposeSession(sessionId);

    [JSExport]
    public static string Execute(int sessionId, string sql) => Sql4CdsExportRuntime.Execute(sessionId, sql);

    [JSExport]
    public static string Explain(int sessionId, string sql) => Sql4CdsExportRuntime.Explain(sessionId, sql);
}

internal static class Sql4CdsExportRuntime
{
    private static readonly JsonSerializerOptions JsonOptions = new JsonSerializerOptions(JsonSerializerDefaults.Web)
    {
        WriteIndented = true
    };

    private static readonly Dictionary<int, Sql4CdsWasmSession> Sessions = new Dictionary<int, Sql4CdsWasmSession>();
    private static int _nextSessionId = 1;

    internal static int CreateLocalSession()
    {
        var id = _nextSessionId++;
        Sessions[id] = Sql4CdsWasmSession.CreateLocal();
        return id;
    }

    internal static int CreateSession(string callbackSetId, string dataSourceName)
    {
        var id = _nextSessionId++;
        Sessions[id] = Sql4CdsWasmSession.CreateRemote(callbackSetId, dataSourceName);
        return id;
    }

    internal static void DisposeSession(int sessionId)
    {
        if (!Sessions.TryGetValue(sessionId, out var session))
            return;

        session.Dispose();
        Sessions.Remove(sessionId);
    }

    internal static string Execute(int sessionId, string sql)
    {
        return JsonSerializer.Serialize(GetSession(sessionId).Execute(sql), JsonOptions);
    }

    internal static string Explain(int sessionId, string sql)
    {
        return JsonSerializer.Serialize(GetSession(sessionId).Explain(sql), JsonOptions);
    }

    private static Sql4CdsWasmSession GetSession(int sessionId)
    {
        if (!Sessions.TryGetValue(sessionId, out var session))
            throw new KeyNotFoundException($"Unknown SQL 4 CDS session id {sessionId}");

        return session;
    }
}
