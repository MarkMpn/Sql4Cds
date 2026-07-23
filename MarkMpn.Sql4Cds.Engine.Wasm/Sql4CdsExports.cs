using System.Collections.Generic;
using System.Text.Json;
using System.Runtime.InteropServices.JavaScript;
using System.Runtime.Versioning;

namespace MarkMpn.Sql4Cds.Engine.Wasm;

[SupportedOSPlatform("browser")]
public static partial class Sql4CdsExports
{
    [JSExport]
    public static int CreateLocalSession() => ExportInvoker.Invoke<int>(nameof(CreateLocalSession));

    [JSExport]
    public static int CreateSession(string callbackSetId, string dataSourceName = "js") => ExportInvoker.Invoke<int>(nameof(CreateSession), callbackSetId, dataSourceName);

    [JSExport]
    public static void DisposeSession(int sessionId) => ExportInvoker.Invoke<object>(nameof(DisposeSession), sessionId);

    [JSExport]
    public static string Execute(int sessionId, string sql) => ExportInvoker.Invoke<string>(nameof(Execute), sessionId, sql);

    [JSExport]
    public static string Explain(int sessionId, string sql) => ExportInvoker.Invoke<string>(nameof(Explain), sessionId, sql);
}

internal static class ExportInvoker
{
    private static readonly Type RuntimeType = typeof(Sql4CdsExports).Assembly.GetType("MarkMpn.Sql4Cds.Engine.Wasm.Sql4CdsExportRuntime", throwOnError: true);

    internal static T Invoke<T>(string methodName, params object[] args)
    {
        var method = RuntimeType.GetMethod(methodName, System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.NonPublic);
        var result = method.Invoke(null, args);
        return result is null ? default : (T)result;
    }
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
