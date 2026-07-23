using System.Runtime.Versioning;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Runtime.InteropServices.JavaScript;
using MarkMpn.Sql4Cds.Engine;

namespace MarkMpn.Sql4Cds.Engine.Wasm;

[SupportedOSPlatform("browser")]
public static partial class Sql4CdsExports
{
    [JSExport]
    public static void Reset()
    {
    }

    [JSExport]
    public static string Execute(string sql)
    {
        using var engine = new Sql4CdsLocalEngine();
        return JsonSerializer.Serialize(engine.Execute(sql), Sql4CdsJsonContext.Default.Sql4CdsLocalExecutionResult);
    }

    [JSExport]
    public static string Explain(string sql)
    {
        using var engine = new Sql4CdsLocalEngine();
        return JsonSerializer.Serialize(engine.Explain(sql), Sql4CdsJsonContext.Default.Sql4CdsLocalPlan);
    }
}

[JsonSourceGenerationOptions(WriteIndented = true)]
[JsonSerializable(typeof(Sql4CdsLocalExecutionResult))]
[JsonSerializable(typeof(Sql4CdsLocalPlan))]
internal partial class Sql4CdsJsonContext : JsonSerializerContext
{
}
