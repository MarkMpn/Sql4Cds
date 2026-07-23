using System.Text.Json;
using System.Runtime.InteropServices.JavaScript;
using MarkMpn.Sql4Cds.Engine;

namespace MarkMpn.Sql4Cds.Engine.Wasm;

public static class Sql4CdsExports
{
    private static readonly JsonSerializerOptions JsonOptions = new JsonSerializerOptions(JsonSerializerDefaults.Web)
    {
        WriteIndented = true
    };

    private static Sql4CdsLocalEngine _engine = new Sql4CdsLocalEngine();

    [JSExport]
    public static void Reset()
    {
        _engine.Dispose();
        _engine = new Sql4CdsLocalEngine();
    }

    [JSExport]
    public static string Execute(string sql)
    {
        return JsonSerializer.Serialize(_engine.Execute(sql), JsonOptions);
    }

    [JSExport]
    public static string Explain(string sql)
    {
        return JsonSerializer.Serialize(_engine.Explain(sql), JsonOptions);
    }
}
