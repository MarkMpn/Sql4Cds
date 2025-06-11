using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.VisualStudio.DebuggerVisualizers;
using Newtonsoft.Json;

namespace MarkMpn.Sql4Cds.ScriptDom.DebugVisualizer.DebugeeSide
{
    public class ScriptDomObjectSource : VisualizerObjectSource
    {
        public override void GetData(object target, Stream outgoingData)
        {
            var settings = new JsonSerializerSettings
            {
                TypeNameHandling = TypeNameHandling.Auto
            };
            var json = JsonConvert.SerializeObject(target, typeof(TSqlFragment), settings);
            SerializeAsJson(outgoingData, new SerializedFragment { Fragment = json });
        }
    }
}
