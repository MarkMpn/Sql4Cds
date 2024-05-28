using System;
using System.Collections.Generic;
using System.Text;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using Newtonsoft.Json;

namespace MarkMpn.Sql4Cds.Engine
{
    public static class ExecutionPlanSerializer
    {
        public static string Serialize(IRootExecutionPlanNode plan)
        {
            var settings = new JsonSerializerSettings
            {
                TypeNameHandling = TypeNameHandling.Auto
            };
            return JsonConvert.SerializeObject(plan, typeof(IRootExecutionPlanNode), settings);
        }

        public static IRootExecutionPlanNode Deserialize(string plan)
        {
            var settings = new JsonSerializerSettings
            {
                TypeNameHandling = TypeNameHandling.Auto
            };
            return JsonConvert.DeserializeObject<IRootExecutionPlanNode>(plan, settings);
        }
    }
}
