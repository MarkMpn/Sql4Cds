using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    public class NodeSchema
    {
        public string PrimaryKey { get; set; }

        public IDictionary<string, Type> Schema { get; set; } = new Dictionary<string, Type>(StringComparer.OrdinalIgnoreCase);

        public IDictionary<string, List<string>> Aliases { get; set; } = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);

        public bool ContainsColumn(string column, out string normalized)
        {
            if (Schema.TryGetValue(column, out _))
            {
                normalized = Schema.Keys.Single(k => k.Equals(column, StringComparison.OrdinalIgnoreCase));
                return true;
            }

            if (Aliases.TryGetValue(column, out var names) && names.Count == 1)
            {
                normalized = names[0];
                return true;
            }

            normalized = null;
            return false;
        }
    }
}
