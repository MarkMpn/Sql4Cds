using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// Describes the schema of data produced by a node in an execution plan
    /// </summary>
    public class NodeSchema
    {
        /// <summary>
        /// The name of the column that forms the primary key
        /// </summary>
        public string PrimaryKey { get; set; }

        /// <summary>
        /// A mapping of column names to the types of data stored in them
        /// </summary>
        public IDictionary<string, Type> Schema { get; set; } = new Dictionary<string, Type>(StringComparer.OrdinalIgnoreCase);

        /// <summary>
        /// A mapping of names that can be used as column aliases to the list of columns the name could refer to
        /// </summary>
        public IDictionary<string, List<string>> Aliases { get; set; } = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);

        /// <summary>
        /// Checks if a column exists in the schema
        /// </summary>
        /// <param name="column">The name of the column being requested</param>
        /// <param name="normalized">The normalized name of the requested column</param>
        /// <returns><c>true</c> if the column name exists, or <c>false</c> otherwise</returns>
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
