using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// Describes the schema of data produced by a node in an execution plan
    /// </summary>
    public class NodeSchema : INodeSchema
    {
        /// <summary>
        /// Creates a new <see cref="NodeSchema"/>
        /// </summary>
        public NodeSchema(IReadOnlyDictionary<string, DataTypeReference> schema, IReadOnlyDictionary<string, IReadOnlyList<string>> aliases, string primaryKey, IReadOnlyList<string> notNullColumns, IReadOnlyList<string> sortOrder)
        {
            PrimaryKey = primaryKey;
            Schema = schema ?? new Dictionary<string, DataTypeReference>();
            Aliases = aliases ?? new Dictionary<string, IReadOnlyList<string>>();
            SortOrder = sortOrder ?? Array.Empty<string>();
            NotNullColumns = notNullColumns ?? Array.Empty<string>();
        }

        /// <summary>
        /// Creates a new <see cref="NodeSchema"/> as a copy of the supplied schema
        /// </summary>
        /// <param name="copy">The schema to copy</param>
        public NodeSchema(INodeSchema copy)
        {
            PrimaryKey = copy.PrimaryKey;

            if (copy.Schema is Dictionary<string, DataTypeReference> schema)
            {
                Schema = new Dictionary<string, DataTypeReference>(schema, StringComparer.OrdinalIgnoreCase);
            }
            else
            {
                schema = new Dictionary<string, DataTypeReference>(copy.Schema.Count, StringComparer.OrdinalIgnoreCase);

                foreach (var kvp in copy.Schema)
                    schema[kvp.Key] = kvp.Value;

                Schema = schema;
            }

            if (copy.Aliases is Dictionary<string, IReadOnlyList<string>> aliases)
            {
                Aliases = new Dictionary<string, IReadOnlyList<string>>(aliases, StringComparer.OrdinalIgnoreCase);
            }
            else
            {
                aliases = new Dictionary<string, IReadOnlyList<string>>(copy.Aliases.Count, StringComparer.OrdinalIgnoreCase);

                foreach (var kvp in copy.Aliases)
                    aliases[kvp.Key] = new List<string>(kvp.Value);

                Aliases = aliases;
            }

            SortOrder = new List<string>(copy.SortOrder);
            NotNullColumns = new List<string>(copy.NotNullColumns);
        }

        /// <inheritdoc cref="INodeSchema.PrimaryKey"/>
        public string PrimaryKey { get; }

        /// <inheritdoc cref="INodeSchema.Schema"/>
        public IReadOnlyDictionary<string, DataTypeReference> Schema { get; }

        /// <inheritdoc cref="INodeSchema.Aliases"/>
        public IReadOnlyDictionary<string, IReadOnlyList<string>> Aliases { get; }

        /// <inheritdoc cref="INodeSchema.NotNullColumns"/>
        public IReadOnlyList<string> NotNullColumns { get; }

        /// <inheritdoc cref="INodeSchema.SortOrder"/>
        public IReadOnlyList<string> SortOrder { get; }

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

        /// <summary>
        /// Checks if the data is sorted by the required fields
        /// </summary>
        /// <param name="requiredSorts">The fields the data must be sorted by</param>
        /// <returns><c>true</c> if the data is sorted by the required columns, irrespective of the column ordering, or <c>false</c> otherwise</returns>
        public bool IsSortedBy(ISet<string> requiredSorts)
        {
            if (requiredSorts.Count > SortOrder.Count)
                return false;

            for (var i = 0; i < requiredSorts.Count; i++)
            {
                if (!requiredSorts.Contains(SortOrder[i]))
                    return false;
            }

            return true;
        }
    }

    /// <summary>
    /// Describes the schema of data produced by a node in an execution plan
    /// </summary>
    public interface INodeSchema
    {
        /// <summary>
        /// The name of the column that forms the primary key
        /// </summary>
        string PrimaryKey { get; }

        /// <summary>
        /// A mapping of column names to the types of data stored in them
        /// </summary>
        IReadOnlyDictionary<string, DataTypeReference> Schema { get; }

        /// <summary>
        /// A mapping of names that can be used as column aliases to the list of columns the name could refer to
        /// </summary>
        IReadOnlyDictionary<string, IReadOnlyList<string>> Aliases { get; }

        /// <summary>
        /// A list of the columns by which the data is sorted
        /// </summary>
        IReadOnlyList<string> SortOrder { get; }

        /// <summary>
        /// A list of the columns which are known to be non-null
        /// </summary>
        IReadOnlyList<string> NotNullColumns { get; }

        /// <summary>
        /// Checks if a column exists in the schema
        /// </summary>
        /// <param name="column">The name of the column being requested</param>
        /// <param name="normalized">The normalized name of the requested column</param>
        /// <returns><c>true</c> if the column name exists, or <c>false</c> otherwise</returns>
        bool ContainsColumn(string column, out string normalized);

        /// <summary>
        /// Checks if the data is sorted by the required fields
        /// </summary>
        /// <param name="requiredSorts">The fields the data must be sorted by</param>
        /// <returns><c>true</c> if the data is sorted by the required columns, irrespective of the column ordering, or <c>false</c> otherwise</returns>
        bool IsSortedBy(ISet<string> requiredSorts);
    }
}
