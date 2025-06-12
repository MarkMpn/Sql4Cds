using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// Describes the schema of data produced by a node in an execution plan
    /// </summary>
    class NodeSchema : INodeSchema
    {
        /// <summary>
        /// Creates a new <see cref="NodeSchema"/>
        /// </summary>
        public NodeSchema(IReadOnlyDictionary<string, IColumnDefinition> schema, IReadOnlyDictionary<string, IReadOnlyList<string>> aliases, string primaryKey, IReadOnlyList<string> sortOrder)
        {
            PrimaryKey = primaryKey;
            Schema = schema ?? new ColumnList();
            Aliases = aliases ?? new Dictionary<string, IReadOnlyList<string>>();
            SortOrder = sortOrder ?? Array.Empty<string>();
        }

        /// <summary>
        /// Creates a new <see cref="NodeSchema"/> as a copy of the supplied schema
        /// </summary>
        /// <param name="copy">The schema to copy</param>
        public NodeSchema(INodeSchema copy)
        {
            PrimaryKey = copy.PrimaryKey;

            var schema = new ColumnList();

            foreach (var kvp in copy.Schema)
                schema[kvp.Key] = kvp.Value;

            Schema = schema;

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
        }

        /// <inheritdoc cref="INodeSchema.PrimaryKey"/>
        public string PrimaryKey { get; }

        /// <inheritdoc cref="INodeSchema.Schema"/>
        public IReadOnlyDictionary<string, IColumnDefinition> Schema { get; }

        /// <inheritdoc cref="INodeSchema.Aliases"/>
        public IReadOnlyDictionary<string, IReadOnlyList<string>> Aliases { get; }

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
            if (Aliases.TryGetValue(column, out var names))
            {
                if (names.Count > 1)
                {
                    normalized = null;
                    return false;
                }

                if (names.Count == 1)
                {
                    normalized = names[0];
                    return true;
                }
            }

            if (Schema.TryGetValue(column, out _))
            {
                normalized = Schema.Keys.Single(k => k.Equals(column, StringComparison.OrdinalIgnoreCase));
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

    class ColumnDefinition : IColumnDefinition
    {
        public ColumnDefinition(DataTypeReference type, bool isNullable, bool isCalculated, bool isVisible = true, bool isWildcardable = false)
        {
            Type = type;
            IsNullable = isNullable;
            IsCalculated = isCalculated;
            IsVisible = isVisible;
            IsWildcardable = isWildcardable;
        }

        public string SourceServer => null;

        public string SourceSchema => null;

        public string SourceTable => null;

        public string SourceAlias => null;

        public string SourceColumn => null;

        public DataTypeReference Type { get; }

        public bool IsNullable { get; }

        public bool IsCalculated { get; }

        public bool IsVisible { get; }

        public bool IsWildcardable { get; }

        public override string ToString()
        {
            return $"{Type.ToSql()} {(IsNullable ? "NULL" : "NOT NULL")}";
        }
    }

    class LazyColumnDefinition : IColumnDefinition
    {
        private readonly Lazy<DataTypeReference> _type;

        public LazyColumnDefinition(Func<DataTypeReference> typeLoader, bool isNullable, bool isCalculated, bool isVisible = true, bool isWildcardable = false)
        {
            _type = new Lazy<DataTypeReference>(typeLoader);
            IsNullable = isNullable;
            IsCalculated = isCalculated;
            IsVisible = isVisible;
            IsWildcardable = isWildcardable;
        }

        public string SourceServer => null;

        public string SourceSchema => null;

        public string SourceTable => null;

        public string SourceAlias => null;

        public string SourceColumn => null;

        public DataTypeReference Type => _type.Value;

        public bool IsNullable { get; }

        public bool IsCalculated { get; }

        public bool IsVisible { get; }

        public bool IsWildcardable { get; }

        public override string ToString()
        {
            return $"{Type.ToSql()} {(IsNullable ? "NULL" : "NOT NULL")}";
        }
    }

    static class ColumnDefinitionExtensions
    {
        class NullableColumnDefinition : IColumnDefinition
        {
            private readonly IColumnDefinition _inner;

            public NullableColumnDefinition(IColumnDefinition inner, bool nullable)
            {
                _inner = inner;
                IsNullable = nullable;
            }

            public string SourceServer => _inner.SourceServer;

            public string SourceSchema => _inner.SourceSchema;

            public string SourceTable => _inner.SourceTable;

            public string SourceAlias => _inner.SourceAlias;

            public string SourceColumn => _inner.SourceColumn;

            public DataTypeReference Type => _inner.Type;

            public bool IsNullable { get; }

            public bool IsCalculated => _inner.IsCalculated;

            public bool IsVisible => _inner.IsVisible;

            public bool IsWildcardable => _inner.IsWildcardable;

            public override string ToString()
            {
                return $"{Type.ToSql()} {(IsNullable ? "NULL" : "NOT NULL")}";
            }
        }

        class VisibleColumnDefinition : IColumnDefinition
        {
            private readonly IColumnDefinition _inner;

            public VisibleColumnDefinition(IColumnDefinition inner, bool visible)
            {
                _inner = inner;
                IsVisible = visible;
            }

            public string SourceServer => _inner.SourceServer;

            public string SourceSchema => _inner.SourceSchema;

            public string SourceTable => _inner.SourceTable;

            public string SourceAlias => _inner.SourceAlias;

            public string SourceColumn => _inner.SourceColumn;

            public DataTypeReference Type => _inner.Type;

            public bool IsNullable => _inner.IsNullable;

            public bool IsCalculated => _inner.IsCalculated;

            public bool IsVisible { get; }

            public bool IsWildcardable => _inner.IsWildcardable;

            public override string ToString()
            {
                return _inner.ToString();
            }
        }

        class CalculatedColumnDefinition : IColumnDefinition
        {
            private readonly IColumnDefinition _inner;

            public CalculatedColumnDefinition(IColumnDefinition inner, bool calculated)
            {
                _inner = inner;
                IsCalculated = calculated;
            }

            public string SourceServer => _inner.SourceServer;

            public string SourceSchema => _inner.SourceSchema;

            public string SourceTable => _inner.SourceTable;

            public string SourceAlias => _inner.SourceAlias;

            public string SourceColumn => _inner.SourceColumn;

            public DataTypeReference Type => _inner.Type;

            public bool IsNullable => _inner.IsNullable;

            public bool IsCalculated { get; }

            public bool IsVisible => _inner.IsVisible;

            public bool IsWildcardable => _inner.IsWildcardable;

            public override string ToString()
            {
                return _inner.ToString();
            }
        }

        class WildcardableColumnDefinition : IColumnDefinition
        {
            private readonly IColumnDefinition _inner;

            public WildcardableColumnDefinition(IColumnDefinition inner, bool wildcardable)
            {
                _inner = inner;
                IsWildcardable = wildcardable;
            }

            public string SourceServer => _inner.SourceServer;

            public string SourceSchema => _inner.SourceSchema;

            public string SourceTable => _inner.SourceTable;

            public string SourceAlias => _inner.SourceAlias;

            public string SourceColumn => _inner.SourceColumn;

            public DataTypeReference Type => _inner.Type;

            public bool IsNullable => _inner.IsNullable;

            public bool IsCalculated => _inner.IsCalculated;

            public bool IsVisible => _inner.IsVisible;

            public bool IsWildcardable { get; }

            public override string ToString()
            {
                return _inner.ToString();
            }
        }

        class SourceColumnDefinition : IColumnDefinition
        {
            private readonly IColumnDefinition _inner;

            public SourceColumnDefinition(IColumnDefinition inner, string server, string schema, string table, string alias, string column)
            {
                _inner = inner;
                SourceServer = server;
                SourceSchema = schema;
                SourceTable = table;
                SourceAlias = alias;
                SourceColumn = column;
            }

            public string SourceServer { get; }

            public string SourceSchema { get; }

            public string SourceTable { get; }

            public string SourceAlias { get; }

            public string SourceColumn { get; }

            public DataTypeReference Type => _inner.Type;

            public bool IsNullable => _inner.IsNullable;

            public bool IsCalculated => _inner.IsCalculated;

            public bool IsVisible => _inner.IsVisible;

            public bool IsWildcardable => _inner.IsWildcardable;

            public override string ToString()
            {
                return _inner.ToString();
            }
        }

        public static IColumnDefinition NotNull(this IColumnDefinition col)
        {
            return new NullableColumnDefinition(col, false);
        }

        public static IColumnDefinition Null(this IColumnDefinition col)
        {
            return new NullableColumnDefinition(col, true);
        }

        public static IColumnDefinition Invisible(this IColumnDefinition col)
        {
            return new VisibleColumnDefinition(col, false);
        }

        public static IColumnDefinition Calculated(this IColumnDefinition col)
        {
            return new CalculatedColumnDefinition(col, true);
        }

        public static IColumnDefinition NotCalculated(this IColumnDefinition col)
        {
            return new CalculatedColumnDefinition(col, false);
        }

        public static IColumnDefinition Wildcardable(this IColumnDefinition col, bool isWildcardable = true)
        {
            return new WildcardableColumnDefinition(col, isWildcardable);
        }

        public static IColumnDefinition FromSource(this IColumnDefinition col, string server, string schema, string table, string alias, string column)
        {
            return new SourceColumnDefinition(col, server, schema, table, alias, column);
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
        IReadOnlyDictionary<string, IColumnDefinition> Schema { get; }

        /// <summary>
        /// A mapping of names that can be used as column aliases to the list of columns the name could refer to
        /// </summary>
        IReadOnlyDictionary<string, IReadOnlyList<string>> Aliases { get; }

        /// <summary>
        /// A list of the columns by which the data is sorted
        /// </summary>
        IReadOnlyList<string> SortOrder { get; }

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

    /// <summary>
    /// Describes the schema of a column in a table
    /// </summary>
    public interface IColumnDefinition
    {
        /// <summary>
        /// The name of the data source the column is being read from
        /// </summary>
        string SourceServer { get; }

        /// <summary>
        /// The name of the schema the column is being read from
        /// </summary>
        string SourceSchema { get; }

        /// <summary>
        /// The name of the table the column is being read from
        /// </summary>
        string SourceTable { get; }

        /// <summary>
        /// The alias of the table the column is being read from
        /// </summary>
        string SourceAlias { get; }

        /// <summary>
        /// The name of the column the column is being read from
        /// </summary>
        string SourceColumn { get; }

        /// <summary>
        /// The data type of the column
        /// </summary>
        DataTypeReference Type { get; }

        /// <summary>
        /// Indicates if the column can contain null values
        /// </summary>
        bool IsNullable { get; }

        /// <summary>
        /// Indicates if the column is the result of an internal calculation
        /// </summary>
        bool IsCalculated { get; }

        /// <summary>
        /// Indicates if the column is visible to the user for use in the SELECT clause
        /// </summary>
        bool IsVisible { get; }

        /// <summary>
        /// Indicates if the column should be included in a wildcard SELECT * clause
        /// </summary>
        bool IsWildcardable { get; }
    }

    class ColumnList : IDictionary<string, IColumnDefinition>, IReadOnlyDictionary<string, IColumnDefinition>
    {
        private readonly OrderedDictionary _inner;

        public ColumnList()
        {
            _inner = new OrderedDictionary(StringComparer.OrdinalIgnoreCase);
        }

        public IColumnDefinition this[string key]
        {
            get => (IColumnDefinition)_inner[key];
            set => _inner[key] = value;
        }

        public ICollection<string> Keys => _inner.Keys.Cast<string>().ToList();

        public ICollection<IColumnDefinition> Values => _inner.Values.Cast<IColumnDefinition>().ToList();

        public int Count => _inner.Count;

        public bool IsReadOnly => false;

        IEnumerable<string> IReadOnlyDictionary<string, IColumnDefinition>.Keys => _inner.Keys.Cast<string>();

        IEnumerable<IColumnDefinition> IReadOnlyDictionary<string, IColumnDefinition>.Values => _inner.Values.Cast<IColumnDefinition>();

        public void Add(string key, IColumnDefinition value)
        {
            _inner.Add(key, value);
        }

        public void Add(KeyValuePair<string, IColumnDefinition> item)
        {
            _inner.Add(item.Key, item.Value);
        }

        public void Clear()
        {
            _inner.Clear();
        }

        public bool Contains(KeyValuePair<string, IColumnDefinition> item)
        {
            return TryGetValue(item.Key, out var value) && value == item.Value;
        }

        public bool ContainsKey(string key)
        {
            return _inner.Contains(key);
        }

        public void CopyTo(KeyValuePair<string, IColumnDefinition>[] array, int arrayIndex)
        {
            _inner.CopyTo(array, arrayIndex);
        }

        public IEnumerator<KeyValuePair<string, IColumnDefinition>> GetEnumerator()
        {
            var enumerator = _inner.GetEnumerator();

            while (enumerator.MoveNext())
                yield return new KeyValuePair<string, IColumnDefinition>((string)enumerator.Key, (IColumnDefinition)enumerator.Value);
        }

        public bool Remove(string key)
        {
            if (!_inner.Contains(key))
                return false;

            _inner.Remove(key);
            return true;
        }

        public bool Remove(KeyValuePair<string, IColumnDefinition> item)
        {
            if (!Contains(item))
                return false;

            _inner.Remove(item.Key);
            return true;
        }

        public bool TryGetValue(string key, out IColumnDefinition value)
        {
            if (!_inner.Contains(key))
            {
                value = null;
                return false;
            }

            value = (IColumnDefinition)_inner[key];
            return true;
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }
    }
}
