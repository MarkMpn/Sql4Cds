using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk.Metadata;

namespace MarkMpn.Sql4Cds.Engine.Visitors
{
    /// <summary>
    /// Checks if a SQL statement uses any features not supported by the TDS Endpoint but are supported by
    /// SQL 4 CDS.
    /// </summary>
    /// <seealso cref="https://docs.microsoft.com/en-us/powerapps/developer/data-platform/how-dataverse-sql-differs-from-transact-sql?tabs=not-supported"/>
    class TDSEndpointCompatibilityVisitor : TDSEndpointPossibleCompatibilityVisitor
    {
        private readonly IDbConnection _con;
        private readonly IAttributeMetadataCache _metadata;
        private readonly Dictionary<string, string> _tableNames;
        private HashSet<string> _supportedTables;

        /// <summary>
        /// Creates a new <see cref="TDSEndpointCompatibilityVisitor"/>
        /// </summary>
        /// <param name="con">A connection to the TDS Endpoint</param>
        /// <param name="metadata">The metadata cache for the primary data source</param>
        /// <param name="isEntireBatch">Indicates if this query is the entire SQL batch, or a single statement within it</param>
        /// <param name="outerTableNames">A mapping of table aliases to table names available from the outer query</param>
        /// <param name="supportedTables">A pre-calculated list of supported tables</param>
        /// <param name="ctes">A mapping of CTE names to their definitions from the outer query</param>
        /// <param name="parameterTypes">A mapping of parameter names to their types</param>
        public TDSEndpointCompatibilityVisitor(IDbConnection con, IAttributeMetadataCache metadata, bool? isEntireBatch = null, Dictionary<string, string> outerTableNames = null, HashSet<string> supportedTables = null, Dictionary<string, CommonTableExpression> ctes = null, IDictionary<string, DataTypeReference> parameterTypes = null)
            : base(isEntireBatch, ctes, parameterTypes)
        {
            _con = con;
            _metadata = metadata;
            _tableNames = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            _supportedTables = supportedTables;

            if (outerTableNames != null)
            {
                foreach (var kvp in outerTableNames)
                    _tableNames[kvp.Key] = kvp.Value;
            }
        }

        protected override TDSEndpointPossibleCompatibilityVisitor CreateChildVisitor()
        {
            return new TDSEndpointCompatibilityVisitor(_con, _metadata, _isEntireBatch, _tableNames, _supportedTables, _ctes, _parameterTypes);
        }

        public override void Visit(NamedTableReference node)
        {
            base.Visit(node);

            if (!IsCompatible)
                return;

            var schemaName = node.SchemaObject.SchemaIdentifier?.Value;
            var tableName = node.SchemaObject.BaseIdentifier.Value;

            // Keep track of what tables are available to use under what names so we can get information for specific columns later
            if (node.Alias != null)
                _tableNames[node.Alias.Value] = tableName.ToLower();
            else
                _tableNames[tableName] = tableName.ToLower();

            if (schemaName == null && _ctes.ContainsKey(tableName))
                return;

            schemaName = schemaName ?? "dbo";

            if (schemaName.Equals("sys", StringComparison.OrdinalIgnoreCase))
            {
                // sys schema validation is done in the base class
                return;
            }

            if (!schemaName.Equals("dbo", StringComparison.OrdinalIgnoreCase))
            {
                IsCompatible = false;
                return;
            }

            // Load the list of tables if we haven't already got it
            if (_supportedTables == null && _con != null)
            {
                _supportedTables = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

                using (var cmd = _con.CreateCommand())
                {
                    cmd.CommandText = "SELECT name FROM sys.tables";

                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                            _supportedTables.Add("dbo." + reader.GetString(0));
                    }
                }

            }

            if (_supportedTables != null && !_supportedTables.Contains(schemaName + "." + tableName) &&
                (node.SchemaObject.Identifiers.Count != 1 || !_ctes.ContainsKey(node.SchemaObject.BaseIdentifier.Value)))
            {
                // Table does not exist in TDS endpoint and is not defined as a CTE
                IsCompatible = false;
                return;
            }
        }

        public override void Visit(ColumnReferenceExpression node)
        {
            if (node.ColumnType == ColumnType.Regular)
            {
                // Check for unsupported column types.
                var columnName = node.MultiPartIdentifier.Identifiers[node.MultiPartIdentifier.Count - 1].Value.ToLower();

                if (node.MultiPartIdentifier.Count == 1)
                {
                    // Table name not specified. Try to find it in the list of current tables
                    foreach (var table in _tableNames.Values)
                    {
                        if (!GetCTECols(table, out _))
                        {
                            var attribute = TryGetEntity(table)?.Attributes?.SingleOrDefault(a => a.LogicalName.Equals(columnName));

                            if (!AttributeIsSupported(attribute))
                            {
                                IsCompatible = false;
                                return;
                            }
                        }
                    }
                }
                else if (GetCTECols(node.MultiPartIdentifier[0].Value, out var cols))
                {
                    if (!cols.Contains(columnName))
                    {
                        IsCompatible = false;
                        return;
                    }
                }
                else
                {
                    var attribute = TryGetEntity(node.MultiPartIdentifier[0].Value)?.Attributes?.SingleOrDefault(a => a.LogicalName.Equals(columnName));

                    if (!AttributeIsSupported(attribute))
                    {
                        IsCompatible = false;
                        return;
                    }
                }
            }

            base.Visit(node);
        }

        private bool GetCTECols(string cteName, out HashSet<string> cols)
        {
            cols = null;

            if (!_ctes.TryGetValue(cteName, out var cte))
                return false;

            cols = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            if (cte.Columns.Count > 0)
            {
                foreach (var col in cte.Columns)
                    cols.Add(col.Value);
            }
            else
            {
                var query = cte.QueryExpression;

                while (query is BinaryQueryExpression bin)
                    query = bin.FirstQueryExpression;

                if (!(query is QuerySpecification spec))
                    return false;

                // Can't easily work out the column names that will be produced by a SELECT * within a CTE
                if (spec.SelectElements.OfType<SelectStarExpression>().Any())
                    return false;

                foreach (var col in spec.SelectElements.OfType<SelectScalarExpression>())
                {
                    if (!(col is SelectScalarExpression sse))
                        return false;

                    if (sse.ColumnName != null)
                        cols.Add(sse.ColumnName.Value);
                    else if (sse.Expression is ColumnReferenceExpression cre)
                        cols.Add(cre.MultiPartIdentifier.Identifiers.Last().Value);
                    else
                        return false;
                }
            }

            return true;
        }

        private EntityMetadata TryGetEntity(string logicalname)
        {
            if (!IsCompatible)
                return null;

            if (_tableNames.TryGetValue(logicalname, out var tableName))
                logicalname = tableName;

            try
            {
                return _metadata[logicalname.ToLower()];
            }
            catch
            {
                return null;
            }
        }

        private bool AttributeIsSupported(AttributeMetadata attribute)
        {
            if (attribute == null)
                return true;

            if (attribute.AttributeType == AttributeTypeCode.PartyList)
                return false;

            return true;
        }
    }
}
