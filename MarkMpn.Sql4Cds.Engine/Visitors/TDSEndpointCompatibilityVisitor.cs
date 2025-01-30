using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk.Metadata;

namespace MarkMpn.Sql4Cds.Engine.Visitors
{
    /// <summary>
    /// Checks if a SQL statement uses any features not supported by the TDS Endpoint but are supported by
    /// SQL 4 CDS.
    /// </summary>
    /// <seealso cref="https://docs.microsoft.com/en-us/powerapps/developer/data-platform/how-dataverse-sql-differs-from-transact-sql?tabs=not-supported"/>
    class TDSEndpointCompatibilityVisitor : TSqlFragmentVisitor
    {
        private readonly IDbConnection _con;
        private readonly IAttributeMetadataCache _metadata;
        private readonly Dictionary<string, string> _tableNames;
        private readonly HashSet<string> _supportedTables;
        private readonly Dictionary<string, CommonTableExpression> _ctes;
        private readonly IDictionary<string, DataTypeReference> _parameterTypes;
        private bool? _isEntireBatch;
        private TSqlFragment _root;

        /// <summary>
        /// Creates a new <see cref="TDSEndpointCompatibilityVisitor"/>
        /// </summary>
        /// <param name="con">A connection to the TDS Endpoint</param>
        /// <param name="metadata">The metadata cache for the primary data source</param>
        /// <param name="isEntireBatch">Indicates if this query is the entire SQL batch, or a single statement within it</param>
        /// <param name="outerTableNames">A mapping of table aliases to table names available from the outer query</param>
        /// <param name="supportedTables">A pre-calculated list of supported tables</param>
        /// <param name="ctes">A mapping of CTE names to their definitions from the outer query</param>
        public TDSEndpointCompatibilityVisitor(IDbConnection con, IAttributeMetadataCache metadata, bool? isEntireBatch = null, Dictionary<string, string> outerTableNames = null, HashSet<string> supportedTables = null, Dictionary<string, CommonTableExpression> ctes = null, IDictionary<string, DataTypeReference> parameterTypes = null)
        {
            _con = con;
            _metadata = metadata;
            _tableNames = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            _ctes = ctes ?? new Dictionary<string, CommonTableExpression>(StringComparer.OrdinalIgnoreCase);
            _parameterTypes = parameterTypes;
            _isEntireBatch = isEntireBatch;

            if (outerTableNames != null)
            {
                foreach (var kvp in outerTableNames)
                    _tableNames[kvp.Key] = kvp.Value;
            }

            if (supportedTables != null)
            {
                _supportedTables = supportedTables;
            }
            else if (con != null)
            {
                _supportedTables = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

                using (var cmd = con.CreateCommand())
                {
                    cmd.CommandText = "SELECT name FROM sys.tables";

                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                            _supportedTables.Add("dbo." + reader.GetString(0));
                    }
                }

                _supportedTables.Add("sys.all_columns");
                _supportedTables.Add("sys.all_objects");
                _supportedTables.Add("sys.check_constraints");
                _supportedTables.Add("sys.columns");
                _supportedTables.Add("sys.computed_columns");
                _supportedTables.Add("sys.default_constraints");
                _supportedTables.Add("sys.foreign_key_columns");
                _supportedTables.Add("sys.foreign_keys");
                _supportedTables.Add("sys.index_columns");
                _supportedTables.Add("sys.objects");
                _supportedTables.Add("sys.sequences");
                _supportedTables.Add("sys.sql_modules");
                _supportedTables.Add("sys.stats");
                _supportedTables.Add("sys.synonyms");
                _supportedTables.Add("sys.table_types");
                _supportedTables.Add("sys.tables");
                _supportedTables.Add("sys.triggers");
            }

            IsCompatible = true;
        }

        public bool IsCompatible { get; private set; }

        public bool RequiresCteRewrite { get; private set; }

        public override void Visit(TSqlFragment node)
        {
            if (_root == null)
            {
                _root = node;

                if (_isEntireBatch == null)
                    _isEntireBatch = node is TSqlScript;
            }

            base.Visit(node);
        }

        public override void Visit(NamedTableReference node)
        {
            if (node.SchemaObject.ServerIdentifier != null ||
                node.SchemaObject.DatabaseIdentifier != null)
            {
                // Can't do cross-instance queries
                // No access to metadata schema
                IsCompatible = false;
                return;
            }

            if (_supportedTables != null && !_supportedTables.Contains((node.SchemaObject.SchemaIdentifier?.Value ?? "dbo") + "." + node.SchemaObject.BaseIdentifier.Value) &&
                (node.SchemaObject.Identifiers.Count != 1 || !_ctes.ContainsKey(node.SchemaObject.BaseIdentifier.Value)))
            {
                // Table does not exist in TDS endpoint and is not defined as a CTE
                IsCompatible = false;
                return;
            }

            // Keep track of what tables are available to use under what names so we can get information for specific columns later
            if (node.Alias != null)
                _tableNames[node.Alias.Value] = node.SchemaObject.BaseIdentifier.Value.ToLower();
            else
                _tableNames[node.SchemaObject.BaseIdentifier.Value] = node.SchemaObject.BaseIdentifier.Value.ToLower();

            base.Visit(node);
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

        public override void Visit(DataModificationStatement node)
        {
            // Can't do any sort of data modification - INSERT, UPDATE, DELETE
            IsCompatible = false;
        }

        public override void Visit(ExecuteStatement node)
        {
            // Can't use stored procedures
            IsCompatible = false;
        }

        public override void Visit(SchemaObjectFunctionTableReference node)
        {
            // Can't use messages as TVFs
            IsCompatible = false;
        }

        public override void Visit(IfStatement node)
        {
            // Can't use IF statement
            IsCompatible = false;
        }

        public override void Visit(WhileStatement node)
        {
            // Can't use WHILE statement
            IsCompatible = false;
        }

        public override void Visit(ThrowStatement node)
        {
            // Can't use THROW statement
            IsCompatible = false;
        }

        public override void Visit(RaiseErrorStatement node)
        {
            // Can't use RAISERROR statement
            IsCompatible = false;
        }

        public override void Visit(GlobalVariableExpression node)
        {
            if (node.Name.Equals("@@IDENTITY", StringComparison.OrdinalIgnoreCase) ||
                node.Name.Equals("@@SERVERNAME", StringComparison.OrdinalIgnoreCase) ||
                node.Name.Equals("@@ERROR", StringComparison.OrdinalIgnoreCase))
            {
                IsCompatible = false;
                return;
            }

            if (_isEntireBatch == false && node.Name.Equals("@@ROWCOUNT", StringComparison.OrdinalIgnoreCase))
            {
                IsCompatible = false;
                return;
            }

            base.Visit(node);
        }

        public override void Visit(ScalarSubquery node)
        {
            // Name resolution needs to be scoped to the query, so create a new sub-visitor
            if (IsCompatible && _root != node)
            {
                var subVisitor = new TDSEndpointCompatibilityVisitor(_con, _metadata, _isEntireBatch, _tableNames, _supportedTables, _ctes, _parameterTypes);
                node.Accept(subVisitor);

                if (!subVisitor.IsCompatible)
                    IsCompatible = false;

                if (subVisitor.RequiresCteRewrite)
                    RequiresCteRewrite = true;

                return;
            }

            base.Visit(node);
        }

        public override void Visit(QueryDerivedTable node)
        {
            // Name resolution needs to be scoped to the query, so create a new sub-visitor
            if (IsCompatible && _root != node)
            {
                var subVisitor = new TDSEndpointCompatibilityVisitor(_con, _metadata, _isEntireBatch, supportedTables: _supportedTables, ctes: _ctes, parameterTypes: _parameterTypes);
                node.Accept(subVisitor);

                if (!subVisitor.IsCompatible)
                    IsCompatible = false;

                if (subVisitor.RequiresCteRewrite)
                    RequiresCteRewrite = true;

                return;
            }

            base.Visit(node);
        }

        public override void ExplicitVisit(SelectStatement node)
        {
            // Name resolution needs to be scoped to the query, so create a new sub-visitor
            if (IsCompatible && _root != node)
            {
                var subVisitor = new TDSEndpointCompatibilityVisitor(_con, _metadata, _isEntireBatch, supportedTables: _supportedTables, ctes: _ctes, parameterTypes: _parameterTypes);
                subVisitor._root = node;

                // Visit CTEs first
                if (node.WithCtesAndXmlNamespaces != null)
                    node.WithCtesAndXmlNamespaces.Accept(subVisitor);

                node.Accept(subVisitor);

                if (!subVisitor.IsCompatible)
                    IsCompatible = false;

                if (subVisitor.RequiresCteRewrite)
                    RequiresCteRewrite = true;

                return;
            }

            // For the root query we can't return any EntityReference values as they will be implicitly converted
            // to guids and lose the associated type information.
            if (_root == node && _parameterTypes != null && _parameterTypes.Any(p => p.Value.IsEntityReference()) &&
                node.QueryExpression is QuerySpecification querySpec)
            {
                if (querySpec.SelectElements
                    .OfType<SelectScalarExpression>()
                    .Select(sse => sse.Expression)
                    .OfType<VariableReference>()
                    .Any(v => _parameterTypes.TryGetValue(v.Name, out var type) && type.IsEntityReference()))
                {
                    IsCompatible = false;
                    return;
                }
            }

            base.ExplicitVisit(node);
        }

        public override void Visit(QuerySpecification node)
        {
            // Ensure we process the FROM clause first, so we understand what tables are involved before
            // we try to process column names
            if (node.FromClause != null)
                node.FromClause.Accept(this);

            base.Visit(node);
        }

        public override void Visit(PrintStatement node)
        {
            // Can't use PRINT statement
            IsCompatible = false;
        }

        public override void Visit(WaitForStatement node)
        {
            // Can't use WAITFOR statement
            IsCompatible = false;
        }

        public override void Visit(FunctionCall node)
        {
            switch (node.FunctionName.Value.ToUpperInvariant())
            {
                // Can't use JSON functions
                case "JSON_VALUE":
                case "JSON_PATH_EXISTS":
                case "SQL_VARIANT_PROPERTY":

                // Can't use error handling functions
                case "ERROR_LINE":
                case "ERROR_MESSAGE":
                case "ERROR_NUMBER":
                case "ERROR_PROCEDURE":
                case "ERROR_SEVERITY":
                case "ERROR_STATE":

                // Can't use custom SQL 4 CDS functions
                case "CREATELOOKUP":

                    IsCompatible = false;
                    break;
            }

            // Can't use XML data type methods
            if (node.CallTarget != null)
                IsCompatible = false;

            base.Visit(node);
        }

        public override void Visit(ForClause node)
        {
            // Can't use FOR XML clause
            IsCompatible = false;
        }

        public override void Visit(DistinctPredicate node)
        {
            // Can't use IS [NOT] DISTINCT FROM
            IsCompatible = false;
        }

        public override void Visit(CommonTableExpression node)
        {
            var cteValidator = new CteValidatorVisitor();
            node.Accept(cteValidator);

            if (cteValidator.IsRecursive)
            {
                IsCompatible = false;
            }
            else
            {
                // TDS Endpoint doesn't support CTEs but we can rewrite non-recursive ones as subqueries
                RequiresCteRewrite = true;
            }

            _ctes[node.ExpressionName.Value] = node;
        }

        public override void Visit(GeneralSetCommand node)
        {
            if (node.CommandType == GeneralSetCommandType.DateFormat)
            {
                // SET DATEFORMAT does work, but isn't persisted correctly across the session
                // Mark it as not compatible so we can track the selected format internally
                // and pass it to the TDS Endpoint on each call as required.
                IsCompatible = false;
            }
            
            base.Visit(node);
        }

        public override void Visit(UserDataTypeReference node)
        {
            // Can't use EntityReference type
            if (node.IsEntityReference())
                IsCompatible = false;

            base.Visit(node);
        }

        public override void Visit(ExecuteAsStatement node)
        {
            // EXECUTE AS is not supported
            IsCompatible = false;
        }

        public override void Visit(RevertStatement node)
        {
            // REVERT is not supported
            IsCompatible = false;
        }

        public override void Visit(DeclareCursorStatement node)
        {
            // Cursors are not supported
            IsCompatible = false;
        }

        public override void Visit(CursorStatement node)
        {
            // Cursors are not supported
            IsCompatible = false;
        }
    }
}
