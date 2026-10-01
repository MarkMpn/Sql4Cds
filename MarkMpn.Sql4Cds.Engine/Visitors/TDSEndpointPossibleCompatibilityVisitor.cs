using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace MarkMpn.Sql4Cds.Engine.Visitors
{
    /// <summary>
    /// Checks if a SQL statement uses any features not supported by the TDS Endpoint but are supported by
    /// SQL 4 CDS, doing a basic level of validation that does not require an actual connection to the TDS Endpoint.
    /// </summary>
    /// <seealso cref="https://docs.microsoft.com/en-us/powerapps/developer/data-platform/how-dataverse-sql-differs-from-transact-sql?tabs=not-supported"/>
    class TDSEndpointPossibleCompatibilityVisitor : TSqlFragmentVisitor
    {
        private static readonly HashSet<string> _sysTables;

        protected readonly Dictionary<string, CommonTableExpression> _ctes;
        protected readonly IDictionary<string, DataTypeReference> _parameterTypes;
        protected bool? _isEntireBatch;
        private TSqlFragment _root;

        static TDSEndpointPossibleCompatibilityVisitor()
        {
            _sysTables = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            _sysTables.Add("all_columns");
            _sysTables.Add("all_objects");
            _sysTables.Add("check_constraints");
            _sysTables.Add("columns");
            _sysTables.Add("computed_columns");
            _sysTables.Add("default_constraints");
            _sysTables.Add("foreign_key_columns");
            _sysTables.Add("foreign_keys");
            _sysTables.Add("index_columns");
            _sysTables.Add("objects");
            _sysTables.Add("sequences");
            _sysTables.Add("sql_modules");
            _sysTables.Add("stats");
            _sysTables.Add("synonyms");
            _sysTables.Add("table_types");
            _sysTables.Add("tables");
            _sysTables.Add("triggers");
        }

        /// <summary>
        /// Creates a new <see cref="TDSEndpointPossibleCompatibilityVisitor"/>
        /// </summary>
        /// <param name="isEntireBatch">Indicates if this query is the entire SQL batch, or a single statement within it</param>
        /// <param name="ctes">A mapping of CTE names to their definitions from the outer query</param>
        /// <param name="parameterTypes">A mapping of parameter names to their types</param>
        public TDSEndpointPossibleCompatibilityVisitor(bool? isEntireBatch = null, Dictionary<string, CommonTableExpression> ctes = null, IDictionary<string, DataTypeReference> parameterTypes = null)
        {
            _ctes = ctes ?? new Dictionary<string, CommonTableExpression>(StringComparer.OrdinalIgnoreCase);
            _parameterTypes = parameterTypes;
            _isEntireBatch = isEntireBatch;

            IsCompatible = true;
        }

        public bool IsCompatible { get; protected set; }

        public bool RequiresCteRewrite { get; private set; }

        protected virtual TDSEndpointPossibleCompatibilityVisitor CreateChildVisitor()
        {
            return new TDSEndpointPossibleCompatibilityVisitor(_isEntireBatch, _ctes, _parameterTypes);
        }

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

            var schemaName = node.SchemaObject.SchemaIdentifier?.Value ?? "dbo";
            var tableName = node.SchemaObject.BaseIdentifier.Value;

            if (schemaName.Equals("sys", StringComparison.OrdinalIgnoreCase))
            {
                // Only a known set of sys tables are supported, no need to actually hit the TDS Endpoint to load them
                if (!_sysTables.Contains(tableName))
                    IsCompatible = false;

                return;
            }

            if (!schemaName.Equals("dbo", StringComparison.OrdinalIgnoreCase))
            {
                IsCompatible = false;
                return;
            }

            base.Visit(node);
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
                var subVisitor = CreateChildVisitor();
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
                var subVisitor = CreateChildVisitor();
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
                var subVisitor = CreateChildVisitor();
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

        public override void Visit(CreateTableStatement node)
        {
            // CREATE TABLE is not supported
            IsCompatible = false;
        }

        public override void Visit(DropTableStatement node)
        {
            // DROP TABLE is not supported
            IsCompatible = false;
        }
    }
}
