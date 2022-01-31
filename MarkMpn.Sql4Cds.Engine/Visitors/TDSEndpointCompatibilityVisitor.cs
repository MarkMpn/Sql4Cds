using System;
using System.Collections.Generic;
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
        private readonly IAttributeMetadataCache _metadata;
        private readonly Dictionary<string, string> _tableNames;
        private TSqlFragment _root;

        /// <summary>
        /// Creates a new <see cref="TDSEndpointCompatibilityVisitor"/>
        /// </summary>
        /// <param name="metadata">The metadata cache for the primary data source</param>
        public TDSEndpointCompatibilityVisitor(IAttributeMetadataCache metadata, Dictionary<string, string> outerTableNames = null)
        {
            _metadata = metadata;
            _tableNames = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            if (outerTableNames != null)
            {
                foreach (var kvp in outerTableNames)
                    _tableNames[kvp.Key] = kvp.Value;
            }

            IsCompatible = true;
        }

        public bool IsCompatible { get; private set; }

        public override void Visit(TSqlFragment node)
        {
            if (_root == null)
                _root = node;

            base.Visit(node);
        }

        public override void Visit(NamedTableReference node)
        {
            if (node.SchemaObject.ServerIdentifier != null ||
                node.SchemaObject.DatabaseIdentifier != null ||
                (!String.IsNullOrEmpty(node.SchemaObject.SchemaIdentifier?.Value) && !node.SchemaObject.SchemaIdentifier.Value.Equals("dbo", StringComparison.OrdinalIgnoreCase)))
            {
                // Can't do cross-instance queries
                // No access to metadata schema
                IsCompatible = false;
                return;
            }

            var entity = TryGetEntity(node.SchemaObject.BaseIdentifier.Value);
            if (entity?.DataProviderId != null)
            {
                // No access to entities with custom data providers
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
                        var attribute = TryGetEntity(table)?.Attributes?.SingleOrDefault(a => a.LogicalName.Equals(columnName));

                        if (!AttributeIsSupported(attribute))
                        {
                            IsCompatible = false;
                            return;
                        }
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

        private EntityMetadata TryGetEntity(string logicalname)
        {
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

        public override void Visit(GlobalVariableExpression node)
        {
            if (node.Name.Equals("@@IDENTITY", StringComparison.OrdinalIgnoreCase))
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
                var subVisitor = new TDSEndpointCompatibilityVisitor(_metadata, _tableNames);
                node.Accept(subVisitor);

                if (!subVisitor.IsCompatible)
                    IsCompatible = false;

                return;
            }

            base.Visit(node);
        }

        public override void Visit(QueryDerivedTable node)
        {
            // Name resolution needs to be scoped to the query, so create a new sub-visitor
            if (IsCompatible && _root != node)
            {
                var subVisitor = new TDSEndpointCompatibilityVisitor(_metadata);
                node.Accept(subVisitor);

                if (!subVisitor.IsCompatible)
                    IsCompatible = false;

                return;
            }

            base.Visit(node);
        }

        public override void Visit(SelectStatement node)
        {
            // Name resolution needs to be scoped to the query, so create a new sub-visitor
            if (IsCompatible && _root != node)
            {
                var subVisitor = new TDSEndpointCompatibilityVisitor(_metadata);
                node.Accept(subVisitor);

                if (!subVisitor.IsCompatible)
                    IsCompatible = false;

                return;
            }

            base.Visit(node);
        }

        public override void Visit(QuerySpecification node)
        {
            // Ensure we process the FROM clause first, so we understand what tables are involved before
            // we try to process column names
            if (node.FromClause != null)
                node.FromClause.Accept(this);

            base.Visit(node);
        }
    }
}
