using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk.Metadata;

namespace MarkMpn.Sql4Cds.Engine.Visitors
{
    class DuplicateTableNameValidatingVisitor : TSqlFragmentVisitor
    {
        private readonly Dictionary<string, NamedTableReference> _tableNames = new Dictionary<string, NamedTableReference>(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, TableReferenceWithAlias> _tableAliases = new Dictionary<string, TableReferenceWithAlias>(StringComparer.OrdinalIgnoreCase);

        public override void ExplicitVisit(QuerySpecification node)
        {
            // Do not recurse into sub-queries, but start a new visitor with its own state
            if (node.FromClause != null)
                node.FromClause.Accept(new DuplicateTableNameValidatingVisitor());
        }

        public override void Visit(NamedTableReference node)
        {
            base.Visit(node);

            if (node.Alias == null)
            {
                if (_tableNames.TryGetValue(node.SchemaObject.BaseIdentifier.Value, out var table))
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.DuplicateTable(node.SchemaObject, table.SchemaObject));

                if (_tableAliases.TryGetValue(node.SchemaObject.BaseIdentifier.Value, out var alias))
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.AliasNameSameAsTableName(alias.Alias, node.SchemaObject.BaseIdentifier));

                _tableNames.Add(node.SchemaObject.BaseIdentifier.Value, node);
            }
        }

        public override void Visit(TableReferenceWithAlias node)
        {
            base.Visit(node);

            if (node.Alias != null)
            {
                if (_tableAliases.ContainsKey(node.Alias.Value))
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.DuplicateAlias(node.Alias));

                if (_tableNames.TryGetValue(node.Alias.Value, out var table))
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.AliasNameSameAsTableName(node.Alias, table.SchemaObject.BaseIdentifier));

                _tableAliases.Add(node.Alias.Value, node);
            }
        }
    }
}
