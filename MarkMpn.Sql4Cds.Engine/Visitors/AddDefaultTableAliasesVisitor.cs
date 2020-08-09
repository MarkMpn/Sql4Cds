using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace MarkMpn.Sql4Cds.Engine.Visitors
{
    /// <summary>
    /// Works around a bug in the preview T-SQL endpoint by applying an alias to all table references that
    /// don't already have one, so that the alias matches the original table name after the query has been
    /// rewritten by CDS
    /// </summary>
    class AddDefaultTableAliasesVisitor : TSqlFragmentVisitor
    {
        public override void ExplicitVisit(NamedTableReference node)
        {
            base.ExplicitVisit(node);

            if (node.Alias == null)
                node.Alias = new Identifier { Value = node.SchemaObject.BaseIdentifier.Value };
        }
    }
}
