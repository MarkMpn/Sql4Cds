using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace MarkMpn.Sql4Cds.Engine.Visitors
{
    /// <summary>
    /// Applies a fixed, unique alias to all columns in a SELECT statement so they can be reliably indexed
    /// </summary>
    class AddDefaultColumnAliasesVisitor : TSqlFragmentVisitor
    {
        private int _counter;
        private readonly HashSet<string> _names = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        public override void ExplicitVisit(SelectScalarExpression node)
        {
            base.ExplicitVisit(node);

            if (node.ColumnName == null || !_names.Add(node.ColumnName.Value))
            {
                var baseName = "Expr";

                if (node.Expression is ColumnReferenceExpression col)
                    baseName = col.MultiPartIdentifier.Identifiers.Last().Value;

                var name = baseName + (_counter++);

                while (!_names.Add(name))
                    name = baseName + (_counter++);

                node.ColumnName = new IdentifierOrValueExpression { Identifier = new Identifier { Value = name } };
            }
        }
    }
}
