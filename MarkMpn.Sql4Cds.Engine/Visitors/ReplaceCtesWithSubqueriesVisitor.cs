using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace MarkMpn.Sql4Cds.Engine.Visitors
{
    /// <summary>
    /// Finds references to non-recursive CTEs and replaces them with subqueries
    /// </summary>
    class ReplaceCtesWithSubqueriesVisitor : TSqlFragmentVisitor
    {
        private Dictionary<string, CommonTableExpression> _cteQueries;

        public ReplaceCtesWithSubqueriesVisitor()
        {
            _cteQueries = new Dictionary<string, CommonTableExpression>(StringComparer.OrdinalIgnoreCase);
        }

        public override void Visit(CommonTableExpression node)
        {
            base.Visit(node);

            _cteQueries[node.ExpressionName.Value] = node;
        }

        public override void Visit(SelectStatement node)
        {
            // Visit the CTEs first
            if (node.WithCtesAndXmlNamespaces != null)
                node.WithCtesAndXmlNamespaces.Accept(this);

            base.Visit(node);

            // Should have visited the CTEs now, so remove them from the query
            node.WithCtesAndXmlNamespaces = null;
        }

        public override void Visit(FromClause node)
        {
            base.Visit(node);

            for (var i = 0; i < node.TableReferences.Count; i++)
            {
                if (node.TableReferences[i] is NamedTableReference ntr &&
                    TryGetCteDefinition(ntr, out var subquery))
                {
                    node.TableReferences[i] = subquery;
                }
            }
        }

        public override void Visit(QualifiedJoin node)
        {
            base.Visit(node);

            if (node.FirstTableReference is NamedTableReference table1 &&
                TryGetCteDefinition(table1, out var subquery1))
            {
                node.FirstTableReference = subquery1;
            }

            if (node.SecondTableReference is NamedTableReference table2 &&
                TryGetCteDefinition(table2, out var subquery2))
            {
                node.SecondTableReference = subquery2;
            }
        }

        public override void Visit(UnqualifiedJoin node)
        {
            base.Visit(node);

            if (node.FirstTableReference is NamedTableReference table1 &&
                TryGetCteDefinition(table1, out var subquery1))
            {
                node.FirstTableReference = subquery1;
            }

            if (node.SecondTableReference is NamedTableReference table2 &&
                TryGetCteDefinition(table2, out var subquery2))
            {
                node.SecondTableReference = subquery2;
            }
        }

        private bool TryGetCteDefinition(NamedTableReference table, out QueryDerivedTable subquery)
        {
            subquery = null;

            if (table.SchemaObject.Identifiers.Count > 1)
                return false;

            if (!_cteQueries.TryGetValue(table.SchemaObject.BaseIdentifier.Value, out var cte))
                return false;

            subquery = new QueryDerivedTable
            {
                Alias = table.Alias ?? cte.ExpressionName,
                QueryExpression = cte.QueryExpression
            };

            foreach (var col in cte.Columns)
                subquery.Columns.Add(col);

            return true;
        }
    }
}
