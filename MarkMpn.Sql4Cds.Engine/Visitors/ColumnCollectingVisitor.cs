using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Generic;
using System.Linq;

namespace MarkMpn.Sql4Cds.Engine.Visitors
{
    /// <summary>
    /// Collect all the columns used throughout a query
    /// </summary>
    class ColumnCollectingVisitor : TSqlFragmentVisitor
    {
        private readonly ISet<string> _selectAliases = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        private bool _ignoreAliases;

        /// <summary>
        /// Returns the list of columns used throughout the query
        /// </summary>
        public IList<ColumnReferenceExpression> Columns { get; } = new List<ColumnReferenceExpression>();

        public override void ExplicitVisit(ColumnReferenceExpression node)
        {
            base.ExplicitVisit(node);

            if (_ignoreAliases &&
                node.MultiPartIdentifier?.Identifiers.Count == 1 &&
                _selectAliases.Contains(node.MultiPartIdentifier.Identifiers[0].Value))
                return;

            if (!Columns.Contains(node))
                Columns.Add(node);
        }

        public override void ExplicitVisit(FunctionCall node)
        {
            if (node.FunctionName.Value.Equals("DATEDIFF", StringComparison.OrdinalIgnoreCase) ||
                node.FunctionName.Value.Equals("DATEADD", StringComparison.OrdinalIgnoreCase) ||
                node.FunctionName.Value.Equals("DATEPART", StringComparison.OrdinalIgnoreCase))
            {
                // Skip the first parameter as it's not really a column
                foreach (var param in node.Parameters.Skip(1))
                    param.Accept(this);
            }
            else
            {
                base.ExplicitVisit(node);
            }
        }

        public override void ExplicitVisit(SelectScalarExpression node)
        {
            base.ExplicitVisit(node);

            // Keep track of aliases introduced in the SELECT clause so we can ignore them later
            if (!String.IsNullOrEmpty(node.ColumnName?.Identifier?.Value))
                _selectAliases.Add(node.ColumnName.Identifier.Value);
        }

        public override void ExplicitVisit(OrderByClause node)
        {
            _ignoreAliases = true;
            base.ExplicitVisit(node);
            _ignoreAliases = false;
        }

        public override void ExplicitVisit(HavingClause node)
        {
            _ignoreAliases = true;
            base.ExplicitVisit(node);
            _ignoreAliases = false;
        }
    }
}
