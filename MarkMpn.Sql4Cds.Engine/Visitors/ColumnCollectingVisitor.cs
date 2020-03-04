using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Generic;
using System.Linq;

namespace MarkMpn.Sql4Cds.Engine
{
    /// <summary>
    /// Collect all the columns used throughout a query
    /// </summary>
    class ColumnCollectingVisitor : TSqlFragmentVisitor
    {
        /// <summary>
        /// Returns the list of columns used throughout the query
        /// </summary>
        public IList<ColumnReferenceExpression> Columns { get; } = new List<ColumnReferenceExpression>();

        public override void ExplicitVisit(ColumnReferenceExpression node)
        {
            base.ExplicitVisit(node);

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
    }
}
