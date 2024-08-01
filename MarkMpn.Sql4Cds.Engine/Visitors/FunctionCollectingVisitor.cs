using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Generic;
using System.Linq;

namespace MarkMpn.Sql4Cds.Engine.Visitors
{
    /// <summary>
    /// Collect all the functions used throughout a query
    /// </summary>
    class FunctionCollectingVisitor : TSqlFragmentVisitor
    {
        /// <summary>
        /// Returns the list of functions used throughout the query
        /// </summary>
        public IList<FunctionCall> Functions { get; } = new List<FunctionCall>();

        public override void ExplicitVisit(FunctionCall node)
        {
            base.ExplicitVisit(node);

            Functions.Add(node);
        }
    }
}
