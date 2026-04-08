using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Generic;
using System.Linq;

namespace MarkMpn.Sql4Cds.Engine.Visitors
{
    /// <summary>
    /// Collect all the table call targets used throughout a query
    /// </summary>
    class MultiPartIdentifierCallTargetCollectingVisitor : TSqlFragmentVisitor
    {
        /// <summary>
        /// Returns the list of table call targets used throughout the query
        /// </summary>
        public IList<MultiPartIdentifierCallTarget> CallTargets { get; } = new List<MultiPartIdentifierCallTarget>();

        public override void ExplicitVisit(MultiPartIdentifierCallTarget node)
        {
            base.ExplicitVisit(node);

            CallTargets.Add(node);
        }
    }
}
