using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Generic;
using System.Linq;

namespace MarkMpn.Sql4Cds.Engine.Visitors
{
    /// <summary>
    /// Collect all the parameterless calls used throughout a query
    /// </summary>
    class ParameterlessCollectingVisitor : TSqlFragmentVisitor
    {
        /// <summary>
        /// Returns the list of parameterless functions used throughout the query
        /// </summary>
        public IList<ParameterlessCall> ParameterlessCalls { get; } = new List<ParameterlessCall>();

        public override void ExplicitVisit(ParameterlessCall node)
        {
            base.ExplicitVisit(node);

            ParameterlessCalls.Add(node);
        }
    }
}
