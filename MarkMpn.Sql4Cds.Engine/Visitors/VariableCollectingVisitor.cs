﻿using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Generic;
using System.Linq;

namespace MarkMpn.Sql4Cds.Engine.Visitors
{
    /// <summary>
    /// Collect all the variables used throughout a query
    /// </summary>
    class VariableCollectingVisitor : TSqlFragmentVisitor
    {
        /// <summary>
        /// Returns the list of columns used throughout the query
        /// </summary>
        public IList<VariableReference> Variables { get; } = new List<VariableReference>();

        public IList<GlobalVariableExpression> GlobalVariables { get; } = new List<GlobalVariableExpression>();

        public override void ExplicitVisit(VariableReference node)
        {
            base.ExplicitVisit(node);

            Variables.Add(node);
        }

        public override void ExplicitVisit(GlobalVariableExpression node)
        {
            base.ExplicitVisit(node);

            GlobalVariables.Add(node);
        }
    }
}
