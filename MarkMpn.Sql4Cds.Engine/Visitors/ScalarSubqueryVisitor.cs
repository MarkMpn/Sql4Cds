using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace MarkMpn.Sql4Cds.Engine.Visitors
{
    class ScalarSubqueryVisitor : TSqlFragmentVisitor
    {
        public List<ScalarSubquery> Subqueries { get; } = new List<ScalarSubquery>();

        public override void ExplicitVisit(ScalarSubquery node)
        {
            Subqueries.Add(node);
        }

        public override void ExplicitVisit(GroupByClause node)
        {
            // Subqueries aren't allowed in the GROUP BY clause - don't collect them so they are still present to produce validation errors later
        }
    }
}
