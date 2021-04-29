using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace MarkMpn.Sql4Cds.Engine.Visitors
{
    class InSubqueryVisitor : TSqlFragmentVisitor
    {
        public List<InPredicate> InSubqueries { get; } = new List<InPredicate>();

        public override void Visit(InPredicate node)
        {
            base.Visit(node);

            if (node.Subquery != null)
                InSubqueries.Add(node);
        }
    }
}
