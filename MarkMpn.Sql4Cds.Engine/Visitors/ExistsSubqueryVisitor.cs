using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace MarkMpn.Sql4Cds.Engine.Visitors
{
    class ExistsSubqueryVisitor : TSqlFragmentVisitor
    {
        public List<ExistsPredicate> ExistsSubqueries { get; } = new List<ExistsPredicate>();

        public override void Visit(ExistsPredicate node)
        {
            base.Visit(node);
            ExistsSubqueries.Add(node);
        }
    }
}
