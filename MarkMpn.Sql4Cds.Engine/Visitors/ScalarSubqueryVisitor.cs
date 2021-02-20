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

        public override void Visit(ScalarSubquery node)
        {
            base.Visit(node);

            Subqueries.Add(node);
        }
    }
}
