using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace MarkMpn.Sql4Cds.Engine.Visitors
{
    class GroupByValidatingVisitor : TSqlFragmentVisitor
    {
        public Sql4CdsError Error { get; private set; }

        public override void Visit(GroupByClause node)
        {
            if (node.All == true)
                Error = Sql4CdsError.NotSupported(node, "GROUP BY ALL");

            if (node.GroupByOption != GroupByOption.None)
                Error = Sql4CdsError.NotSupported(node, $"GROUP BY {node.GroupByOption}");
        }

        public override void Visit(ScalarSubquery node)
        {
            Error = Sql4CdsError.InvalidAggregateOrSubqueryInGroupByClause(node);
        }

        public override void Visit(FunctionCall node)
        {
            if (AggregateCollectingVisitor.IsAggregate(node))
                Error = Sql4CdsError.InvalidAggregateOrSubqueryInGroupByClause(node);
        }
    }
}
