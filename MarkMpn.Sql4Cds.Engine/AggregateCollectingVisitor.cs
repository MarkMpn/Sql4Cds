using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Generic;
using System.Linq;

namespace MarkMpn.Sql4Cds.Engine
{
    class AggregateCollectingVisitor : TSqlFragmentVisitor
    {
        private ISet<string> _aggregates = new HashSet<string>();

        public IList<SelectScalarExpression> SelectAggregates { get; } = new List<SelectScalarExpression>();

        public IList<FunctionCall> Aggregates { get; } = new List<FunctionCall>();

        public void GetAggregates(QuerySpecification querySpec)
        {
            // Extract the aggregates used directly in the SELECT clause separately to keep the associated aliases
            foreach (var element in querySpec.SelectElements.OfType<SelectScalarExpression>())
            {
                if (element.Expression is FunctionCall func && IsAggregate(func))
                {
                    SelectAggregates.Add(element);
                    _aggregates.Add(Serialize(func));
                }
            }

            // Then handle the rest of the query
            querySpec.Accept(this);
        }

        public override void ExplicitVisit(FunctionCall node)
        {
            base.ExplicitVisit(node);

            if (IsAggregate(node) && _aggregates.Add(Serialize(node)))
                Aggregates.Add(node);
        }

        public override void ExplicitVisit(SelectElement node)
        {
            // Do not recuse into SELECT clauses as we've already handled them separately
        }

        private bool IsAggregate(FunctionCall func)
        {
            if (func.FunctionName.Value.Equals("SUM", StringComparison.OrdinalIgnoreCase) ||
                func.FunctionName.Value.Equals("MIN", StringComparison.OrdinalIgnoreCase) ||
                func.FunctionName.Value.Equals("MAX", StringComparison.OrdinalIgnoreCase) ||
                func.FunctionName.Value.Equals("COUNT", StringComparison.OrdinalIgnoreCase) ||
                func.FunctionName.Value.Equals("AVG", StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }

            return false;
        }

        private string Serialize(ScalarExpression expr)
        {
            new Sql150ScriptGenerator().GenerateScript(expr, out var sql);
            return sql;
        }
    }
}
