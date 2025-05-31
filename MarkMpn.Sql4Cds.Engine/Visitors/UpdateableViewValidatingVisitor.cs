using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace MarkMpn.Sql4Cds.Engine.Visitors
{
    /// <summary>
    /// Checks if a query defined table used in a DML query follows the rules of an updateable view
    /// https://learn.microsoft.com/en-us/sql/t-sql/statements/create-view-transact-sql?view=sql-server-ver16#updatable-views
    /// </summary>
    class UpdateableViewValidatingVisitor : TSqlFragmentVisitor
    {
        private readonly UpdateableViewModificationType _modificationType;
        private QueryDerivedTable _subquery;
        private NamedTableReference _namedTable;

        public UpdateableViewValidatingVisitor(UpdateableViewModificationType modificationType)
        {
            _modificationType = modificationType;
        }

        public NamedTableReference Target => _namedTable;

        public override void Visit(QueryDerivedTable node)
        {
            if (_subquery == null)
                _subquery = node;

            base.Visit(node);
        }

        public override void Visit(FunctionCall node)
        {
            if (AggregateCollectingVisitor.IsAggregate(node))
                throw new NotSupportedQueryFragmentException(Sql4CdsError.DerivedTableContainsAggregatesDistinctOrGroupBy(_subquery));

            base.Visit(node);
        }

        public override void Visit(BinaryQueryExpression node)
        {
            switch (node.BinaryQueryExpressionType)
            {
                case BinaryQueryExpressionType.Union:
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.DerivedTableContainsUnion(_subquery));

                default:
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.DerivedTableContainsAggregatesDistinctOrGroupBy(_subquery));
            }
        }

        public override void Visit(NamedTableReference node)
        {
            if (_namedTable != null && _modificationType == UpdateableViewModificationType.Delete)
                throw new NotSupportedQueryFragmentException(Sql4CdsError.DerivedTableAffectsMultipleTables(_subquery));

            _namedTable = node;

            base.Visit(node);
        }

        public override void Visit(QuerySpecification node)
        {
            if (node.UniqueRowFilter == UniqueRowFilter.Distinct)
                throw new NotSupportedQueryFragmentException(Sql4CdsError.DerivedTableContainsAggregatesDistinctOrGroupBy(_subquery));

            base.Visit(node);
        }

        public override void Visit(GroupByClause node)
        {
            throw new NotSupportedQueryFragmentException(Sql4CdsError.DerivedTableContainsAggregatesDistinctOrGroupBy(_subquery));
        }
    }

    enum UpdateableViewModificationType
    {
        Insert,
        Update,
        Delete
    }
}
