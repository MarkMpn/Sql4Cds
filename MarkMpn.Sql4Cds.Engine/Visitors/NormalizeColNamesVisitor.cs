using System;
using System.Collections.Generic;
using System.Globalization;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace MarkMpn.Sql4Cds.Engine.Visitors
{
    /// <summary>
    /// Replaces column references using an alias with the fully qualified name
    /// </summary>
    class NormalizeColNamesVisitor : RewriteVisitorBase
    {
        private readonly INodeSchema _schema;
        private readonly HashSet<string> _selectAliases;

        public NormalizeColNamesVisitor(INodeSchema schema)
        {
            _schema = schema;
            _selectAliases = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        }

        protected override ScalarExpression ReplaceExpression(ScalarExpression expression, out string name)
        {
            name = null;

            if (expression is ColumnReferenceExpression col &&
                col.ColumnType == ColumnType.Regular)
            {
                var colName = col.GetColumnName();

                if (!_schema.ContainsColumn(colName, out var normalizedColName))
                    return expression;

                if (colName == normalizedColName)
                    return expression;

                var normalizedCol = normalizedColName.ToColumnReference();
                normalizedCol.Collation = col.Collation;
                return normalizedCol;
            }

            return expression;
        }

        protected override BooleanExpression ReplaceExpression(BooleanExpression expression)
        {
            return expression;
        }

        public override void ExplicitVisit(ScalarSubquery node)
        {
            // No not recurse into subqueries
        }

        public override void Visit(SelectScalarExpression node)
        {
            base.Visit(node);

            // Log the names available as aliases in the SELECT clause - we need to treat these differently in the ORDER BY clause
            if (node.ColumnName != null)
                _selectAliases.Add(node.ColumnName.Value);
        }

        public override void ExplicitVisit(ExpressionWithSortOrder node)
        {
            // Sorting by an alias from the SELECT clause with the same name as a field from the underlying data source should
            // sort by the alias in preference. This only applies when the alias is the only part of the sort expression.
            if (node.Expression is ColumnReferenceExpression col &&
                _selectAliases.Contains(col.GetColumnName()))
                return;

            base.ExplicitVisit(node);
        }

        public override void ExplicitVisit(QuerySpecification node)
        {
            // Make sure we visit the SELECT clause first to capture the column aliases before visiting the ORDER BY clause
            foreach (var select in node.SelectElements)
                select.Accept(this);

            base.ExplicitVisit(node);
        }
    }
}
