using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;

namespace MarkMpn.Sql4Cds.Engine.Visitors
{
    /// <summary>
    /// Checks if the GROUP BY clause and aggregate functions used in a query are valid for conversion directly to FetchXML
    /// </summary>
    class GroupValidationVisitor : TSqlFragmentVisitor
    {
        /// <summary>
        /// Indicates if the query is valid for conversion to FetchXML
        /// </summary>
        public bool Valid { get; private set; } = true;

        /// <summary>
        /// Returns the SQL fragment that was found to make the query not valid for conversion directly to FetchXML
        /// </summary>
        public TSqlFragment InvalidFragment { get; private set; }

        public override void ExplicitVisit(GroupingSpecification node)
        {
            // Check that the GROUP BY clause is valid for conversion to FetchXML.
            // FetchXML can handle grouping by a column or a DATEPART function of a column
            base.ExplicitVisit(node);

            if (!Valid)
                return;

            if (!(node is ExpressionGroupingSpecification exprGroup))
            {
                Valid = false;
                InvalidFragment = node;
                return;
            }

            if (exprGroup.Expression is ColumnReferenceExpression)
                return;

            if (!(exprGroup.Expression is FunctionCall func))
            {
                Valid = false;
                InvalidFragment = node;
                return;
            }

            if (!func.FunctionName.Value.Equals("DATEPART", StringComparison.OrdinalIgnoreCase))
            {
                Valid = false;
                InvalidFragment = node;
                return;
            }

            if (func.Parameters.Count != 2)
            {
                Valid = false;
                InvalidFragment = node;
                return;
            }

            if (!(func.Parameters[1] is ColumnReferenceExpression))
            {
                Valid = false;
                InvalidFragment = node;
                return;
            }
        }

        public override void ExplicitVisit(FunctionCall node)
        {
            // Check that aggregate functions throughout the query (SELECT, HAVING, ORDER BY clauses)
            // are valid for conversion to FetchXML
            // Only functions with a single column reference parameter are valid
            base.ExplicitVisit(node);

            if (!Valid)
                return;

            if (node.FunctionName.Value.Equals("SUM", StringComparison.OrdinalIgnoreCase) ||
                node.FunctionName.Value.Equals("MIN", StringComparison.OrdinalIgnoreCase) ||
                node.FunctionName.Value.Equals("MAX", StringComparison.OrdinalIgnoreCase) ||
                node.FunctionName.Value.Equals("COUNT", StringComparison.OrdinalIgnoreCase) ||
                node.FunctionName.Value.Equals("AVG", StringComparison.OrdinalIgnoreCase))
            {
                if (node.Parameters.Count != 1)
                {
                    Valid = false;
                    InvalidFragment = node;
                    return;
                }

                if (node.Parameters[0] is Literal lit)
                {
                    if (node.FunctionName.Value.Equals("COUNT", StringComparison.OrdinalIgnoreCase) ||
                        node.FunctionName.Value.Equals("SUM", StringComparison.OrdinalIgnoreCase) && lit.Value == "1")
                    {
                        // COUNT(1) and SUM(1) can be translated to COUNT(*)
                        node.Parameters[0] = new ColumnReferenceExpression { ColumnType = ColumnType.Wildcard };
                        node.FunctionName.Value = "COUNT";
                    }
                }

                if (!(node.Parameters[0] is ColumnReferenceExpression))
                {
                    Valid = false;
                    InvalidFragment = node;
                    return;
                }
            }
        }
    }
}
