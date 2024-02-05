using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Xml;
using Wmhelp.XPath2;
using Wmhelp.XPath2.AST;

namespace MarkMpn.Sql4Cds.Engine.Visitors
{
    /// <summary>
    /// Collect all the columns used throughout a query
    /// </summary>
    class ColumnCollectingVisitor : TSqlFragmentVisitor
    {
        private readonly ISet<string> _selectAliases = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        private bool _ignoreAliases;

        /// <summary>
        /// Returns the list of columns used throughout the query
        /// </summary>
        public IList<ColumnReferenceExpression> Columns { get; } = new List<ColumnReferenceExpression>();

        public override void ExplicitVisit(ColumnReferenceExpression node)
        {
            base.ExplicitVisit(node);

            if (_ignoreAliases &&
                node.MultiPartIdentifier?.Identifiers.Count == 1 &&
                _selectAliases.Contains(node.MultiPartIdentifier.Identifiers[0].Value))
                return;

            if (!Columns.Contains(node))
                Columns.Add(node);
        }

        public override void ExplicitVisit(FunctionCall node)
        {
            if (node.CallTarget != null)
            {
                if (node.FunctionName.Value.Equals("query", StringComparison.OrdinalIgnoreCase) ||
                    node.FunctionName.Value.Equals("value", StringComparison.OrdinalIgnoreCase))
                {
                    // XQuery can contain column references in the sql:column function. Parse the XQuery to check
                    if (node.Parameters.Count > 0 && node.Parameters[0] is StringLiteral xquery)
                    {
                        try
                        {
                            var nt = new XmlNamespaceManager(new NameTable());
                            nt.AddNamespace("sql", "https://markcarrington.dev/sql-4-cds");
                            var compiled = XPath2Expression.Compile(xquery.Value, nt);
                            compiled.ExpressionTree.TraverseSubtree(n =>
                            {
                                // FuncNode doesn't expose the details of the function it's bound to, so assume it's sql:column
                                // so long as it's got one literal string parameter.
                                if (n is FuncNode f &&
                                    f.Count == 1 &&
                                    f[0] is ValueNode v &&
                                    v.Content is string col)
                                    Columns.Add(col.ToColumnReference());
                            });
                        }
                        catch
                        {
                            // Ignore any errors parsing the XQuery
                        }
                    }
                }

                base.ExplicitVisit(node);
            }
            else
            {
                if (node.FunctionName.Value.Equals("DATEDIFF", StringComparison.OrdinalIgnoreCase) ||
                    node.FunctionName.Value.Equals("DATEADD", StringComparison.OrdinalIgnoreCase) ||
                    node.FunctionName.Value.Equals("DATEPART", StringComparison.OrdinalIgnoreCase))
                {
                    // Skip the first parameter as it's not really a column
                    foreach (var param in node.Parameters.Skip(1))
                        param.Accept(this);
                }
                else
                {
                    base.ExplicitVisit(node);
                }
            }
        }

        public override void ExplicitVisit(SelectScalarExpression node)
        {
            base.ExplicitVisit(node);

            // Keep track of aliases introduced in the SELECT clause so we can ignore them later
            if (!String.IsNullOrEmpty(node.ColumnName?.Value))
                _selectAliases.Add(node.ColumnName.Value);
        }

        public override void ExplicitVisit(OrderByClause node)
        {
            _ignoreAliases = true;
            base.ExplicitVisit(node);
            _ignoreAliases = false;
        }

        public override void ExplicitVisit(HavingClause node)
        {
            _ignoreAliases = true;
            base.ExplicitVisit(node);
            _ignoreAliases = false;
        }
    }
}
