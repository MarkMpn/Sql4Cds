using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace MarkMpn.Sql4Cds.Engine.Visitors
{
    /// <summary>
    /// Finds recursive references in a CTE definition and removes them, moving join predicates to the WHERE clause
    /// </summary>
    class RemoveRecursiveCTETableReferencesVisitor : TSqlConcreteFragmentVisitor
    {
        private readonly string _name;
        private readonly string[] _columnNames;
        private readonly Dictionary<string, string> _outerReferences;
        private BooleanExpression _joinPredicate;
        private int _inUnqualifiedJoin;

        public RemoveRecursiveCTETableReferencesVisitor(string name, string[] columnNames, Dictionary<string, string> outerReferences)
        {
            _name = name;
            _columnNames = columnNames;
            _outerReferences = outerReferences;
        }

        private bool IsRecursiveReference(TableReference tableReference)
        {
            if (!(tableReference is NamedTableReference namedTable))
                return false;

            if (namedTable.SchemaObject.Identifiers.Count != 1)
                return false;

            return namedTable.SchemaObject.BaseIdentifier.Value.Equals(_name, StringComparison.OrdinalIgnoreCase);
        }

        private InlineDerivedTable CreateInlineDerivedTable()
        {
            var table = new InlineDerivedTable
            {
                Alias = new Identifier { Value = _name },
                RowValues = { new RowValue() }
            };

            foreach (var col in _columnNames)
            {
                table.Columns.Add(new Identifier { Value = col });
                table.RowValues[0].ColumnValues.Add(new VariableReference { Name = _outerReferences[col] });
            }

            return table;
        }

        private bool RemoveRecursiveJoin(TableReference tableReference, out TableReference removed)
        {
            removed = null;

            if (!(tableReference is JoinTableReference join))
                return false;

            if (IsRecursiveReference(join.FirstTableReference))
            {
                if (_inUnqualifiedJoin > 0)
                {
                    join.FirstTableReference = CreateInlineDerivedTable();
                    return false;
                }

                _joinPredicate = (join as QualifiedJoin)?.SearchCondition;
                removed = join.SecondTableReference;
                return true;
            }

            if (IsRecursiveReference(join.SecondTableReference))
            {
                if (_inUnqualifiedJoin > 0)
                {
                    join.SecondTableReference = CreateInlineDerivedTable();
                    return false;
                }

                _joinPredicate = (join as QualifiedJoin)?.SearchCondition;
                removed = join.FirstTableReference;
                return true;
            }

            return false;
        }

        public override void Visit(FromClause node)
        {
            base.Visit(node);

            for (var i = 0; i < node.TableReferences.Count; i++)
            {
                if (IsRecursiveReference(node.TableReferences[i]))
                {
                    if (_inUnqualifiedJoin > 0)
                        node.TableReferences[i] = CreateInlineDerivedTable();
                    else
                        node.TableReferences.RemoveAt(i);
                }
                else if (RemoveRecursiveJoin(node.TableReferences[i], out var removed))
                {
                    node.TableReferences[i] = removed;
                }
            }
        }

        public override void Visit(QualifiedJoin node)
        {
            base.Visit(node);

            if (RemoveRecursiveJoin(node.FirstTableReference, out var removed))
                node.FirstTableReference = removed;

            if (RemoveRecursiveJoin(node.SecondTableReference, out removed))
                node.SecondTableReference = removed;
        }

        public override void Visit(UnqualifiedJoin node)
        {
            base.Visit(node);

            if (RemoveRecursiveJoin(node.FirstTableReference, out var removed))
                node.FirstTableReference = removed;
        }

        public override void ExplicitVisit(UnqualifiedJoin node)
        {
            node.FirstTableReference.Accept(this);

            _inUnqualifiedJoin++;

            if (RemoveRecursiveJoin(node.SecondTableReference, out var removed))
                node.SecondTableReference = removed;

            node.SecondTableReference.Accept(this);
            _inUnqualifiedJoin--;
        }

        public override void ExplicitVisit(QuerySpecification node)
        {
            base.ExplicitVisit(node);

            if (_joinPredicate != null)
            {
                if (node.WhereClause == null)
                {
                    node.WhereClause = new WhereClause { SearchCondition = _joinPredicate };
                }
                else
                {
                    node.WhereClause.SearchCondition = new BooleanBinaryExpression
                    {
                        FirstExpression = node.WhereClause.SearchCondition,
                        BinaryExpressionType = BooleanBinaryExpressionType.And,
                        SecondExpression = _joinPredicate
                    };
                }
            }
        }
    }
}
