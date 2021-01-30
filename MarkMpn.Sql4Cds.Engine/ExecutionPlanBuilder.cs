using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.ServiceModel;
using System.Text;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace MarkMpn.Sql4Cds.Engine
{
    public class ExecutionPlanBuilder
    {
        public ExecutionPlanBuilder(IAttributeMetadataCache metadata, bool quotedIdentifiers)
        {
            Metadata = metadata;
            QuotedIdentifiers = quotedIdentifiers;
        }

        /// <summary>
        /// Returns the metadata cache that will be used by this conversion
        /// </summary>
        public IAttributeMetadataCache Metadata { get; set; }

        /// <summary>
        /// Returns or sets a value indicating if SQL will be parsed using quoted identifiers
        /// </summary>
        public bool QuotedIdentifiers { get; set; }

        /// <summary>
        /// Indicates if column comparison conditions are supported
        /// </summary>
        public bool ColumnComparisonAvailable { get; set; }

        public IExecutionPlanNode[] Build(string sql)
        {
            var queries = new List<IExecutionPlanNode>();

            // Parse the SQL DOM
            var dom = new TSql150Parser(QuotedIdentifiers);
            var fragment = dom.Parse(new StringReader(sql), out var errors);

            // Check if there were any parse errors
            if (errors.Count > 0)
                throw new QueryParseException(errors[0]);

            var script = (TSqlScript)fragment;
            var optimizer = new ExecutionPlanOptimizer(Metadata, ColumnComparisonAvailable);

            // Convert each statement in turn to the appropriate query type
            foreach (var batch in script.Batches)
            {
                foreach (var statement in batch.Statements)
                {
                    var index = statement.StartOffset;
                    var length = statement.ScriptTokenStream[statement.LastTokenIndex].Offset + statement.ScriptTokenStream[statement.LastTokenIndex].Text.Length - index;
                    var originalSql = statement.ToSql();

                    IExecutionPlanNode plan;

                    if (statement is SelectStatement select)
                        plan = ConvertSelectStatement(select);
                    /*else if (statement is UpdateStatement update)
                        query = ConvertUpdateStatement(update);
                    else if (statement is DeleteStatement delete)
                        query = ConvertDeleteStatement(delete);
                    else if (statement is InsertStatement insert)
                        query = ConvertInsertStatement(insert);
                    else if (statement is ExecuteAsStatement impersonate)
                        query = ConvertExecuteAsStatement(impersonate);
                    else if (statement is RevertStatement revert)
                        query = ConvertRevertStatement(revert);*/
                    else
                        throw new NotSupportedQueryFragmentException("Unsupported statement", statement);

                    plan.Sql = originalSql;
                    plan.Index = index;
                    plan.Length = length;

                    plan = optimizer.Optimize(plan);
                    queries.Add(plan);
                }
            }

            return queries.ToArray();
        }

        private IExecutionPlanNode ConvertSelectStatement(SelectStatement select)
        {
            if (!(select.QueryExpression is QuerySpecification querySpec))
                throw new NotSupportedQueryFragmentException("Unhandled SELECT query expression", select.QueryExpression);

            // Each table in the FROM clause starts as a separate FetchXmlScan node. Add appropriate join nodes
            // TODO: Handle queries without a FROM clause
            var node = ConvertFromClause(querySpec.FromClause.TableReferences);

            // Add filters from WHERE
            node = ConvertWhereClause(node, querySpec.WhereClause);

            // Add aggregates from GROUP BY/SELECT/HAVING/ORDER BY

            // Add filters from HAVING

            // Add sorts from ORDER BY
            node = ConvertOrderByClause(node, querySpec.OrderByClause);

            // Add TOP

            // Add SELECT
            node = ConvertSelectClause(querySpec.SelectElements, node);

            return node;
        }

        private IExecutionPlanNode ConvertOrderByClause(IExecutionPlanNode source, OrderByClause orderByClause)
        {
            if (orderByClause == null)
                return source;

            var sort = new SortNode { Source = source };
            sort.Sorts.AddRange(orderByClause.OrderByElements);

            return sort;
        }

        private IExecutionPlanNode ConvertWhereClause(IExecutionPlanNode source, WhereClause whereClause)
        {
            if (whereClause == null)
                return source;

            if (whereClause.Cursor != null)
                throw new NotSupportedQueryFragmentException("Unsupported cursor", whereClause.Cursor);

            return new FilterNode
            {
                Filter = whereClause.SearchCondition,
                Source = source
            };
        }

        private IExecutionPlanNode ConvertSelectClause(IList<SelectElement> selectElements, IExecutionPlanNode node)
        {
            var select = new SelectNode
            {
                Source = node
            };

            foreach (var element in selectElements)
            {   
                if (element is SelectScalarExpression scalar)
                {
                    if (scalar.Expression is ColumnReferenceExpression col)
                    {
                        var colName = String.Join(".", col.MultiPartIdentifier.Identifiers.Select(id => id.Value));
                        var alias = scalar.ColumnName?.Value ?? colName;

                        select.ColumnSet.Add(new SelectColumn
                        {
                            SourceColumn = colName,
                            OutputColumn = alias
                        });
                    }
                    else
                    {
                        // TODO: Handle calculations in separate CalculateScalarNode
                        throw new NotSupportedQueryFragmentException("Unsupported SELECT expression", scalar.Expression);
                    }
                }
                else if (element is SelectStarExpression star)
                {
                    var colName = star.Qualifier == null ? null : String.Join(".", star.Qualifier.Identifiers.Select(id => id.Value));

                    select.ColumnSet.Add(new SelectColumn
                    {
                        SourceColumn = colName,
                        AllColumns = true
                    });
                }
            }

            return select;
        }

        private IExecutionPlanNode ConvertFromClause(IList<TableReference> tables)
        {
            var node = ConvertTableReference(tables[0]);

            for (var i = 1; i < tables.Count; i++)
            {
                var nextTable = ConvertTableReference(tables[i]);

                // TODO: See if we can lift a join predicate from the WHERE clause
                nextTable = new TableSpoolNode { Source = nextTable };

                node = new NestedLoopNode { LeftSource = node, RightSource = nextTable };
            }

            return node;
        }

        private IExecutionPlanNode ConvertTableReference(TableReference reference)
        {
            if (reference is NamedTableReference table)
            {
                var entityName = table.SchemaObject.BaseIdentifier.Value;

                try
                {
                    var meta = Metadata[entityName];
                }
                catch (FaultException ex)
                {
                    throw new NotSupportedQueryFragmentException(ex.Message, reference);
                }

                // Convert to a simple FetchXML source
                return new FetchXmlScan
                {
                    FetchXml = new FetchXml.FetchType
                    {
                        Items = new object[]
                        {
                            new FetchXml.FetchEntityType
                            {
                                name = entityName
                            }
                        }
                    },
                    Alias = table.Alias?.Value ?? entityName,
                    ReturnFullSchema = true
                };
            }

            if (reference is QualifiedJoin join)
            {
                // If the join involves the primary key of one table we can safely use a merge join.
                // Otherwise use a nested loop join
                var lhs = ConvertTableReference(join.FirstTableReference);
                var rhs = ConvertTableReference(join.SecondTableReference);
                var lhsSchema = lhs.GetSchema(Metadata);
                var rhsSchema = rhs.GetSchema(Metadata);

                var joinConditionVisitor = new JoinConditionVisitor(lhsSchema, rhsSchema);
                join.SearchCondition.Accept(joinConditionVisitor);

                if (joinConditionVisitor.LhsKey != null && joinConditionVisitor.RhsKey != null)
                {
                    BaseJoinNode joinNode;

                    var lhsKey = joinConditionVisitor.LhsKey.GetColumnName();
                    if (lhsSchema.ContainsColumn(lhsKey, out var lhsNormalizedKey) && lhsNormalizedKey.Equals(lhsSchema.PrimaryKey, StringComparison.OrdinalIgnoreCase))
                    {
                        // Sort by keys before merge join but ignore during FetchXML folding
                        lhs = new SortNode
                        {
                            Source = lhs,
                            Sorts =
                            {
                                new ExpressionWithSortOrder
                                {
                                    Expression = joinConditionVisitor.LhsKey,
                                    SortOrder = SortOrder.Ascending
                                }
                            },
                            IgnoreForFetchXmlFolding = true
                        };

                        rhs = new SortNode
                        {
                            Source = rhs,
                            Sorts =
                            {
                                new ExpressionWithSortOrder
                                {
                                    Expression = joinConditionVisitor.RhsKey,
                                    SortOrder = SortOrder.Ascending
                                }
                            },
                            IgnoreForFetchXmlFolding = true
                        };

                        joinNode = new MergeJoinNode
                        {
                            LeftSource = lhs,
                            LeftAttribute = joinConditionVisitor.LhsKey,
                            RightSource = rhs,
                            RightAttribute = joinConditionVisitor.RhsKey,
                            JoinType = join.QualifiedJoinType,
                            AdditionalJoinCriteria = join.SearchCondition.RemoveCondition(joinConditionVisitor.JoinCondition)
                        };
                    }
                    else if (rhsSchema.ContainsColumn(lhsKey, out var rhsNormalizedKey) && rhsNormalizedKey.Equals(rhsSchema.PrimaryKey, StringComparison.OrdinalIgnoreCase))
                    {
                        // Sort by keys before merge join but ignore during FetchXML folding
                        lhs = new SortNode
                        {
                            Source = lhs,
                            Sorts =
                            {
                                new ExpressionWithSortOrder
                                {
                                    Expression = joinConditionVisitor.LhsKey,
                                    SortOrder = SortOrder.Ascending
                                }
                            },
                            IgnoreForFetchXmlFolding = true
                        };

                        rhs = new SortNode
                        {
                            Source = rhs,
                            Sorts =
                            {
                                new ExpressionWithSortOrder
                                {
                                    Expression = joinConditionVisitor.RhsKey,
                                    SortOrder = SortOrder.Ascending
                                }
                            },
                            IgnoreForFetchXmlFolding = true
                        };

                        joinNode = new MergeJoinNode
                        {
                            LeftSource = rhs,
                            LeftAttribute = joinConditionVisitor.RhsKey,
                            RightSource = lhs,
                            RightAttribute = joinConditionVisitor.LhsKey,
                            JoinType = join.QualifiedJoinType,
                            AdditionalJoinCriteria = join.SearchCondition.RemoveCondition(joinConditionVisitor.JoinCondition)
                        };
                    }
                    else
                    {
                        // TODO: Hash join type that can be folded into FetchXML or implement
                        // many-to-many joins in MergeJoinNode
                        throw new NotImplementedException();
                    }

                    return joinNode;
                }
                else
                {
                    return new NestedLoopNode
                    {
                        LeftSource = lhs,
                        RightSource = rhs,
                        JoinType = join.QualifiedJoinType,
                        JoinCondition = join.SearchCondition
                    };
                }
            }

            throw new NotImplementedException();
        }
    }
}
