using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.ServiceModel;
using System.Text;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using MarkMpn.Sql4Cds.Engine.Visitors;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Metadata;

namespace MarkMpn.Sql4Cds.Engine
{
    public class ExecutionPlanBuilder
    {
        private int _colNameCounter;

        public ExecutionPlanBuilder(IAttributeMetadataCache metadata, ITableSizeCache tableSize, IQueryExecutionOptions options)
        {
            Metadata = metadata;
            TableSize = tableSize;
            Options = options;
        }

        /// <summary>
        /// Returns the metadata cache that will be used by this conversion
        /// </summary>
        public IAttributeMetadataCache Metadata { get; set; }

        /// <summary>
        /// Returns the size of each table
        /// </summary>
        public ITableSizeCache TableSize { get; set; }

        /// <summary>
        /// Returns or sets a value indicating if SQL will be parsed using quoted identifiers
        /// </summary>
        public bool QuotedIdentifiers { get; set; }

        /// <summary>
        /// Indicates how the query will be executed
        /// </summary>
        public IQueryExecutionOptions Options { get; set; }

        /// <summary>
        /// Indicates if the TDS Endpoint is available to use if necessary
        /// </summary>
        public bool TDSEndpointAvailable { get; set; }

        public IRootExecutionPlanNode[] Build(string sql)
        {
            var queries = new List<IRootExecutionPlanNode>();

            // Parse the SQL DOM
            var dom = new TSql150Parser(QuotedIdentifiers);
            var fragment = dom.Parse(new StringReader(sql), out var errors);

            // Check if there were any parse errors
            if (errors.Count > 0)
                throw new QueryParseException(errors[0]);

            var script = (TSqlScript)fragment;
            var optimizer = new ExecutionPlanOptimizer(Metadata, Options);

            // Convert each statement in turn to the appropriate query type
            foreach (var batch in script.Batches)
            {
                foreach (var statement in batch.Statements)
                {
                    var index = statement.StartOffset;
                    var length = statement.ScriptTokenStream[statement.LastTokenIndex].Offset + statement.ScriptTokenStream[statement.LastTokenIndex].Text.Length - index;
                    var originalSql = statement.ToSql();

                    IRootExecutionPlanNode plan;

                    if (statement is SelectStatement select)
                        plan = ConvertSelectStatement(select);
                    else if (statement is UpdateStatement update)
                        plan = ConvertUpdateStatement(update);
                    /*else if (statement is DeleteStatement delete)
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

                    SetParent(plan);
                    plan = optimizer.Optimize(plan);
                    queries.Add(plan);
                }
            }

            return queries.ToArray();
        }

        private void SetParent(IExecutionPlanNode plan)
        {
            foreach (var child in plan.GetSources())
            {
                child.Parent = plan;
                SetParent(child);
            }
        }

        private UpdateNode ConvertUpdateStatement(UpdateStatement update)
        {
            if (update.WithCtesAndXmlNamespaces != null)
                throw new NotSupportedQueryFragmentException("Unsupported CTE clause", update.WithCtesAndXmlNamespaces);

            return ConvertUpdateStatement(update.UpdateSpecification, update.OptimizerHints);
        }

        private UpdateNode ConvertUpdateStatement(UpdateSpecification update, IList<OptimizerHint> hints)
        {
            if (update.OutputClause != null)
                throw new NotSupportedQueryFragmentException("Unsupported OUTPUT clause", update.OutputClause);

            if (update.OutputIntoClause != null)
                throw new NotSupportedQueryFragmentException("Unsupported OUTPUT INTO clause", update.OutputIntoClause);

            if (!(update.Target is NamedTableReference target))
                throw new NotSupportedQueryFragmentException("Unsupported UPDATE target", update.Target);

            if (update.WhereClause == null && Options.BlockUpdateWithoutWhere)
            {
                throw new NotSupportedQueryFragmentException("UPDATE without WHERE is blocked by your settings", update)
                {
                    Suggestion = "Add a WHERE clause to limit the records that will be affected by the update, or disable the \"Prevent UPDATE without WHERE\" option in the settings window"
                };
            }

            // Create the SELECT statement that generates the required information
            var queryExpression = new QuerySpecification
            {
                FromClause = update.FromClause ?? new FromClause { TableReferences = { target } },
                WhereClause = update.WhereClause,
                UniqueRowFilter = UniqueRowFilter.Distinct,
                TopRowFilter = update.TopRowFilter
            };

            var updateTarget = new UpdateTargetVisitor(target.SchemaObject.BaseIdentifier.Value);
            queryExpression.FromClause.Accept(updateTarget);

            if (String.IsNullOrEmpty(updateTarget.TargetEntityName))
                throw new NotSupportedQueryFragmentException("Target table not found in FROM clause", target);

            if (updateTarget.Ambiguous)
                throw new NotSupportedQueryFragmentException("Target table name is ambiguous", target);

            var targetAlias = updateTarget.TargetAliasName ?? updateTarget.TargetEntityName;
            var targetLogicalName = updateTarget.TargetEntityName;

            var targetMetadata = Metadata[targetLogicalName];
            queryExpression.SelectElements.Add(new SelectScalarExpression
            {
                Expression = new ColumnReferenceExpression
                {
                    MultiPartIdentifier = new MultiPartIdentifier
                    {
                        Identifiers =
                        {
                            new Identifier { Value = targetAlias },
                            new Identifier { Value = targetMetadata.PrimaryIdAttribute }
                        }
                    }
                },
                ColumnName = new IdentifierOrValueExpression
                {
                    Identifier = new Identifier { Value = targetMetadata.PrimaryIdAttribute }
                }
            });

            var attributeNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            foreach (var set in update.SetClauses)
            {
                if (!(set is AssignmentSetClause assignment))
                    throw new NotSupportedQueryFragmentException("Unhandled SET clause", set);

                if (assignment.Variable != null)
                    throw new NotSupportedQueryFragmentException("Unhandled variable SET clause", set);

                switch (assignment.AssignmentKind)
                {
                    case AssignmentKind.AddEquals:
                        assignment.NewValue = new BinaryExpression { FirstExpression = assignment.Column, BinaryExpressionType = BinaryExpressionType.Add, SecondExpression = assignment.NewValue };
                        break;

                    case AssignmentKind.BitwiseAndEquals:
                        assignment.NewValue = new BinaryExpression { FirstExpression = assignment.Column, BinaryExpressionType = BinaryExpressionType.BitwiseAnd, SecondExpression = assignment.NewValue };
                        break;

                    case AssignmentKind.BitwiseOrEquals:
                        assignment.NewValue = new BinaryExpression { FirstExpression = assignment.Column, BinaryExpressionType = BinaryExpressionType.BitwiseOr, SecondExpression = assignment.NewValue };
                        break;

                    case AssignmentKind.BitwiseXorEquals:
                        assignment.NewValue = new BinaryExpression { FirstExpression = assignment.Column, BinaryExpressionType = BinaryExpressionType.BitwiseXor, SecondExpression = assignment.NewValue };
                        break;

                    case AssignmentKind.DivideEquals:
                        assignment.NewValue = new BinaryExpression { FirstExpression = assignment.Column, BinaryExpressionType = BinaryExpressionType.Divide, SecondExpression = assignment.NewValue };
                        break;

                    case AssignmentKind.ModEquals:
                        assignment.NewValue = new BinaryExpression { FirstExpression = assignment.Column, BinaryExpressionType = BinaryExpressionType.Modulo, SecondExpression = assignment.NewValue };
                        break;

                    case AssignmentKind.MultiplyEquals:
                        assignment.NewValue = new BinaryExpression { FirstExpression = assignment.Column, BinaryExpressionType = BinaryExpressionType.Multiply, SecondExpression = assignment.NewValue };
                        break;

                    case AssignmentKind.SubtractEquals:
                        assignment.NewValue = new BinaryExpression { FirstExpression = assignment.Column, BinaryExpressionType = BinaryExpressionType.Subtract, SecondExpression = assignment.NewValue };
                        break;
                }

                // Validate the target attribute
                var targetAttrName = assignment.Column.MultiPartIdentifier.Identifiers.Last().Value;
                var targetAttribute = targetMetadata.Attributes.SingleOrDefault(attr => attr.LogicalName.Equals(targetAttrName));

                if (targetAttribute == null)
                    throw new NotSupportedQueryFragmentException("Unknown column name", assignment.Column);

                if (targetAttribute.IsValidForUpdate == false)
                    throw new NotSupportedQueryFragmentException("Column cannot be updated", assignment.Column);

                if (!attributeNames.Add(targetAttribute.LogicalName))
                    throw new NotSupportedQueryFragmentException("Duplicate column name", assignment.Column);

                queryExpression.SelectElements.Add(new SelectScalarExpression { Expression = assignment.NewValue, ColumnName = new IdentifierOrValueExpression { Identifier = new Identifier { Value = targetAttribute.LogicalName } } });
            }

            var selectStatement = new SelectStatement { QueryExpression = queryExpression };

            foreach (var hint in hints)
                selectStatement.OptimizerHints.Add(hint);

            var source = ConvertSelectStatement(selectStatement);

            // Add UPDATE
            var updateNode = ConvertSetClause(update.SetClauses, source, targetLogicalName, targetAlias);

            return updateNode;
        }

        private UpdateNode ConvertSetClause(IList<SetClause> setClauses, IExecutionPlanNode node, string targetLogicalName, string targetAlias)
        {
            var targetMetadata = Metadata[targetLogicalName];
            var update = new UpdateNode
            {
                LogicalName = targetMetadata.LogicalName
            };

            if (node is SelectNode select)
            {
                update.Source = select.Source;
                update.PrimaryIdSource = $"{targetAlias}.{targetMetadata.PrimaryIdAttribute}";

                var schema = select.Source.GetSchema(Metadata, null);

                foreach (var assignment in setClauses.Cast<AssignmentSetClause>())
                {
                    // Validate the type conversion
                    var targetAttrName = assignment.Column.MultiPartIdentifier.Identifiers.Last().Value;
                    var targetAttribute = targetMetadata.Attributes.Single(attr => attr.LogicalName.Equals(targetAttrName));

                    var sourceColName = select.ColumnSet.Single(col => col.OutputColumn == targetAttribute.LogicalName).SourceColumn;
                    var sourceCol = sourceColName.ToColumnReference();
                    var sourceType = sourceCol.GetType(schema, null);
                    var targetType = targetAttribute.GetAttributeType();

                    if (!SqlTypeConverter.CanChangeType(sourceType, targetType))
                        throw new NotSupportedQueryFragmentException($"Cannot convert value of type {sourceType} to {targetType}", assignment);

                    if (update.ColumnMappings.ContainsKey(targetAttribute.LogicalName))
                        throw new NotSupportedQueryFragmentException("Duplicate target column", assignment.Column);

                    // Normalize the column name
                    schema.ContainsColumn(sourceColName, out sourceColName);
                    update.ColumnMappings[targetAttribute.LogicalName] = sourceColName;
                }
            }
            else
            {
                update.Source = node;
                update.PrimaryIdSource = targetMetadata.PrimaryIdAttribute;

                foreach (var assignment in setClauses.Cast<AssignmentSetClause>())
                {
                    var targetAttrName = assignment.Column.MultiPartIdentifier.Identifiers.Last().Value;
                    var targetAttribute = targetMetadata.Attributes.Single(attr => attr.LogicalName.Equals(targetAttrName));

                    update.ColumnMappings[targetAttribute.LogicalName] = targetAttribute.LogicalName;
                }
            }

            // If any of the updates are for a polymorphic lookup field, make sure we've got an update for the associated type field too
            foreach (var assignment in setClauses.Cast<AssignmentSetClause>())
            {
                var targetAttrName = assignment.Column.MultiPartIdentifier.Identifiers.Last().Value;
                var targetLookupAttribute = targetMetadata.Attributes.Single(attr => attr.LogicalName.Equals(targetAttrName)) as LookupAttributeMetadata;

                if (targetLookupAttribute == null)
                    continue;

                if (targetLookupAttribute.Targets.Length > 1 && !update.ColumnMappings.ContainsKey(targetAttrName + "type"))
                {
                    // Check we're not just setting the lookup column to null - no need to set the corresponding type then
                    if (assignment.NewValue is NullLiteral)
                        continue;

                    throw new NotSupportedQueryFragmentException("Updating a polymorphic lookup field requires setting the associated type column as well", assignment.Column)
                    {
                        Suggestion = $"Add a SET clause for the {targetLookupAttribute.LogicalName}type column and set it to one of the following values:\r\n{String.Join("\r\n", targetLookupAttribute.Targets.Select(t => $"* {t}"))}"
                    };
                }
            }

            return update;
        }

        private IRootExecutionPlanNode ConvertSelectStatement(SelectStatement select)
        {
            if (Options.UseTDSEndpoint)
            {
                select.ScriptTokenStream = null;
                return new SqlNode { Sql = select.ToSql() };
            }

            if (select.ComputeClauses != null && select.ComputeClauses.Count > 0)
                throw new NotSupportedQueryFragmentException("Unsupported COMPUTE clause", select.ComputeClauses[0]);

            if (select.Into != null)
                throw new NotSupportedQueryFragmentException("Unsupported INTO clause", select.Into);

            if (select.On != null)
                throw new NotSupportedQueryFragmentException("Unsupported ON clause", select.On);

            if (select.WithCtesAndXmlNamespaces != null)
                throw new NotSupportedQueryFragmentException("Unsupported CTE clause", select.WithCtesAndXmlNamespaces);

            return ConvertSelectStatement(select.QueryExpression, select.OptimizerHints, null, null, null);
        }

        private SelectNode ConvertSelectStatement(QueryExpression query, IList<OptimizerHint> hints, NodeSchema outerSchema, Dictionary<string,string> outerReferences, IDictionary<string, Type> parameterTypes)
        {
            if (query is QuerySpecification querySpec)
                return ConvertSelectQuerySpec(querySpec, hints, outerSchema, outerReferences, parameterTypes);

            if (query is BinaryQueryExpression binary)
                return ConvertBinaryQuery(binary, hints, outerSchema, outerReferences, parameterTypes);

            throw new NotSupportedQueryFragmentException("Unhandled SELECT query expression", query);
        }

        private SelectNode ConvertBinaryQuery(BinaryQueryExpression binary, IList<OptimizerHint> hints, NodeSchema outerSchema, Dictionary<string, string> outerReferences, IDictionary<string, Type> parameterTypes)
        {
            if (binary.BinaryQueryExpressionType != BinaryQueryExpressionType.Union)
                throw new NotSupportedQueryFragmentException($"Unhandled {binary.BinaryQueryExpressionType} query type", binary);

            if (binary.ForClause != null)
                throw new NotSupportedQueryFragmentException("Unhandled FOR clause", binary.ForClause);

            var left = ConvertSelectStatement(binary.FirstQueryExpression, hints, outerSchema, outerReferences, parameterTypes);
            var right = ConvertSelectStatement(binary.SecondQueryExpression, hints, outerSchema, outerReferences, parameterTypes);

            var concat = left.Source as ConcatenateNode;

            if (concat == null)
            {
                concat = new ConcatenateNode();

                concat.Sources.Add(left.Source);

                foreach (var col in left.ColumnSet)
                {
                    concat.ColumnSet.Add(new ConcatenateColumn
                    {
                        OutputColumn = col.OutputColumn,
                        SourceColumns = { col.SourceColumn }
                    });
                }
            }

            concat.Sources.Add(right.Source);

            if (concat.ColumnSet.Count != right.ColumnSet.Count)
                throw new NotSupportedQueryFragmentException("UNION must have the same number of columns in each query", binary);

            for (var i = 0; i < concat.ColumnSet.Count; i++)
                concat.ColumnSet[i].SourceColumns.Add(right.ColumnSet[i].SourceColumn);

            var node = (IDataExecutionPlanNode)concat;

            if (!binary.All)
            {
                var distinct = new DistinctNode { Source = node };
                distinct.Columns.AddRange(concat.ColumnSet.Select(col => col.OutputColumn));
                node = distinct;
            }

            node = ConvertOrderByClause(node, hints, binary.OrderByClause, concat.ColumnSet.Select(col => new ColumnReferenceExpression { MultiPartIdentifier = new MultiPartIdentifier { Identifiers = { new Identifier { Value = col.OutputColumn } } } }).ToArray(), binary, parameterTypes, outerSchema, outerReferences);
            node = ConvertOffsetClause(node, binary.OffsetClause, parameterTypes);

            var select = new SelectNode { Source = node };
            select.ColumnSet.AddRange(concat.ColumnSet.Select(col => new SelectColumn { SourceColumn = col.OutputColumn, OutputColumn = col.OutputColumn }));

            return select;
        }

        private SelectNode ConvertSelectQuerySpec(QuerySpecification querySpec, IList<OptimizerHint> hints, NodeSchema outerSchema, Dictionary<string,string> outerReferences, IDictionary<string, Type> parameterTypes)
        {
            // Each table in the FROM clause starts as a separate FetchXmlScan node. Add appropriate join nodes
            // TODO: Handle queries without a FROM clause
            var node = ConvertFromClause(querySpec.FromClause.TableReferences, hints, querySpec, outerSchema, outerReferences, parameterTypes);

            node = ConvertInSubqueries(node, hints, querySpec, parameterTypes, outerSchema, outerReferences);
            node = ConvertExistsSubqueries(node, hints, querySpec, parameterTypes, outerSchema, outerReferences);

            // Add filters from WHERE
            node = ConvertWhereClause(node, hints, querySpec.WhereClause, outerSchema, outerReferences, parameterTypes, querySpec);

            // Add aggregates from GROUP BY/SELECT/HAVING/ORDER BY
            node = ConvertGroupByAggregates(node, querySpec, parameterTypes, outerSchema, outerReferences);

            // Add filters from HAVING
            node = ConvertHavingClause(node, hints, querySpec.HavingClause, parameterTypes, outerSchema, outerReferences, querySpec);

            // Add sorts from ORDER BY
            var selectFields = new List<ScalarExpression>();
            var preOrderSchema = node.GetSchema(Metadata, parameterTypes);
            foreach (var el in querySpec.SelectElements)
            {
                if (el is SelectScalarExpression expr)
                {
                    selectFields.Add(expr.Expression);
                }
                else if (el is SelectStarExpression star)
                {
                    foreach (var field in preOrderSchema.Schema.Keys.OrderBy(f => f))
                    {
                        if (star.Qualifier == null || field.StartsWith(String.Join(".", star.Qualifier.Identifiers.Select(id => id.Value)) + "."))
                        {
                            var colRef = new ColumnReferenceExpression { MultiPartIdentifier = new MultiPartIdentifier() };
                            foreach (var part in field.Split('.'))
                                colRef.MultiPartIdentifier.Identifiers.Add(new Identifier { Value = part });

                            selectFields.Add(colRef);
                        }
                    }
                }
            }

            node = ConvertOrderByClause(node, hints, querySpec.OrderByClause, selectFields.ToArray(), querySpec, parameterTypes, outerSchema, outerReferences);

            // Add DISTINCT
            var distinct = querySpec.UniqueRowFilter == UniqueRowFilter.Distinct ? new DistinctNode { Source = node } : null;
            node = distinct ?? node;

            // Add TOP/OFFSET
            if (querySpec.TopRowFilter != null && querySpec.OffsetClause != null)
                throw new NotSupportedQueryFragmentException("A TOP can not be used in the same query or sub-query as a OFFSET.", querySpec.TopRowFilter);

            node = ConvertTopClause(node, querySpec.TopRowFilter, parameterTypes);
            node = ConvertOffsetClause(node, querySpec.OffsetClause, parameterTypes);

            // Add SELECT
            var selectNode = ConvertSelectClause(querySpec.SelectElements, hints, node, distinct, querySpec, parameterTypes, outerSchema, outerReferences);

            return selectNode;
        }

        private IDataExecutionPlanNode ConvertInSubqueries(IDataExecutionPlanNode source, IList<OptimizerHint> hints, TSqlFragment query, IDictionary<string, Type> parameterTypes, NodeSchema outerSchema, IDictionary<string,string> outerReferences)
        {
            var visitor = new InSubqueryVisitor();
            query.Accept(visitor);

            if (visitor.InSubqueries.Count == 0)
                return source;

            var computeScalar = source as ComputeScalarNode;
            var rewrites = new Dictionary<BooleanExpression, BooleanExpression>();
            var schema = source.GetSchema(Metadata, parameterTypes);

            foreach (var inSubquery in visitor.InSubqueries)
            {
                // Each query of the format "col1 IN (SELECT col2 FROM source)" becomes a left outer join:
                // LEFT JOIN source ON col1 = col2
                // and the result is col2 IS NOT NULL

                // Ensure the left hand side is a column
                if (!(inSubquery.Expression is ColumnReferenceExpression lhsCol))
                {
                    if (computeScalar == null)
                    {
                        computeScalar = new ComputeScalarNode { Source = source };
                        source = computeScalar;
                    }

                    var alias = $"Expr{++_colNameCounter}";
                    computeScalar.Columns[alias] = inSubquery.Expression;
                    lhsCol = alias.ToColumnReference();
                }

                var parameters = parameterTypes == null ? new Dictionary<string, Type>() : new Dictionary<string, Type>(parameterTypes);
                var references = new Dictionary<string, string>();
                var innerQuery = ConvertSelectStatement(inSubquery.Subquery.QueryExpression, hints, schema, references, parameters);

                // Create the join
                BaseJoinNode join;
                var testColumn = innerQuery.ColumnSet[0].SourceColumn;

                if (references.Count == 0)
                {
                    if (UseMergeJoin(source, innerQuery.Source, references, testColumn, lhsCol.GetColumnName(), out var outputCol, out var merge))
                    {
                        testColumn = outputCol;
                        join = merge;
                    }
                    else
                    {
                        // We need the inner list to be distinct to avoid creating duplicates during the join
                        var innerSchema = innerQuery.Source.GetSchema(Metadata, parameters);
                        if (innerQuery.ColumnSet[0].SourceColumn != innerSchema.PrimaryKey && !(innerQuery.Source is DistinctNode))
                        {
                            innerQuery.Source = new DistinctNode
                            {
                                Source = innerQuery.Source,
                                Columns = { innerQuery.ColumnSet[0].SourceColumn }
                            };
                        }

                        // This isn't a correlated subquery, so we can use a foldable join type
                        join = new MergeJoinNode
                        {
                            LeftSource = source,
                            LeftAttribute = lhsCol,
                            RightSource = innerQuery.Source,
                            RightAttribute = innerQuery.ColumnSet[0].SourceColumn.ToColumnReference()
                        };
                    }
                }
                else
                {
                    // We need to use nested loops for correlated subqueries
                    // TODO: We could use a hash join where there is a simple correlation, but followed by a distinct node to eliminate duplicates
                    // We could also move the correlation criteria out of the subquery and into the join condition. We would then make one request to
                    // get all the related records and spool that in memory to get the relevant results in the nested loop. Need to understand how 
                    // many rows are likely from the outer query to work out if this is going to be more efficient or not.
                    if (innerQuery.Source is ISingleSourceExecutionPlanNode loopRightSourceSimple)
                        InsertCorrelatedSubquerySpool(loopRightSourceSimple, source, parameterTypes);

                    var definedValue = $"Expr{++_colNameCounter}";

                    join = new NestedLoopNode
                    {
                        LeftSource = source,
                        RightSource = innerQuery.Source,
                        OuterReferences = references,
                        JoinCondition = new BooleanComparisonExpression
                        {
                            FirstExpression = lhsCol,
                            ComparisonType = BooleanComparisonType.Equals,
                            SecondExpression = innerQuery.ColumnSet[0].SourceColumn.ToColumnReference()
                        },
                        SemiJoin = true,
                        DefinedValues = { [definedValue] = innerQuery.ColumnSet[0].SourceColumn }
                    };

                    testColumn = definedValue;
                }

                join.JoinType = QualifiedJoinType.LeftOuter;

                rewrites[inSubquery] = new BooleanIsNullExpression
                {
                    IsNot = !inSubquery.NotDefined,
                    Expression = testColumn.ToColumnReference()
                };

                source = join;
            }

            query.Accept(new BooleanRewriteVisitor(rewrites));

            return source;
        }

        private IDataExecutionPlanNode ConvertExistsSubqueries(IDataExecutionPlanNode source, IList<OptimizerHint> hints, TSqlFragment query, IDictionary<string, Type> parameterTypes, NodeSchema outerSchema, IDictionary<string, string> outerReferences)
        {
            var visitor = new ExistsSubqueryVisitor();
            query.Accept(visitor);

            if (visitor.ExistsSubqueries.Count == 0)
                return source;

            var rewrites = new Dictionary<BooleanExpression, BooleanExpression>();
            var schema = source.GetSchema(Metadata, parameterTypes);

            foreach (var existsSubquery in visitor.ExistsSubqueries)
            {
                // Each query of the format "EXISTS (SELECT * FROM source)" becomes a outer semi join
                var parameters = parameterTypes == null ? new Dictionary<string, Type>() : new Dictionary<string, Type>(parameterTypes);
                var references = new Dictionary<string, string>();
                var innerQuery = ConvertSelectStatement(existsSubquery.Subquery.QueryExpression, hints, schema, references, parameters);
                var innerSchema = innerQuery.Source.GetSchema(Metadata, parameters);

                // Create the join
                BaseJoinNode join;
                string testColumn;
                if (references.Count == 0)
                {
                    // We only need one record to check for EXISTS
                    if (!(innerQuery.Source is TopNode) && !(innerQuery.Source is OffsetFetchNode))
                    {
                        innerQuery.Source = new TopNode
                        {
                            Source = innerQuery.Source,
                            Top = new IntegerLiteral { Value = "1" }
                        };
                    }

                    // We need a non-null value to use
                    if (innerSchema.PrimaryKey == null)
                    {
                        innerSchema.PrimaryKey = $"Expr{++_colNameCounter}";

                        if (!(innerQuery.Source is ComputeScalarNode computeScalar))
                        {
                            computeScalar = new ComputeScalarNode { Source = innerQuery.Source };
                            innerQuery.Source = computeScalar;
                        }

                        computeScalar.Columns[innerSchema.PrimaryKey] = new IntegerLiteral { Value = "1" };
                    }

                    // We can spool the results for reuse each time
                    innerQuery.Source = new TableSpoolNode
                    {
                        Source = innerQuery.Source
                    };

                    testColumn = $"Expr{++_colNameCounter}";

                    join = new NestedLoopNode
                    {
                        LeftSource = source,
                        RightSource = innerQuery.Source,
                        JoinType = QualifiedJoinType.LeftOuter,
                        SemiJoin = true,
                        OuterReferences = references,
                        DefinedValues =
                        {
                            [testColumn] = innerSchema.PrimaryKey
                        }
                    };
                }
                else if (UseMergeJoin(source, innerQuery.Source, references, null, null, out _, out var merge))
                {
                    join = merge;
                    testColumn = merge.RightAttribute.GetColumnName();
                }
                else
                {
                    // We need to use nested loops for correlated subqueries
                    // TODO: We could use a hash join where there is a simple correlation, but followed by a distinct node to eliminate duplicates
                    // We could also move the correlation criteria out of the subquery and into the join condition. We would then make one request to
                    // get all the related records and spool that in memory to get the relevant results in the nested loop. Need to understand how 
                    // many rows are likely from the outer query to work out if this is going to be more efficient or not.
                    if (innerQuery.Source is ISingleSourceExecutionPlanNode loopRightSourceSimple)
                        InsertCorrelatedSubquerySpool(loopRightSourceSimple, source, parameterTypes);

                    // We only need one record to check for EXISTS
                    if (!(innerQuery.Source is TopNode) && !(innerQuery.Source is OffsetFetchNode))
                    {
                        innerQuery.Source = new TopNode
                        {
                            Source = innerQuery.Source,
                            Top = new IntegerLiteral { Value = "1" }
                        };
                    }

                    // We need a non-null value to use
                    if (innerSchema.PrimaryKey == null)
                    {
                        innerSchema.PrimaryKey = $"Expr{++_colNameCounter}";

                        if (!(innerQuery.Source is ComputeScalarNode computeScalar))
                        {
                            computeScalar = new ComputeScalarNode { Source = innerQuery.Source };
                            innerQuery.Source = computeScalar;
                        }

                        computeScalar.Columns[innerSchema.PrimaryKey] = new IntegerLiteral { Value = "1" };
                    }

                    var definedValue = $"Expr{++_colNameCounter}";

                    join = new NestedLoopNode
                    {
                        LeftSource = source,
                        RightSource = innerQuery.Source,
                        OuterReferences = references,
                        SemiJoin = true,
                        DefinedValues = { [definedValue] = innerSchema.PrimaryKey }
                    };

                    testColumn = definedValue;
                }

                join.JoinType = QualifiedJoinType.LeftOuter;

                rewrites[existsSubquery] = new BooleanIsNullExpression
                {
                    IsNot = true,
                    Expression = testColumn.ToColumnReference()
                };

                source = join;
            }

            query.Accept(new BooleanRewriteVisitor(rewrites));

            return source;
        }

        private IDataExecutionPlanNode ConvertHavingClause(IDataExecutionPlanNode source, IList<OptimizerHint> hints, HavingClause havingClause, IDictionary<string, Type> parameterTypes, NodeSchema outerSchema, IDictionary<string, string> outerReferences, TSqlFragment query)
        {
            if (havingClause == null)
                return source;

            CaptureOuterReferences(outerSchema, source, havingClause, parameterTypes, outerReferences);

            var computeScalar = new ComputeScalarNode { Source = source };
            ConvertScalarSubqueries(havingClause.SearchCondition, hints, ref source, computeScalar, parameterTypes, query);

            // Validate the final expression
            havingClause.SearchCondition.GetType(source.GetSchema(Metadata, parameterTypes), parameterTypes);

            return new FilterNode
            {
                Filter = havingClause.SearchCondition,
                Source = source
            };
        }

        private IDataExecutionPlanNode ConvertGroupByAggregates(IDataExecutionPlanNode source, QuerySpecification querySpec, IDictionary<string, Type> parameterTypes, NodeSchema outerSchema, IDictionary<string, string> outerReferences)
        {
            // Check if there is a GROUP BY clause or aggregate functions to convert
            if (querySpec.GroupByClause == null)
            {
                var aggregates = new AggregateCollectingVisitor();
                aggregates.GetAggregates(querySpec);
                if (aggregates.SelectAggregates.Count == 0 && aggregates.Aggregates.Count == 0)
                    return source;
            }
            else
            {
                if (querySpec.GroupByClause.All == true)
                    throw new NotSupportedQueryFragmentException("Unhandled GROUP BY ALL clause", querySpec.GroupByClause);

                if (querySpec.GroupByClause.GroupByOption != GroupByOption.None)
                    throw new NotSupportedQueryFragmentException("Unhandled GROUP BY option", querySpec.GroupByClause);
            }

            var schema = source.GetSchema(Metadata, parameterTypes);

            // Create the grouping expressions. Grouping is done on single columns only - if a grouping is a more complex expression,
            // create a new calculated column using a Compute Scalar node first.
            var groupings = new Dictionary<ScalarExpression, ColumnReferenceExpression>();

            if (querySpec.GroupByClause != null)
            {
                CaptureOuterReferences(outerSchema, source, querySpec.GroupByClause, parameterTypes, outerReferences);

                foreach (var grouping in querySpec.GroupByClause.GroupingSpecifications)
                {
                    if (!(grouping is ExpressionGroupingSpecification exprGroup))
                        throw new NotSupportedQueryFragmentException("Unhandled GROUP BY expression", grouping);

                    if (!(exprGroup.Expression is ColumnReferenceExpression col))
                    {
                        // Use generic name for computed columns by default. Special case for DATEPART functions which
                        // could be folded down to FetchXML directly, so make these nicer names
                        string name = null;

                        if (exprGroup.Expression is FunctionCall func &&
                            func.FunctionName.Value.Equals("DATEPART", StringComparison.OrdinalIgnoreCase) &&
                            func.Parameters.Count == 2 &&
                            func.Parameters[0] is ColumnReferenceExpression datepart &&
                            func.Parameters[1] is ColumnReferenceExpression datepartCol)
                        {
                            var partName = datepart.GetColumnName();

                            // Not all DATEPART part types are supported in FetchXML. The supported ones in FetchXML are:
                            // * day
                            // * week
                            // * month
                            // * quarter
                            // * year
                            // * fiscal period
                            // * fiscal year
                            //
                            // Fiscal period/year do not have a T-SQL equivalent
                            var partnames = new Dictionary<string, FetchXml.DateGroupingType>(StringComparer.OrdinalIgnoreCase)
                            {
                                ["year"] = FetchXml.DateGroupingType.year,
                                ["yy"] = FetchXml.DateGroupingType.year,
                                ["yyyy"] = FetchXml.DateGroupingType.year,
                                ["quarter"] = FetchXml.DateGroupingType.quarter,
                                ["qq"] = FetchXml.DateGroupingType.quarter,
                                ["q"] = FetchXml.DateGroupingType.quarter,
                                ["month"] = FetchXml.DateGroupingType.month,
                                ["mm"] = FetchXml.DateGroupingType.month,
                                ["m"] = FetchXml.DateGroupingType.month,
                                ["day"] = FetchXml.DateGroupingType.day,
                                ["dd"] = FetchXml.DateGroupingType.day,
                                ["d"] = FetchXml.DateGroupingType.day,
                                ["week"] = FetchXml.DateGroupingType.week,
                                ["wk"] = FetchXml.DateGroupingType.week,
                                ["ww"] = FetchXml.DateGroupingType.week
                            };

                            if (partnames.TryGetValue(partName, out var dateGrouping))
                            {
                                var colName = datepartCol.GetColumnName();
                                schema.ContainsColumn(colName, out colName);

                                name = colName.Split('.').Last() + "_" + dateGrouping;
                                var baseName = name;

                                var suffix = 0;

                                while (groupings.Values.Any(grp => grp.GetColumnName().Equals(name, StringComparison.OrdinalIgnoreCase)))
                                    name = $"{baseName}_{++suffix}";
                            }
                        }

                        if (name == null)
                            name = $"Expr{++_colNameCounter}";

                        col = new ColumnReferenceExpression
                        {
                            MultiPartIdentifier = new MultiPartIdentifier
                            {
                                Identifiers =
                                {
                                    new Identifier { Value = name }
                                }
                            }
                        };
                    }

                    groupings[exprGroup.Expression] = col;
                }
            }

            if (groupings.Any(kvp => kvp.Key != kvp.Value))
            {
                var computeScalar = new ComputeScalarNode { Source = source };
                var rewrites = new Dictionary<ScalarExpression, string>();

                foreach (var calc in groupings.Where(kvp => kvp.Key != kvp.Value))
                {
                    rewrites[calc.Key] = calc.Value.GetColumnName();
                    computeScalar.Columns[calc.Value.GetColumnName()] = calc.Key;
                }

                source = computeScalar;

                querySpec.Accept(new RewriteVisitor(rewrites));
            }

            var hashMatch = new HashMatchAggregateNode
            {
                Source = source
            };

            foreach (var grouping in groupings)
                hashMatch.GroupBy.Add(grouping.Value);

            // Create the aggregate functions
            var aggregateCollector = new AggregateCollectingVisitor();
            aggregateCollector.GetAggregates(querySpec);
            var aggregateRewrites = new Dictionary<ScalarExpression, string>();

            foreach (var aggregate in aggregateCollector.Aggregates.Select(a => new { Expression = a, Alias = (string)null }).Concat(aggregateCollector.SelectAggregates.Select(s => new { Expression = (FunctionCall)s.Expression, Alias = s.ColumnName?.Identifier?.Value })))
            {
                CaptureOuterReferences(outerSchema, source, aggregate.Expression, parameterTypes, outerReferences);

                var converted = new Aggregate
                {
                    Distinct = aggregate.Expression.UniqueRowFilter == UniqueRowFilter.Distinct
                };

                if (!(aggregate.Expression.Parameters[0] is ColumnReferenceExpression col) || col.ColumnType != ColumnType.Wildcard)
                    converted.Expression = aggregate.Expression.Parameters[0];

                switch (aggregate.Expression.FunctionName.Value.ToUpper())
                {
                    case "AVG":
                        converted.AggregateType = AggregateType.Average;
                        break;

                    case "COUNT":
                        if (converted.Expression == null)
                            converted.AggregateType = AggregateType.CountStar;
                        else
                            converted.AggregateType = AggregateType.Count;
                        break;

                    case "MAX":
                        converted.AggregateType = AggregateType.Max;
                        break;

                    case "MIN":
                        converted.AggregateType = AggregateType.Min;
                        break;

                    case "SUM":
                        converted.AggregateType = AggregateType.Sum;
                        break;

                    default:
                        throw new NotSupportedQueryFragmentException("Unknown aggregate function", aggregate.Expression);
                }

                // Create a name for the column that holds the aggregate value in the result set.
                string aggregateName;

                if (aggregate.Alias != null)
                {
                    aggregateName = aggregate.Alias;
                }
                else if (aggregate.Expression.Parameters[0] is ColumnReferenceExpression colRef)
                {
                    if (colRef.ColumnType == ColumnType.Wildcard)
                    {
                        aggregateName = aggregate.Expression.FunctionName.Value.ToLower();
                    }
                    else
                    {
                        aggregateName = colRef.GetColumnName().Replace('.', '_') + "_" + aggregate.Expression.FunctionName.Value.ToLower();
                    }
                }
                else
                {
                    aggregateName = $"Expr{++_colNameCounter}";
                }

                hashMatch.Aggregates[aggregateName] = converted;
                aggregateRewrites[aggregate.Expression] = aggregateName;
            }

            querySpec.Accept(new RewriteVisitor(aggregateRewrites));
            return hashMatch;
        }

        private IDataExecutionPlanNode ConvertOffsetClause(IDataExecutionPlanNode source, OffsetClause offsetClause, IDictionary<string, Type> parameterTypes)
        {
            if (offsetClause == null)
                return source;

            var offsetType = offsetClause.OffsetExpression.GetType(null, parameterTypes);
            var fetchType = offsetClause.FetchExpression.GetType(null, parameterTypes);

            if (!SqlTypeConverter.CanChangeType(offsetType, typeof(int)))
                throw new NotSupportedQueryFragmentException("Unexpected OFFSET type", offsetClause.OffsetExpression);

            if (!SqlTypeConverter.CanChangeType(fetchType, typeof(int)))
                throw new NotSupportedQueryFragmentException("Unexpected FETCH type", offsetClause.FetchExpression);

            return new OffsetFetchNode
            {
                Source = source,
                Offset = offsetClause.OffsetExpression,
                Fetch = offsetClause.FetchExpression
            };
        }

        private IDataExecutionPlanNode ConvertTopClause(IDataExecutionPlanNode source, TopRowFilter topRowFilter, IDictionary<string, Type> parameterTypes)
        {
            if (topRowFilter == null)
                return source;

            // TOP x PERCENT requires evaluating the source twice - once to get the total count and again to get the top
            // records. Cache the results in a table spool node.
            if (topRowFilter.Percent)
                source = new TableSpoolNode { Source = source };

            var topType = topRowFilter.Expression.GetType(null, parameterTypes);
            var targetType = topRowFilter.Percent ? typeof(float) : typeof(int);

            if (!SqlTypeConverter.CanChangeType(topType, targetType))
                throw new NotSupportedQueryFragmentException("Unexpected TOP type", topRowFilter.Expression);

            return new TopNode
            {
                Source = source,
                Top = topRowFilter.Expression,
                Percent = topRowFilter.Percent,
                WithTies = topRowFilter.WithTies
            };
        }

        private IDataExecutionPlanNode ConvertOrderByClause(IDataExecutionPlanNode source, IList<OptimizerHint> hints, OrderByClause orderByClause, ScalarExpression[] selectList, TSqlFragment query, IDictionary<string, Type> parameterTypes, NodeSchema outerSchema, Dictionary<string, string> outerReferences)
        {
            if (orderByClause == null)
                return source;

            CaptureOuterReferences(outerSchema, source, orderByClause, parameterTypes, outerReferences);

            var computeScalar = new ComputeScalarNode { Source = source };
            ConvertScalarSubqueries(orderByClause, hints, ref source, computeScalar, parameterTypes, query);

            var schema = source.GetSchema(Metadata, parameterTypes);
            var sort = new SortNode { Source = source };

            // Check if any of the order expressions need pre-calculation
            foreach (var orderBy in orderByClause.OrderByElements)
            {
                // If the order by element is a numeric literal, use the corresponding expression from the select list at that index
                if (orderBy.Expression is IntegerLiteral literal)
                {
                    var index = Int32.Parse(literal.Value) - 1;

                    if (index < 0 || index >= selectList.Length)
                    {
                        throw new NotSupportedQueryFragmentException("Invalid ORDER BY index", literal)
                        {
                            Suggestion = $"Must be between 1 and {selectList.Length}"
                        };
                    }

                    orderBy.Expression = selectList[index];
                }

                // Anything complex expressoin should be pre-calculated
                if (!(orderBy.Expression is ColumnReferenceExpression) &&
                    !(orderBy.Expression is VariableReference) &&
                    !(orderBy.Expression is Literal))
                {
                    var calculated = ComputeScalarExpression(orderBy.Expression, hints, query, computeScalar, parameterTypes, ref source);
                    sort.Source = source;
                    schema = source.GetSchema(Metadata, parameterTypes);
                }

                // Validate the expression
                orderBy.Expression.GetType(schema, parameterTypes);

                sort.Sorts.Add(orderBy);
            }

            if (computeScalar.Columns.Any())
                sort.Source = computeScalar;

            return sort;
        }

        private IDataExecutionPlanNode ConvertWhereClause(IDataExecutionPlanNode source, IList<OptimizerHint> hints, WhereClause whereClause, NodeSchema outerSchema, Dictionary<string,string> outerReferences, IDictionary<string, Type> parameterTypes, TSqlFragment query)
        {
            if (whereClause == null)
                return source;

            if (whereClause.Cursor != null)
                throw new NotSupportedQueryFragmentException("Unsupported cursor", whereClause.Cursor);

            CaptureOuterReferences(outerSchema, source, whereClause.SearchCondition, parameterTypes, outerReferences);

            var computeScalar = new ComputeScalarNode { Source = source };
            ConvertScalarSubqueries(whereClause.SearchCondition, hints, ref source, computeScalar, parameterTypes, query);

            // Validate the final expression
            whereClause.SearchCondition.GetType(source.GetSchema(Metadata, parameterTypes), parameterTypes);

            return new FilterNode
            {
                Filter = whereClause.SearchCondition,
                Source = source
            };
        }

        private TSqlFragment CaptureOuterReferences(NodeSchema outerSchema, IDataExecutionPlanNode source, TSqlFragment query, IDictionary<string,Type> parameterTypes, IDictionary<string,string> outerReferences)
        {
            if (outerSchema == null)
                return query;

            // We're in a subquery. Check if any columns in the WHERE clause are from the outer query
            // so we know which columns to pass through and rewrite the filter to use parameters
            var rewrites = new Dictionary<ScalarExpression, ScalarExpression>();
            var innerSchema = source.GetSchema(Metadata, parameterTypes);
            var columns = query.GetColumns();

            foreach (var column in columns)
            {
                // Column names could be ambiguous between the inner and outer data sources. The inner
                // data source is used in preference.
                // Ref: https://docs.microsoft.com/en-us/sql/relational-databases/performance/subqueries?view=sql-server-ver15#qualifying
                var fromInner = innerSchema.ContainsColumn(column, out _);

                if (fromInner)
                    continue;

                var fromOuter = outerSchema.ContainsColumn(column, out var outerColumn);

                if (fromOuter)
                {
                    var paramName = $"@Expr{++_colNameCounter}";
                    outerReferences.Add(outerColumn, paramName);
                    parameterTypes[paramName] = outerSchema.Schema[outerColumn];

                    rewrites.Add(
                        new ColumnReferenceExpression { MultiPartIdentifier = new MultiPartIdentifier { Identifiers = { new Identifier { Value = column } } } },
                        new VariableReference { Name = paramName });
                }
            }

            if (rewrites.Any())
                query.Accept(new RewriteVisitor(rewrites));

            if (query is ScalarExpression scalar && rewrites.TryGetValue(scalar, out var rewritten))
                return rewritten;

            return query;
        }

        private SelectNode ConvertSelectClause(IList<SelectElement> selectElements, IList<OptimizerHint> hints, IDataExecutionPlanNode node, DistinctNode distinct, TSqlFragment query, IDictionary<string, Type> parameterTypes, NodeSchema outerSchema, IDictionary<string,string> outerReferences)
        {
            var schema = node.GetSchema(Metadata, parameterTypes);

            var select = new SelectNode
            {
                Source = node
            };

            var computeScalar = new ComputeScalarNode
            {
                Source = distinct?.Source ?? node
            };

            foreach (var element in selectElements)
            {
                CaptureOuterReferences(outerSchema, computeScalar, element, parameterTypes, outerReferences);

                if (element is SelectScalarExpression scalar)
                {
                    if (scalar.Expression is ColumnReferenceExpression col)
                    {
                        var colName = col.GetColumnName();

                        if (!schema.ContainsColumn(colName, out colName))
                        {
                            if (!schema.Aliases.TryGetValue(col.GetColumnName(), out var normalized))
                                throw new NotSupportedQueryFragmentException("Unknown column", col);

                            throw new NotSupportedQueryFragmentException("Ambiguous column reference", col)
                            {
                                Suggestion = $"Did you mean:\r\n{String.Join("\r\n", normalized.Select(c => $"* {c}"))}"
                            };
                        }

                        var alias = scalar.ColumnName?.Value ?? col.MultiPartIdentifier.Identifiers.Last().Value;

                        select.ColumnSet.Add(new SelectColumn
                        {
                            SourceColumn = colName,
                            OutputColumn = alias
                        });
                    }
                    else
                    {
                        var scalarSource = distinct?.Source ?? node;
                        var alias = ComputeScalarExpression(scalar.Expression, hints, query, computeScalar, parameterTypes, ref scalarSource);

                        if (distinct != null)
                            distinct.Source = scalarSource;
                        else
                            node = scalarSource;

                        select.ColumnSet.Add(new SelectColumn
                        {
                            SourceColumn = alias,
                            OutputColumn = scalar.ColumnName?.Value ?? alias
                        });
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

            if (computeScalar.Columns.Count > 0)
            {
                if (distinct != null)
                    distinct.Source = computeScalar;
                else
                    select.Source = computeScalar;
            }

            if (distinct != null)
            {
                foreach (var col in select.ColumnSet)
                {
                    if (col.AllColumns)
                    {
                        var distinctSchema = distinct.GetSchema(Metadata, parameterTypes);
                        distinct.Columns.AddRange(distinctSchema.Schema.Keys.Where(k => col.SourceColumn == null || (k.Split('.')[0] + ".*") == col.SourceColumn));
                    }
                    else
                    {
                        distinct.Columns.Add(col.SourceColumn);
                    }
                }
            }

            return select;
        }

        private string ComputeScalarExpression(ScalarExpression expression, IList<OptimizerHint> hints, TSqlFragment query, ComputeScalarNode computeScalar, IDictionary<string, Type> parameterTypes, ref IDataExecutionPlanNode node)
        {
            var computedColumn = ConvertScalarSubqueries(expression, hints, ref node, computeScalar, parameterTypes, query);

            if (computedColumn != null)
                expression = computedColumn;

            // Check the type of this expression now so any errors can be reported
            var computeScalarSchema = computeScalar.GetSchema(Metadata, parameterTypes);
            expression.GetType(computeScalarSchema, parameterTypes);

            var alias = $"Expr{++_colNameCounter}";
            computeScalar.Columns[alias] = expression;
            return alias;
        }

        private ColumnReferenceExpression ConvertScalarSubqueries(TSqlFragment expression, IList<OptimizerHint> hints, ref IDataExecutionPlanNode node, ComputeScalarNode computeScalar, IDictionary<string, Type> parameterTypes, TSqlFragment query)
        {
            /*
             * Possible subquery execution plans:
             * 1. Nested loop. Simple but inefficient as ends up making at least 1 FetchXML request per outer row
             * 2. Spooled nested loop. Useful when there is no correlation and so the same results can be used for each outer record
             * 3. Spooled nested loop with correlation criteria pulled into loop. Useful when there are a large number of outer records or a small number of inner records
             * 4. Merge join. Useful when the correlation criteria is based on the equality of the primary key of the inner table
             */
            // If scalar.Expression contains a subquery, create nested loop to evaluate it in the context
            // of the current record
            var subqueryVisitor = new ScalarSubqueryVisitor();
            expression.Accept(subqueryVisitor);
            var rewrites = new Dictionary<ScalarExpression, string>();

            foreach (var subquery in subqueryVisitor.Subqueries)
            {
                var outerSchema = node.GetSchema(Metadata, parameterTypes);
                var outerReferences = new Dictionary<string, string>();
                var innerParameterTypes = parameterTypes == null ? new Dictionary<string, Type>() : new Dictionary<string, Type>(parameterTypes);
                var subqueryPlan = ConvertSelectStatement(subquery.QueryExpression, hints, outerSchema, outerReferences, innerParameterTypes);

                // Scalar subquery must return exactly one column and one row
                if (subqueryPlan.ColumnSet.Count != 1)
                    throw new NotSupportedQueryFragmentException("Scalar subquery must return exactly one column", subquery);

                string outputcol;
                var subqueryCol = subqueryPlan.ColumnSet[0].SourceColumn;
                BaseJoinNode join = null;
                if (UseMergeJoin(node, subqueryPlan.Source, outerReferences, subqueryCol, null, out outputcol, out var merge))
                {
                    join = merge;
                }
                else
                {
                    outputcol = $"Expr{++_colNameCounter}";

                    var loopRightSource = subqueryPlan.Source;

                    // Unless the subquery has got an explicit TOP 1 clause, insert an aggregate and assertion nodes
                    // to check for one row
                    if (!(subqueryPlan.Source is TopNode top) || !(top.Top is IntegerLiteral topValue) || topValue.Value != "1")
                    {
                        subqueryCol = $"Expr{++_colNameCounter}";
                        var rowCountCol = $"Expr{++_colNameCounter}";
                        var aggregate = new HashMatchAggregateNode
                        {
                            Source = loopRightSource,
                            Aggregates =
                            {
                                [subqueryCol] = new Aggregate
                                {
                                    AggregateType = AggregateType.First,
                                    Expression = new ColumnReferenceExpression
                                    {
                                        MultiPartIdentifier = new MultiPartIdentifier
                                        {
                                            Identifiers = { new Identifier { Value = subqueryPlan.ColumnSet[0].SourceColumn } }
                                        }
                                    }
                                },
                                [rowCountCol] = new Aggregate
                                {
                                    AggregateType = AggregateType.CountStar
                                }
                            }
                        };
                        var assert = new AssertNode
                        {
                            Source = aggregate,
                            Assertion = e => e.GetAttributeValue<int>(rowCountCol) <= 1,
                            ErrorMessage = "Subquery produced more than 1 row"
                        };
                        loopRightSource = assert;
                    }

                    // If the subquery is uncorrelated, add a table spool to cache the results
                    // If it is correlated, add a spool where possible closer to the data source
                    if (outerReferences.Count == 0)
                    {
                        var spool = new TableSpoolNode { Source = loopRightSource };
                        loopRightSource = spool;
                    }
                    else if (loopRightSource is ISingleSourceExecutionPlanNode loopRightSourceSimple)
                    {
                        InsertCorrelatedSubquerySpool(loopRightSourceSimple, node, parameterTypes);
                    }

                    // Add a nested loop to call the subquery
                    if (join == null)
                    {
                        join = new NestedLoopNode
                        {
                            LeftSource = node,
                            RightSource = loopRightSource,
                            OuterReferences = outerReferences,
                            JoinType = QualifiedJoinType.LeftOuter,
                            SemiJoin = true,
                            DefinedValues = { [outputcol] = subqueryCol }
                        };
                    }
                }

                node = join;
                computeScalar.Source = join;

                rewrites[subquery] = outputcol;
            }

            if (rewrites.Any())
                query.Accept(new RewriteVisitor(rewrites));

            if (expression is ScalarExpression scalar && rewrites.ContainsKey(scalar))
                return new ColumnReferenceExpression { MultiPartIdentifier = new MultiPartIdentifier { Identifiers = { new Identifier { Value = rewrites[scalar] } } } };

            return null;
        }

        private bool UseMergeJoin(IDataExecutionPlanNode node, IDataExecutionPlanNode subqueryPlan, Dictionary<string, string> outerReferences, string subqueryCol, string inPredicateCol, out string outputCol, out MergeJoinNode merge)
        {
            outputCol = null;
            merge = null;

            // We can use a merge join for a scalar subquery when the subquery is simply SELECT [TOP 1] <column> FROM <table> WHERE <table>.<key> = <outertable>.<column>
            // The filter must be on the inner table's primary key
            var subNode = subqueryPlan;

            var alias = subNode as AliasNode;
            if (alias != null)
                subNode = alias.Source;

            if (subNode is TopNode top && top.Top is IntegerLiteral topLiteral && topLiteral.Value == "1")
                subNode = top.Source;

            var filter = subNode as FilterNode;
            if (filter != null)
                subNode = filter.Source;
            else if (inPredicateCol == null)
                return false;

            if (!(subNode is FetchXmlScan fetch))
                return false;

            var outerKey = (string)null;
            var innerKey = (string)null;

            if (inPredicateCol != null)
            {
                outerKey = inPredicateCol;
                innerKey = subqueryCol;
            }
            else
            {
                if (!(filter.Filter is BooleanComparisonExpression cmp))
                    return false;

                if (cmp.ComparisonType != BooleanComparisonType.Equals)
                    return false;

                var col1 = cmp.FirstExpression as ColumnReferenceExpression;
                var var1 = cmp.FirstExpression as VariableReference;

                var col2 = cmp.SecondExpression as ColumnReferenceExpression;
                var var2 = cmp.SecondExpression as VariableReference;

                var col = col1 ?? col2;
                var var = var1 ?? var2;

                if (col == null || var == null)
                    return false;

                foreach (var outerReference in outerReferences)
                {
                    if (outerReference.Value == var.Name)
                    {
                        outerKey = outerReference.Key;
                        break;
                    }
                }

                innerKey = col.GetColumnName();
            }

            if (outerKey == null)
                return false;

            var innerSchema = fetch.GetSchema(Metadata, null);

            if (!innerSchema.ContainsColumn(innerKey, out innerKey))
                return false;

            if (innerSchema.PrimaryKey != innerKey)
                return false;

            // Give the inner fetch a unique alias
            if (alias != null)
                fetch.Alias = alias.Alias;
            else
                fetch.Alias = $"Expr{++_colNameCounter}";

            // Add the required column with the expected alias (used for scalar subqueries and IN predicates, not for CROSS/OUTER APPLY
            if (subqueryCol != null)
            {
                var attr = new FetchXml.FetchAttributeType { name = subqueryCol.Split('.').Last() };
                fetch.Entity.AddItem(attr);
                outputCol = fetch.Alias + "." + attr.name;
            }

            // Regenerate the schema after changing the alias
            innerSchema = fetch.GetSchema(Metadata, null);

            merge = new MergeJoinNode
            {
                LeftSource = node,
                LeftAttribute = outerKey.ToColumnReference(),
                RightSource = inPredicateCol != null ? (IDataExecutionPlanNode) filter ?? fetch : fetch,
                RightAttribute = innerSchema.PrimaryKey.ToColumnReference(),
                JoinType = QualifiedJoinType.LeftOuter
            };

            return true;
        }
        
        private void InsertCorrelatedSubquerySpool(ISingleSourceExecutionPlanNode node, IDataExecutionPlanNode outerSource, IDictionary<string, Type> parameterTypes)
        {
            // Look for a simple case where there is a reference to the outer table in a filter node. Extract the minimal
            // amount of that filter to a new filter node and place a table spool between the correlated filter and its source

            // Skip over simple leading nodes to try to find a Filter node
            var lastCorrelatedStep = node;
            ISingleSourceExecutionPlanNode parentNode = null;
            FilterNode filter = null;
            FetchXmlScan fetchXml = null;

            while (node != null)
            {
                if (node is FilterNode f)
                {
                    filter = f;
                    break;
                }

                if (node is FetchXmlScan fetch)
                {
                    fetchXml = fetch;
                    break;
                }

                parentNode = node;

                if (node is AssertNode assert)
                {
                    node = assert.Source as ISingleSourceExecutionPlanNode;
                }
                else if (node is HashMatchAggregateNode agg)
                {
                    if (agg.Aggregates.Values.Any(a => a.Expression != null && a.Expression.GetVariables().Any()))
                        lastCorrelatedStep = agg;

                    node = agg.Source as ISingleSourceExecutionPlanNode;
                }
                else if (node is ComputeScalarNode cs)
                {
                    if (cs.Columns.Values.Any(col => col.GetVariables().Any()))
                        lastCorrelatedStep = cs;

                    node = cs.Source as ISingleSourceExecutionPlanNode;
                }
                else if (node is SortNode sort)
                {
                    if (sort.Sorts.Any(s => s.Expression.GetVariables().Any()))
                        lastCorrelatedStep = sort;

                    node = sort.Source as ISingleSourceExecutionPlanNode;
                }
                else if (node is TopNode top)
                {
                    if (top.Top.GetVariables().Any())
                        lastCorrelatedStep = top;

                    node = top.Source as ISingleSourceExecutionPlanNode;
                }
                else if (node is OffsetFetchNode offset)
                {
                    if (offset.Offset.GetVariables().Any() || offset.Fetch.GetVariables().Any())
                        lastCorrelatedStep = offset;

                    node = offset.Source as ISingleSourceExecutionPlanNode;
                }
                else if (node is AliasNode alias)
                {
                    node = alias.Source as ISingleSourceExecutionPlanNode;
                }
                else
                {
                    return;
                }
            }

            if (filter != null)
            {
                fetchXml = filter.Source as FetchXmlScan;

                // TODO: If the filter is on a join we need to do some more complex checking that there's no outer references
                // in use by the join before we know we can safely spool the results
                if (fetchXml == null)
                    return;
            }

            if (filter != null && filter.Filter.GetVariables().Any())
            {
                // The filter is correlated. Check if there's any non-correlated criteria we can split out into a separate node
                // that could be folded into the data source first
                if (SplitCorrelatedCriteria(filter.Filter, out var correlatedFilter, out var nonCorrelatedFilter))
                {
                    filter.Filter = correlatedFilter;
                    filter.Source = new FilterNode
                    {
                        Filter = nonCorrelatedFilter,
                        Source = filter.Source
                    };
                }

                lastCorrelatedStep = filter;
            }

            if (lastCorrelatedStep == null)
                return;

            // TODO: Check the estimated counts for the outer loop and the source at the point we'd insert the spool
            // If the outer loop is non-trivial (>= 100 rows) or the inner loop is small (<= 5000 records) then we want
            // to use the spool.
            var outerCount = outerSource.EstimateRowsOut(Metadata, parameterTypes, TableSize);
            var innerCount = outerCount >= 100 ? -1 : lastCorrelatedStep.Source.EstimateRowsOut(Metadata, parameterTypes, TableSize);

            if (outerCount >= 100 || innerCount <= 5000)
            {
                var spool = new TableSpoolNode
                {
                    Source = lastCorrelatedStep.Source
                };

                lastCorrelatedStep.Source = spool;
            }
        }

        private bool SplitCorrelatedCriteria(BooleanExpression filter, out BooleanExpression correlatedFilter, out BooleanExpression nonCorrelatedFilter)
        {
            correlatedFilter = null;
            nonCorrelatedFilter = null;

            if (!filter.GetVariables().Any())
            {
                nonCorrelatedFilter = filter;
                return true;
            }

            if (filter is BooleanBinaryExpression bin && bin.BinaryExpressionType == BooleanBinaryExpressionType.And)
            {
                var splitLhs = SplitCorrelatedCriteria(bin.FirstExpression, out var correlatedLhs, out var nonCorrelatedLhs);
                var splitRhs = SplitCorrelatedCriteria(bin.SecondExpression, out var correlatedRhs, out var nonCorrelatedRhs);

                if (splitLhs || splitRhs)
                {
                    if (correlatedLhs != null && correlatedRhs != null)
                    {
                        correlatedFilter = new BooleanBinaryExpression
                        {
                            FirstExpression = correlatedLhs,
                            BinaryExpressionType = BooleanBinaryExpressionType.And,
                            SecondExpression = correlatedRhs
                        };
                    }
                    else
                    {
                        correlatedFilter = correlatedLhs ?? correlatedRhs;
                    }

                    if (nonCorrelatedLhs != null && nonCorrelatedRhs != null)
                    {
                        nonCorrelatedFilter = new BooleanBinaryExpression
                        {
                            FirstExpression = nonCorrelatedLhs,
                            BinaryExpressionType = BooleanBinaryExpressionType.And,
                            SecondExpression = nonCorrelatedRhs
                        };
                    }
                    else
                    {
                        nonCorrelatedFilter = nonCorrelatedLhs ?? nonCorrelatedRhs;
                    }

                    return true;
                }
            }

            correlatedFilter = filter;
            return false;
        }

        private IDataExecutionPlanNode ConvertFromClause(IList<TableReference> tables, IList<OptimizerHint> hints, TSqlFragment query, NodeSchema outerSchema, Dictionary<string, string> outerReferences, IDictionary<string, Type> parameterTypes)
        {
            var node = ConvertTableReference(tables[0], hints, query, outerSchema, outerReferences, parameterTypes);

            for (var i = 1; i < tables.Count; i++)
            {
                var nextTable = ConvertTableReference(tables[i], hints, query, outerSchema, outerReferences, parameterTypes);

                // TODO: See if we can lift a join predicate from the WHERE clause
                nextTable = new TableSpoolNode { Source = nextTable };

                node = new NestedLoopNode { LeftSource = node, RightSource = nextTable };
            }

            return node;
        }

        private IDataExecutionPlanNode ConvertTableReference(TableReference reference, IList<OptimizerHint> hints, TSqlFragment query, NodeSchema outerSchema, Dictionary<string, string> outerReferences, IDictionary<string, Type> parameterTypes)
        {
            if (reference is NamedTableReference table)
            {
                var entityName = table.SchemaObject.BaseIdentifier.Value;

                if (table.SchemaObject.SchemaIdentifier?.Value?.Equals("metadata", StringComparison.OrdinalIgnoreCase) == true)
                {
                    // We're asking for metadata - check the type
                    if (entityName.Equals("entity", StringComparison.OrdinalIgnoreCase))
                    {
                        return new MetadataQueryNode
                        {
                            MetadataSource = MetadataSource.Entity,
                            EntityAlias = table.Alias?.Value ?? entityName
                        };
                    }

                    if (entityName.Equals("attribute", StringComparison.OrdinalIgnoreCase))
                    {
                        return new MetadataQueryNode
                        {
                            MetadataSource = MetadataSource.Attribute,
                            AttributeAlias = table.Alias?.Value ?? entityName
                        };
                    }

                    if (entityName.Equals("relationship_1_n", StringComparison.OrdinalIgnoreCase))
                    {
                        return new MetadataQueryNode
                        {
                            MetadataSource = MetadataSource.OneToManyRelationship,
                            OneToManyRelationshipAlias = table.Alias?.Value ?? entityName
                        };
                    }

                    if (entityName.Equals("relationship_n_1", StringComparison.OrdinalIgnoreCase))
                    {
                        return new MetadataQueryNode
                        {
                            MetadataSource = MetadataSource.ManyToOneRelationship,
                            ManyToOneRelationshipAlias = table.Alias?.Value ?? entityName
                        };
                    }

                    if (entityName.Equals("relationship_n_n", StringComparison.OrdinalIgnoreCase))
                    {
                        return new MetadataQueryNode
                        {
                            MetadataSource = MetadataSource.ManyToManyRelationship,
                            ManyToManyRelationshipAlias = table.Alias?.Value ?? entityName
                        };
                    }

                    if (entityName.Equals("globaloptionset", StringComparison.OrdinalIgnoreCase))
                    {
                        return new GlobalOptionSetQueryNode
                        {
                            Alias = table.Alias?.Value ?? entityName
                        };
                    }
                }

                // Validate the entity name
                try
                {
                    var meta = Metadata[entityName];
                }
                catch (FaultException ex)
                {
                    throw new NotSupportedQueryFragmentException(ex.Message, reference);
                }

                var unsupportedHint = table.TableHints.FirstOrDefault(hint => hint.HintKind != TableHintKind.NoLock);
                if (unsupportedHint != null)
                    throw new NotSupportedQueryFragmentException("Unsupported table hint", unsupportedHint);

                if (table.TableSampleClause != null)
                    throw new NotSupportedQueryFragmentException("Unsupported table sample clause", table.TableSampleClause);

                if (table.TemporalClause != null)
                    throw new NotSupportedQueryFragmentException("Unsupported temporal clause", table.TemporalClause);

                // Convert to a simple FetchXML source
                return new FetchXmlScan
                {
                    FetchXml = new FetchXml.FetchType
                    {
                        nolock = table.TableHints.Any(hint => hint.HintKind == TableHintKind.NoLock),
                        Items = new object[]
                        {
                            new FetchXml.FetchEntityType
                            {
                                name = entityName
                            }
                        },
                        options = ConvertQueryHints(hints)
                    },
                    Alias = table.Alias?.Value ?? entityName,
                    ReturnFullSchema = true
                };
            }

            if (reference is QualifiedJoin join)
            {
                // If the join involves the primary key of one table we can safely use a merge join.
                // Otherwise use a nested loop join
                var lhs = ConvertTableReference(join.FirstTableReference, hints, query, outerSchema, outerReferences, parameterTypes);
                var rhs = ConvertTableReference(join.SecondTableReference, hints, query, outerSchema, outerReferences, parameterTypes);
                var lhsSchema = lhs.GetSchema(Metadata, parameterTypes);
                var rhsSchema = rhs.GetSchema(Metadata, parameterTypes);

                var joinConditionVisitor = new JoinConditionVisitor(lhsSchema, rhsSchema);
                join.SearchCondition.Accept(joinConditionVisitor);

                // If we didn't find any join criteria equating two columns in the table, try again
                // but allowing computed columns instead. This lets us use more efficient join types (merge or hash join)
                // by pre-computing the values of the expressions to use as the join keys
                if (joinConditionVisitor.LhsKey == null || joinConditionVisitor.RhsKey == null)
                {
                    joinConditionVisitor = new JoinConditionVisitor(lhsSchema, rhsSchema);
                    joinConditionVisitor.AllowExpressions = true;

                    join.SearchCondition.Accept(joinConditionVisitor);

                    if (joinConditionVisitor.LhsExpression != null && joinConditionVisitor.RhsExpression != null)
                    {
                        // Calculate the two join expressions
                        if (joinConditionVisitor.LhsKey == null)
                        {
                            if (!(lhs is ComputeScalarNode lhsComputeScalar))
                            {
                                lhsComputeScalar = new ComputeScalarNode { Source = lhs };
                                lhs = lhsComputeScalar;
                            }

                            var lhsColumn = ComputeScalarExpression(joinConditionVisitor.LhsExpression, hints, query, lhsComputeScalar, parameterTypes, ref lhs);
                            joinConditionVisitor.LhsKey = new ColumnReferenceExpression { MultiPartIdentifier = new MultiPartIdentifier { Identifiers = { new Identifier { Value = lhsColumn } } } };
                        }

                        if (joinConditionVisitor.RhsKey == null)
                        {
                            if (!(rhs is ComputeScalarNode rhsComputeScalar))
                            {
                                rhsComputeScalar = new ComputeScalarNode { Source = rhs };
                                rhs = rhsComputeScalar;
                            }

                            var rhsColumn = ComputeScalarExpression(joinConditionVisitor.RhsExpression, hints, query, rhsComputeScalar, parameterTypes, ref lhs);
                            joinConditionVisitor.RhsKey = new ColumnReferenceExpression { MultiPartIdentifier = new MultiPartIdentifier { Identifiers = { new Identifier { Value = rhsColumn } } } };
                        }
                    }
                }

                BaseJoinNode joinNode;

                if (joinConditionVisitor.LhsKey != null && joinConditionVisitor.RhsKey != null && joinConditionVisitor.LhsKey.GetColumnName() == lhsSchema.PrimaryKey)
                {
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
                else if (joinConditionVisitor.LhsKey != null && joinConditionVisitor.RhsKey != null && joinConditionVisitor.RhsKey.GetColumnName() == rhsSchema.PrimaryKey)
                {
                    joinNode = new MergeJoinNode
                    {
                        LeftSource = rhs,
                        LeftAttribute = joinConditionVisitor.RhsKey,
                        RightSource = lhs,
                        RightAttribute = joinConditionVisitor.LhsKey,
                        AdditionalJoinCriteria = join.SearchCondition.RemoveCondition(joinConditionVisitor.JoinCondition)
                    };

                    switch (join.QualifiedJoinType)
                    {
                        case QualifiedJoinType.Inner:
                            joinNode.JoinType = QualifiedJoinType.Inner;
                            break;

                        case QualifiedJoinType.LeftOuter:
                            joinNode.JoinType = QualifiedJoinType.RightOuter;
                            break;

                        case QualifiedJoinType.RightOuter:
                            joinNode.JoinType = QualifiedJoinType.LeftOuter;
                            break;

                        case QualifiedJoinType.FullOuter:
                            joinNode.JoinType = QualifiedJoinType.FullOuter;
                            break;
                    }
                }
                else if (joinConditionVisitor.LhsKey != null && joinConditionVisitor.RhsKey != null)
                {
                    joinNode = new HashJoinNode
                    {
                        LeftSource = lhs,
                        LeftAttribute = joinConditionVisitor.LhsKey,
                        RightSource = rhs,
                        RightAttribute = joinConditionVisitor.RhsKey,
                        JoinType = join.QualifiedJoinType,
                        AdditionalJoinCriteria = join.SearchCondition.RemoveCondition(joinConditionVisitor.JoinCondition)
                    };
                }
                else
                {
                    joinNode = new NestedLoopNode
                    {
                        LeftSource = lhs,
                        RightSource = rhs,
                        JoinType = join.QualifiedJoinType,
                        JoinCondition = join.SearchCondition
                    };
                }

                // Validate the join condition
                var joinSchema = joinNode.GetSchema(Metadata, parameterTypes);
                join.SearchCondition.GetType(joinSchema, parameterTypes);

                return joinNode;
            }

            if (reference is QueryDerivedTable queryDerivedTable)
            {
                if (queryDerivedTable.Columns.Count > 0)
                    throw new NotSupportedQueryFragmentException("Unhandled query derived table column list", queryDerivedTable);

                var select = ConvertSelectStatement(queryDerivedTable.QueryExpression, hints, outerSchema, outerReferences, parameterTypes);
                var alias = new AliasNode(select);
                alias.Alias = queryDerivedTable.Alias.Value;

                return alias;
            }

            if (reference is InlineDerivedTable inlineDerivedTable)
            {
                // Check all the rows have the expected number of values and column names are unique
                var columnNames = inlineDerivedTable.Columns.Select(col => col.Value).ToList();

                for (var i = 1; i < columnNames.Count; i++)
                {
                    if (columnNames.Take(i).Any(prevCol => prevCol.Equals(columnNames[i], StringComparison.OrdinalIgnoreCase)))
                        throw new NotSupportedQueryFragmentException("Duplicate column name", inlineDerivedTable.Columns[i]);
                }

                var firstMismatchRow = inlineDerivedTable.RowValues.FirstOrDefault(row => row.ColumnValues.Count != columnNames.Count);
                if (firstMismatchRow != null)
                    throw new NotSupportedQueryFragmentException($"Expected {columnNames.Count} columns, got {firstMismatchRow.ColumnValues.Count}", firstMismatchRow);

                // Work out the column types
                var types = inlineDerivedTable.RowValues[0].ColumnValues.Select(val => val.GetType(null, null)).ToList();
                
                foreach (var row in inlineDerivedTable.RowValues.Skip(1))
                {
                    for (var colIndex = 0; colIndex < types.Count; colIndex++)
                    {
                        if (!SqlTypeConverter.CanMakeConsistentTypes(types[colIndex], row.ColumnValues[colIndex].GetType(null, null), out var colType))
                            throw new NotSupportedQueryFragmentException("No available implicit type conversion", row.ColumnValues[colIndex]);

                        types[colIndex] = colType;
                    }
                }

                // Convert the values
                var constantScan = new ConstantScanNode();

                foreach (var row in inlineDerivedTable.RowValues)
                {
                    var entity = new Entity();

                    for (var colIndex = 0; colIndex < types.Count; colIndex++)
                        entity[columnNames[colIndex]] = SqlTypeConverter.ChangeType(row.ColumnValues[colIndex].GetValue(null, null, null, null), types[colIndex]);

                    constantScan.Values.Add(entity);
                }

                // Build the schema
                for (var colIndex = 0; colIndex < types.Count; colIndex++)
                    constantScan.Schema[inlineDerivedTable.Columns[colIndex].Value] = types[colIndex];

                constantScan.Alias = inlineDerivedTable.Alias.Value;

                return constantScan;
            }

            if (reference is UnqualifiedJoin unqualifiedJoin)
            {
                var lhs = ConvertTableReference(unqualifiedJoin.FirstTableReference, hints, query, outerSchema, outerReferences, parameterTypes);
                IDataExecutionPlanNode rhs;
                Dictionary<string, string> lhsReferences;

                if (unqualifiedJoin.UnqualifiedJoinType == UnqualifiedJoinType.CrossJoin)
                {
                    rhs = ConvertTableReference(unqualifiedJoin.SecondTableReference, hints, query, outerSchema, outerReferences, parameterTypes);
                    lhsReferences = null;
                }
                else
                {
                    // CROSS APPLY / OUTER APPLY - treat the second table as a correlated subquery
                    var lhsSchema = lhs.GetSchema(Metadata, parameterTypes);
                    lhsReferences = new Dictionary<string, string>();
                    var innerParameterTypes = parameterTypes == null ? new Dictionary<string, Type>() : new Dictionary<string, Type>(parameterTypes);
                    var subqueryPlan = ConvertTableReference(unqualifiedJoin.SecondTableReference, hints, query, lhsSchema, lhsReferences, innerParameterTypes);
                    rhs = subqueryPlan;

                    // If the subquery is uncorrelated, add a table spool to cache the results
                    // If it is correlated, add a spool where possible closer to the data source
                    if (lhsReferences.Count == 0)
                    {
                        var spool = new TableSpoolNode { Source = rhs };
                        rhs = spool;
                    }
                    else if (UseMergeJoin(lhs, subqueryPlan, lhsReferences, null, null, out _, out var merge))
                    {
                        if (unqualifiedJoin.UnqualifiedJoinType == UnqualifiedJoinType.CrossApply)
                            merge.JoinType = QualifiedJoinType.Inner;

                        return merge;
                    }
                    else if (rhs is ISingleSourceExecutionPlanNode loopRightSourceSimple)
                    {
                        InsertCorrelatedSubquerySpool(loopRightSourceSimple, lhs, parameterTypes);
                    }
                }

                // For cross joins there is no outer reference so the entire result can be spooled for reuse
                if (unqualifiedJoin.UnqualifiedJoinType == UnqualifiedJoinType.CrossJoin)
                    rhs = new TableSpoolNode { Source = rhs };
                
                return new NestedLoopNode
                {
                    LeftSource = lhs,
                    RightSource = rhs,
                    JoinType = unqualifiedJoin.UnqualifiedJoinType == UnqualifiedJoinType.OuterApply ? QualifiedJoinType.LeftOuter : QualifiedJoinType.Inner,
                    OuterReferences = lhsReferences
                };
            }

            throw new NotSupportedQueryFragmentException("Unhandled table reference", reference);
        }

        private string ConvertQueryHints(IList<OptimizerHint> hints)
        {
            if (hints.Count == 0)
                return null;

            return String.Join(",", hints.Select(hint =>
                {
                    switch (hint.HintKind)
                    {
                        case OptimizerHintKind.OptimizeFor:
                            if (!((OptimizeForOptimizerHint)hint).IsForUnknown)
                                return null;

                            return "OptimizeForUnknown";

                        case OptimizerHintKind.ForceOrder:
                            return "ForceOrder";

                        case OptimizerHintKind.Recompile:
                            return "Recompile";

                        case OptimizerHintKind.Unspecified:
                            if (!(hint is UseHintList useHint))
                                return null;

                            return String.Join(",", useHint.Hints.Select(hintLiteral =>
                            {
                                switch (hintLiteral.Value.ToUpperInvariant())
                                {
                                    case "DISABLE_OPTIMIZER_ROWGOAL":
                                        return "DisableRowGoal";

                                    case "ENABLE_QUERY_OPTIMIZER_HOTFIXES":
                                        return "EnableOptimizerHotfixes";

                                    default:
                                        return null;
                                }
                            }));

                        case OptimizerHintKind.LoopJoin:
                            return "LoopJoin";

                        case OptimizerHintKind.MergeJoin:
                            return "MergeJoin";

                        case OptimizerHintKind.HashJoin:
                            return "HashJoin";

                        case OptimizerHintKind.NoPerformanceSpool:
                            return "NO_PERFORMANCE_SPOOL";

                        case OptimizerHintKind.MaxRecursion:
                            return $"MaxRecursion={((LiteralOptimizerHint)hint).Value.Value}";

                        default:
                            return null;
                    }
                })
                .Where(hint => hint != null));
        }
    }
}
