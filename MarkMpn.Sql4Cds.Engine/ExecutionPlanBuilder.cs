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

namespace MarkMpn.Sql4Cds.Engine
{
    public class ExecutionPlanBuilder
    {
        private int _colNameCounter;

        public ExecutionPlanBuilder(IAttributeMetadataCache metadata, IQueryExecutionOptions options)
        {
            Metadata = metadata;
            Options = options;
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
        /// Indicates how the query will be executed
        /// </summary>
        public IQueryExecutionOptions Options { get; set; }

        /// <summary>
        /// Indicates if the TDS Endpoint is available to use if necessary
        /// </summary>
        public bool TDSEndpointAvailable { get; set; }

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
            var optimizer = new ExecutionPlanOptimizer(Metadata, Options);

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
                        plan = ConvertSelectStatement(select.QueryExpression, null, null);
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

        private SelectNode ConvertSelectStatement(QueryExpression query, NodeSchema outerSchema, Dictionary<string,string> outerReferences)
        {
            if (query is QuerySpecification querySpec)
                return ConvertSelectQuerySpec(querySpec, outerSchema, outerReferences);

            if (query is BinaryQueryExpression binary)
                return ConvertBinaryQuery(binary, outerSchema, outerReferences);

            throw new NotSupportedQueryFragmentException("Unhandled SELECT query expression", query);
        }

        private SelectNode ConvertBinaryQuery(BinaryQueryExpression binary, NodeSchema outerSchema, Dictionary<string, string> outerReferences)
        {
            if (binary.BinaryQueryExpressionType != BinaryQueryExpressionType.Union)
                throw new NotSupportedQueryFragmentException($"Unhandled {binary.BinaryQueryExpressionType} query type", binary);

            if (binary.ForClause != null)
                throw new NotSupportedQueryFragmentException("Unhandled FOR clause", binary.ForClause);

            var left = ConvertSelectStatement(binary.FirstQueryExpression, outerSchema, outerReferences);
            var right = ConvertSelectStatement(binary.SecondQueryExpression, outerSchema, outerReferences);

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

            var node = (IExecutionPlanNode)concat;

            if (!binary.All)
            {
                var distinct = new DistinctNode { Source = node };
                distinct.Columns.AddRange(concat.ColumnSet.Select(col => col.OutputColumn));
                node = distinct;
            }

            node = ConvertOrderByClause(node, binary.OrderByClause, concat.ColumnSet.Select(col => new ColumnReferenceExpression { MultiPartIdentifier = new MultiPartIdentifier { Identifiers = { new Identifier { Value = col.OutputColumn } } } }).ToArray(), binary);
            node = ConvertOffsetClause(node, binary.OffsetClause);

            var select = new SelectNode { Source = node };
            select.ColumnSet.AddRange(concat.ColumnSet.Select(col => new SelectColumn { SourceColumn = col.OutputColumn, OutputColumn = col.OutputColumn }));

            return select;
        }

        private SelectNode ConvertSelectQuerySpec(QuerySpecification querySpec, NodeSchema outerSchema, Dictionary<string,string> outerReferences)
        {
            // Each table in the FROM clause starts as a separate FetchXmlScan node. Add appropriate join nodes
            // TODO: Handle queries without a FROM clause
            var node = ConvertFromClause(querySpec.FromClause.TableReferences, querySpec);

            // Add filters from WHERE
            node = ConvertWhereClause(node, querySpec.WhereClause, outerSchema, outerReferences);

            // Add aggregates from GROUP BY/SELECT/HAVING/ORDER BY
            node = ConvertGroupByAggregates(node, querySpec);

            // Add filters from HAVING
            node = ConvertHavingClause(node, querySpec.HavingClause);

            // Add sorts from ORDER BY
            var selectFields = new List<ScalarExpression>();
            var preOrderSchema = node.GetSchema(Metadata);
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

            node = ConvertOrderByClause(node, querySpec.OrderByClause, selectFields.ToArray(), querySpec);

            // Add DISTINCT
            var distinct = querySpec.UniqueRowFilter == UniqueRowFilter.Distinct ? new DistinctNode { Source = node } : null;
            node = distinct ?? node;

            // Add TOP/OFFSET
            if (querySpec.TopRowFilter != null && querySpec.OffsetClause != null)
                throw new NotSupportedQueryFragmentException("A TOP can not be used in the same query or sub-query as a OFFSET.", querySpec.TopRowFilter);

            node = ConvertTopClause(node, querySpec.TopRowFilter);
            node = ConvertOffsetClause(node, querySpec.OffsetClause);

            // Add SELECT
            var selectNode = ConvertSelectClause(querySpec.SelectElements, node, distinct, querySpec);

            return selectNode;
        }

        private IExecutionPlanNode ConvertHavingClause(IExecutionPlanNode source, HavingClause havingClause)
        {
            if (havingClause == null)
                return source;

            return new FilterNode
            {
                Filter = havingClause.SearchCondition,
                Source = source
            };
        }

        private IExecutionPlanNode ConvertGroupByAggregates(IExecutionPlanNode source, QuerySpecification querySpec)
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

            var schema = source.GetSchema(Metadata);

            // Create the grouping expressions. Grouping is done on single columns only - if a grouping is a more complex expression,
            // create a new calculated column using a Compute Scalar node first.
            var groupings = new Dictionary<ScalarExpression, ColumnReferenceExpression>();

            if (querySpec.GroupByClause != null)
            {
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

        private IExecutionPlanNode ConvertOffsetClause(IExecutionPlanNode source, OffsetClause offsetClause)
        {
            if (offsetClause == null)
                return source;

            var schema = source.GetSchema(Metadata);
            var offset = SqlTypeConverter.ChangeType<int>(offsetClause.OffsetExpression.GetValue(null, schema));
            var fetch = SqlTypeConverter.ChangeType<int>(offsetClause.FetchExpression.GetValue(null, schema));

            if (offset < 0)
                throw new NotSupportedQueryFragmentException("The offset specified in a OFFSET clause may not be negative.", offsetClause.OffsetExpression);

            if (fetch <= 0)
                throw new NotSupportedQueryFragmentException("The number of rows provided for a FETCH clause must be greater then zero.", offsetClause.FetchExpression);

            return new OffsetFetchNode
            {
                Source = source,
                Offset = offset,
                Fetch = fetch
            };
        }

        private IExecutionPlanNode ConvertTopClause(IExecutionPlanNode source, TopRowFilter topRowFilter)
        {
            if (topRowFilter == null)
                return source;

            // TOP x PERCENT requires evaluating the source twice - once to get the total count and again to get the top
            // records. Cache the results in a table spool node.
            if (topRowFilter.Percent)
                source = new TableSpoolNode { Source = source };

            var schema = source.GetSchema(Metadata);
            var topCount = topRowFilter.Expression.GetValue(null, schema);

            return new TopNode
            {
                Source = source,
                Top = topRowFilter.Percent ? SqlTypeConverter.ChangeType<float>(topCount) : (float)SqlTypeConverter.ChangeType<int>(topCount),
                Percent = topRowFilter.Percent,
                WithTies = topRowFilter.WithTies
            };
        }

        private IExecutionPlanNode ConvertOrderByClause(IExecutionPlanNode source, OrderByClause orderByClause, ScalarExpression[] selectList, TSqlFragment query)
        {
            if (orderByClause == null)
                return source;

            var schema = source.GetSchema(Metadata);
            var sort = new SortNode { Source = source };

            // Check if any of the order expressions need pre-calculation
            var computeScalar = new ComputeScalarNode { Source = source };

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

                // Anything other than fields should be pre-calculated
                if (!(orderBy.Expression is ColumnReferenceExpression))
                {
                    var calculated = ComputeScalarExpression(orderBy.Expression, query, computeScalar, ref source);
                    sort.Source = source;
                    schema = source.GetSchema(Metadata);
                }

                // Validate the expression
                orderBy.Expression.GetType(schema);

                sort.Sorts.Add(orderBy);
            }

            if (computeScalar.Columns.Any())
                sort.Source = computeScalar;

            return sort;
        }

        private IExecutionPlanNode ConvertWhereClause(IExecutionPlanNode source, WhereClause whereClause, NodeSchema outerSchema, Dictionary<string,string> outerReferences)
        {
            if (whereClause == null)
                return source;

            if (whereClause.Cursor != null)
                throw new NotSupportedQueryFragmentException("Unsupported cursor", whereClause.Cursor);

            if (outerSchema != null)
            {
                // We're in a subquery. Check if any columns in the WHERE clause are from the outer query
                // so we know which columns to pass through and rewrite the filter to use parameters
                var rewrites = new Dictionary<ScalarExpression, ScalarExpression>();
                var innerSchema = source.GetSchema(Metadata);
                var columns = whereClause.SearchCondition.GetColumns();

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

                        rewrites.Add(
                            new ColumnReferenceExpression { MultiPartIdentifier = new MultiPartIdentifier { Identifiers = { new Identifier { Value = column } } } },
                            new VariableReference { Name = paramName });
                    }
                }

                if (rewrites.Any())
                    whereClause.SearchCondition.Accept(new RewriteVisitor(rewrites));
            }

            return new FilterNode
            {
                Filter = whereClause.SearchCondition,
                Source = source
            };
        }

        private SelectNode ConvertSelectClause(IList<SelectElement> selectElements, IExecutionPlanNode node, DistinctNode distinct, TSqlFragment query)
        {
            var schema = node.GetSchema(Metadata);

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
                        var alias = ComputeScalarExpression(scalar.Expression, query, computeScalar, ref scalarSource);

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
                        var distinctSchema = distinct.GetSchema(Metadata);
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

        private string ComputeScalarExpression(ScalarExpression expression, TSqlFragment query, ComputeScalarNode computeScalar, ref IExecutionPlanNode node)
        {
            var alias = $"Expr{++_colNameCounter}";

            // If scalar.Expression contains a subquery, create nested loop to evaluate it in the context
            // of the current record
            var subqueryVisitor = new ScalarSubqueryVisitor();
            expression.Accept(subqueryVisitor);
            var rewrites = new Dictionary<ScalarExpression, string>();

            foreach (var subquery in subqueryVisitor.Subqueries)
            {
                var outerSchema = node.GetSchema(Metadata);
                var outerReferences = new Dictionary<string, string>();
                var subqueryPlan = ConvertSelectStatement(subquery.QueryExpression, outerSchema, outerReferences);

                // Scalar subquery must return exactly one column and one row
                if (subqueryPlan.ColumnSet.Count != 1)
                    throw new NotSupportedQueryFragmentException("Scalar subquery must return exactly one column", subquery);

                // Insert an aggregate and assertion nodes to check for one row
                var subqueryCol = $"Expr{++_colNameCounter}";
                var rowCountCol = $"Expr{++_colNameCounter}";
                var aggregate = new HashMatchAggregateNode
                {
                    Source = subqueryPlan.Source,
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

                // Add a nested loop to call the subquery
                var loop = new NestedLoopNode
                {
                    LeftSource = node,
                    RightSource = assert,
                    JoinType = QualifiedJoinType.LeftOuter,
                    OuterReferences = outerReferences
                };

                node = loop;
                computeScalar.Source = loop;

                rewrites[subquery] = subqueryCol;
            }

            if (rewrites.Any())
                query.Accept(new RewriteVisitor(rewrites));

            if (rewrites.ContainsKey(expression))
                expression = new ColumnReferenceExpression { MultiPartIdentifier = new MultiPartIdentifier { Identifiers = { new Identifier { Value = rewrites[expression] } } } };

            // Check the type of this expression now so any errors can be reported
            var computeScalarSchema = computeScalar.GetSchema(Metadata);
            expression.GetType(computeScalarSchema);

            computeScalar.Columns[alias] = expression;

            return alias;
        }

        private IExecutionPlanNode ConvertFromClause(IList<TableReference> tables, TSqlFragment query)
        {
            var node = ConvertTableReference(tables[0], query);

            for (var i = 1; i < tables.Count; i++)
            {
                var nextTable = ConvertTableReference(tables[i], query);

                // TODO: See if we can lift a join predicate from the WHERE clause
                nextTable = new TableSpoolNode { Source = nextTable };

                node = new NestedLoopNode { LeftSource = node, RightSource = nextTable };
            }

            return node;
        }

        private IExecutionPlanNode ConvertTableReference(TableReference reference, TSqlFragment query)
        {
            if (reference is NamedTableReference table)
            {
                var entityName = table.SchemaObject.BaseIdentifier.Value;

                // Validate the entity name
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
                var lhs = ConvertTableReference(join.FirstTableReference, query);
                var rhs = ConvertTableReference(join.SecondTableReference, query);
                var lhsSchema = lhs.GetSchema(Metadata);
                var rhsSchema = rhs.GetSchema(Metadata);

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

                            var lhsColumn = ComputeScalarExpression(joinConditionVisitor.LhsExpression, query, lhsComputeScalar, ref lhs);
                            joinConditionVisitor.LhsKey = new ColumnReferenceExpression { MultiPartIdentifier = new MultiPartIdentifier { Identifiers = { new Identifier { Value = lhsColumn } } } };
                        }

                        if (joinConditionVisitor.RhsKey == null)
                        {
                            if (!(rhs is ComputeScalarNode rhsComputeScalar))
                            {
                                rhsComputeScalar = new ComputeScalarNode { Source = rhs };
                                rhs = rhsComputeScalar;
                            }

                            var rhsColumn = ComputeScalarExpression(joinConditionVisitor.RhsExpression, query, rhsComputeScalar, ref lhs);
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
                var joinSchema = joinNode.GetSchema(Metadata);
                join.SearchCondition.GetType(joinSchema);

                return joinNode;
            }

            throw new NotImplementedException();
        }
    }
}
