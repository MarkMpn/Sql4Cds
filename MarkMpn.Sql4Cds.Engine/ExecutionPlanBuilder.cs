﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlTypes;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.ServiceModel;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using MarkMpn.Sql4Cds.Engine.Visitors;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Metadata;
using SelectColumn = MarkMpn.Sql4Cds.Engine.ExecutionPlan.SelectColumn;

namespace MarkMpn.Sql4Cds.Engine
{
    class ExecutionPlanBuilder
    {
        private ExpressionCompilationContext _staticContext;
        private NodeCompilationContext _nodeContext;
        private Dictionary<string, AliasNode> _cteSubplans;

        public ExecutionPlanBuilder(SessionContext session, IQueryExecutionOptions options)
        {
            // Clone the session so any changes we make to the tempdb while building the query aren't
            // exposed when we come to run the query
            Session = new SessionContext(session);
            Options = options;

            if (!Session.DataSources.ContainsKey(Options.PrimaryDataSource))
                throw new ArgumentOutOfRangeException(nameof(options), "Primary data source " + options.PrimaryDataSource + " not found");

            EstimatedPlanOnly = true;
        }

        /// <summary>
        /// The session that the query will be executed in
        /// </summary>
        public SessionContext Session { get; }

        /// <summary>
        /// Indicates how the query will be executed
        /// </summary>
        public IQueryExecutionOptions Options { get; }

        /// <summary>
        /// Indicates if only a simplified plan for display purposes is required
        /// </summary>
        public bool EstimatedPlanOnly { get; set; }

        /// <summary>
        /// A callback function to log messages
        /// </summary>
        public Action<Sql4CdsError> Log { get; set; }

        private DataSource PrimaryDataSource => Session.DataSources[Options.PrimaryDataSource];

        /// <summary>
        /// Builds the execution plans for a SQL command
        /// </summary>
        /// <param name="sql">The SQL command to generate the execution plans for</param>
        /// <param name="parameters">The types of parameters that are available to the SQL command</param>
        /// <param name="useTDSEndpointDirectly">Indicates if the SQL command should be executed directly against the TDS endpoint</param>
        /// <returns></returns>
        public IRootExecutionPlanNode[] Build(string sql, IDictionary<string, DataTypeReference> parameters, out bool useTDSEndpointDirectly)
        {
            // Take a copy of the defined parameters so we can add more while we're building the query without
            // affecting the original collection until the query is actually run
            var localParameterTypes = new Dictionary<string, DataTypeReference>(StringComparer.OrdinalIgnoreCase);

            if (parameters != null)
            {
                foreach (var param in parameters)
                    localParameterTypes[param.Key] = param.Value;
            }

            _staticContext = new ExpressionCompilationContext(Session, Options, localParameterTypes, null, null);
            _nodeContext = new NodeCompilationContext(Session, Options, localParameterTypes, Log);

            var queries = new List<IRootExecutionPlanNodeInternal>();

            // Parse the SQL DOM
            var dom = new TSql160Parser(Options.QuotedIdentifiers);
            var fragment = dom.Parse(new StringReader(sql), out var errors);

            // Check if there were any parse errors
            if (errors.Count > 0)
                throw new QueryParseException(errors[0]);

            // Validate any query hints
            var hintValidator = new OptimizerHintValidatingVisitor(false);
            fragment.Accept(hintValidator);

            if (hintValidator.TdsCompatible && TDSEndpoint.CanUseTDSEndpoint(Options, PrimaryDataSource.Connection))
            {
                using (var con = PrimaryDataSource.Connection == null ? null : TDSEndpoint.Connect(PrimaryDataSource.Connection))
                {
                    var tdsEndpointCompatibilityVisitor = new TDSEndpointCompatibilityVisitor(con, PrimaryDataSource.Metadata);
                    fragment.Accept(tdsEndpointCompatibilityVisitor);

                    if (tdsEndpointCompatibilityVisitor.IsCompatible && !tdsEndpointCompatibilityVisitor.RequiresCteRewrite)
                    {
                        useTDSEndpointDirectly = true;
                        var sqlNode = new SqlNode
                        {
                            DataSource = Options.PrimaryDataSource,
                            Sql = sql,
                            Index = 0,
                            Length = sql.Length
                        };

                        if (parameters != null)
                        {
                            foreach (var param in parameters.Keys)
                                sqlNode.Parameters.Add(param);
                        }

                        return new IRootExecutionPlanNode[] { sqlNode };
                    }
                }
            }

            useTDSEndpointDirectly = false;

            var script = (TSqlScript)fragment;
            script.Accept(new ReplacePrimaryFunctionsVisitor());
            script.Accept(new ExplicitCollationVisitor());
            var optimizer = new ExecutionPlanOptimizer(Session, Options, localParameterTypes, !EstimatedPlanOnly, _nodeContext.Log);

            // Convert each statement in turn to the appropriate query type
            foreach (var batch in script.Batches)
            {
                foreach (var statement in batch.Statements)
                    queries.AddRange(ConvertStatement(statement, optimizer));
            }

            // Ensure GOTOs only reference valid labels
            var labels = GetLabels(queries)
                .GroupBy(l => l.Label, StringComparer.OrdinalIgnoreCase)
                .ToDictionary(g => g.Key, g => g.ToList(), StringComparer.OrdinalIgnoreCase);

            foreach (var gotoNode in queries.OfType<GoToNode>())
            {
                if (!labels.ContainsKey(gotoNode.Label))
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.UnknownGotoLabel((GoToStatement)gotoNode.Statement));
            }

            // Ensure all labels are unique
            foreach (var kvp in labels)
            {
                if (kvp.Value.Count > 1)
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.DuplicateGotoLabel(kvp.Value[1].Statement));
            }

            // Ensure GOTOs don't enter a TRY or CATCH block
            foreach (var gotoNode in queries.OfType<GoToNode>())
            {
                var label = labels[gotoNode.Label][0];

                if (!TryCatchPath(gotoNode, queries).StartsWith(TryCatchPath(label, queries)))
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.GotoIntoTryOrCatch((GoToStatement)gotoNode.Statement));
            }

            // Ensure rethrows are within a CATCH block
            foreach (var rethrow in queries.OfType<ThrowNode>().Where(@throw => @throw.ErrorNumber == null))
            {
                if (!TryCatchPath(rethrow, queries).Contains("/catch-"))
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.ThrowOutsideCatch(rethrow.Statement));
            }

            if (EstimatedPlanOnly)
            {
                foreach (var node in queries)
                    EstimateRowsOut(node, _nodeContext);
            }

            return queries.ToArray();
        }

        private string TryCatchPath(IRootExecutionPlanNodeInternal node, List<IRootExecutionPlanNodeInternal> queries)
        {
            var path = new Stack<string>();

            for (var i = 0; i < queries.Count; i++)
            {
                if (queries[i] == node)
                    break;

                if (queries[i] is BeginTryNode)
                    path.Push("try-" + i);
                else if (queries[i] is EndTryNode)
                    path.Pop();
                else if (queries[i] is BeginCatchNode)
                    path.Push("catch-" + i);
                else if (queries[i] is EndCatchNode)
                    path.Pop();
            }

            if (path.Count == 0)
                return "/";

            return "/" + String.Join("/", path.Reverse()) + "/";
        }

        private IRootExecutionPlanNodeInternal[] ConvertStatement(TSqlStatement statement, ExecutionPlanOptimizer optimizer)
        {
            if (statement is BeginEndBlockStatement block)
            {
                return block.StatementList.Statements
                    .SelectMany(stmt => ConvertStatement(stmt, optimizer))
                    .ToArray();
            }

            var lineNumber = statement.StartLine;
            var index = statement.StartOffset;
            var length = statement.ScriptTokenStream[statement.LastTokenIndex].Offset + statement.ScriptTokenStream[statement.LastTokenIndex].Text.Length - index;
            var originalSql = statement.ToSql();

            var converted = ConvertControlOfFlowStatement(statement, optimizer);

            if (converted != null)
            {
                foreach (var qry in converted)
                {
                    if (qry.Sql == null)
                        qry.Sql = originalSql;

                    qry.LineNumber = lineNumber;
                    qry.Index = index;
                    qry.Length = length;
                }
            }

            if (converted == null)
                converted = ConvertStatementInternal(statement, optimizer);

            return converted;
        }

        private IRootExecutionPlanNodeInternal[] ConvertControlOfFlowStatement(TSqlStatement statement, ExecutionPlanOptimizer optimizer)
        {
            if (statement is LabelStatement label)
            {
                return new[] { ConvertLabelStatement(label) };
            }
            else if (statement is GoToStatement gotoStmt)
            {
                return new[] { ConvertGoToStatement(gotoStmt) };
            }
            else if (!EstimatedPlanOnly && statement is IfStatement ifStmt)
            {
                var converted = ConvertIfStatement(ifStmt, optimizer, false);
                SetParent(converted);
                return converted.FoldQuery(_nodeContext, null);
            }
            else if (!EstimatedPlanOnly && statement is WhileStatement whileStmt)
            {
                var converted = ConvertWhileStatement(whileStmt, optimizer, false);
                SetParent(converted);
                return converted.FoldQuery(_nodeContext, null);
            }
            else if (statement is BreakStatement breakStmt)
            {
                return new[] { ConvertBreakStatement(breakStmt) };
            }
            else if (statement is ContinueStatement continueStmt)
            {
                return new[] { ConvertContinueStatement(continueStmt) };
            }
            else if (statement is TryCatchStatement tryCatch)
            {
                return ConvertTryCatchStatement(tryCatch, optimizer);
            }
            else if (statement is ThrowStatement @throw && @throw.ErrorNumber == null)
            {
                return ConvertThrowStatement(@throw);
            }
            else if (!EstimatedPlanOnly)
            {
                return new[] { new UnparsedStatementNode { Statement = statement, Compiler = this, Optimizer = optimizer } };
            }

            return null;
        }

        private IRootExecutionPlanNodeInternal[] ConvertTryCatchStatement(TryCatchStatement tryCatch, ExecutionPlanOptimizer optimizer)
        {
            var nodes = new List<IRootExecutionPlanNodeInternal>();

            nodes.Add(new BeginTryNode());
            nodes.AddRange(tryCatch.TryStatements.Statements.SelectMany(s => ConvertStatement(s, optimizer)));
            nodes.Add(new EndTryNode());
            nodes.Add(new BeginCatchNode());
            nodes.AddRange(tryCatch.CatchStatements.Statements.SelectMany(s => ConvertStatement(s, optimizer)));
            nodes.Add(new EndCatchNode());

            return nodes.ToArray();
        }

        internal IRootExecutionPlanNodeInternal[] ConvertStatementInternal(TSqlStatement statement, ExecutionPlanOptimizer optimizer)
        {
            var lineNumber = statement.StartLine;
            var index = statement.StartOffset;
            var length = statement.ScriptTokenStream[statement.LastTokenIndex].Offset + statement.ScriptTokenStream[statement.LastTokenIndex].Text.Length - index;
            var originalSql = statement.ToSql();

            IRootExecutionPlanNodeInternal[] plans;
            IList<OptimizerHint> hints = null;
            _cteSubplans = new Dictionary<string, AliasNode>(StringComparer.OrdinalIgnoreCase);

            if (statement is StatementWithCtesAndXmlNamespaces stmtWithCtes)
                hints = stmtWithCtes.OptimizerHints;

            if (statement is SelectStatement select)
                plans = new[] { ConvertSelectStatement(select) };
            else if (statement is UpdateStatement update)
                plans = new[] { ConvertUpdateStatement(update) };
            else if (statement is DeleteStatement delete)
                plans = new[] { ConvertDeleteStatement(delete) };
            else if (statement is InsertStatement insert)
                plans = new[] { ConvertInsertStatement(insert) };
            else if (statement is ExecuteAsStatement impersonate)
                plans = new[] { ConvertExecuteAsStatement(impersonate) };
            else if (statement is RevertStatement revert)
                plans = new[] { ConvertRevertStatement(revert) };
            else if (statement is DeclareVariableStatement declare)
                plans = ConvertDeclareVariableStatement(declare);
            else if (statement is SetVariableStatement set)
                plans = new[] { ConvertSetVariableStatement(set) };
            else if (statement is IfStatement ifStmt)
                plans = new[] { ConvertIfStatement(ifStmt, optimizer, true) };
            else if (statement is WhileStatement whileStmt)
                plans = new[] { ConvertWhileStatement(whileStmt, optimizer, true) };
            else if (statement is PrintStatement print)
                plans = new[] { ConvertPrintStatement(print, optimizer) };
            else if (statement is WaitForStatement waitFor)
                plans = new[] { ConvertWaitForStatement(waitFor) };
            else if (statement is ExecuteStatement execute)
                plans = ConvertExecuteStatement(execute);
            else if (statement is ThrowStatement @throw)
                plans = ConvertThrowStatement(@throw);
            else if (statement is RaiseErrorStatement raiserror)
                plans = ConvertRaiseErrorStatement(raiserror);
            else if (statement is SetCommandStatement setCommand)
                plans = ConvertSetCommandStatement(setCommand);
            else if (statement is CreateTableStatement createTable)
                plans = ConvertCreateTableStatement(createTable);
            else if (statement is DropTableStatement dropTable)
                plans = ConvertDropTableStatement(dropTable);
            else if (statement is DeclareCursorStatement declareCursor)
                plans = ConvertDeclareCursorStatement(declareCursor);
            else if (statement is OpenCursorStatement openCursor)
                plans = ConvertOpenCursorStatement(openCursor);
            else if (statement is FetchCursorStatement fetchCursor)
                plans = ConvertFetchCursorStatement(fetchCursor);
            else if (statement is CloseCursorStatement closeCursor)
                plans = ConvertCloseCursorStatement(closeCursor);
            else if (statement is DeallocateCursorStatement deallocateCursor)
                plans = ConvertDeallocateCursorStatement(deallocateCursor);
            else
                throw new NotSupportedQueryFragmentException(Sql4CdsError.NotSupported(statement, statement.GetType().Name.Replace("Statement", "").ToUpperInvariant()));

            var output = new List<IRootExecutionPlanNodeInternal>();

            foreach (var plan in plans)
            {
                SetParent(plan);
                var optimized = optimizer.Optimize(plan, hints);

                foreach (var qry in optimized)
                {
                    if (qry.Sql == null)
                        qry.Sql = originalSql;

                    qry.LineNumber = lineNumber;
                    qry.Index = index;
                    qry.Length = length;
                }

                output.AddRange(optimized);

                if (plan is CreateTableNode createTable)
                {
                    // Create the table now in the local copy of the tempdb to allow converting later statements
                    if (!Session.TempDb.Tables.Contains(createTable.TableDefinition.TableName))
                        Session.TempDb.Tables.Add(createTable.TableDefinition.Clone());
                }
                else if (plan is DropTableNode dropTable)
                {
                    // Remove the table now in the local copy of the tempdb for validating later statements
                    if (Session.TempDb.Tables.Contains(dropTable.TableName))
                        Session.TempDb.Tables.Remove(dropTable.TableName);
                }
            }

            return output.ToArray();
        }

        private void ConvertCTEs(StatementWithCtesAndXmlNamespaces stmtWithCtes)
        {
            if (stmtWithCtes.WithCtesAndXmlNamespaces == null)
                return;
            
            foreach (var cte in stmtWithCtes.WithCtesAndXmlNamespaces.CommonTableExpressions)
            {
                if (_cteSubplans.ContainsKey(cte.ExpressionName.Value))
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.CteDuplicateName(cte.ExpressionName));

                var cteValidator = new CteValidatorVisitor();
                cte.Accept(cteValidator);

                // Start by converting the anchor query to a subquery
                var plan = ConvertSelectStatement(cteValidator.AnchorQuery, stmtWithCtes.OptimizerHints, null, null, _nodeContext);

                plan.ExpandWildcardColumns(_nodeContext);

                // Apply column aliases
                if (cte.Columns.Count > 0)
                {
                    if (cte.Columns.Count < plan.ColumnSet.Count)
                        throw new NotSupportedQueryFragmentException(Sql4CdsError.TableValueConstructorTooManyColumns(cte.ExpressionName));

                    if (cte.Columns.Count > plan.ColumnSet.Count)
                        throw new NotSupportedQueryFragmentException(Sql4CdsError.TableValueConstructorTooFewColumns(cte.ExpressionName));

                    for (var i = 0; i < cte.Columns.Count; i++)
                        plan.ColumnSet[i].OutputColumn = cte.Columns[i].Value;
                }

                for (var i = 0; i < plan.ColumnSet.Count; i++)
                {
                    if (plan.ColumnSet[i].OutputColumn == null)
                        throw new NotSupportedQueryFragmentException(Sql4CdsError.CteUnnamedColumn(cte.ExpressionName, i + 1));
                }

                var anchorQuery = new AliasNode(plan, cte.ExpressionName, _nodeContext);
                _cteSubplans.Add(cte.ExpressionName.Value, anchorQuery);

                if (cteValidator.RecursiveQueries.Count > 0)
                {
                    anchorQuery = (AliasNode)anchorQuery.Clone();
                    var ctePlan = anchorQuery.Source;
                    var anchorSchema = anchorQuery.GetSchema(_nodeContext);

                    // Add a ComputeScalar node to add the initial recursion depth (0)
                    var recursionDepthField = _nodeContext.GetExpressionName();
                    var initialRecursionDepthComputeScalar = new ComputeScalarNode
                    {
                        Source = ctePlan,
                        Columns =
                                {
                                    [recursionDepthField] = new IntegerLiteral { Value = "0" }
                                }
                    };

                    // Add a ConcatenateNode to combine the anchor results with the recursion results
                    var recurseConcat = new ConcatenateNode
                    {
                        Sources = { initialRecursionDepthComputeScalar },
                    };

                    foreach (var col in anchorQuery.ColumnSet)
                    {
                        var concatCol = new ConcatenateColumn
                        {
                            SourceColumns = { col.SourceColumn },
                            OutputColumn = col.OutputColumn
                        };

                        recurseConcat.ColumnSet.Add(concatCol);

                        col.SourceColumn = col.OutputColumn;
                    }

                    recurseConcat.ColumnSet.Add(new ConcatenateColumn
                    {
                        SourceColumns = { recursionDepthField },
                        OutputColumn = recursionDepthField
                    });

                    // Add an IndexSpool node in stack mode to enable the recursion
                    var recurseIndexStack = new IndexSpoolNode
                    {
                        Source = recurseConcat,
                        WithStack = true
                    };

                    // Pull the same records into the recursive loop
                    var recurseTableSpool = new TableSpoolNode
                    {
                        Producer = recurseIndexStack,
                        SpoolType = SpoolType.Lazy
                    };

                    // Increment the depth
                    var incrementedDepthField = _nodeContext.GetExpressionName();
                    var incrementRecursionDepthComputeScalar = new ComputeScalarNode
                    {
                        Source = recurseTableSpool,
                        Columns =
                                {
                                    [incrementedDepthField] = new BinaryExpression
                                    {
                                        FirstExpression = recursionDepthField.ToColumnReference(),
                                        BinaryExpressionType = BinaryExpressionType.Add,
                                        SecondExpression = new IntegerLiteral { Value = "1" }
                                    }
                                }
                    };

                    // Use a nested loop to pass through the records to the recusive queries
                    var recurseLoop = new NestedLoopNode
                    {
                        LeftSource = incrementRecursionDepthComputeScalar,
                        JoinType = QualifiedJoinType.Inner,
                        OuterReferences = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
                    };

                    // Capture all CTE fields in the outer references
                    foreach (var col in anchorSchema.Schema)
                        recurseLoop.OuterReferences[col.Key.SplitMultiPartIdentifier().Last().EscapeIdentifier()] = "@" + _nodeContext.GetExpressionName();

                    if (cteValidator.RecursiveQueries.Count > 1)
                    {
                        // Combine the results of each recursive query with a concat node
                        var concat = new ConcatenateNode();
                        recurseLoop.RightSource = concat;

                        foreach (var qry in cteValidator.RecursiveQueries)
                        {
                            var rightSource = ConvertRecursiveCTEQuery(qry, anchorSchema, cteValidator, recurseLoop.OuterReferences);
                            concat.Sources.Add(rightSource.Source);

                            if (concat.Sources.Count == 1)
                            {
                                for (var i = 0; i < rightSource.ColumnSet.Count; i++)
                                {
                                    var col = rightSource.ColumnSet[i];
                                    var expr = _nodeContext.GetExpressionName();
                                    concat.ColumnSet.Add(new ConcatenateColumn { OutputColumn = expr });
                                    recurseLoop.DefinedValues.Add(expr, expr);
                                    recurseConcat.ColumnSet[i].SourceColumns.Add(expr);
                                }
                            }

                            for (var i = 0; i < rightSource.ColumnSet.Count; i++)
                                concat.ColumnSet[i].SourceColumns.Add(rightSource.ColumnSet[i].SourceColumn);
                        }
                    }
                    else
                    {
                        var rightSource = ConvertRecursiveCTEQuery(cteValidator.RecursiveQueries[0], anchorSchema, cteValidator, recurseLoop.OuterReferences);
                        recurseLoop.RightSource = rightSource.Source;

                        for (var i = 0; i < rightSource.ColumnSet.Count; i++)
                        {
                            var col = rightSource.ColumnSet[i];
                            var expr = _nodeContext.GetExpressionName();

                            recurseLoop.DefinedValues.Add(expr, col.SourceColumn);
                            recurseConcat.ColumnSet[i].SourceColumns.Add(expr);
                        }
                    }

                    // Ensure we don't get stuck in an infinite loop
                    var maxRecursionHint = stmtWithCtes.OptimizerHints
                        .OfType<LiteralOptimizerHint>()
                        .Where(hint => hint.HintKind == OptimizerHintKind.MaxRecursion)
                        .FirstOrDefault();

                    var maxRecursion = maxRecursionHint
                        ?.Value
                        ?.Value
                        ?? "100";

                    if (!Int32.TryParse(maxRecursion, out var max) || max < 0)
                        throw new NotSupportedQueryFragmentException(Sql4CdsError.SyntaxError(maxRecursionHint)) { Suggestion = "Invalid MAXRECURSION hint" };

                    if (max > 32767)
                        throw new NotSupportedQueryFragmentException(Sql4CdsError.ExceededMaxRecursion(maxRecursionHint, 32767, max));

                    if (max > 0)
                    {
                        var assert = new AssertNode
                        {
                            Source = recurseLoop,
                            Assertion = e =>
                            {
                                var depth = e.GetAttributeValue<SqlInt32>(incrementedDepthField);
                                return depth.Value < max;
                            },
                            ErrorMessage = "Recursion depth exceeded"
                        };

                        // Combine the recursion results into the main results
                        recurseConcat.Sources.Add(assert);
                    }
                    else
                    {
                        recurseConcat.Sources.Add(recurseLoop);
                    }

                    recurseConcat.ColumnSet.Last().SourceColumns.Add(incrementedDepthField);

                    anchorQuery.Source = recurseIndexStack;
                    _cteSubplans[cte.ExpressionName.Value] = anchorQuery;
                }
            }
        }

        private IRootExecutionPlanNodeInternal[] ConvertDeallocateCursorStatement(DeallocateCursorStatement deallocateCursor)
        {
            return new[] { new DeallocateCursorNode { CursorName = deallocateCursor.Cursor.Name.Value } };
        }

        private IRootExecutionPlanNodeInternal[] ConvertCloseCursorStatement(CloseCursorStatement closeCursor)
        {
            return new[] { new CloseCursorNode { CursorName = closeCursor.Cursor.Name.Value } };
        }

        private IRootExecutionPlanNodeInternal[] ConvertFetchCursorStatement(FetchCursorStatement fetchCursor)
        {
            if (fetchCursor.FetchType?.RowOffset != null)
            {
                // Validate the row offset
                var ecc = new ExpressionCompilationContext(_nodeContext, null, null);
                fetchCursor.FetchType.RowOffset.GetType(ecc, out var rowOffsetType);

                if (!rowOffsetType.IsSameAs(DataTypeHelpers.TinyInt) &&
                    !rowOffsetType.IsSameAs(DataTypeHelpers.SmallInt) &&
                    !rowOffsetType.IsSameAs(DataTypeHelpers.Int))
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidParameterValue(fetchCursor.FetchType.RowOffset, "FETCH", "rownumber-type"));

                // Use a consistent int type
                if (!rowOffsetType.IsSameAs(DataTypeHelpers.Int))
                    fetchCursor.FetchType.RowOffset = new ConvertCall { Parameter = fetchCursor.FetchType.RowOffset, DataType = DataTypeHelpers.Int };
            }

            if (fetchCursor.IntoVariables == null || fetchCursor.IntoVariables.Count == 0)
                return new[] { new FetchCursorNode { CursorName = fetchCursor.Cursor.Name.Value, Orientation = fetchCursor.FetchType?.Orientation ?? FetchOrientation.Next, RowOffset = fetchCursor.FetchType?.RowOffset } };
            else
                return new[] { new FetchCursorIntoNode { CursorName = fetchCursor.Cursor.Name.Value, Variables = fetchCursor.IntoVariables, Orientation = fetchCursor.FetchType?.Orientation ?? FetchOrientation.Next, RowOffset = fetchCursor.FetchType?.RowOffset } };
        }

        private IRootExecutionPlanNodeInternal[] ConvertOpenCursorStatement(OpenCursorStatement openCursor)
        {
            return new[] { new OpenCursorNode { CursorName = openCursor.Cursor.Name.Value } };
        }

        private IRootExecutionPlanNodeInternal[] ConvertCreateTableStatement(CreateTableStatement createTable)
        {
            return new[] { CreateTableNode.FromStatement(createTable) };
        }

        private IRootExecutionPlanNodeInternal[] ConvertDropTableStatement(DropTableStatement dropTable)
        {
            var nodes = new List<IRootExecutionPlanNodeInternal>();
            var errors = new List<Sql4CdsError>();
            var suggestions = new HashSet<string>();

            foreach (var table in dropTable.Objects)
            {
                // Only drop temporary tables for now
                if (table.DatabaseIdentifier != null)
                {
                    errors.Add(Sql4CdsError.NotSupported(table, "Database name"));
                    suggestions.Add("Only temporary tables are supported");
                    continue;
                }
                else if (table.SchemaIdentifier != null)
                {
                    errors.Add(Sql4CdsError.NotSupported(table, "Schema name"));
                    suggestions.Add("Only temporary tables are supported");
                    continue;
                }
                else if (!table.BaseIdentifier.Value.StartsWith("#"))
                {
                    errors.Add(Sql4CdsError.NotSupported(table, "Non-temporary table"));
                    suggestions.Add("Only temporary tables are supported");
                    continue;
                }

                nodes.Add(new DropTableNode
                {
                    TableName = table.BaseIdentifier.Value,
                    IfExists = dropTable.IsIfExists
                });
            }

            if (errors.Count > 0)
                throw new NotSupportedQueryFragmentException(errors.ToArray(), null) { Suggestion = String.Join(Environment.NewLine, suggestions) };

            return nodes.ToArray();
        }

        private IRootExecutionPlanNodeInternal[] ConvertDeclareCursorStatement(DeclareCursorStatement declareCursor)
        {
            // Validate the combination of cursor options
            var options = declareCursor.CursorDefinition.Options.ToDictionary(o => o.OptionKind);
            var errors = new List<Sql4CdsError>();

            var localOrGlobal = new[] { CursorOptionKind.Local, CursorOptionKind.Global };
            var fowardOnlyOrScroll = new[] { CursorOptionKind.ForwardOnly, CursorOptionKind.Scroll };
            var cursorType = new[] { CursorOptionKind.Static, CursorOptionKind.Keyset, CursorOptionKind.Dynamic, CursorOptionKind.FastForward };
            var lockType = new[] { CursorOptionKind.ReadOnly, CursorOptionKind.ScrollLocks, CursorOptionKind.Optimistic };

            var exclusiveOptions = new[]
            {
                localOrGlobal,
                fowardOnlyOrScroll,
                cursorType,
                lockType,
            };

            foreach (var exclusiveSet in exclusiveOptions)
            {
                var matchingOptions = declareCursor.CursorDefinition.Options.Where(o => exclusiveSet.Contains(o.OptionKind)).ToList();

                for (var i = 1; i < matchingOptions.Count; i++)
                    errors.Add(Sql4CdsError.ConflictingCursorOption(matchingOptions[0], matchingOptions[i]));
            }

            // We don't support all cursor options for now
            var supportedOptions = new[]
            {
                CursorOptionKind.Local,
                CursorOptionKind.Global,
                CursorOptionKind.ForwardOnly,
                CursorOptionKind.Scroll,
                CursorOptionKind.Static,
                CursorOptionKind.ReadOnly,
                CursorOptionKind.Insensitive,
            };

            foreach (var option in declareCursor.CursorDefinition.Options)
            {
                if (!supportedOptions.Contains(option.OptionKind))
                    errors.Add(Sql4CdsError.NotSupported(option, option.ToNormalizedSql().Trim()));
            }

            if (errors.Count > 0)
                throw new NotSupportedQueryFragmentException(errors.ToArray(), null);

            // Convert the query as normal
            var select = ConvertSelectStatement(declareCursor.CursorDefinition.Select);

            // Create the appropriate type of cursor
            var type = GetCursorOption(options, cursorType);

            CursorDeclarationBaseNode cursor;

            switch (type)
            {
                case CursorOptionKind.Static:
                    cursor = StaticCursorNode.FromQuery(_nodeContext, select);
                    break;

                default:
                    throw new NotImplementedException();
            }

            cursor.CursorName = declareCursor.Name.Value;
            cursor.Scope = GetCursorOption(options, localOrGlobal);
            cursor.Direction = GetCursorOption(options, fowardOnlyOrScroll, cursor.Direction);

            return new[] { cursor };
        }

        private CursorOptionKind GetCursorOption(Dictionary<CursorOptionKind, CursorOption> options, CursorOptionKind[] allowedOptions)
        {
            return GetCursorOption(options, allowedOptions, allowedOptions[0]);
        }

        private CursorOptionKind GetCursorOption(Dictionary<CursorOptionKind, CursorOption> options, CursorOptionKind[] allowedOptions, CursorOptionKind defaultValue)
        {
            foreach (var option in allowedOptions)
            {
                if (options.ContainsKey(option))
                    return option;
            }

            return defaultValue;
        }

        private IDmlQueryExecutionPlanNode[] ConvertSetCommandStatement(SetCommandStatement setCommand)
        {
            return setCommand.Commands
                .Select(c => ConvertSetCommand(c))
                .ToArray();
        }

        private IDmlQueryExecutionPlanNode ConvertSetCommand(SetCommand setCommand)
        {
            if (setCommand is GeneralSetCommand cmd)
            {
                switch (cmd.CommandType)
                {
                    case GeneralSetCommandType.DateFormat:
                        return new SetDateFormatNode(cmd.Parameter);
                }
            }

            throw new NotSupportedQueryFragmentException(Sql4CdsError.NotSupported(setCommand, setCommand.ToNormalizedSql()));
        }

        private SelectNode ConvertRecursiveCTEQuery(QueryExpression queryExpression, INodeSchema anchorSchema, CteValidatorVisitor cteValidator, Dictionary<string, string> outerReferences)
        {
            // Convert the query using the anchor query as a subquery to check for ambiguous column names
            ConvertSelectStatement(queryExpression.Clone(), null, null, null, _nodeContext);

            // Remove recursive references from the FROM clause, moving join predicates to the WHERE clause
            // If the recursive reference was in an unqualified join, replace it with (SELECT @Expr1, @Expr2) AS cte (field1, field2)
            // Otherwise, remove it entirely and replace column references with variables
            var cteReplacer = new RemoveRecursiveCTETableReferencesVisitor(cteValidator.Name, anchorSchema.Schema.Keys.ToArray(), outerReferences);
            queryExpression.Accept(cteReplacer);

            // Convert the modified query.
            var childContext = _nodeContext.CreateChildContext(outerReferences.ToDictionary(kvp => kvp.Value, kvp => anchorSchema.Schema[cteValidator.Name.EscapeIdentifier() + "." + kvp.Key.EscapeIdentifier()].Type, StringComparer.OrdinalIgnoreCase));
            var converted = ConvertSelectStatement(queryExpression, null, null, null, childContext);
            converted.ExpandWildcardColumns(childContext);
            return converted;
        }

        private IRootExecutionPlanNodeInternal[] ConvertThrowStatement(ThrowStatement @throw)
        {
            var ecc = new ExpressionCompilationContext(_nodeContext, null, null);

            if (@throw.ErrorNumber != null)
            {
                @throw.ErrorNumber.GetType(ecc, out var type);
                if (!SqlTypeConverter.CanChangeTypeImplicit(type, DataTypeHelpers.Int))
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.TypeClash(@throw.ErrorNumber, type, DataTypeHelpers.Int));
            }

            if (@throw.Message != null)
            {
                @throw.Message.GetType(ecc, out var type);
                var messageType = DataTypeHelpers.NVarChar(2048, _nodeContext.PrimaryDataSource.DefaultCollation, CollationLabel.CoercibleDefault);
                if (!SqlTypeConverter.CanChangeTypeImplicit(type, messageType))
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.TypeClash(@throw.Message, type, messageType));
            }

            if (@throw.State != null)
            {
                @throw.State.GetType(ecc, out var type);
                if (!SqlTypeConverter.CanChangeTypeImplicit(type, DataTypeHelpers.Int))
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.TypeClash(@throw.State, type, DataTypeHelpers.Int));
            }

            return new[]
            {
                new ThrowNode
                {
                    ErrorNumber = @throw.ErrorNumber,
                    ErrorMessage = @throw.Message,
                    State = @throw.State,
                    Statement = @throw
                }
            };
        }

        private IRootExecutionPlanNodeInternal[] ConvertRaiseErrorStatement(RaiseErrorStatement raiserror)
        {
            var ecc = new ExpressionCompilationContext(_nodeContext, null, null);
            raiserror.FirstParameter.GetType(ecc, out var msgType);

            // T-SQL supports using integer values for RAISERROR but we don't have sys.messages available so require a string
            if ((!(msgType is SqlDataTypeReference msgSqlType) || !msgSqlType.SqlDataTypeOption.IsStringType()) &&
                !msgType.IsSameAs(DataTypeHelpers.Int))
                throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidRaiseErrorParameterType(raiserror.FirstParameter, msgType, 1));

            // Severity and State must be integers
            raiserror.SecondParameter.GetType(ecc, out var severityType);

            if (!SqlTypeConverter.CanChangeTypeImplicit(severityType, DataTypeHelpers.Int))
                throw new NotSupportedQueryFragmentException(Sql4CdsError.TypeClash(raiserror.SecondParameter, severityType, DataTypeHelpers.Int));

            raiserror.ThirdParameter.GetType(ecc, out var stateType);

            if (!SqlTypeConverter.CanChangeTypeImplicit(stateType, DataTypeHelpers.Int))
                throw new NotSupportedQueryFragmentException(Sql4CdsError.TypeClash(raiserror.ThirdParameter, stateType, DataTypeHelpers.Int));

            // Can't support more than 20 parameters
            if (raiserror.OptionalParameters.Count > 20)
                throw new NotSupportedQueryFragmentException(Sql4CdsError.ExceedeMaxRaiseErrorParameters(raiserror.OptionalParameters[20], 20));

            // All parameters must be tinyint, smallint, int, char, varchar, nchar, nvarchar, binary, or varbinary.
            var allowedParamTypes = new[]
            {
                SqlDataTypeOption.TinyInt,
                SqlDataTypeOption.SmallInt,
                SqlDataTypeOption.Int,
                SqlDataTypeOption.Char,
                SqlDataTypeOption.VarChar,
                SqlDataTypeOption.NChar,
                SqlDataTypeOption.NVarChar,
                SqlDataTypeOption.Binary,
                SqlDataTypeOption.VarBinary
            };

            for (var i = 0; i < raiserror.OptionalParameters.Count; i++)
            {
                raiserror.OptionalParameters[i].GetType(ecc, out var paramType);

                if (!(paramType is SqlDataTypeReference paramSqlType) || !allowedParamTypes.Contains(paramSqlType.SqlDataTypeOption))
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidRaiseErrorParameterType(raiserror.OptionalParameters[i], paramType, i + 4));
            }

            if (raiserror.RaiseErrorOptions.HasFlag(RaiseErrorOptions.Log))
                throw new NotSupportedQueryFragmentException(Sql4CdsError.SysAdminRequired(raiserror, "WITH LOG", "RAISERROR"));

            return new[]
            {
                new RaiseErrorNode
                {
                    ErrorNumber = msgType.IsSameAs(DataTypeHelpers.Int) ? raiserror.FirstParameter : null,
                    ErrorMessage = msgType.IsSameAs(DataTypeHelpers.Int) ? null : raiserror.FirstParameter,
                    Severity = raiserror.SecondParameter,
                    State = raiserror.ThirdParameter,
                    Parameters = raiserror.OptionalParameters.ToArray()
                }
            };
        }

        private IRootExecutionPlanNodeInternal[] ConvertExecuteStatement(ExecuteStatement execute)
        {
            var nodes = new List<IRootExecutionPlanNodeInternal>();

            if (execute.Options != null && execute.Options.Count > 0)
                throw new NotSupportedQueryFragmentException(Sql4CdsError.NotSupported(execute.Options[0], "EXECUTE WITH"));

            if (execute.ExecuteSpecification.ExecuteContext != null)
                throw new NotSupportedQueryFragmentException(Sql4CdsError.NotSupported(execute.ExecuteSpecification.ExecuteContext, "EXECUTE AS"));

            if (!(execute.ExecuteSpecification.ExecutableEntity is ExecutableProcedureReference sproc))
                throw new NotSupportedQueryFragmentException(Sql4CdsError.NotSupported(execute.ExecuteSpecification.ExecutableEntity, "EXECUTE <string>")) { Suggestion = "EXECUTE can only be used to execute messages as stored procedures" };

            if (sproc.AdHocDataSource != null)
                throw new NotSupportedQueryFragmentException(Sql4CdsError.NotSupported(sproc.AdHocDataSource, "AT DATA_SOURCE"));

            if (sproc.ProcedureReference.ProcedureVariable != null)
                throw new NotSupportedQueryFragmentException(Sql4CdsError.NotSupported(sproc.ProcedureReference.ProcedureVariable, "stored procedure variable name"));

            var dataSource = SelectDataSource(sproc.ProcedureReference.ProcedureReference.Name);

            var node = ExecuteMessageNode.FromMessage(sproc, dataSource, _staticContext);
            var schema = node.GetSchema(_nodeContext);

            dataSource.MessageCache.TryGetValue(node.MessageName, out var message);

            var outputParams = sproc.Parameters.Where(p => p.IsOutput).ToList();

            foreach (var outputParam in outputParams)
            {
                if (!message.OutputParameters.Any(p => p.IsScalarType() && p.Name.Equals(outputParam.Variable.Name.Substring(1), StringComparison.OrdinalIgnoreCase)))
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidParameterName(outputParam, sproc.ProcedureReference.ProcedureReference.Name));
            }

            if (message.OutputParameters.Count == 0 || outputParams.Count == 0)
            {
                if (message.OutputParameters.Any(p => !p.IsScalarType()))
                {
                    // Expose the produced data set
                    var select = new SelectNode { Source = node, LogicalSourceSchema = schema };

                    foreach (var col in schema.Schema.Keys.OrderBy(col => col))
                        select.ColumnSet.Add(new SelectColumn { SourceColumn = col, OutputColumn = col });

                    nodes.Add(select);
                }
                else
                {
                    nodes.Add(node);
                }
            }
            else
            {
                // Capture scalar output variables
                var assignVariablesNode = new AssignVariablesNode { Source = node };

                foreach (var outputParam in outputParams)
                {
                    var sourceCol = outputParam.Variable.Name.Substring(1);

                    if (!schema.ContainsColumn(sourceCol, out sourceCol))
                        throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidParameterName(outputParam, sproc.ProcedureReference.ProcedureReference.Name));

                    if (!(outputParam.ParameterValue is VariableReference targetVariable))
                        throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidOutputConstant(outputParam, sproc.ProcedureReference.ProcedureReference.Name));

                    if (!_nodeContext.ParameterTypes.TryGetValue(targetVariable.Name, out var targetVariableType))
                        throw new NotSupportedQueryFragmentException(Sql4CdsError.UndeclaredVariable(targetVariable));

                    var sourceType = schema.Schema[sourceCol].Type;

                    if (!SqlTypeConverter.CanChangeTypeImplicit(sourceType, targetVariableType))
                    {
                        var err = Sql4CdsError.TypeClash(outputParam, sourceType, targetVariableType);
                        err.Procedure = sproc.ProcedureReference.ProcedureReference.Name.BaseIdentifier.Value;
                        throw new NotSupportedQueryFragmentException(err);
                    }

                    assignVariablesNode.Variables.Add(new VariableAssignment
                    {
                        SourceColumn = sourceCol,
                        VariableName = targetVariable.Name
                    });
                }

                nodes.Add(assignVariablesNode);
            }

            // Capture single return values in execute.ExecuteSpecification.Variable
            if (execute.ExecuteSpecification.Variable != null)
            {
                // Variable should be set to 1 when sproc executes successfully.
                if (!_nodeContext.ParameterTypes.TryGetValue(execute.ExecuteSpecification.Variable.Name, out var returnStatusType))
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.UndeclaredVariable(execute.ExecuteSpecification.Variable));

                if (!SqlTypeConverter.CanChangeTypeImplicit(DataTypeHelpers.Int, returnStatusType))
                {
                    var err = Sql4CdsError.TypeClash(execute.ExecuteSpecification.Variable, DataTypeHelpers.Int, returnStatusType);
                    err.Procedure = sproc.ProcedureReference.ProcedureReference.Name.BaseIdentifier.Value;
                    throw new NotSupportedQueryFragmentException(err);
                }

                var constName = _nodeContext.GetExpressionName();

                nodes.Add(new AssignVariablesNode
                {
                    Variables =
                    {
                        new VariableAssignment
                        {
                            VariableName = execute.ExecuteSpecification.Variable.Name,
                            SourceColumn = constName
                        }
                    },
                    Source = new ConstantScanNode
                    {
                        Schema =
                        {
                            [constName] = new ExecutionPlan.ColumnDefinition(DataTypeHelpers.Int, false, true)
                        },
                        Values =
                        {
                            new Dictionary<string, ScalarExpression>
                            {
                                [constName] = new IntegerLiteral { Value = "1" }
                            }
                        }
                    }
                });
            }

            return nodes.ToArray();
        }

        private IRootExecutionPlanNodeInternal ConvertWaitForStatement(WaitForStatement waitFor)
        {
            if (waitFor.WaitForOption == WaitForOption.Statement)
                throw new NotSupportedQueryFragmentException(Sql4CdsError.NotSupported(waitFor, "WAITFOR <statement>"));

            waitFor.Parameter.GetType(_staticContext, out _);

            return new WaitForNode
            {
                Time = waitFor.Parameter.Clone(),
                WaitType = waitFor.WaitForOption
            };
        }

        private IRootExecutionPlanNodeInternal ConvertContinueStatement(ContinueStatement continueStmt)
        {
            return new ContinueBreakNode { Type = ContinueBreakNodeType.Continue };
        }

        private IRootExecutionPlanNodeInternal ConvertBreakStatement(BreakStatement breakStmt)
        {
            return new ContinueBreakNode { Type = ContinueBreakNodeType.Break };
        }

        private IRootExecutionPlanNodeInternal ConvertGoToStatement(GoToStatement gotoStmt)
        {
            return new GoToNode { Label = gotoStmt.LabelName.Value, Statement = gotoStmt };
        }

        private IRootExecutionPlanNodeInternal ConvertLabelStatement(LabelStatement label)
        {
            return new GotoLabelNode { Label = label.Value.TrimEnd(':'), Statement = label };
        }

        private IEnumerable<GotoLabelNode> GetLabels(IEnumerable<IRootExecutionPlanNodeInternal> queries)
        {
            return queries.OfType<GotoLabelNode>()
                .Concat(queries.SelectMany(q => GetLabels(q.GetSources().OfType<IRootExecutionPlanNodeInternal>())));
        }

        private IRootExecutionPlanNodeInternal ConvertPrintStatement(PrintStatement print, ExecutionPlanOptimizer optimizer)
        {
            // Check if the value is a simple expression or requires a query. Subqueries are not allowed
            var subqueryVisitor = new ScalarSubqueryVisitor();
            print.Expression.Accept(subqueryVisitor);

            if (subqueryVisitor.Subqueries.Count > 0)
                throw new NotSupportedQueryFragmentException(Sql4CdsError.SubqueriesNotAllowed(print.Expression));

            // Check the expression for errors. Ensure it can be converted to a string
            var expr = print.Expression.Clone();

            if (expr.GetType(_staticContext, out _) != typeof(SqlString))
            {
                expr = new ConvertCall
                {
                    DataType = typeof(SqlString).ToSqlType(PrimaryDataSource),
                    Parameter = expr
                };

                expr.GetType(_staticContext, out _);
            }

            return new PrintNode
            {
                Expression = expr
            };
        }

        private IRootExecutionPlanNodeInternal ConvertIfWhileStatement(TSqlStatement statement, ConditionalNodeType type, BooleanExpression predicate, TSqlStatement trueStatement, TSqlStatement falseStatement, ExecutionPlanOptimizer optimizer, bool optimize)
        {
            if (!optimize)
            {
                var unparsed = new UnparsedConditionalNode
                {
                    Compiler = this,
                    Optimizer = optimizer,
                    Predicate = predicate,
                    Statement = statement,
                    Type = type,
                };

                if (trueStatement != null)
                    unparsed.TrueStatements.AddRange(ConvertStatement(trueStatement, optimizer));

                if (falseStatement != null)
                    unparsed.FalseStatements.AddRange(ConvertStatement(falseStatement, optimizer));

                return unparsed;
            }

            ConvertPredicateQuery(predicate, out var predicateSource, out var sourceCol);

            // Convert the true & false branches
            var trueQueries = ConvertStatement(trueStatement, optimizer);

            IRootExecutionPlanNodeInternal[] falseQueries = null;

            if (falseStatement != null)
                falseQueries = ConvertStatement(falseStatement, optimizer);

            return new ConditionalNode
            {
                Condition = predicateSource == null ? predicate.Clone() : null,
                Source = predicateSource,
                SourceColumn = sourceCol,
                TrueStatements = trueQueries.ToArray(),
                FalseStatements = falseQueries?.ToArray(),
                Type = type
            };
        }

        internal bool ConvertPredicateQuery(BooleanExpression predicate, out IDataExecutionPlanNodeInternal predicateSource, out string sourceCol)
        {
            predicateSource = null;
            sourceCol = null;

            // Check if the predicate is a simple expression or requires a query
            var subqueryVisitor = new ScalarSubqueryVisitor();
            predicate.Accept(subqueryVisitor);
            
            if (subqueryVisitor.Subqueries.Count == 0)
            {
                // Check the predicate for errors
                predicate.GetType(_staticContext, out _);
                return false;
            }
            
            // Convert predicate to query - IF EXISTS(qry) => SELECT CASE WHEN EXISTS(qry) THEN 1 ELSE 0 END
            var select = new QuerySpecification
            {
                SelectElements =
                {
                    new SelectScalarExpression
                    {
                        Expression = new SearchedCaseExpression
                        {
                            WhenClauses =
                            {
                                new SearchedWhenClause
                                {
                                    WhenExpression = predicate,
                                    ThenExpression = new IntegerLiteral { Value = "1" }
                                }
                            },
                            ElseExpression = new IntegerLiteral { Value = "0" }
                        }
                    }
                }
            };

            var selectQry = ConvertSelectQuerySpec(select, Array.Empty<OptimizerHint>(), null, null, _nodeContext);
            predicateSource = selectQry.Source;
            sourceCol = selectQry.ColumnSet[0].SourceColumn;
            return true;
        }

        private IRootExecutionPlanNodeInternal ConvertWhileStatement(WhileStatement whileStmt, ExecutionPlanOptimizer optimizer, bool optimize)
        {
            return ConvertIfWhileStatement(whileStmt, ConditionalNodeType.While, whileStmt.Predicate, whileStmt.Statement, null, optimizer, optimize);
        }

        private IRootExecutionPlanNodeInternal ConvertIfStatement(IfStatement ifStmt, ExecutionPlanOptimizer optimizer, bool optimize)
        {
            return ConvertIfWhileStatement(ifStmt, ConditionalNodeType.If, ifStmt.Predicate, ifStmt.ThenStatement, ifStmt.ElseStatement, optimizer, optimize);
        }

        private IRootExecutionPlanNodeInternal ConvertSetVariableStatement(SetVariableStatement set)
        {
            if (set.CursorDefinition != null)
                throw new NotSupportedQueryFragmentException(Sql4CdsError.NotSupported(set.CursorDefinition, "CURSOR"));

            if (set.FunctionCallExists)
                throw new NotSupportedQueryFragmentException(Sql4CdsError.NotSupported(set, "custom functions"));

            if (set.Identifier != null)
                throw new NotSupportedQueryFragmentException(Sql4CdsError.NotSupported(set, "user defined types"));

            if (set.Parameters != null && set.Parameters.Count > 0)
                throw new NotSupportedQueryFragmentException(Sql4CdsError.NotSupported(set.Parameters[0], "custom functions"));

            if (!_nodeContext.ParameterTypes.TryGetValue(set.Variable.Name, out var paramType))
                throw new NotSupportedQueryFragmentException(Sql4CdsError.UndeclaredVariable(set.Variable));

            // Create the SELECT statement that generates the required information
            var expr = set.Expression;

            switch (set.AssignmentKind)
            {
                case AssignmentKind.AddEquals:
                    expr = new BinaryExpression { FirstExpression = set.Variable, BinaryExpressionType = BinaryExpressionType.Add, SecondExpression = expr };
                    break;

                case AssignmentKind.BitwiseAndEquals:
                    expr = new BinaryExpression { FirstExpression = set.Variable, BinaryExpressionType = BinaryExpressionType.BitwiseAnd, SecondExpression = expr };
                    break;

                case AssignmentKind.BitwiseOrEquals:
                    expr = new BinaryExpression { FirstExpression = set.Variable, BinaryExpressionType = BinaryExpressionType.BitwiseOr, SecondExpression = expr };
                    break;

                case AssignmentKind.BitwiseXorEquals:
                    expr = new BinaryExpression { FirstExpression = set.Variable, BinaryExpressionType = BinaryExpressionType.BitwiseXor, SecondExpression = expr };
                    break;

                case AssignmentKind.DivideEquals:
                    expr = new BinaryExpression { FirstExpression = set.Variable, BinaryExpressionType = BinaryExpressionType.Divide, SecondExpression = expr };
                    break;

                case AssignmentKind.ModEquals:
                    expr = new BinaryExpression { FirstExpression = set.Variable, BinaryExpressionType = BinaryExpressionType.Modulo, SecondExpression = expr };
                    break;

                case AssignmentKind.MultiplyEquals:
                    expr = new BinaryExpression { FirstExpression = set.Variable, BinaryExpressionType = BinaryExpressionType.Multiply, SecondExpression = expr };
                    break;

                case AssignmentKind.SubtractEquals:
                    expr = new BinaryExpression { FirstExpression = set.Variable, BinaryExpressionType = BinaryExpressionType.Subtract, SecondExpression = expr };
                    break;
            }

            expr = new ConvertCall { DataType = paramType, Parameter = expr };
            expr.ScriptTokenStream = null;

            var queryExpression = new QuerySpecification();
            queryExpression.SelectElements.Add(new SelectScalarExpression { Expression = expr, ColumnName = new IdentifierOrValueExpression { Identifier = new Identifier { Value = "Value" } } });
            var selectStatement = new SelectStatement { QueryExpression = queryExpression };

            var source = ConvertSelectStatement(selectStatement);

            var node = new AssignVariablesNode();

            if (source is SelectNode select)
            {
                node.Source = select.Source;
                node.Variables.Add(new VariableAssignment { VariableName = set.Variable.Name, SourceColumn = select.ColumnSet[0].SourceColumn });
            }
            else
            {
                node.Source = (IDataExecutionPlanNodeInternal)source;
                node.Variables.Add(new VariableAssignment { VariableName = set.Variable.Name, SourceColumn = "Value" });
            }

            return node;
        }

        private IRootExecutionPlanNodeInternal[] ConvertDeclareVariableStatement(DeclareVariableStatement declare)
        {
            var nodes = new List<IRootExecutionPlanNodeInternal>();
            var declareNode = new DeclareVariablesNode();
            nodes.Add(declareNode);

            foreach (var declaration in declare.Declarations)
            {
                if (_nodeContext.ParameterTypes.ContainsKey(declaration.VariableName.Value))
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.DuplicateVariable(declaration));

                // Apply default maximum length for [n][var]char types
                if (declaration.DataType is SqlDataTypeReference dataType)
                {
                    if (dataType.SqlDataTypeOption == SqlDataTypeOption.Cursor)
                        throw new NotSupportedQueryFragmentException(Sql4CdsError.NotSupported(dataType, "CURSOR"));

                    if (dataType.SqlDataTypeOption == SqlDataTypeOption.Table)
                        throw new NotSupportedQueryFragmentException(Sql4CdsError.NotSupported(dataType, "TABLE"));

                    if (dataType.SqlDataTypeOption == SqlDataTypeOption.Char ||
                        dataType.SqlDataTypeOption == SqlDataTypeOption.NChar ||
                        dataType.SqlDataTypeOption == SqlDataTypeOption.VarChar ||
                        dataType.SqlDataTypeOption == SqlDataTypeOption.NVarChar)
                    {
                        if (dataType.Parameters.Count == 0)
                            dataType.Parameters.Add(new IntegerLiteral { Value = "1" });

                        declaration.DataType = new SqlDataTypeReferenceWithCollation
                        {
                            SqlDataTypeOption = dataType.SqlDataTypeOption,
                            Parameters = { dataType.Parameters[0] },
                            Collation = _nodeContext.PrimaryDataSource.DefaultCollation,
                            CollationLabel = CollationLabel.CoercibleDefault
                        };
                    }
                }

                declareNode.Variables[declaration.VariableName.Value] = declaration.DataType;

                // Make the variables available in our local copy of parameters so later statements
                // in the same batch can use them
                _nodeContext.ParameterTypes[declaration.VariableName.Value] = declaration.DataType;

                if (declaration.Value != null)
                {
                    var setStatement = new SetVariableStatement
                    {
                        Variable = new VariableReference { Name = declaration.VariableName.Value },
                        AssignmentKind = AssignmentKind.Equals,
                        Expression = declaration.Value
                    };

                    nodes.Add(ConvertSetVariableStatement(setStatement));
                }
            }

            return nodes.ToArray();
        }

        private void SetParent(IExecutionPlanNodeInternal plan)
        {
            foreach (IExecutionPlanNodeInternal child in plan.GetSources())
            {
                child.Parent = plan;
                SetParent(child);
            }
        }

        private ExecuteAsNode ConvertExecuteAsStatement(ExecuteAsStatement impersonate)
        {
            // Check for any DOM elements we don't support converting
            if (impersonate.Cookie != null)
                throw new NotSupportedQueryFragmentException(Sql4CdsError.NotSupported(impersonate.Cookie, "WITH COOKIE"));

            if (impersonate.WithNoRevert)
                throw new NotSupportedQueryFragmentException(Sql4CdsError.NotSupported(impersonate, "WITH NO REVERT"));

            if (impersonate.ExecuteContext.Kind != ExecuteAsOption.Login &&
                impersonate.ExecuteContext.Kind != ExecuteAsOption.User)
                throw new NotSupportedQueryFragmentException(Sql4CdsError.NotSupported(impersonate.ExecuteContext, impersonate.ExecuteContext.Kind.ToString()));

            var subqueries = new ScalarSubqueryVisitor();
            impersonate.ExecuteContext.Principal.Accept(subqueries);
            if (subqueries.Subqueries.Count > 0)
                throw new NotSupportedQueryFragmentException(Sql4CdsError.SubqueriesNotAllowed(subqueries.Subqueries[0]));

            var columns = new ColumnCollectingVisitor();
            impersonate.ExecuteContext.Principal.Accept(columns);
            if (columns.Columns.Count > 0)
                throw new NotSupportedQueryFragmentException(Sql4CdsError.ConstantExpressionsOnly(columns.Columns[0]));

            // Validate the expression
            var ecc = new ExpressionCompilationContext(_nodeContext, null, null);
            var type = impersonate.ExecuteContext.Principal.GetType(ecc, out _);

            if (type != typeof(SqlString) && type != typeof(SqlEntityReference) && type != typeof(SqlGuid))
                throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidTypeForStatement(impersonate.ExecuteContext.Principal, "Execute As"));

            IDataExecutionPlanNodeInternal source;

            // Create a SELECT query to find the user ID
            var selectStatement = new SelectStatement
            {
                QueryExpression = new QuerySpecification
                {
                    SelectElements =
                    {
                        new SelectScalarExpression
                        {
                            Expression = impersonate.ExecuteContext.Principal,
                            ColumnName = new IdentifierOrValueExpression { Identifier = new Identifier { Value = "username" } }
                        },
                        new SelectScalarExpression
                        {
                            Expression = new FunctionCall
                            {
                                FunctionName = new Identifier { Value = "max" },
                                Parameters =
                                {
                                    "systemuserid".ToColumnReference()
                                }
                            },
                            ColumnName = new IdentifierOrValueExpression { Identifier = new Identifier { Value = "systemuserid" } }
                        },
                        new SelectScalarExpression
                        {
                            Expression = new FunctionCall
                            {
                                FunctionName = new Identifier { Value = "count" },
                                Parameters =
                                {
                                    new ColumnReferenceExpression { ColumnType = ColumnType.Wildcard }
                                }
                            },
                            ColumnName = new IdentifierOrValueExpression { Identifier = new Identifier { Value = "count" } }
                        }
                    },
                    FromClause = new FromClause
                    {
                        TableReferences =
                        {
                            new NamedTableReference { SchemaObject = new SchemaObjectName { Identifiers = { new Identifier { Value = "systemuser" } } } }
                        }
                    },
                    WhereClause = new WhereClause
                    {
                        SearchCondition = new BooleanComparisonExpression
                        {
                            FirstExpression = impersonate.ExecuteContext.Kind == ExecuteAsOption.Login ? "domainname".ToColumnReference() : "systemuserid".ToColumnReference(),
                            ComparisonType = BooleanComparisonType.Equals,
                            SecondExpression = impersonate.ExecuteContext.Principal
                        }
                    }
                }
            };

            if (type != typeof(SqlString))
            {
                ((SelectScalarExpression)((QuerySpecification)selectStatement.QueryExpression).SelectElements[0]).Expression = new ConvertCall
                {
                    Parameter = impersonate.ExecuteContext.Principal,
                    DataType = DataTypeHelpers.NVarChar(Int32.MaxValue, PrimaryDataSource.DefaultCollation, CollationLabel.CoercibleDefault)
                };
            }

            var userIdSource = "systemuserid";
            var filterValueSource = "username";
            var countSource = "count";

            var select = ConvertSelectStatement(selectStatement);

            if (select is SelectNode selectNode)
            {
                source = selectNode.Source;
                userIdSource = selectNode.ColumnSet.Single(c => c.OutputColumn == userIdSource).SourceColumn;
                filterValueSource = selectNode.ColumnSet.Single(c => c.OutputColumn == filterValueSource).SourceColumn;
                countSource = selectNode.ColumnSet.Single(c => c.OutputColumn == countSource).SourceColumn;
            }
            else
            {
                source = (IDataExecutionPlanNodeInternal)select;
            }

            return new ExecuteAsNode
            {
                UserIdSource = userIdSource,
                Source = source,
                DataSource = Options.PrimaryDataSource,
                FilterValueSource = filterValueSource,
                CountSource = countSource
            };
        }

        private RevertNode ConvertRevertStatement(RevertStatement revert)
        {
            return new RevertNode
            {
                DataSource = Options.PrimaryDataSource
            };
        }

        /// <summary>
        /// Convert an INSERT statement from SQL
        /// </summary>
        /// <param name="insert">The parsed INSERT statement</param>
        /// <returns>The equivalent query converted for execution against CDS</returns>
        private InsertNode ConvertInsertStatement(InsertStatement insert)
        {
            // Check for any DOM elements that don't have an equivalent in CDS
            if (insert.WithCtesAndXmlNamespaces != null)
                throw new NotSupportedQueryFragmentException(Sql4CdsError.NotSupported(insert.WithCtesAndXmlNamespaces, "WITH"));

            if (insert.InsertSpecification.Columns == null)
                throw new NotSupportedQueryFragmentException(Sql4CdsError.NotSupported(insert, "INSERT without column specification")) { Suggestion = "Define the column names to insert the values into, e.g. INSERT INTO table (col1, col2) VALUES (val1, val2)" };

            if (insert.InsertSpecification.OutputClause != null)
                throw new NotSupportedQueryFragmentException(Sql4CdsError.NotSupported(insert.InsertSpecification.OutputClause, "OUTPUT"));

            if (insert.InsertSpecification.OutputIntoClause != null)
                throw new NotSupportedQueryFragmentException(Sql4CdsError.NotSupported(insert.InsertSpecification.OutputIntoClause, "OUTPUT INTO"));

            if (!(insert.InsertSpecification.Target is NamedTableReference target))
                throw new NotSupportedQueryFragmentException(Sql4CdsError.NotSupported(insert.InsertSpecification.Target, "non-table INSERT target"));

            // Check if we are inserting constant values or the results of a SELECT statement and perform the appropriate conversion
            IExecutionPlanNodeInternal source;
            string[] columns;

            if (insert.InsertSpecification.InsertSource is ValuesInsertSource values)
                source = ConvertInsertValuesSource(values, insert.OptimizerHints, null, null, _nodeContext, out columns);
            else if (insert.InsertSpecification.InsertSource is SelectInsertSource select)
                source = ConvertInsertSelectSource(select, insert.OptimizerHints, out columns);
            else
                throw new NotSupportedQueryFragmentException(Sql4CdsError.NotSupported(insert.InsertSpecification.InsertSource, "unknown INSERT source"));

            return ConvertInsertSpecification(target, insert.InsertSpecification.Columns, source, columns, insert.OptimizerHints, insert);
        }

        private IDataExecutionPlanNodeInternal ConvertInsertValuesSource(ValuesInsertSource values, IList<OptimizerHint> hints, INodeSchema outerSchema, Dictionary<string, string> outerReferences, NodeCompilationContext context, out string[] columns)
        {
            // Convert the values to an InlineDerviedTable
            var table = new InlineDerivedTable
            {
                Alias = new Identifier { Value = context.GetExpressionName() }
            };

            foreach (var col in values.RowValues[0].ColumnValues)
                table.Columns.Add(new Identifier { Value = context.GetExpressionName() });

            foreach (var row in values.RowValues)
                table.RowValues.Add(row);

            columns = table.Columns.Select(col => table.Alias.Value + "." + col.Value.EscapeIdentifier()).ToArray();
            return ConvertInlineDerivedTable(table, hints, outerSchema, outerReferences, context);
        }

        private IExecutionPlanNodeInternal ConvertInsertSelectSource(SelectInsertSource selectSource, IList<OptimizerHint> hints, out string[] columns)
        {
            var selectStatement = new SelectStatement { QueryExpression = selectSource.Select };
            CopyDmlHintsToSelectStatement(hints, selectStatement);

            var select = ConvertSelectStatement(selectStatement);

            if (select is SelectNode selectNode)
            {
                columns = selectNode.ColumnSet.Select(col => col.SourceColumn).ToArray();
                return selectNode.Source;
            }

            if (select is SqlNode sql)
            {
                columns = sql.GetSchema(_nodeContext).Schema.Select(col => col.Key).ToArray();
                return sql;
            }

            throw new NotSupportedQueryFragmentException(Sql4CdsError.NotSupported(selectSource, "INSERT"));
        }

        private DataSource SelectDataSource(SchemaObjectName schemaObject)
        {
            var databaseName = schemaObject.DatabaseIdentifier?.Value ?? Options.PrimaryDataSource;
            
            if (!Session.DataSources.TryGetValue(databaseName, out var dataSource))
                throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidObjectName(schemaObject)) { Suggestion = $"Available database names:\r\n* {String.Join("\r\n* ", Session.DataSources.Keys.OrderBy(k => k))}" };

            return dataSource;
        }

        private InsertNode ConvertInsertSpecification(NamedTableReference target, IList<ColumnReferenceExpression> targetColumns, IExecutionPlanNodeInternal source, string[] sourceColumns, IList<OptimizerHint> queryHints, InsertStatement insertStatement)
        {
            var dataSource = SelectDataSource(target.SchemaObject);

            ValidateDMLSchema(target, false);

            // Validate the entity name
            var logicalName = target.SchemaObject.BaseIdentifier.Value;
            EntityReader reader;

            if (target.SchemaObject.DatabaseIdentifier == null && target.SchemaObject.SchemaIdentifier == null && logicalName.StartsWith("#"))
            {
                var table = Session.TempDb.Tables[logicalName];

                if (table == null)
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidObjectName(target.SchemaObject));

                reader = new EntityReader(table, _nodeContext, dataSource, insertStatement, target, source);
                logicalName = table.TableName;

                if (targetColumns.Count == 0)
                {
                    // Support INSERT without listed column names for temp tables - they often have a limited number of columns
                    // and can be inserted into easily.
                    var tableScan = new TableScanNode { TableName = logicalName, Alias = logicalName };
                    var tableSchema = tableScan.GetSchema(_nodeContext);

                    foreach (var col in tableSchema.Schema)
                    {
                        if (!col.Value.IsVisible)
                            continue;

                        targetColumns.Add(col.Key.SplitMultiPartIdentifier().Last().ToColumnReference());
                    }
                }
            }
            else
            {
                EntityMetadata metadata;

                try
                {
                    metadata = dataSource.Metadata[logicalName];
                }
                catch (FaultException ex)
                {
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidObjectName(target.SchemaObject), ex);
                }

                reader = new EntityReader(metadata, _nodeContext, dataSource, insertStatement, target, source);
                logicalName = metadata.LogicalName;

                // Do not support INSERT without listed column names for Dataverse entities - they tend to have lots of columns
                // some of which are not valid for insert so the implicit mapping of columns is not obvious and could lead to
                // mistakes.
            }

            var node = new InsertNode
            {
                DataSource = dataSource.Name,
                LogicalName = logicalName,
                Source = reader.Source,
                Accessors = reader.ValidateInsertColumnMapping(targetColumns, sourceColumns)
            };

            return node;
        }

        private void ValidateDMLSchema(NamedTableReference target, bool allowBin)
        {
            if (String.IsNullOrEmpty(target.SchemaObject.SchemaIdentifier?.Value))
                return;

            if (target.SchemaObject.SchemaIdentifier.Value.Equals("dbo", StringComparison.OrdinalIgnoreCase))
                return;

            if (target.SchemaObject.SchemaIdentifier.Value.Equals("archive", StringComparison.OrdinalIgnoreCase))
                throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidObjectName(target.SchemaObject)) { Suggestion = "Archive tables are read-only" };

            if (target.SchemaObject.SchemaIdentifier.Value.Equals("metadata", StringComparison.OrdinalIgnoreCase))
                throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidObjectName(target.SchemaObject)) { Suggestion = "Metadata tables are read-only" };

            if (target.SchemaObject.SchemaIdentifier.Value.Equals("bin", StringComparison.OrdinalIgnoreCase))
            {
                if (allowBin)
                    return;

                throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidObjectName(target.SchemaObject)) { Suggestion = "Recycle bin tables are valid for SELECT and DELETE only" };
            }

            throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidObjectName(target.SchemaObject)) { Suggestion = "All data tables are in the 'dbo' schema" };
        }

        private DeleteNode ConvertDeleteStatement(DeleteStatement delete)
        {
            if (delete.DeleteSpecification.OutputClause != null)
                throw new NotSupportedQueryFragmentException(Sql4CdsError.NotSupported(delete.DeleteSpecification.OutputClause, "OUTPUT"));

            if (delete.DeleteSpecification.OutputIntoClause != null)
                throw new NotSupportedQueryFragmentException(Sql4CdsError.NotSupported(delete.DeleteSpecification.OutputIntoClause, "OUTPUT INTO"));

            if (!(delete.DeleteSpecification.Target is NamedTableReference target))
                throw new NotSupportedQueryFragmentException(Sql4CdsError.NotSupported(delete.DeleteSpecification.Target, "non-table DELETE target"));

            if (delete.DeleteSpecification.WhereClause == null && Options.BlockDeleteWithoutWhere)
            {
                throw new NotSupportedQueryFragmentException("DELETE without WHERE is blocked by your settings", delete)
                {
                    Suggestion = "Add a WHERE clause to limit the records that will be deleted, or disable the \"Prevent DELETE without WHERE\" option in the settings window"
                };
            }

            // Create the SELECT statement that generates the required information
            var queryExpression = new QuerySpecification
            {
                FromClause = delete.DeleteSpecification.FromClause ?? new FromClause { TableReferences = { target } },
                WhereClause = delete.DeleteSpecification.WhereClause,
                UniqueRowFilter = UniqueRowFilter.Distinct,
                TopRowFilter = delete.DeleteSpecification.TopRowFilter,
            };

            var selectStatement = new SelectStatement
            {
                QueryExpression = queryExpression,
                WithCtesAndXmlNamespaces = delete.WithCtesAndXmlNamespaces
            };
            CopyDmlHintsToSelectStatement(delete.OptimizerHints, selectStatement);

            var deleteTarget = new UpdateTargetVisitor(target.SchemaObject, Options.PrimaryDataSource);
            selectStatement.Accept(deleteTarget);

            if (deleteTarget.Ambiguous)
                throw new NotSupportedQueryFragmentException(Sql4CdsError.AmbiguousTable(target));

            DataSource dataSource;

            if (deleteTarget.TargetSubquery == null && deleteTarget.TargetCTE == null)
            {
                // DELETE target is a simple table name
                if (String.IsNullOrEmpty(deleteTarget.TargetEntityName))
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidObjectName(target.SchemaObject)) { Suggestion = $"Target table '{target.ToSql()}' not found in FROM clause" };

                if (!Session.DataSources.TryGetValue(deleteTarget.TargetDataSource, out dataSource))
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidObjectName(target.SchemaObject)) { Suggestion = $"Available database names:\r\n* {String.Join("\r\n*", Session.DataSources.Keys.OrderBy(k => k))}" };

                target = deleteTarget.Target;
            }
            else
            {
                // DELETE target is a subquery or CTE - check that subquery follows the rules of updateable views
                var targetSubquery = (TSqlFragment)deleteTarget.TargetSubquery ?? deleteTarget.TargetCTE;
                var updateableViewValidator = new UpdateableViewValidatingVisitor(UpdateableViewModificationType.Delete);
                targetSubquery.Accept(updateableViewValidator);

                target = updateableViewValidator.Target;

                var dataSourceName = target.SchemaObject.DatabaseIdentifier?.Value ?? PrimaryDataSource.Name;
                if (!Session.DataSources.TryGetValue(dataSourceName, out dataSource))
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidObjectName(target.SchemaObject)) { Suggestion = $"Available database names:\r\n* {String.Join("\r\n*", Session.DataSources.Keys.OrderBy(k => k))}" };
            }

            ValidateDMLSchema(target, true);

            var targetLogicalName = target.SchemaObject.BaseIdentifier.Value;
            var targetSchema = target.SchemaObject.SchemaIdentifier?.Value;
            var targetDatabase = target.SchemaObject.DatabaseIdentifier?.Value;
            var targetAlias = deleteTarget.TargetAliasName ?? deleteTarget.TargetSubquery?.Alias.Value ?? deleteTarget.TargetCTE?.ExpressionName.Value ?? targetLogicalName;

            EntityMetadata targetMetadata = null;
            DataTable targetTable = null;

            if (targetDatabase == null && targetSchema == null && targetLogicalName.StartsWith("#"))
            {
                targetTable = Session.TempDb.Tables[targetLogicalName];

                if (targetTable == null)
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidObjectName(target.SchemaObject));

                targetLogicalName = targetTable.TableName;
            }
            else
            {
                try
                {
                    targetMetadata = dataSource.Metadata[targetLogicalName];
                    targetLogicalName = targetMetadata.LogicalName;
                }
                catch (FaultException ex)
                {
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidObjectName(target.SchemaObject), ex);
                }
            }

            var primaryKeyFields = EntityReader.GetPrimaryKeyFields(targetMetadata, targetTable, out _);
            var columnMappings = primaryKeyFields.ToDictionary(f => f);

            if (targetLogicalName == "principalobjectaccess")
            {
                // principalid and objectid are polymorphic lookups, for compatibility with TDS Endpoint
                // make sure we include the type values as well
                columnMappings["principaltypecode"] = "principaltypecode";
                columnMappings["objecttypecode"] = "objecttypecode";
            }

            if (deleteTarget.TargetSubquery != null)
            {
                // Modify the subquery to include the required key values. They might already exist, but to simplify the
                // query generation process add them again with a unique alias
                var sourceAlias = target.Alias?.Value ?? targetLogicalName;

                foreach (var col in columnMappings.Keys.ToArray())
                {
                    var colAlias = "PK_" + Guid.NewGuid().ToString("N");
                    columnMappings[col] = colAlias;

                    ((QuerySpecification)deleteTarget.TargetSubquery.QueryExpression).SelectElements.Add(new SelectScalarExpression
                    {
                        Expression = new ColumnReferenceExpression
                        {
                            MultiPartIdentifier = new MultiPartIdentifier
                            {
                                Identifiers =
                                {
                                    new Identifier { Value = sourceAlias },
                                    new Identifier { Value = col }
                                }
                            }
                        },
                        ColumnName = new IdentifierOrValueExpression { Identifier = new Identifier { Value = colAlias } }
                    });
                }
            }
            else if (deleteTarget.TargetCTE != null)
            {
                // Modify the CTE to include the required key values. They might already exist, but to simplify the
                // query generation process add them again with a unique alias
                var sourceAlias = target.Alias?.Value ?? targetLogicalName;

                foreach (var col in columnMappings.Keys.ToArray())
                {
                    var colAlias = "PK_" + Guid.NewGuid().ToString("N");
                    columnMappings[col] = colAlias;

                    ((QuerySpecification)deleteTarget.TargetCTE.QueryExpression).SelectElements.Add(new SelectScalarExpression
                    {
                        Expression = new ColumnReferenceExpression
                        {
                            MultiPartIdentifier = new MultiPartIdentifier
                            {
                                Identifiers =
                                {
                                    new Identifier { Value = sourceAlias },
                                    new Identifier { Value = col }
                                }
                            }
                        },
                        ColumnName = new IdentifierOrValueExpression { Identifier = new Identifier { Value = colAlias } }
                    });
                    deleteTarget.TargetCTE.Columns.Add(new Identifier { Value = colAlias });
                }
            }

            if (targetSchema?.Equals("bin", StringComparison.OrdinalIgnoreCase) == true)
            {
                // Deleting from the recycle bin needs to be translated to deleting the associated records from the deleteditemreference table
                if (dataSource.Metadata.RecycleBinEntities == null || !dataSource.Metadata.RecycleBinEntities.Contains(targetLogicalName))
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidObjectName(target.SchemaObject)) { Suggestion = "Ensure restoring of deleted records is enabled for this table - see https://learn.microsoft.com/en-us/power-platform/admin/restore-deleted-table-records?WT.mc_id=DX-MVP-5004203" };

                // We need to join the recycle bin entry to the deleteditemreference table to get the actual record to delete. We can only
                // do this on a single ID field, so reject any deletes that need composite keys
                if (columnMappings.Count > 1)
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.NotSupported(delete, "Recycle bin records using a composite key cannot be deleted directly. Delete the associated record from the dbo.deleteitemreference table instead"));

                var primaryKey = columnMappings.Single().Key;

                queryExpression.SelectElements.Add(new SelectScalarExpression
                {
                    Expression = new ColumnReferenceExpression
                    {
                        MultiPartIdentifier = new MultiPartIdentifier
                        {
                            Identifiers =
                            {
                                new Identifier { Value = targetAlias },
                                new Identifier { Value = primaryKey }
                            }
                        }
                    },
                    ColumnName = new IdentifierOrValueExpression
                    {
                        Identifier = new Identifier { Value = primaryKey }
                    }
                });

                queryExpression = new QuerySpecification
                {
                    FromClause = new FromClause
                    {
                        TableReferences =
                        {
                            new QualifiedJoin
                            {
                                FirstTableReference = new NamedTableReference
                                {
                                    SchemaObject = new SchemaObjectName
                                    {
                                        Identifiers =
                                        {
                                            new Identifier { Value = "deleteditemreference" }
                                        }
                                    }
                                },
                                SecondTableReference = new QueryDerivedTable
                                {
                                    QueryExpression = queryExpression,
                                    Alias = new Identifier { Value = targetLogicalName }
                                },
                                QualifiedJoinType = QualifiedJoinType.Inner,
                                SearchCondition = new BooleanComparisonExpression
                                {
                                    FirstExpression = new ColumnReferenceExpression
                                    {
                                        MultiPartIdentifier = new MultiPartIdentifier
                                        {
                                            Identifiers =
                                            {
                                                new Identifier { Value = targetLogicalName },
                                                new Identifier { Value = primaryKey }
                                            }
                                        }
                                    },
                                    ComparisonType = BooleanComparisonType.Equals,
                                    SecondExpression = new ColumnReferenceExpression
                                    {
                                        MultiPartIdentifier = new MultiPartIdentifier
                                        {
                                            Identifiers =
                                            {
                                                new Identifier { Value = "deleteditemreference" },
                                                new Identifier { Value = "deletedobject" }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    },
                    WhereClause = new WhereClause
                    {
                        SearchCondition = new BooleanComparisonExpression
                        {
                            FirstExpression = new ColumnReferenceExpression
                            {
                                MultiPartIdentifier = new MultiPartIdentifier
                                {
                                    Identifiers =
                                    {
                                        new Identifier { Value = "deleteditemreference" },
                                        new Identifier { Value = "deletedobjecttype" }
                                    }
                                }
                            },
                            ComparisonType = BooleanComparisonType.Equals,
                            SecondExpression = new IntegerLiteral { Value = targetMetadata.ObjectTypeCode.ToString() }
                        }
                    }
                };

                columnMappings.Clear();
                columnMappings["deleteditemreferenceid"] = "deleteditemreferenceid";
                targetMetadata = dataSource.Metadata["deleteditemreference"];
                targetAlias = "deleteditemreference";
            }

            foreach (var columnMapping in columnMappings)
            {
                ScalarExpression expression = new ColumnReferenceExpression
                {
                    MultiPartIdentifier = new MultiPartIdentifier
                    {
                        Identifiers =
                        {
                            new Identifier { Value = targetAlias },
                            new Identifier { Value = columnMapping.Value }
                        }
                    }
                };

                if (targetLogicalName == "principalobjectaccess" && columnMapping.Key == "objecttypecode")
                {
                    // In case any of the records are for an activity, include the activitytypecode by joining to the activitypointer table
                    expression = new CoalesceExpression
                    {
                        Expressions =
                        {
                            new ScalarSubquery
                            {
                                QueryExpression = new QuerySpecification
                                {
                                    SelectElements =
                                    {
                                        new SelectScalarExpression
                                        {
                                            Expression = new ColumnReferenceExpression
                                            {
                                                MultiPartIdentifier = new MultiPartIdentifier
                                                {
                                                    Identifiers =
                                                    {
                                                        new Identifier { Value = "activitypointer" },
                                                        new Identifier { Value = "activitytypecode" }
                                                    }
                                                }
                                            }
                                        }
                                    },
                                    FromClause = new FromClause
                                    {
                                        TableReferences =
                                        {
                                            new NamedTableReference
                                            {
                                                SchemaObject = new SchemaObjectName
                                                {
                                                    Identifiers =
                                                    {
                                                        new Identifier { Value = "activitypointer" }
                                                    }
                                                }
                                            }
                                        }
                                    },
                                    WhereClause = new WhereClause
                                    {
                                        SearchCondition = new BooleanComparisonExpression
                                        {
                                            FirstExpression = new ColumnReferenceExpression
                                            {
                                                MultiPartIdentifier = new MultiPartIdentifier
                                                {
                                                    Identifiers =
                                                    {
                                                        new Identifier { Value = targetAlias },
                                                        new Identifier { Value = "objectid" }
                                                    }
                                                }
                                            },
                                            ComparisonType = BooleanComparisonType.Equals,
                                            SecondExpression = new ColumnReferenceExpression
                                            {
                                                MultiPartIdentifier = new MultiPartIdentifier
                                                {
                                                    Identifiers =
                                                    {
                                                        new Identifier { Value = "activitypointer" },
                                                        new Identifier { Value = "activityid" }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            },
                            expression
                        }
                    };
                }

                queryExpression.SelectElements.Add(
                    new SelectScalarExpression
                    {
                        Expression = expression,
                        ColumnName = new IdentifierOrValueExpression { Identifier = new Identifier { Value = columnMapping.Key } }
                    }
                );
            }

            selectStatement.Accept(new ReplacePrimaryFunctionsVisitor());
            var source = ConvertSelectStatement(selectStatement);

            // Add DELETE
            var deleteNode = new DeleteNode
            {
                LogicalName = targetLogicalName,
                DataSource = dataSource.Name
            };

            var reader = targetMetadata != null
                ? new EntityReader(targetMetadata, _nodeContext, dataSource, delete, target, source)
                : new EntityReader(targetTable, _nodeContext, dataSource, delete, target, source);
            deleteNode.Source = reader.Source;
            deleteNode.PrimaryIdAccessors = reader.ValidateDeleteColumnMapping(columnMappings.ToDictionary(kvp => kvp.Key, kvp => kvp.Key));

            return deleteNode;
        }

        private UpdateNode ConvertUpdateStatement(UpdateStatement update)
        {
            if (update.UpdateSpecification.OutputClause != null)
                throw new NotSupportedQueryFragmentException(Sql4CdsError.NotSupported(update.UpdateSpecification.OutputClause, "OUTPUT"));

            if (update.UpdateSpecification.OutputIntoClause != null)
                throw new NotSupportedQueryFragmentException(Sql4CdsError.NotSupported(update.UpdateSpecification.OutputIntoClause, "OUTPUT INTO"));

            if (!(update.UpdateSpecification.Target is NamedTableReference target))
                throw new NotSupportedQueryFragmentException(Sql4CdsError.NotSupported(update.UpdateSpecification.Target, "non-table UPDATE target"));

            if (update.UpdateSpecification.WhereClause == null && Options.BlockUpdateWithoutWhere)
            {
                throw new NotSupportedQueryFragmentException("UPDATE without WHERE is blocked by your settings", update)
                {
                    Suggestion = "Add a WHERE clause to limit the records that will be affected by the update, or disable the \"Prevent UPDATE without WHERE\" option in the settings window"
                };
            }

            // Create the SELECT statement that generates the required information
            var queryExpression = new QuerySpecification
            {
                FromClause = update.UpdateSpecification.FromClause.Clone()  ?? new FromClause { TableReferences = { target.Clone() } },
                WhereClause = update.UpdateSpecification.WhereClause?.Clone(),
                UniqueRowFilter = UniqueRowFilter.Distinct,
                TopRowFilter = update.UpdateSpecification.TopRowFilter?.Clone()
            };

            var selectStatement = new SelectStatement
            {
                QueryExpression = queryExpression,
                WithCtesAndXmlNamespaces = update.WithCtesAndXmlNamespaces
            };
            CopyDmlHintsToSelectStatement(update.OptimizerHints, selectStatement);

            var updateTarget = new UpdateTargetVisitor(target.SchemaObject, Options.PrimaryDataSource);
            selectStatement.Accept(updateTarget);

            if (updateTarget.Ambiguous)
                throw new NotSupportedQueryFragmentException(Sql4CdsError.AmbiguousTable(target));

            DataSource dataSource;
            var columnRenamings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            if (updateTarget.TargetSubquery == null && updateTarget.TargetCTE == null)
            {
                if (String.IsNullOrEmpty(updateTarget.TargetEntityName))
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidObjectName(target.SchemaObject)) { Suggestion = $"Target table '{target.ToSql()}' not found in FROM clause" };

                if (!Session.DataSources.TryGetValue(updateTarget.TargetDataSource, out dataSource))
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidObjectName(target.SchemaObject)) { Suggestion = $"Available database names:\r\n* {String.Join("\r\n*", Session.DataSources.Keys.OrderBy(k => k))}" };

                target = updateTarget.Target;
            }
            else
            {
                // UPDATE target is a subquery or CTE - check it follows the rules of updateable views
                var targetSubquery = (TSqlFragment)updateTarget.TargetSubquery ?? updateTarget.TargetCTE;
                var updateableViewValidator = new UpdateableViewValidatingVisitor(UpdateableViewModificationType.Update);
                targetSubquery.Accept(updateableViewValidator);

                // Check that the columns being updated all come from the same table
                ConvertCTEs(selectStatement.Clone());
                var selectNode = ConvertSelectQuerySpec(queryExpression.Clone(), update.OptimizerHints, null, null, _nodeContext);
                _cteSubplans.Clear();
                var schema = selectNode.Source.GetSchema(_nodeContext);
                target = null;

                foreach (var set in update.UpdateSpecification.SetClauses)
                {
                    if (!(set is AssignmentSetClause assignment))
                        continue;

                    if (assignment.Column == null)
                        continue;

                    var targetCol = assignment.Column.GetColumnName();

                    if (!schema.ContainsColumn(targetCol, out targetCol))
                        continue;

                    var colDetails = schema.Schema[targetCol];

                    var targetTable = new NamedTableReference
                    {
                        SchemaObject = new SchemaObjectName
                        {
                            Identifiers =
                            {
                                new Identifier { Value = colDetails.SourceServer },
                                new Identifier { Value = colDetails.SourceSchema },
                                new Identifier { Value = colDetails.SourceTable },
                            }
                        },
                        Alias = new Identifier { Value = colDetails.SourceAlias ?? colDetails.SourceTable }
                    };

                    if (target == null)
                    {
                        target = targetTable;
                    }
                    else
                    {
                        if (!target.Alias.Value.Equals(targetTable.Alias.Value, StringComparison.OrdinalIgnoreCase))
                            throw new NotSupportedQueryFragmentException(Sql4CdsError.DerivedTableAffectsMultipleTables(targetSubquery, updateTarget.TargetAliasName));

                        for (var i = 0; i < targetTable.SchemaObject.Identifiers.Count; i++)
                        {
                            if (!target.SchemaObject.Identifiers[i].Value.Equals(targetTable.SchemaObject.Identifiers[i].Value, StringComparison.OrdinalIgnoreCase))
                                throw new NotSupportedQueryFragmentException(Sql4CdsError.DerivedTableAffectsMultipleTables(targetSubquery, updateTarget.TargetAliasName));
                        }
                    }

                    // The subquery might expose the column as a different name - track the name mappings we're interested in
                    columnRenamings[colDetails.SourceColumn] = targetCol.SplitMultiPartIdentifier().Last();
                }

                var dataSourceName = target.SchemaObject.DatabaseIdentifier?.Value ?? PrimaryDataSource.Name;
                if (!Session.DataSources.TryGetValue(dataSourceName, out dataSource))
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidObjectName(target.SchemaObject)) { Suggestion = $"Available database names:\r\n* {String.Join("\r\n*", Session.DataSources.Keys.OrderBy(k => k))}" };
            }

            ValidateDMLSchema(target, false);

            var targetLogicalName = target.SchemaObject.BaseIdentifier.Value;
            var targetSchema = target.SchemaObject.SchemaIdentifier?.Value;
            var targetDatabase = target.SchemaObject.DatabaseIdentifier?.Value;
            var targetAlias = updateTarget.TargetAliasName ?? updateTarget.TargetSubquery?.Alias.Value ?? updateTarget.TargetCTE?.ExpressionName.Value ?? targetLogicalName;

            DataTable dataTable = null;
            EntityMetadata targetMetadata = null;

            if (targetDatabase == null && targetSchema == null && targetLogicalName.StartsWith("#"))
            {
                dataTable = Session.TempDb.Tables[targetLogicalName];

                if (dataTable == null)
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidObjectName(target.SchemaObject));

                targetLogicalName = dataTable.TableName;
            }
            else
            {
                try
                {
                    targetMetadata = dataSource.Metadata[targetLogicalName];
                    targetLogicalName = targetMetadata.LogicalName;
                }
                catch (FaultException ex)
                {
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidObjectName(target.SchemaObject), ex);
                }
            }

            var primaryKeyMappings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            var newValueMappings = new Dictionary<ColumnReferenceExpression, string>();
            var existingValueMappings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            var existingAttributes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            var primaryKeyFields = EntityReader.GetPrimaryKeyFields(targetMetadata, dataTable, out var isIntersect);

            if (updateTarget.TargetSubquery != null)
            {
                // Modify the subquery to include the required key values. They might already exist, but to simplify the
                // query generation process add them again with a unique alias
                var sourceAlias = target.Alias.Value;

                foreach (var col in primaryKeyFields)
                {
                    var colAlias = "PK_" + Guid.NewGuid().ToString("N");
                    columnRenamings[col] = colAlias;

                    ((QuerySpecification)updateTarget.TargetSubquery.QueryExpression).SelectElements.Add(new SelectScalarExpression
                    {
                        Expression = new ColumnReferenceExpression
                        {
                            MultiPartIdentifier = new MultiPartIdentifier
                            {
                                Identifiers =
                                {
                                    new Identifier { Value = sourceAlias },
                                    new Identifier { Value = col }
                                }
                            }
                        },
                        ColumnName = new IdentifierOrValueExpression { Identifier = new Identifier { Value = colAlias } }
                    });
                }
            }
            else if (updateTarget.TargetCTE != null)
            {
                // Modify the CTE to include the required key values. They might already exist, but to simplify the
                // query generation process add them again with a unique alias
                var sourceAlias = target.Alias.Value;

                foreach (var col in primaryKeyFields)
                {
                    var colAlias = "PK_" + Guid.NewGuid().ToString("N");
                    columnRenamings[col] = colAlias;

                    ((QuerySpecification)updateTarget.TargetCTE.QueryExpression).SelectElements.Add(new SelectScalarExpression
                    {
                        Expression = new ColumnReferenceExpression
                        {
                            MultiPartIdentifier = new MultiPartIdentifier
                            {
                                Identifiers =
                                {
                                    new Identifier { Value = sourceAlias },
                                    new Identifier { Value = col }
                                }
                            }
                        },
                        ColumnName = new IdentifierOrValueExpression { Identifier = new Identifier { Value = colAlias } }
                    });
                    updateTarget.TargetCTE.Columns.Add(new Identifier { Value = colAlias });
                }
            }

            if (!isIntersect)
            {
                foreach (var primaryKey in primaryKeyFields)
                {
                    if (!columnRenamings.TryGetValue(primaryKey, out var sourceField))
                        sourceField = primaryKey;

                    queryExpression.SelectElements.Add(new SelectScalarExpression
                    {
                        Expression = new ColumnReferenceExpression
                        {
                            MultiPartIdentifier = new MultiPartIdentifier
                            {
                                Identifiers =
                                {
                                    new Identifier { Value = targetAlias },
                                    new Identifier { Value = sourceField }
                                }
                            }
                        },
                        ColumnName = new IdentifierOrValueExpression
                        {
                            Identifier = new Identifier { Value = primaryKey }
                        }
                    });

                    primaryKeyMappings[primaryKey] = primaryKey;
                }
            }
            else
            {
                foreach (var primaryKey in primaryKeyFields)
                    existingAttributes.Add(primaryKey);
            }

            var useStateTransitions = targetMetadata != null && !update.OptimizerHints.OfType<UseHintList>().Any(h => h.Hints.Any(s => s.Value.Equals("DISABLE_STATE_TRANSITIONS", StringComparison.OrdinalIgnoreCase)));
            var stateTransitions = useStateTransitions ? StateTransitionLoader.LoadStateTransitions(targetMetadata) : null;
            var minimalUpdates = update.OptimizerHints != null && update.OptimizerHints.OfType<UseHintList>().Any(h => h.Hints.Any(s => s.Value.Equals("MINIMAL_UPDATES", StringComparison.OrdinalIgnoreCase)));

            foreach (var set in update.UpdateSpecification.SetClauses)
            {
                if (!(set is AssignmentSetClause assignment))
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.NotSupported(set, "SET"));

                if (assignment.Variable != null)
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.NotSupported(set, "variable reference in SET clause"));

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
                var targetAttrName = assignment.Column.MultiPartIdentifier.Identifiers.Last().Value.ToLower();

                queryExpression.SelectElements.Add(new SelectScalarExpression { Expression = assignment.NewValue, ColumnName = new IdentifierOrValueExpression { Identifier = new Identifier { Value = "new_" + targetAttrName } } });
                newValueMappings[assignment.Column] = "new_" + targetAttrName;

                // If we're changing the status of a record where only specific state transitions are allowed, we need to include the
                // current statecode and statuscode values
                if ((targetAttrName == "statecode" || targetAttrName == "statuscode") && stateTransitions != null)
                {
                    existingAttributes.Add("statecode");
                    existingAttributes.Add("statuscode");
                }

                // Changing principalobjectaccess.accesstypemask requires the existing value to determine which rights to remove
                if (targetLogicalName == "principalobjectaccess" && targetAttrName == "accessrightsmask")
                    existingAttributes.Add("accessrightsmask");

                // Changing the solutioncomponent.rootcomponentbehavior can be done efficiently for some combinations of new and old values
                if (targetLogicalName == "solutioncomponent" && targetAttrName == "rootcomponentbehavior")
                    existingAttributes.Add("rootcomponentbehavior");
    
                // If selected, include the existing value of all attributes to avoid excessive updates
                if (minimalUpdates)
                    existingAttributes.Add(targetAttrName);
            }

            // quote/salesorder/invoice have custom logic for updating closed records, so load in the existing statecode & statuscode fields too
            if (useStateTransitions && (targetLogicalName == "quote" || targetLogicalName == "salesorder" || targetLogicalName == "invoice"))
            {
                existingAttributes.Add("statecode");
                existingAttributes.Add("statuscode");
            }

            foreach (var existingAttribute in existingAttributes)
            {
                var expression = (ScalarExpression)new ColumnReferenceExpression
                {
                    MultiPartIdentifier = new MultiPartIdentifier
                    {
                        Identifiers =
                        {
                            new Identifier { Value = targetAlias },
                            new Identifier { Value = existingAttribute }
                        }
                    }
                };

                if (targetLogicalName == "principalobjectaccess" && existingAttribute == "objecttypecode")
                {
                    // In case any of the records are for an activity, include the activitytypecode by joining to the activitypointer table
                    expression = new CoalesceExpression
                    {
                        Expressions =
                        {
                            new ScalarSubquery
                            {
                                QueryExpression = new QuerySpecification
                                {
                                    SelectElements =
                                    {
                                        new SelectScalarExpression
                                        {
                                            Expression = new ColumnReferenceExpression
                                            {
                                                MultiPartIdentifier = new MultiPartIdentifier
                                                {
                                                    Identifiers =
                                                    {
                                                        new Identifier { Value = "activitypointer" },
                                                        new Identifier { Value = "activitytypecode" }
                                                    }
                                                }
                                            }
                                        }
                                    },
                                    FromClause = new FromClause
                                    {
                                        TableReferences =
                                        {
                                            new NamedTableReference
                                            {
                                                SchemaObject = new SchemaObjectName
                                                {
                                                    Identifiers =
                                                    {
                                                        new Identifier { Value = "activitypointer" }
                                                    }
                                                }
                                            }
                                        }
                                    },
                                    WhereClause = new WhereClause
                                    {
                                        SearchCondition = new BooleanComparisonExpression
                                        {
                                            FirstExpression = new ColumnReferenceExpression
                                            {
                                                MultiPartIdentifier = new MultiPartIdentifier
                                                {
                                                    Identifiers =
                                                    {
                                                        new Identifier { Value = targetAlias },
                                                        new Identifier { Value = "objectid" }
                                                    }
                                                }
                                            },
                                            ComparisonType = BooleanComparisonType.Equals,
                                            SecondExpression = new ColumnReferenceExpression
                                            {
                                                MultiPartIdentifier = new MultiPartIdentifier
                                                {
                                                    Identifiers =
                                                    {
                                                        new Identifier { Value = "activitypointer" },
                                                        new Identifier { Value = "activityid" }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            },
                            expression
                        }
                    };
                }

                queryExpression.SelectElements.Add(new SelectScalarExpression
                {
                    Expression = expression,
                    ColumnName = new IdentifierOrValueExpression
                    {
                        Identifier = new Identifier { Value = "existing_" + existingAttribute }
                    }
                });

                existingValueMappings[existingAttribute] = "existing_" + existingAttribute;
            }

            selectStatement.Accept(new ReplacePrimaryFunctionsVisitor());
            var source = ConvertSelectStatement(selectStatement);

            // Add UPDATE
            var reader = targetMetadata != null
                ? new EntityReader(targetMetadata, _nodeContext, dataSource, update, target, source)
                : new EntityReader(dataTable, _nodeContext, dataSource, update, target, source);

            var updateNode = new UpdateNode
            {
                LogicalName = targetLogicalName,
                DataSource = dataSource.Name,
                PrimaryIdAccessors = reader.ValidateUpdatePrimaryKeyColumnMapping(primaryKeyMappings),
                NewValueAccessors = reader.ValidateUpdateNewValueColumnMapping(newValueMappings),
                ExistingValueAccessors = reader.ValidateUpdateExistingValueColumnMapping(existingValueMappings),
                StateTransitions = stateTransitions,
                MinimalUpdates = minimalUpdates,
                Source = reader.Source
            };

            return updateNode;
        }

        private void CopyDmlHintsToSelectStatement(IList<OptimizerHint> hints, SelectStatement selectStatement)
        {
            foreach (var hint in hints)
            {
                if (hint is UseHintList list)
                {
                    // Clone the list of hints so it can be modified later without affecting the original - we may need to
                    // adjust the hints to make them compatible with TDS Endpoint
                    var clone = new UseHintList();

                    foreach (var name in list.Hints)
                        clone.Hints.Add(name);

                    selectStatement.OptimizerHints.Add(clone);
                }
                else
                {
                    selectStatement.OptimizerHints.Add(hint);
                }
            }
        }

        private IRootExecutionPlanNodeInternal ConvertSelectStatement(SelectStatement select)
        {
            if (TDSEndpoint.CanUseTDSEndpoint(Options, PrimaryDataSource.Connection))
            {
                using (var con = PrimaryDataSource.Connection == null ? null : TDSEndpoint.Connect(PrimaryDataSource.Connection))
                {
                    var tdsEndpointCompatibilityVisitor = new TDSEndpointCompatibilityVisitor(con, PrimaryDataSource.Metadata, false, parameterTypes: _nodeContext.ParameterTypes);
                    select.Accept(tdsEndpointCompatibilityVisitor);

                    // Remove any custom optimizer hints
                    var hintCompatibilityVisitor = new OptimizerHintValidatingVisitor(true);
                    select.Accept(hintCompatibilityVisitor);

                    if (tdsEndpointCompatibilityVisitor.IsCompatible && hintCompatibilityVisitor.TdsCompatible)
                    {
                        if (tdsEndpointCompatibilityVisitor.RequiresCteRewrite)
                            select.Accept(new ReplaceCtesWithSubqueriesVisitor());

                        select.ScriptTokenStream = null;
                        var sql = new SqlNode
                        {
                            DataSource = Options.PrimaryDataSource,
                            Sql = select.ToSql(),
                            SelectStatement = select
                        };

                        var variables = new VariableCollectingVisitor();
                        select.Accept(variables);

                        foreach (var variable in variables.Variables)
                            sql.Parameters.Add(variable.Name);

                        return sql;
                    }
                }
            }

            if (select.ComputeClauses != null && select.ComputeClauses.Count > 0)
                throw new NotSupportedQueryFragmentException(Sql4CdsError.NotSupported(select.ComputeClauses[0], "COMPUTE"));

            if (select.Into != null)
                throw new NotSupportedQueryFragmentException(Sql4CdsError.NotSupported(select.Into, "INTO"));

            if (select.On != null)
                throw new NotSupportedQueryFragmentException(Sql4CdsError.NotSupported(select.On, "ON"));

            ConvertCTEs(select);

            var variableAssignments = new List<string>();
            SelectElement firstNonSetSelectElement = null;

            if (select.QueryExpression is QuerySpecification querySpec)
            {
                for (var i = 0; i < querySpec.SelectElements.Count; i++)
                {
                    var selectElement = querySpec.SelectElements[i];

                    if (selectElement is SelectSetVariable set)
                    {
                        if (firstNonSetSelectElement != null)
                            throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidSelectVariableAssignment(selectElement));

                        variableAssignments.Add(set.Variable.Name);

                        if (!_nodeContext.ParameterTypes.TryGetValue(set.Variable.Name, out var paramType))
                            throw new NotSupportedQueryFragmentException(Sql4CdsError.UndeclaredVariable(set.Variable));

                        // Create the SELECT statement that generates the required information
                        var expr = set.Expression;

                        switch (set.AssignmentKind)
                        {
                            case AssignmentKind.AddEquals:
                                expr = new BinaryExpression { FirstExpression = set.Variable, BinaryExpressionType = BinaryExpressionType.Add, SecondExpression = expr };
                                break;

                            case AssignmentKind.BitwiseAndEquals:
                                expr = new BinaryExpression { FirstExpression = set.Variable, BinaryExpressionType = BinaryExpressionType.BitwiseAnd, SecondExpression = expr };
                                break;

                            case AssignmentKind.BitwiseOrEquals:
                                expr = new BinaryExpression { FirstExpression = set.Variable, BinaryExpressionType = BinaryExpressionType.BitwiseOr, SecondExpression = expr };
                                break;

                            case AssignmentKind.BitwiseXorEquals:
                                expr = new BinaryExpression { FirstExpression = set.Variable, BinaryExpressionType = BinaryExpressionType.BitwiseXor, SecondExpression = expr };
                                break;

                            case AssignmentKind.DivideEquals:
                                expr = new BinaryExpression { FirstExpression = set.Variable, BinaryExpressionType = BinaryExpressionType.Divide, SecondExpression = expr };
                                break;

                            case AssignmentKind.ModEquals:
                                expr = new BinaryExpression { FirstExpression = set.Variable, BinaryExpressionType = BinaryExpressionType.Modulo, SecondExpression = expr };
                                break;

                            case AssignmentKind.MultiplyEquals:
                                expr = new BinaryExpression { FirstExpression = set.Variable, BinaryExpressionType = BinaryExpressionType.Multiply, SecondExpression = expr };
                                break;

                            case AssignmentKind.SubtractEquals:
                                expr = new BinaryExpression { FirstExpression = set.Variable, BinaryExpressionType = BinaryExpressionType.Subtract, SecondExpression = expr };
                                break;
                        }

                        expr = new ConvertCall { DataType = paramType, Parameter = expr };
                        expr.ScriptTokenStream = null;

                        querySpec.SelectElements[i] = new SelectScalarExpression { Expression = expr };
                    }
                    else if (firstNonSetSelectElement == null)
                    {
                        if (variableAssignments.Count > 0)
                            throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidSelectVariableAssignment(selectElement));

                        firstNonSetSelectElement = selectElement;
                    }
                }
            }

            var converted = ConvertSelectStatement(select.QueryExpression, select.OptimizerHints, null, null, _nodeContext);

            if (variableAssignments.Count > 0)
            {
                var assign = new AssignVariablesNode
                {
                    Source = converted.Source
                };

                for (var i = 0; i < variableAssignments.Count; i++)
                    assign.Variables.Add(new VariableAssignment { SourceColumn = converted.ColumnSet[i].SourceColumn, VariableName = variableAssignments[i] });

                return assign;
            }

            return converted;
        }

        private SelectNode ConvertSelectStatement(QueryExpression query, IList<OptimizerHint> hints, INodeSchema outerSchema, Dictionary<string,string> outerReferences, NodeCompilationContext context)
        {
            if (query is QuerySpecification querySpec)
                return ConvertSelectQuerySpec(querySpec, hints, outerSchema, outerReferences, context);

            if (query is BinaryQueryExpression binary)
                return ConvertBinaryQuery(binary, hints, outerSchema, outerReferences, context);

            if (query is QueryParenthesisExpression paren)
            {
                paren.QueryExpression.ForClause = paren.ForClause;
                paren.QueryExpression.OffsetClause = paren.OffsetClause;
                paren.QueryExpression.OrderByClause = paren.OrderByClause;
                return ConvertSelectStatement(paren.QueryExpression, hints, outerSchema, outerReferences, context);
            }

            throw new NotSupportedQueryFragmentException(Sql4CdsError.NotSupported(query, "query expression"));
        }

        private SelectNode ConvertBinaryQuery(BinaryQueryExpression binary, IList<OptimizerHint> hints, INodeSchema outerSchema, Dictionary<string, string> outerReferences, NodeCompilationContext context)
        {
            var left = ConvertSelectStatement(binary.FirstQueryExpression, hints, outerSchema, outerReferences, context);
            var right = ConvertSelectStatement(binary.SecondQueryExpression, hints, outerSchema, outerReferences, context);

            left.ExpandWildcardColumns(context);
            right.ExpandWildcardColumns(context);

            if (left.ColumnSet.Count != right.ColumnSet.Count)
                throw new NotSupportedQueryFragmentException(Sql4CdsError.SetOperationWithDifferentColumnCounts(binary));

            IDataExecutionPlanNodeInternal node;
            List<SelectColumn> columns;

            if (binary.BinaryQueryExpressionType == BinaryQueryExpressionType.Union)
            {
                var concat = left.Source as ConcatenateNode;

                if (concat == null)
                {
                    columns = new List<SelectColumn>();
                    concat = new ConcatenateNode();

                    concat.Sources.Add(left.Source);

                    foreach (var col in left.ColumnSet)
                    {
                        var colName = context.GetExpressionName();

                        concat.ColumnSet.Add(new ConcatenateColumn
                        {
                            OutputColumn = colName,
                            SourceColumns = { col.SourceColumn },
                            SourceExpressions = { col.SourceExpression }
                        });

                        columns.Add(new SelectColumn { SourceColumn = colName, OutputColumn = col.OutputColumn });
                    }
                }
                else
                {
                    columns = left.ColumnSet;
                }

                concat.Sources.Add(right.Source);

                for (var i = 0; i < concat.ColumnSet.Count; i++)
                {
                    concat.ColumnSet[i].SourceColumns.Add(right.ColumnSet[i].SourceColumn);
                    concat.ColumnSet[i].SourceExpressions.Add(right.ColumnSet[i].SourceExpression);
                }

                node = concat;

                if (!binary.All)
                {
                    var distinct = new DistinctNode { Source = node };
                    distinct.Columns.AddRange(concat.ColumnSet.Select(col => col.OutputColumn));
                    node = distinct;
                }
            }
            else
            {
                // EXCEPT & INTERSECT require distinct inputs
                var leftSource = left.Source;
                var rightSource = right.Source;

                leftSource = new DistinctNode { Source = leftSource };
                ((DistinctNode)leftSource).Columns.AddRange(left.ColumnSet.Select(col => col.SourceColumn));

                rightSource = new DistinctNode { Source = rightSource };
                ((DistinctNode)rightSource).Columns.AddRange(right.ColumnSet.Select(col => col.SourceColumn));

                // Create the join node
                node = new HashJoinNode
                {
                    LeftSource = leftSource,
                    RightSource = rightSource,
                    JoinType = QualifiedJoinType.LeftOuter,
                    SemiJoin = true,
                    AntiJoin = binary.BinaryQueryExpressionType == BinaryQueryExpressionType.Except,
                    OutputRightSchema = false,
                    ComparisonType = BooleanComparisonType.IsNotDistinctFrom
                };

                // Join on all the columns
                for (var i = 0; i < left.ColumnSet.Count; i++)
                {
                    var leftCol = left.ColumnSet[i];
                    var rightCol = right.ColumnSet[i];

                    ((HashJoinNode)node).LeftAttributes.Add(leftCol.SourceColumn.ToColumnReference());
                    ((HashJoinNode)node).RightAttributes.Add(rightCol.SourceColumn.ToColumnReference());
                }

                columns = left.ColumnSet;
            }

            // Aliases to be used for sorting are:
            // * Column aliases defined in the first query
            // * Source column names from the first query
            // So the query
            //
            // SELECT col1 AS x, col2 + '' AS y FROM table1
            // UNION
            // SELECT col3, col4 FROM table2
            //
            // can sort by col1, x and y
            var leftSchema = left.Source.GetSchema(context);
            var combinedSchema = node.GetSchema(context);
            var aliases = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);

            for (var i = 0; i < left.ColumnSet.Count; i++)
            {
                if (!String.IsNullOrEmpty(left.ColumnSet[i].OutputColumn))
                {
                    if (!aliases.TryGetValue(left.ColumnSet[i].OutputColumn, out var aliasedCols))
                    {
                        aliasedCols = new List<string>();
                        aliases.Add(left.ColumnSet[i].OutputColumn, aliasedCols);
                    }

                    aliasedCols.Add(combinedSchema.Schema.Skip(i).First().Key);
                }

                var leftSourceCol = leftSchema.Schema[left.ColumnSet[i].SourceColumn];

                if (leftSourceCol.IsVisible)
                {
                    var sourceColParts = left.ColumnSet[i].SourceColumn.SplitMultiPartIdentifier();
                    var sourceColName = sourceColParts.Last();

                    if (!sourceColName.Equals(left.ColumnSet[i].OutputColumn, StringComparison.OrdinalIgnoreCase))
                    {
                        if (!aliases.TryGetValue(sourceColName, out var aliasedCols))
                        {
                            aliasedCols = new List<string>();
                            aliases.Add(sourceColName, aliasedCols);
                        }

                        aliasedCols.Add(combinedSchema.Schema.Skip(i).First().Key);
                    }
                }
            }

            node = ConvertOrderByClause(node, hints, binary.OrderByClause, combinedSchema.Schema.Select(col => col.Key.ToColumnReference()).ToArray(), aliases, binary, context, outerSchema, outerReferences, null);
            node = ConvertOffsetClause(node, binary.OffsetClause, context);

            var select = new SelectNode { Source = node, LogicalSourceSchema = combinedSchema };
            select.ColumnSet.AddRange(columns);

            if (binary.ForClause is XmlForClause forXml)
                ConvertForXmlClause(select, forXml, context);
            else if (binary.ForClause != null)
                throw new NotSupportedQueryFragmentException(Sql4CdsError.NotSupported(binary.ForClause, "FOR"));

            return select;
        }

        private SelectNode ConvertSelectQuerySpec(QuerySpecification querySpec, IList<OptimizerHint> hints, INodeSchema outerSchema, Dictionary<string,string> outerReferences, NodeCompilationContext context)
        {
            // Check for any aggregates in the FROM or WHERE clauses
            var aggregateCollector = new AggregateCollectingVisitor();
            if (querySpec.FromClause != null)
            {
                querySpec.FromClause.Accept(aggregateCollector);

                if (aggregateCollector.Aggregates.Any())
                    throw new NotSupportedQueryFragmentException("An aggregate may not appear in the FROM clause", aggregateCollector.Aggregates[0]);
            }
            if (querySpec.WhereClause != null)
            {
                querySpec.WhereClause.Accept(aggregateCollector);

                if (aggregateCollector.Aggregates.Any())
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidAggregateInWhereClause(aggregateCollector.Aggregates[0]));
            }

            // Find any window functions we need to apply
            var windowCollector = new WindowFunctionVisitor();
            querySpec.Accept(windowCollector);

            if (windowCollector.OutOfPlaceWindowFunctions.Count > 0)
            {
                throw new NotSupportedQueryFragmentException(windowCollector.OutOfPlaceWindowFunctions.Select(f => Sql4CdsError.WindowFunctionNotInSelectOrOrderBy(f)).ToArray(), null);
            }

            // Each table in the FROM clause starts as a separate FetchXmlScan node. Add appropriate join nodes
            var node = querySpec.FromClause == null || querySpec.FromClause.TableReferences.Count == 0 ? new ConstantScanNode { Values = { new Dictionary<string, ScalarExpression>() } } : ConvertFromClause(querySpec.FromClause, hints, querySpec, outerSchema, outerReferences, context);
            var logicalSchema = node.GetSchema(context);

            // Rewrite ColumnReferenceExpressions to use the fully qualified column name. This simplifies rewriting the
            // query later on due to aggregates etc. We need to process the SELECT clause first to capture aliases which might
            // be used in the ORDER BY clause
            var normalizeColNamesVisitor = new NormalizeColNamesVisitor(logicalSchema);
            querySpec.Accept(normalizeColNamesVisitor);

            // Now we've got the initial schema we can try processing each remaining part of the query.
            // We can continue processing subsequent parts even if we hit an error as each one doesn't change
            // the schema for later ones, so we can report multiple errors together
            NotSupportedQueryFragmentException exception = null;

            // Generate the joins necesary for any IN or EXISTS subqueries so we can handle them later as simple
            // scalar expressions
            try
            {
                node = ConvertInSubqueries(node, hints, querySpec, context, outerSchema, outerReferences);
            }
            catch (NotSupportedQueryFragmentException ex)
            {
                exception = NotSupportedQueryFragmentException.Combine(exception, ex);
            }

            try
            {
                node = ConvertExistsSubqueries(node, hints, querySpec, context, outerSchema, outerReferences);
            }
            catch (NotSupportedQueryFragmentException ex)
            {
                exception = NotSupportedQueryFragmentException.Combine(exception, ex);
            }

            // Add filters from WHERE
            try
            {
                node = ConvertWhereClause(node, hints, querySpec.WhereClause, outerSchema, outerReferences, context, querySpec);
            }
            catch (NotSupportedQueryFragmentException ex)
            {
                exception = NotSupportedQueryFragmentException.Combine(exception, ex);
            }

            // Add aggregates from GROUP BY/SELECT/HAVING/ORDER BY
            INodeSchema nonAggregateSchema;
            var preGroupByNode = node;

            try
            {
                node = ConvertGroupByAggregates(node, querySpec, context, outerSchema, outerReferences);
                nonAggregateSchema = preGroupByNode == node ? null : preGroupByNode.GetSchema(context);
            }
            catch (NotSupportedQueryFragmentException ex)
            {
                exception = NotSupportedQueryFragmentException.Combine(exception, ex);
                nonAggregateSchema = null;
            }

            // Add filters from HAVING
            try
            {
                node = ConvertHavingClause(node, hints, querySpec.HavingClause, context, outerSchema, outerReferences, querySpec, nonAggregateSchema);
            }
            catch (NotSupportedQueryFragmentException ex)
            {
                exception = NotSupportedQueryFragmentException.Combine(exception, ex);
            }

            // Add window functions
            try
            {
                node = ConvertWindowFunctions(node, hints, querySpec, context, outerSchema, outerReferences, nonAggregateSchema, windowCollector.WindowFunctions);
            }
            catch (NotSupportedQueryFragmentException ex)
            {
                exception = NotSupportedQueryFragmentException.Combine(exception, ex);
            }

            // Add DISTINCT
            var distinct = querySpec.UniqueRowFilter == UniqueRowFilter.Distinct ? new DistinctNode { Source = node } : null;
            node = distinct ?? node;

            // Add SELECT
            SelectNode selectNode = null;

            try
            {
                selectNode = ConvertSelectClause(querySpec.SelectElements, hints, node, distinct, querySpec, context, outerSchema, outerReferences, nonAggregateSchema, logicalSchema);
                node = selectNode.Source;
            }
            catch (NotSupportedQueryFragmentException ex)
            {
                exception = NotSupportedQueryFragmentException.Combine(exception, ex);
            }

            // Add sorts from ORDER BY
            var selectFields = new List<ScalarExpression>();
            var preOrderSchema = node.GetSchema(context);
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
                            selectFields.Add(field.ToColumnReference());
                    }
                }
            }

            var aliases = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);

            if (selectNode != null)
            {
                foreach (var col in selectNode.ColumnSet)
                {
                    if (String.IsNullOrEmpty(col.OutputColumn))
                        continue;

                    if (!aliases.TryGetValue(col.OutputColumn, out var aliasedCols))
                    {
                        aliasedCols = new List<string>();
                        aliases.Add(col.OutputColumn, aliasedCols);
                    }

                    aliasedCols.Add(col.SourceColumn);
                }
            }

            try
            {
                node = ConvertOrderByClause(node, hints, querySpec.OrderByClause, selectFields.ToArray(), aliases, querySpec, context, outerSchema, outerReferences, nonAggregateSchema);
            }
            catch (NotSupportedQueryFragmentException ex)
            {
                exception = NotSupportedQueryFragmentException.Combine(exception, ex);
            }

            // Add TOP/OFFSET
            if (querySpec.TopRowFilter != null && querySpec.OffsetClause != null)
                exception = NotSupportedQueryFragmentException.Combine(exception, Sql4CdsError.InvalidTopWithOffset(querySpec.TopRowFilter));

            try
            {
                node = ConvertTopClause(node, querySpec.TopRowFilter, querySpec.OrderByClause, context);
            }
            catch (NotSupportedQueryFragmentException ex)
            {
                exception = NotSupportedQueryFragmentException.Combine(exception, ex);
            }

            try
            {
                node = ConvertOffsetClause(node, querySpec.OffsetClause, context);
            }
            catch (NotSupportedQueryFragmentException ex)
            {
                exception = NotSupportedQueryFragmentException.Combine(exception, ex);
            }

            if (selectNode != null)
                selectNode.Source = node;

            // Convert to XML
            try
            {
                if (querySpec.ForClause is XmlForClause forXml)
                    ConvertForXmlClause(selectNode, forXml, context);
                else if (querySpec.ForClause != null)
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.NotSupported(querySpec.ForClause, "FOR"));
            }
            catch (NotSupportedQueryFragmentException ex)
            {
                exception = NotSupportedQueryFragmentException.Combine(exception, ex);
            }

            if (exception != null)
                throw exception;

            return selectNode;
        }

        private IDataExecutionPlanNodeInternal ConvertWindowFunctions(IDataExecutionPlanNodeInternal source, IList<OptimizerHint> hints, QuerySpecification querySpec, NodeCompilationContext context, INodeSchema outerSchema, Dictionary<string, string> outerReferences, INodeSchema nonAggregateSchema, List<FunctionCall> windowFunctions)
        {
            if (windowFunctions.Count == 0)
                return source;

            var calculationRewrites = new Dictionary<ScalarExpression, ScalarExpression>();
            NotSupportedQueryFragmentException exception = null;
            var computeScalar = new ComputeScalarNode
            {
                Source = source
            };
            var result = (IDataExecutionPlanNodeInternal)computeScalar;

            foreach (var windowFunction in windowFunctions)
            {
                // Sort the data by the partition and ordering columns
                var sort = new SortNode
                {
                    Source = result
                };

                var partitionCols = new List<string>();
                var orderCols = windowFunction.FunctionName.Value.Equals("RANK", StringComparison.OrdinalIgnoreCase) || windowFunction.FunctionName.Value.Equals("DENSE_RANK", StringComparison.OrdinalIgnoreCase) ? new List<string>() : null;

                foreach (var partition in windowFunction.OverClause.Partitions)
                {
                    var partitionExpr = partition;

                    try
                    {
                        var sortSource = sort.Source;
                        var calculated = ComputeScalarExpression(partitionExpr, hints, querySpec, computeScalar, nonAggregateSchema, context, ref sortSource);
                        sort.Source = sortSource;
                        calculationRewrites[partitionExpr] = calculated.ToColumnReference();
                        partitionExpr = calculated.ToColumnReference();

                        sort.Sorts.Add(new ExpressionWithSortOrder
                        {
                            Expression = partitionExpr,
                            SortOrder = SortOrder.Ascending
                        });

                        partitionCols.Add(calculated);
                    }
                    catch (NotSupportedQueryFragmentException ex)
                    {
                        exception = NotSupportedQueryFragmentException.Combine(exception, ex);
                    }
                }

                foreach (var order in windowFunction.OverClause.OrderByClause?.OrderByElements ?? Array.Empty<ExpressionWithSortOrder>())
                {
                    var sortExpr = order.Expression;

                    try
                    {
                        if (sortExpr is IntegerLiteral)
                            throw new NotSupportedQueryFragmentException(Sql4CdsError.WindowFunctionCannotUseOrderByIndex(sortExpr));

                        if (orderCols != null)
                        {
                            // If we need to segment by the order columns as well, make sure it's a column reference
                            var sortSource = sort.Source;
                            var calculated = ComputeScalarExpression(sortExpr, hints, querySpec, computeScalar, nonAggregateSchema, context, ref sortSource);
                            sort.Source = sortSource;
                            calculationRewrites[sortExpr] = calculated.ToColumnReference();

                            sort.Sorts.Add(new ExpressionWithSortOrder
                            {
                                Expression = calculated.ToColumnReference(),
                                SortOrder = order.SortOrder
                            });

                            orderCols.Add(calculated);
                        }
                        else
                        {
                            sort.Sorts.Add(order);
                        }
                    }
                    catch (NotSupportedQueryFragmentException ex)
                    {
                        exception = NotSupportedQueryFragmentException.Combine(exception, ex);
                    }
                }

                // Generate the segment number
                var segmentCol = context.GetExpressionName();
                var segment = new SegmentNode
                {
                    Source = sort,
                    SegmentColumn = segmentCol,
                    GroupBy = partitionCols
                };

                // Calculate the window function
                var functionCol = context.GetExpressionName();

                switch (windowFunction.FunctionName.Value.ToUpperInvariant())
                {
                    case "ROW_NUMBER":
                        if (windowFunction.Parameters.Count != 0)
                            exception = NotSupportedQueryFragmentException.Combine(exception, Sql4CdsError.FunctionTakesExactlyXArguments(windowFunction, 0));

                        result = new SequenceProjectNode
                        {
                            Source = segment,
                            SegmentColumn = segmentCol,
                            DefinedValues =
                            {
                                [functionCol] = new Aggregate
                                {
                                    AggregateType = AggregateType.RowNumber,
                                    ReturnType = DataTypeHelpers.BigInt
                                }
                            }
                        };
                        break;

                    case "RANK":
                    case "DENSE_RANK":
                        if (windowFunction.Parameters.Count != 0)
                            exception = NotSupportedQueryFragmentException.Combine(exception, Sql4CdsError.FunctionTakesExactlyXArguments(windowFunction, 0));

                        // Segment by ordering columns too
                        var segmentCol2 = context.GetExpressionName();
                        var segment2 = new SegmentNode
                        {
                            Source = segment,
                            SegmentColumn = segmentCol2,
                            GroupBy = orderCols
                        };

                        result = new SequenceProjectNode
                        {
                            Source = segment2,
                            SegmentColumn = segmentCol,
                            SegmentColumn2 = segmentCol2,
                            DefinedValues =
                            {
                                [functionCol] = new Aggregate
                                {
                                    AggregateType = windowFunction.FunctionName.Value.Equals("RANK", StringComparison.OrdinalIgnoreCase) ? AggregateType.Rank : AggregateType.DenseRank,
                                    ReturnType = DataTypeHelpers.BigInt
                                }
                            }
                        };
                        break;

                    case "AVG":
                    case "COUNT":
                    case "MAX":
                    case "MIN":
                    case "SUM":
                        {
                            var converted = new Aggregate();

                            if (windowFunction.Parameters.Count > 0)
                                converted.SqlExpression = windowFunction.Parameters[0].Clone();

                            switch (windowFunction.FunctionName.Value.ToUpper())
                            {
                                case "AVG":
                                    if (windowFunction.Parameters.Count != 1)
                                        throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidFunctionParameterCount(windowFunction.FunctionName, 1));

                                    converted.AggregateType = AggregateType.Average;
                                    break;

                                case "COUNT":
                                    if (windowFunction.Parameters.Count != 1)
                                        throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidFunctionParameterCount(windowFunction.FunctionName, 1));

                                    if ((converted.SqlExpression is ColumnReferenceExpression countCol && countCol.ColumnType == ColumnType.Wildcard) || (converted.SqlExpression is Literal && !(converted.SqlExpression is NullLiteral)))
                                        converted.AggregateType = AggregateType.CountStar;
                                    else
                                        converted.AggregateType = AggregateType.Count;
                                    break;

                                case "MAX":
                                    if (windowFunction.Parameters.Count != 1)
                                        throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidFunctionParameterCount(windowFunction.FunctionName, 1));

                                    converted.AggregateType = AggregateType.Max;
                                    break;

                                case "MIN":
                                    if (windowFunction.Parameters.Count != 1)
                                        throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidFunctionParameterCount(windowFunction.FunctionName, 1));

                                    converted.AggregateType = AggregateType.Min;
                                    break;

                                case "SUM":
                                    if (windowFunction.Parameters.Count != 1)
                                        throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidFunctionParameterCount(windowFunction.FunctionName, 1));

                                    if (converted.SqlExpression is IntegerLiteral sumLiteral && sumLiteral.Value == "1")
                                        converted.AggregateType = AggregateType.CountStar;
                                    else
                                        converted.AggregateType = AggregateType.Sum;
                                    break;

                                default:
                                    throw new NotSupportedQueryFragmentException(Sql4CdsError.NotSupported(windowFunction, windowFunction.FunctionName.Value));
                            }

                            // Validate the aggregate expression
                            if (converted.AggregateType == AggregateType.CountStar)
                            {
                                converted.SqlExpression = null;
                            }
                            else
                            {
                                var schema = source.GetSchema(context);
                                converted.SqlExpression.GetType(GetExpressionContext(schema, context), out var exprType);
                            }

                            // If this is a framed window aggregation (with ROWS or RANGE clause) we need to create a different
                            // execution plan.
                            if (windowFunction.OverClause.WindowFrameClause != null)
                            {
                                // Compute the row number, then calculate the minimum and maximum row number for each window frame.
                                // Pass the results into a window spool, then into a stream aggregate
                                // https://sqlserverfast.com/blog/hugo/2023/11/plansplaining-part-23-t-sql-tuesday-168-window-functions/
                                // Enable fast track optimisation if we are using ROWS UNBOUNDED PRECEDING
                                // https://sqlserverfast.com/blog/hugo/2023/11/plansplaining-part-24-windows-on-the-fast-track/
                                // TODO: Other special cases
                                // https://sqlserverfast.com/blog/hugo/2023/12/plansplaining-part-25-windows-without-upper-bound/
                                if (windowFunction.OverClause.WindowFrameClause.Top.WindowDelimiterType == WindowDelimiterType.UnboundedFollowing)
                                    throw new NotSupportedQueryFragmentException(Sql4CdsError.NotSupported(windowFunction.OverClause.WindowFrameClause.Top));
                                // https://sqlserverfast.com/blog/hugo/2023/12/plansplaining-part-26-windows-with-a-ranged-frame/
                                if (windowFunction.OverClause.WindowFrameClause.WindowFrameType != WindowFrameType.Rows)
                                    throw new NotSupportedQueryFragmentException(Sql4CdsError.NotSupported(windowFunction.OverClause.WindowFrameClause));

                                var rowNumberCol = context.GetExpressionName();

                                var rowNumber = new SequenceProjectNode
                                {
                                    Source = segment,
                                    SegmentColumn = segmentCol,
                                    DefinedValues =
                                    {
                                        [rowNumberCol] = new Aggregate
                                        {
                                            AggregateType = AggregateType.RowNumber,
                                            ReturnType = DataTypeHelpers.BigInt
                                        }
                                    }
                                };

                                var topRowNumberCol = context.GetExpressionName();
                                var bottomRowNumberCol = context.GetExpressionName();

                                var topBottomCalculate = new ComputeScalarNode
                                {
                                    Source = rowNumber
                                };

                                switch (windowFunction.OverClause.WindowFrameClause.Top.WindowDelimiterType)
                                {
                                    case WindowDelimiterType.UnboundedPreceding:
                                        topBottomCalculate.Columns[topRowNumberCol] = new ConvertCall { Parameter = new IntegerLiteral { Value = "1" }, DataType = DataTypeHelpers.BigInt };
                                        break;

                                    case WindowDelimiterType.ValuePreceding:
                                        topBottomCalculate.Columns[topRowNumberCol] = new BinaryExpression
                                        {
                                            FirstExpression = rowNumberCol.ToColumnReference(),
                                            BinaryExpressionType = BinaryExpressionType.Subtract,
                                            SecondExpression = windowFunction.OverClause.WindowFrameClause.Top.OffsetValue
                                        };
                                        break;

                                    case WindowDelimiterType.CurrentRow:
                                        topBottomCalculate.Columns[topRowNumberCol] = rowNumberCol.ToColumnReference();
                                        break;

                                    default:
                                        throw new NotSupportedQueryFragmentException(Sql4CdsError.NotSupported(windowFunction.OverClause.WindowFrameClause.Top));
                                }

                                switch (windowFunction.OverClause.WindowFrameClause.Bottom.WindowDelimiterType)
                                {
                                    case WindowDelimiterType.ValueFollowing:
                                        topBottomCalculate.Columns[bottomRowNumberCol] = new BinaryExpression
                                        {
                                            FirstExpression = rowNumberCol.ToColumnReference(),
                                            BinaryExpressionType = BinaryExpressionType.Add,
                                            SecondExpression = windowFunction.OverClause.WindowFrameClause.Bottom.OffsetValue
                                        };
                                        break;

                                    case WindowDelimiterType.CurrentRow:
                                        topBottomCalculate.Columns[bottomRowNumberCol] = rowNumberCol.ToColumnReference();
                                        break;

                                    default:
                                        throw new NotSupportedQueryFragmentException(Sql4CdsError.NotSupported(windowFunction.OverClause.WindowFrameClause.Bottom));
                                }

                                var windowCountCol = context.GetExpressionName();

                                var spool = new WindowSpoolNode
                                {
                                    Source = topBottomCalculate,
                                    SegmentColumn = segmentCol,
                                    WindowCountColumn = windowCountCol,
                                    RowNumberColumn = rowNumberCol,
                                    TopRowNumberColumn = topRowNumberCol,
                                    BottomRowNumberColumn = bottomRowNumberCol,
                                    UseFastTrackOptimization = windowFunction.OverClause.WindowFrameClause.Top.WindowDelimiterType == WindowDelimiterType.UnboundedPreceding
                                };

                                // Stream aggregate will automatically add FIRST aggregates for all the required columns
                                // to output the non-aggregated values from each record.
                                var aggregate = new StreamAggregateNode
                                {
                                    Source = spool,
                                    GroupBy = { windowCountCol.ToColumnReference() },
                                    Aggregates =
                                    {
                                        [functionCol] = converted
                                    }
                                };

                                result = aggregate;
                            }
                            else
                            {
                                // Create the frameless execution plan - lazy table spool with the segment column set to produce one row per segment
                                // fed into a nested loop. That loop calls another loop which combines the aggregate with the individual
                                // row values
                                // https://sqlserverfast.com/blog/hugo/2018/06/plansplaining-part-6-aggregates-with-over/
                                var spoolProducer = new TableSpoolNode
                                {
                                    Source = segment,
                                    SpoolType = SpoolType.Lazy,
                                    SegmentColumn = segmentCol
                                };

                                var spoolConsumerAggregate = new TableSpoolNode
                                {
                                    Producer = spoolProducer,
                                    SpoolType = spoolProducer.SpoolType
                                };
                                var aggregate = new StreamAggregateNode
                                {
                                    Source = spoolConsumerAggregate,
                                    Aggregates =
                                    {
                                        [functionCol] = converted
                                    }
                                };
                                var spoolConsumer = new TableSpoolNode
                                {
                                    Producer = spoolProducer,
                                    SpoolType = spoolProducer.SpoolType
                                };
                                var loop1 = new NestedLoopNode
                                {
                                    LeftSource = aggregate,
                                    RightSource = spoolConsumer
                                };
                                var loop2 = new NestedLoopNode
                                {
                                    LeftSource = spoolProducer,
                                    RightSource = loop1
                                };

                                result = loop2;
                            }
                        }
                        break;

                    default:
                        exception = NotSupportedQueryFragmentException.Combine(exception, Sql4CdsError.NotSupported(windowFunction));
                        break;
                }

                calculationRewrites[windowFunction] = functionCol.ToColumnReference();
            }

            if (exception != null)
                throw exception;

            if (calculationRewrites.Count > 0)
                querySpec.Accept(new RewriteVisitor(calculationRewrites));

            return result;
        }

        private void ConvertForXmlClause(SelectNode selectNode, XmlForClause forXml, NodeCompilationContext context)
        {
            // We need to know the individual column names, so expand any wildcard columns
            selectNode.ExpandWildcardColumns(context);

            // Create the node to convert the data to XML
            var xmlNode = new XmlWriterNode
            {
                Source = selectNode.Source,
            };

            foreach (var col in selectNode.ColumnSet)
                xmlNode.ColumnSet.Add(col);

            foreach (var option in forXml.Options)
            {
                switch (option.OptionKind)
                {
                    case XmlForClauseOptions.Raw:
                        xmlNode.XmlFormat = XmlFormat.Raw;
                        xmlNode.ElementName = option.Value?.Value ?? "row";
                        break;

                    case XmlForClauseOptions.Explicit:
                        xmlNode.XmlFormat = XmlFormat.Explicit;
                        break;

                    case XmlForClauseOptions.Auto:
                        xmlNode.XmlFormat = XmlFormat.Auto;
                        break;

                    case XmlForClauseOptions.Path:
                        xmlNode.XmlFormat = XmlFormat.Path;
                        xmlNode.ElementName = option.Value?.Value ?? "row";
                        xmlNode.ColumnFormat = XmlColumnFormat.Element;
                        break;

                    case XmlForClauseOptions.Elements:
                    case XmlForClauseOptions.ElementsAbsent:
                        xmlNode.ColumnFormat = XmlColumnFormat.Element;
                        break;

                    case XmlForClauseOptions.ElementsXsiNil:
                        xmlNode.ColumnFormat = XmlColumnFormat.Element | XmlColumnFormat.XsiNil;
                        break;

                    case XmlForClauseOptions.Type:
                        xmlNode.XmlType = true;
                        break;

                    case XmlForClauseOptions.Root:
                        xmlNode.RootName = option.Value?.Value ?? "root";
                        break;

                    default:
                        throw new NotSupportedQueryFragmentException(Sql4CdsError.NotSupported(option, "FOR XML"));
                }
            }

            // Output the final XML
            selectNode.Source = xmlNode;
            selectNode.ColumnSet.Clear();
            selectNode.ColumnSet.Add(new SelectColumn
            {
                SourceColumn = "xml",
                OutputColumn = "xml"
            });
        }

        private IDataExecutionPlanNodeInternal ConvertInSubqueries(IDataExecutionPlanNodeInternal source, IList<OptimizerHint> hints, TSqlFragment query, NodeCompilationContext context, INodeSchema outerSchema, IDictionary<string, string> outerReferences)
        {
            var visitor = new InSubqueryVisitor();
            query.Accept(visitor);

            if (visitor.InSubqueries.Count == 0)
                return source;

            NotSupportedQueryFragmentException exception = null;

            foreach (var inSubquery in visitor.InSubqueries)
            {
                try
                {
                    source = ConvertInSubquery(source, hints, query, inSubquery, context, outerSchema, outerReferences);
                }
                catch (NotSupportedQueryFragmentException ex)
                {
                    exception = NotSupportedQueryFragmentException.Combine(exception, ex);
                }
            }

            if (exception != null)
                throw exception;

            return source;
        }

        private IDataExecutionPlanNodeInternal ConvertInSubquery(IDataExecutionPlanNodeInternal source, IList<OptimizerHint> hints, TSqlFragment query, InPredicate inSubquery, NodeCompilationContext context, INodeSchema outerSchema, IDictionary<string, string> outerReferences)
        {
            var computeScalar = source as ComputeScalarNode;
            var schema = source.GetSchema(context);

            // Validate the LHS expression
            inSubquery.Expression.GetType(GetExpressionContext(schema, context), out _);

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

                var alias = context.GetExpressionName();
                computeScalar.Columns[alias] = inSubquery.Expression.Clone();
                lhsCol = alias.ToColumnReference();
            }
            else
            {
                // Normalize the LHS column
                if (schema.ContainsColumn(lhsCol.GetColumnName(), out var lhsColNormalized))
                    lhsCol = lhsColNormalized.ToColumnReference();
            }

            var innerContext = context.CreateChildContext(null);
            var references = new Dictionary<string, string>();
            var innerQuery = ConvertSelectStatement(inSubquery.Subquery.QueryExpression, hints, schema, references, innerContext);

            // Scalar subquery must return exactly one column and one row
            innerQuery.ExpandWildcardColumns(innerContext);

            if (innerQuery.ColumnSet.Count != 1)
                throw new NotSupportedQueryFragmentException(Sql4CdsError.MultiColumnScalarSubquery(inSubquery.Subquery));

            // Create the join
            BaseJoinNode join;
            var testColumn = innerQuery.ColumnSet[0].SourceColumn;

            if (references.Count == 0)
            {
                if (UseMergeJoin(source, innerQuery.Source, context, references, testColumn, lhsCol.GetColumnName(), true, true, out var outputCol, out var merge))
                {
                    testColumn = outputCol;
                    join = merge;
                }
                else
                {
                    // We need the inner list to be distinct to avoid creating duplicates during the join
                    var innerSchema = innerQuery.Source.GetSchema(innerContext);
                    if (innerQuery.ColumnSet[0].SourceColumn != innerSchema.PrimaryKey && !(innerQuery.Source is DistinctNode))
                    {
                        innerQuery.Source = new DistinctNode
                        {
                            Source = innerQuery.Source,
                            Columns = { innerQuery.ColumnSet[0].SourceColumn }
                        };
                    }

                    // This isn't a correlated subquery, so we can use a foldable join type. Alias the results so there's no conflict with the
                    // same table being used inside the IN subquery and elsewhere
                    var alias = new AliasNode(innerQuery, new Identifier { Value = context.GetExpressionName() }, context);

                    testColumn = $"{alias.Alias}.{alias.ColumnSet[0].OutputColumn}";
                    join = new HashJoinNode
                    {
                        LeftSource = source,
                        LeftAttribute = lhsCol.Clone(),
                        RightSource = alias,
                        RightAttribute = new ColumnReferenceExpression { MultiPartIdentifier = new MultiPartIdentifier { Identifiers = { new Identifier { Value = alias.Alias }, new Identifier { Value = alias.ColumnSet[0].OutputColumn } } } }
                    };
                }

                if (!join.SemiJoin)
                {
                    // Convert the join to a semi join to ensure requests for wildcard columns aren't folded to the IN subquery
                    var definedValue = context.GetExpressionName();
                    join.SemiJoin = true;
                    join.OutputRightSchema = false;
                    join.DefinedValues[definedValue] = testColumn;
                    testColumn = definedValue;
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
                    InsertCorrelatedSubquerySpool(loopRightSourceSimple, source, hints, context, references.Values.ToArray());

                var definedValue = context.GetExpressionName();

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
                    OutputRightSchema = false,
                    DefinedValues = { [definedValue] = innerQuery.ColumnSet[0].SourceColumn }
                };

                testColumn = definedValue;
            }

            join.JoinType = QualifiedJoinType.LeftOuter;

            var rewrites = new Dictionary<BooleanExpression, BooleanExpression>();
            rewrites[inSubquery] = new BooleanIsNullExpression
            {
                IsNot = !inSubquery.NotDefined,
                Expression = testColumn.ToColumnReference()
            };
            query.Accept(new BooleanRewriteVisitor(rewrites));

            return join;
        }

        private IDataExecutionPlanNodeInternal ConvertExistsSubqueries(IDataExecutionPlanNodeInternal source, IList<OptimizerHint> hints, TSqlFragment query, NodeCompilationContext context, INodeSchema outerSchema, IDictionary<string, string> outerReferences)
        {
            var visitor = new ExistsSubqueryVisitor();
            query.Accept(visitor);

            if (visitor.ExistsSubqueries.Count == 0)
                return source;

            NotSupportedQueryFragmentException exception = null;

            foreach (var existsSubquery in visitor.ExistsSubqueries)
            {
                try
                {
                    source = ConvertExistsSubquery(source, hints, query, existsSubquery, context, outerSchema, outerReferences);
                }
                catch (NotSupportedQueryFragmentException ex)
                {
                    exception = NotSupportedQueryFragmentException.Combine(exception, ex);
                }
            }

            if (exception != null)
                throw exception;

            return source;
        }

        private IDataExecutionPlanNodeInternal ConvertExistsSubquery(IDataExecutionPlanNodeInternal source, IList<OptimizerHint> hints, TSqlFragment query, ExistsPredicate existsSubquery, NodeCompilationContext context, INodeSchema outerSchema, IDictionary<string, string> outerReferences)
        {
            var schema = source.GetSchema(context);

            // Each query of the format "EXISTS (SELECT * FROM source)" becomes a outer semi join
            var innerContext = context.CreateChildContext(null);
            var references = new Dictionary<string, string>();
            var innerQuery = ConvertSelectStatement(existsSubquery.Subquery.QueryExpression, hints, schema, references, innerContext);
            var innerSchema = innerQuery.Source.GetSchema(innerContext);
            var innerSchemaPrimaryKey = innerSchema.PrimaryKey;

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
                if (innerSchemaPrimaryKey == null)
                {
                    innerSchemaPrimaryKey = context.GetExpressionName();

                    if (!(innerQuery.Source is ComputeScalarNode computeScalar))
                    {
                        computeScalar = new ComputeScalarNode { Source = innerQuery.Source };
                        innerQuery.Source = computeScalar;
                    }

                    computeScalar.Columns[innerSchemaPrimaryKey] = new IntegerLiteral { Value = "1" };
                }

                // We can spool the results for reuse each time
                innerQuery.Source = new TableSpoolNode
                {
                    Source = innerQuery.Source,
                    SpoolType = SpoolType.Lazy
                };

                testColumn = context.GetExpressionName();

                join = new NestedLoopNode
                {
                    LeftSource = source,
                    RightSource = innerQuery.Source,
                    JoinType = QualifiedJoinType.LeftOuter,
                    SemiJoin = true,
                    OutputRightSchema = false,
                    OuterReferences = references,
                    DefinedValues =
                    {
                        [testColumn] = innerSchemaPrimaryKey
                    }
                };
            }
            else if (UseMergeJoin(source, innerQuery.Source, context, references, null, null, true, false, out testColumn, out var merge))
            {
                join = merge;
            }
            else
            {
                // We need to use nested loops for correlated subqueries
                // TODO: We could use a hash join where there is a simple correlation, but followed by a distinct node to eliminate duplicates
                // We could also move the correlation criteria out of the subquery and into the join condition. We would then make one request to
                // get all the related records and spool that in memory to get the relevant results in the nested loop. Need to understand how 
                // many rows are likely from the outer query to work out if this is going to be more efficient or not.
                if (innerQuery.Source is ISingleSourceExecutionPlanNode loopRightSourceSimple)
                    InsertCorrelatedSubquerySpool(loopRightSourceSimple, source, hints, context, references.Values.ToArray());

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
                if (innerSchemaPrimaryKey == null)
                {
                    innerSchemaPrimaryKey = context.GetExpressionName();

                    if (!(innerQuery.Source is ComputeScalarNode computeScalar))
                    {
                        computeScalar = new ComputeScalarNode { Source = innerQuery.Source };
                        innerQuery.Source = computeScalar;
                    }

                    computeScalar.Columns[innerSchemaPrimaryKey] = new IntegerLiteral { Value = "1" };
                }

                var definedValue = context.GetExpressionName();

                join = new NestedLoopNode
                {
                    LeftSource = source,
                    RightSource = innerQuery.Source,
                    OuterReferences = references,
                    SemiJoin = true,
                    OutputRightSchema = false,
                    DefinedValues = { [definedValue] = innerSchemaPrimaryKey }
                };

                testColumn = definedValue;
            }

            join.JoinType = QualifiedJoinType.LeftOuter;

            var rewrites = new Dictionary<BooleanExpression, BooleanExpression>();
            rewrites[existsSubquery] = new BooleanIsNullExpression
            {
                IsNot = true,
                Expression = testColumn.ToColumnReference()
            };
            query.Accept(new BooleanRewriteVisitor(rewrites));

            return join;
        }

        private IDataExecutionPlanNodeInternal ConvertHavingClause(IDataExecutionPlanNodeInternal source, IList<OptimizerHint> hints, HavingClause havingClause, NodeCompilationContext context, INodeSchema outerSchema, IDictionary<string, string> outerReferences, TSqlFragment query, INodeSchema nonAggregateSchema)
        {
            if (havingClause == null)
                return source;

            CaptureOuterReferences(outerSchema, source, havingClause, context, outerReferences);

            var computeScalar = new ComputeScalarNode { Source = source };
            ConvertScalarSubqueries(havingClause.SearchCondition, hints, ref source, computeScalar, context, query);

            // Validate the final expression
            havingClause.SearchCondition.GetType(GetExpressionContext(source.GetSchema(context), context, nonAggregateSchema), out _);

            return new FilterNode
            {
                Filter = havingClause.SearchCondition.Clone(),
                Source = source
            };
        }

        private IDataExecutionPlanNodeInternal ConvertGroupByAggregates(IDataExecutionPlanNodeInternal source, QuerySpecification querySpec, NodeCompilationContext context, INodeSchema outerSchema, IDictionary<string, string> outerReferences)
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
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.NotSupported(querySpec.GroupByClause, "GROUP BY ALL"));

                if (querySpec.GroupByClause.GroupByOption != GroupByOption.None)
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.NotSupported(querySpec.GroupByClause, $"GROUP BY {querySpec.GroupByClause.GroupByOption}"));

                var groupByValidator = new GroupByValidatingVisitor();
                querySpec.GroupByClause.Accept(groupByValidator);

                if (groupByValidator.Error != null)
                    throw new NotSupportedQueryFragmentException(groupByValidator.Error);
            }

            var schema = source.GetSchema(context);

            // Create the grouping expressions. Grouping is done on single columns only - if a grouping is a more complex expression,
            // create a new calculated column using a Compute Scalar node first.
            var groupings = new Dictionary<ScalarExpression, ColumnReferenceExpression>();

            if (querySpec.GroupByClause != null)
            {
                CaptureOuterReferences(outerSchema, source, querySpec.GroupByClause, context, outerReferences);

                foreach (var grouping in querySpec.GroupByClause.GroupingSpecifications)
                {
                    if (!(grouping is ExpressionGroupingSpecification exprGroup))
                        throw new NotSupportedQueryFragmentException(Sql4CdsError.NotSupported(grouping, "GROUP BY"));

                    // Validate the GROUP BY expression
                    exprGroup.Expression.GetType(GetExpressionContext(schema, context), out _);

                    if (exprGroup.Expression is ColumnReferenceExpression col)
                    {
                        schema.ContainsColumn(col.GetColumnName(), out var groupByColName);

                        if (col.GetColumnName() != groupByColName)
                        {
                            col = groupByColName.ToColumnReference();
                            exprGroup.Expression = col;
                        }
                    }
                    else
                    {
                        // Use generic name for computed columns by default. Special case for DATEPART functions which
                        // could be folded down to FetchXML directly, so make these nicer names
                        string name = null;

                        if (IsDatePartFunc(exprGroup.Expression, out var partName, out var colName))
                        {
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

                            if (partnames.TryGetValue(partName, out var dateGrouping) && schema.ContainsColumn(colName, out colName))
                            {
                                name = colName.SplitMultiPartIdentifier().Last() + "_" + dateGrouping;
                                var baseName = name;

                                var suffix = 0;

                                while (groupings.Values.Any(grp => grp.GetColumnName().Equals(name, StringComparison.OrdinalIgnoreCase)))
                                    name = $"{baseName}_{++suffix}";
                            }
                        }

                        if (name == null)
                            name = context.GetExpressionName();

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
                    computeScalar.Columns[calc.Value.GetColumnName()] = calc.Key.Clone();
                }

                source = computeScalar;

                querySpec.Accept(new RewriteVisitor(rewrites));
            }

            var hashMatch = new HashMatchAggregateNode
            {
                Source = source
            };

            foreach (var grouping in groupings)
                hashMatch.GroupBy.Add(grouping.Value.Clone());

            // Create the aggregate functions
            var aggregateCollector = new AggregateCollectingVisitor();
            aggregateCollector.GetAggregates(querySpec);
            var aggregateRewrites = new Dictionary<ScalarExpression, string>();

            foreach (var aggregate in aggregateCollector.Aggregates.Select(a => new { Expression = a, Alias = (string)null }).Concat(aggregateCollector.SelectAggregates.Select(s => new { Expression = (FunctionCall)s.Expression, Alias = s.ColumnName?.Identifier?.Value })))
            {
                CaptureOuterReferences(outerSchema, source, aggregate.Expression, context, outerReferences);

                var converted = new Aggregate
                {
                    Distinct = aggregate.Expression.UniqueRowFilter == UniqueRowFilter.Distinct
                };

                if (aggregate.Expression.Parameters.Count > 0)
                    converted.SqlExpression = aggregate.Expression.Parameters[0].Clone();

                switch (aggregate.Expression.FunctionName.Value.ToUpper())
                {
                    case "AVG":
                        if (aggregate.Expression.Parameters.Count != 1)
                            throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidFunctionParameterCount(aggregate.Expression.FunctionName, 1));

                        converted.AggregateType = AggregateType.Average;
                        break;

                    case "COUNT":
                        if (aggregate.Expression.Parameters.Count != 1)
                            throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidFunctionParameterCount(aggregate.Expression.FunctionName, 1));

                        if ((converted.SqlExpression is ColumnReferenceExpression countCol && countCol.ColumnType == ColumnType.Wildcard) || (converted.SqlExpression is Literal && !(converted.SqlExpression is NullLiteral)))
                            converted.AggregateType = AggregateType.CountStar;
                        else
                            converted.AggregateType = AggregateType.Count;
                        break;

                    case "MAX":
                        if (aggregate.Expression.Parameters.Count != 1)
                            throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidFunctionParameterCount(aggregate.Expression.FunctionName, 1));

                        converted.AggregateType = AggregateType.Max;
                        break;

                    case "MIN":
                        if (aggregate.Expression.Parameters.Count != 1)
                            throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidFunctionParameterCount(aggregate.Expression.FunctionName, 1));

                        converted.AggregateType = AggregateType.Min;
                        break;

                    case "SUM":
                        if (aggregate.Expression.Parameters.Count != 1)
                            throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidFunctionParameterCount(aggregate.Expression.FunctionName, 1));

                        if (converted.SqlExpression is IntegerLiteral sumLiteral && sumLiteral.Value == "1")
                            converted.AggregateType = AggregateType.CountStar;
                        else
                            converted.AggregateType = AggregateType.Sum;
                        break;

                    case "STRING_AGG":
                        converted.AggregateType = AggregateType.StringAgg;

                        if (aggregate.Expression.Parameters.Count != 2)
                            throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidFunctionParameterCount(aggregate.Expression.FunctionName, 2));

                        if (!aggregate.Expression.Parameters[1].IsConstantValueExpression(new ExpressionCompilationContext(context, null, null), out var separator))
                            throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidStringAggSeparator(aggregate.Expression.Parameters[1]));

                        converted.Separator = separator.Value;
                        break;

                    default:
                        throw new NotSupportedQueryFragmentException(Sql4CdsError.NotSupported(aggregate.Expression, aggregate.Expression.FunctionName.Value));
                }

                // Validate the aggregate expression
                if (converted.AggregateType == AggregateType.CountStar)
                {
                    converted.SqlExpression = null;
                }
                else
                {
                    converted.SqlExpression.GetType(GetExpressionContext(schema, context), out var exprType);

                    if (converted.AggregateType == AggregateType.StringAgg)
                    {
                        // Make the separator have the same collation as the expression
                        if (exprType is SqlDataTypeReferenceWithCollation exprTypeColl)
                            converted.Separator = exprTypeColl.Collation.ToSqlString(converted.Separator.Value);
                        else
                            converted.Separator = context.PrimaryDataSource.DefaultCollation.ToSqlString(converted.Separator.Value);
                    }
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
                        aggregateName = aggregate.Expression.FunctionName.Value.ToLower();
                    else
                        aggregateName = colRef.GetColumnName().Replace('.', '_') + "_" + aggregate.Expression.FunctionName.Value.ToLower();

                    if (converted.Distinct)
                        aggregateName += "_distinct";
                }
                else
                {
                    aggregateName = context.GetExpressionName();
                }

                hashMatch.Aggregates[aggregateName] = converted;
                aggregateRewrites[aggregate.Expression] = aggregateName;

                // Apply the WITHIN GROUP sort
                if (aggregate.Expression.WithinGroupClause != null)
                {
                    if (converted.AggregateType != AggregateType.StringAgg)
                        throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidWithinGroupClause(aggregate.Expression.FunctionName));

                    if (hashMatch.WithinGroupSorts.Any())
                    {
                        // Defining a WITHIN GROUP clause more than once is not allowed - unless they are identical
                        if (aggregate.Expression.WithinGroupClause.OrderByClause.OrderByElements.Count != hashMatch.WithinGroupSorts.Count)
                            throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidWithinGroupOrdering(aggregate.Expression));

                        for (var i = 0; i < aggregate.Expression.WithinGroupClause.OrderByClause.OrderByElements.Count; i++)
                        {
                            var newSort = aggregate.Expression.WithinGroupClause.OrderByClause.OrderByElements[i];
                            var existingSort = hashMatch.WithinGroupSorts[i];

                            if (newSort.ToSql() != existingSort.ToSql())
                                throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidWithinGroupOrdering(aggregate.Expression));
                        }
                    }
                    else
                    {
                        foreach (var order in aggregate.Expression.WithinGroupClause.OrderByClause.OrderByElements)
                            hashMatch.WithinGroupSorts.Add(order);
                    }
                }
            }

            // Use the calculated aggregate values in later parts of the query
            var visitor = new RewriteVisitor(aggregateRewrites);
            foreach (var select in querySpec.SelectElements)
                select.Accept(visitor);
            querySpec.OrderByClause?.Accept(visitor);
            querySpec.HavingClause?.Accept(visitor);
            querySpec.TopRowFilter?.Accept(visitor);
            querySpec.OffsetClause?.Accept(visitor);

            return hashMatch;
        }

        private bool IsDatePartFunc(ScalarExpression expression, out string partName, out string colName)
        {
            partName = null;
            colName = null;

            if (!(expression is FunctionCall func))
                return false;

            if (func.FunctionName.Value.Equals("DATEPART", StringComparison.OrdinalIgnoreCase) &&
                func.Parameters.Count == 2 &&
                func.Parameters[0] is ColumnReferenceExpression datepart &&
                func.Parameters[1] is ColumnReferenceExpression datepartCol)
            {
                partName = datepart.GetColumnName();
                colName = datepartCol.GetColumnName();
                return true;
            }

            if ((
                    func.FunctionName.Value.Equals("YEAR", StringComparison.OrdinalIgnoreCase) ||
                    func.FunctionName.Value.Equals("MONTH", StringComparison.OrdinalIgnoreCase) ||
                    func.FunctionName.Value.Equals("DAY", StringComparison.OrdinalIgnoreCase)
                ) &&
                func.Parameters.Count == 1 &&
                func.Parameters[0] is ColumnReferenceExpression col)
            {
                partName = func.FunctionName.Value;
                colName = col.GetColumnName();
                return true;
            }

            return false;
        }

        private IDataExecutionPlanNodeInternal ConvertOffsetClause(IDataExecutionPlanNodeInternal source, OffsetClause offsetClause, NodeCompilationContext context)
        {
            if (offsetClause == null)
                return source;

            offsetClause.OffsetExpression.GetType(_staticContext, out var offsetType);
            offsetClause.FetchExpression.GetType(_staticContext, out var fetchType);
            var intType = DataTypeHelpers.Int;

            if (!SqlTypeConverter.CanChangeTypeImplicit(offsetType, intType))
                throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidOffsetClause(offsetClause.OffsetExpression));

            if (!SqlTypeConverter.CanChangeTypeImplicit(fetchType, intType))
                throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidTopOrFetchClause(offsetClause.FetchExpression));

            return new OffsetFetchNode
            {
                Source = source,
                Offset = offsetClause.OffsetExpression.Clone(),
                Fetch = offsetClause.FetchExpression.Clone()
            };
        }

        private IDataExecutionPlanNodeInternal ConvertTopClause(IDataExecutionPlanNodeInternal source, TopRowFilter topRowFilter, OrderByClause orderByClause, NodeCompilationContext context)
        {
            if (topRowFilter == null)
                return source;

            topRowFilter.Expression.GetType(_staticContext, out var topType);
            var targetType = topRowFilter.Percent ? DataTypeHelpers.Float : DataTypeHelpers.BigInt;

            if (!SqlTypeConverter.CanChangeTypeImplicit(topType, targetType))
            {
                if (topRowFilter.Percent)
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.TypeClash(topRowFilter.Expression, topType, targetType));
                else
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidTopOrFetchClause(topRowFilter.Expression));
            }

            var tieColumns = new HashSet<string>();

            if (topRowFilter.WithTies)
            {
                if (orderByClause == null)
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.TopNWithTiesRequiresOrderBy(topRowFilter));

                var schema = source.GetSchema(context);

                foreach (var sort in orderByClause.OrderByElements)
                {
                    if (!(sort.Expression is ColumnReferenceExpression sortCol))
                        throw new NotSupportedQueryFragmentException("ORDER BY must reference a column for use with TOP N WITH TIES", sort.Expression);

                    if (!schema.ContainsColumn(sortCol.GetColumnName(), out var colName))
                        throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidColumnName(sortCol));

                    tieColumns.Add(colName);
                }

                // Ensure data is sorted by the tie columns
                if (!schema.IsSortedBy(tieColumns))
                {
                    var sort = new SortNode { Source = source };

                    foreach (var orderBy in orderByClause.OrderByElements)
                        sort.Sorts.Add(orderBy.Clone());

                    source = sort;
                }
            }

            // TOP x PERCENT requires evaluating the source twice - once to get the total count and again to get the top
            // records. Cache the results in a table spool node.
            if (topRowFilter.Percent)
                source = new TableSpoolNode { Source = source, SpoolType = SpoolType.Eager };

            return new TopNode
            {
                Source = source,
                Top = topRowFilter.Expression.Clone(),
                Percent = topRowFilter.Percent,
                WithTies = topRowFilter.WithTies,
                TieColumns = topRowFilter.WithTies ? tieColumns.ToList() : null
            };
        }

        private IDataExecutionPlanNodeInternal ConvertOrderByClause(IDataExecutionPlanNodeInternal source, IList<OptimizerHint> hints, OrderByClause orderByClause, ScalarExpression[] selectList, Dictionary<string,List<string>> aliases, TSqlFragment query, NodeCompilationContext context, INodeSchema outerSchema, Dictionary<string, string> outerReferences, INodeSchema nonAggregateSchema)
        {
            if (orderByClause == null)
                return source;

            NotSupportedQueryFragmentException exception = null;

            CaptureOuterReferences(outerSchema, source, orderByClause, context, outerReferences);

            var computeScalar = new ComputeScalarNode { Source = source };

            try
            {
                ConvertScalarSubqueries(orderByClause, hints, ref source, computeScalar, context, query);
            }
            catch (NotSupportedQueryFragmentException ex)
            {
                exception = NotSupportedQueryFragmentException.Combine(exception, ex);
            }

            var schema = source.GetSchema(context);
            var sort = new SortNode { Source = source };

            // Sorts can use aliases from the SELECT clause, but only as the entire sort expression, not a calculation.
            // Aliases must not be duplicated
            foreach (var order in orderByClause.OrderByElements)
            {
                if (order.Expression is ColumnReferenceExpression orderByCol &&
                    aliases.TryGetValue(orderByCol.GetColumnName(), out var aliasedCols))
                {
                    if (aliasedCols.Count > 1)
                        exception = NotSupportedQueryFragmentException.Combine(exception, Sql4CdsError.AmbiguousColumnName(orderByCol));

                    order.Expression = aliasedCols[0].ToColumnReference();
                    order.ScriptTokenStream = null;
                }
            }

            // Check if any of the order expressions need pre-calculation
            var calculationRewrites = new Dictionary<ScalarExpression, ScalarExpression>();

            foreach (var orderBy in orderByClause.OrderByElements)
            {
                // If the order by element is a numeric literal, use the corresponding expression from the select list at that index
                if (orderBy.Expression is IntegerLiteral literal)
                {
                    var index = int.Parse(literal.Value, CultureInfo.InvariantCulture) - 1;

                    if (index < 0 || index >= selectList.Length)
                    {
                        exception = NotSupportedQueryFragmentException.Combine(exception, new NotSupportedQueryFragmentException(Sql4CdsError.InvalidOrderByColumnNumber(orderBy))
                        {
                            Suggestion = $"Must be between 1 and {selectList.Length}"
                        });
                    }
                    else
                    {
                        orderBy.Expression = selectList[index];
                        orderBy.ScriptTokenStream = null;
                    }
                }

                // Anything complex expression should be pre-calculated
                if (!(orderBy.Expression is ColumnReferenceExpression) &&
                    !(orderBy.Expression is VariableReference) &&
                    !(orderBy.Expression is Literal))
                {
                    try
                    {
                        var calculated = ComputeScalarExpression(orderBy.Expression, hints, query, computeScalar, nonAggregateSchema, context, ref source);
                        sort.Source = source;
                        schema = source.GetSchema(context);

                        calculationRewrites[orderBy.Expression] = calculated.ToColumnReference();
                    }
                    catch (NotSupportedQueryFragmentException ex)
                    {
                        exception = NotSupportedQueryFragmentException.Combine(exception, ex);
                    }
                }

                // Validate the expression
                try
                {
                    orderBy.Expression.GetType(GetExpressionContext(schema, context, nonAggregateSchema), out _);
                }
                catch (NotSupportedQueryFragmentException ex)
                {
                    exception = NotSupportedQueryFragmentException.Combine(exception, ex);
                }

                sort.Sorts.Add(orderBy.Clone());
            }

            if (exception != null)
                throw exception;

            // Use the calculated expressions in the sort and anywhere else that uses the same expression
            if (calculationRewrites.Any())
                query.Accept(new RewriteVisitor(calculationRewrites));

            if (computeScalar.Columns.Any())
                sort.Source = computeScalar;
            else
                sort.Source = computeScalar.Source;

            return sort;
        }

        private IDataExecutionPlanNodeInternal ConvertWhereClause(IDataExecutionPlanNodeInternal source, IList<OptimizerHint> hints, WhereClause whereClause, INodeSchema outerSchema, Dictionary<string,string> outerReferences, NodeCompilationContext context, TSqlFragment query)
        {
            if (whereClause == null)
                return source;

            if (whereClause.Cursor != null)
                throw new NotSupportedQueryFragmentException(Sql4CdsError.NotSupported(whereClause.Cursor, "CURSOR"));

            CaptureOuterReferences(outerSchema, source, whereClause.SearchCondition, context, outerReferences);

            var computeScalar = new ComputeScalarNode { Source = source };
            ConvertScalarSubqueries(whereClause.SearchCondition, hints, ref source, computeScalar, context, query);

            // Validate the final expression
            whereClause.SearchCondition.GetType(GetExpressionContext(source.GetSchema(context), context), out _);

            return new FilterNode
            {
                Filter = whereClause.SearchCondition.Clone(),
                Source = source
            };
        }

        private TSqlFragment CaptureOuterReferences(INodeSchema outerSchema, IDataExecutionPlanNodeInternal source, TSqlFragment query, NodeCompilationContext context, IDictionary<string,string> outerReferences)
        {
            if (outerSchema == null)
                return query;

            // We're in a subquery. Check if any columns in the WHERE clause are from the outer query
            // so we know which columns to pass through and rewrite the filter to use parameters
            var rewrites = new Dictionary<ScalarExpression, ScalarExpression>();
            var innerSchema = source?.GetSchema(context);
            var columns = query.GetColumns();

            foreach (var column in columns)
            {
                // Column names could be ambiguous between the inner and outer data sources. The inner
                // data source is used in preference.
                // Ref: https://docs.microsoft.com/en-us/sql/relational-databases/performance/subqueries?view=sql-server-ver15#qualifying
                var fromInner = innerSchema?.ContainsColumn(column, out _) == true;

                if (fromInner)
                    continue;

                var fromOuter = outerSchema.ContainsColumn(column, out var outerColumn);

                if (fromOuter)
                {
                    if (!outerReferences.TryGetValue(outerColumn, out var paramName))
                    {
                        // If the column is not already being passed through, add it to the list
                        paramName = "@" + context.GetExpressionName();
                        outerReferences.Add(outerColumn, paramName);
                        context.ParameterTypes[paramName] = outerSchema.Schema[outerColumn].Type;
                    }

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

        private SelectNode ConvertSelectClause(IList<SelectElement> selectElements, IList<OptimizerHint> hints, IDataExecutionPlanNodeInternal node, DistinctNode distinct, QuerySpecification query, NodeCompilationContext context, INodeSchema outerSchema, IDictionary<string,string> outerReferences, INodeSchema nonAggregateSchema, INodeSchema logicalSourceSchema)
        {
            var schema = node.GetSchema(context);

            var select = new SelectNode
            {
                Source = node,
                LogicalSourceSchema = logicalSourceSchema
            };

            var computeScalar = new ComputeScalarNode
            {
                Source = distinct?.Source ?? node
            };

            NotSupportedQueryFragmentException exception = null;

            foreach (var element in selectElements)
            {
                try
                {
                    CaptureOuterReferences(outerSchema, computeScalar, element, context, outerReferences);

                    if (element is SelectScalarExpression scalar)
                    {
                        if (scalar.Expression is ColumnReferenceExpression col)
                        {
                            // Check the expression is valid. This will throw an exception in case of missing columns etc.
                            col.GetType(GetExpressionContext(schema, context, nonAggregateSchema), out var colType);
                            if (colType is SqlDataTypeReferenceWithCollation colTypeColl && colTypeColl.CollationLabel == CollationLabel.NoCollation)
                                throw new NotSupportedQueryFragmentException(colTypeColl.CollationConflictError);

                            var colName = col.GetColumnName();

                            schema.ContainsColumn(colName, out colName);

                            var alias = scalar.ColumnName?.Value;

                            // If column has come from a calculation, don't expose the internal Expr{x} name
                            if (alias == null && !schema.Schema[colName].IsCalculated)
                                alias = col.MultiPartIdentifier.Identifiers.Last().Value;

                            select.ColumnSet.Add(new SelectColumn
                            {
                                SourceColumn = colName,
                                SourceExpression = scalar.Expression,
                                OutputColumn = alias
                            });
                        }
                        else
                        {
                            var scalarSource = distinct?.Source ?? node;
                            var alias = ComputeScalarExpression(scalar.Expression, hints, query, computeScalar, nonAggregateSchema, context, ref scalarSource);

                            var scalarSchema = computeScalar.GetSchema(context);
                            var colType = scalarSchema.Schema[alias].Type;
                            if (colType is SqlDataTypeReferenceWithCollation colTypeColl && colTypeColl.CollationLabel == CollationLabel.NoCollation)
                                throw new NotSupportedQueryFragmentException(colTypeColl.CollationConflictError);

                            if (distinct != null)
                                distinct.Source = scalarSource;
                            else
                                node = scalarSource;

                            select.ColumnSet.Add(new SelectColumn
                            {
                                SourceColumn = alias,
                                SourceExpression = scalar.Expression,
                                OutputColumn = scalar.ColumnName?.Value
                            });
                        }
                    }
                    else if (element is SelectStarExpression star)
                    {
                        var colName = star.Qualifier == null ? null : String.Join(".", star.Qualifier.Identifiers.Select(id => id.Value));

                        var cols = schema.Schema.Keys
                            .Where(col => colName == null || col.StartsWith(colName + ".", StringComparison.OrdinalIgnoreCase))
                            .ToList();

                        if (colName != null && cols.Count == 0)
                            throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidColumnPrefix(star));

                        // Can't select no-collation columns
                        foreach (var col in cols)
                        {
                            var colType = schema.Schema[col].Type;
                            if (colType is SqlDataTypeReferenceWithCollation colTypeColl && colTypeColl.CollationLabel == CollationLabel.NoCollation)
                                exception = NotSupportedQueryFragmentException.Combine(exception, colTypeColl.CollationConflictError);
                        }

                        select.ColumnSet.Add(new SelectColumn
                        {
                            SourceColumn = colName,
                            SourceExpression = star,
                            AllColumns = true
                        });
                    }
                    else
                    {
                        throw new NotSupportedQueryFragmentException(Sql4CdsError.NotSupported(element, "SELECT"));
                    }
                }
                catch (NotSupportedQueryFragmentException ex)
                {
                    exception = NotSupportedQueryFragmentException.Combine(exception, ex);
                }
            }

            if (exception != null)
                throw exception;

            var newSource = computeScalar.Columns.Any() ? computeScalar : computeScalar.Source;

            if (distinct != null)
                distinct.Source = newSource;
            else
                select.Source = newSource;

            if (computeScalar.Columns.Count > 0 && query.OrderByClause != null)
            {
                // Reuse the same calculations for the ORDER BY clause as well
                var calculationRewrites = computeScalar.Columns.ToDictionary(kvp => kvp.Value, kvp => kvp.Key);
                var rewrite = new RewriteVisitor(calculationRewrites);

                query.OrderByClause.Accept(rewrite);

                // Need to rewrite the select elements as well if the order by clause references a column by index
                if (query.OrderByClause.OrderByElements.Any(order => order.Expression is IntegerLiteral))
                {
                    foreach (var element in selectElements)
                        element.Accept(rewrite);
                }
            }

            if (distinct != null)
            {
                foreach (var col in select.ColumnSet)
                {
                    if (col.AllColumns)
                    {
                        var distinctSchema = distinct.GetSchema(context);
                        distinct.Columns.AddRange(distinctSchema.Schema.Keys.Where(k => col.SourceColumn == null || (k.SplitMultiPartIdentifier()[0] + ".*") == col.SourceColumn));
                    }
                    else
                    {
                        distinct.Columns.Add(col.SourceColumn);
                    }
                }
            }

            return select;
        }

        private string ComputeScalarExpression(ScalarExpression expression, IList<OptimizerHint> hints, TSqlFragment query, ComputeScalarNode computeScalar, INodeSchema nonAggregateSchema, NodeCompilationContext context, ref IDataExecutionPlanNodeInternal node)
        {
            var computedColumn = ConvertScalarSubqueries(expression, hints, ref node, computeScalar, context, query);

            if (computedColumn != null)
                expression = computedColumn;

            // Check the type of this expression now so any errors can be reported
            var computeScalarSchema = computeScalar.Source.GetSchema(context);
            expression.GetType(GetExpressionContext(computeScalarSchema, context, nonAggregateSchema), out _);

            // Don't need to compute the expression if it's just a column reference
            if (expression is ColumnReferenceExpression col)
                return col.GetColumnName();

            var alias = context.GetExpressionName();
            computeScalar.Columns[alias] = expression.Clone();
            return alias;
        }

        private ColumnReferenceExpression ConvertScalarSubqueries(TSqlFragment expression, IList<OptimizerHint> hints, ref IDataExecutionPlanNodeInternal node, ComputeScalarNode computeScalar, NodeCompilationContext context, TSqlFragment query)
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
                var outerSchema = node.GetSchema(context);
                var outerReferences = new Dictionary<string, string>();
                var innerContext = context.CreateChildContext(null);
                var subqueryPlan = ConvertSelectStatement(subquery.QueryExpression, hints, outerSchema, outerReferences, innerContext);

                // Scalar subquery must return exactly one column and one row
                subqueryPlan.ExpandWildcardColumns(innerContext);

                if (subqueryPlan.ColumnSet.Count != 1)
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.MultiColumnScalarSubquery(subquery));

                string outputcol;
                var subqueryCol = subqueryPlan.ColumnSet[0].SourceColumn;
                BaseJoinNode join = null;
                if (UseMergeJoin(node, subqueryPlan.Source, context, outerReferences, subqueryCol, null, false, true, out outputcol, out var merge))
                {
                    join = merge;
                }
                else
                {
                    outputcol = context.GetExpressionName();

                    var loopRightSource = subqueryPlan.Source;

                    // Unless the subquery has got an explicit TOP 1 clause, insert an aggregate and assertion nodes
                    // to check for one row
                    if (!(subqueryPlan.Source.EstimateRowsOut(context) is RowCountEstimateDefiniteRange range) || range.Maximum > 1)
                    {
                        // No point getting more than 2 rows as we only need to know if there is more than 1
                        var top = new TopNode
                        {
                            Source = loopRightSource,
                            Top = new IntegerLiteral { Value = "2" }
                        };
                        loopRightSource = top;
                        
                        var rowCountCol = context.GetExpressionName();
                        var aggregate = new HashMatchAggregateNode
                        {
                            Source = loopRightSource,
                            Aggregates =
                            {
                                [outputcol] = new Aggregate
                                {
                                    AggregateType = AggregateType.First,
                                    SqlExpression = subqueryCol.ToColumnReference()
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
                            Assertion = e => e.GetAttributeValue<SqlInt32>(rowCountCol).Value <= 1,
                            ErrorMessage = "Subquery produced more than 1 row"
                        };
                        loopRightSource = assert;

                        subqueryCol = outputcol;
                    }

                    // If the subquery is uncorrelated, add a table spool to cache the results
                    // If it is correlated, add a spool where possible closer to the data source
                    if (outerReferences.Count == 0)
                    {
                        if (EstimateRowsOut(node, context) > 1)
                        {
                            var spool = new TableSpoolNode { Source = loopRightSource, SpoolType = SpoolType.Lazy, IsPerformanceSpool = true };
                            loopRightSource = spool;
                        }
                    }
                    else if (loopRightSource is ISingleSourceExecutionPlanNode loopRightSourceSimple)
                    {
                        InsertCorrelatedSubquerySpool(loopRightSourceSimple, node, hints, context, outerReferences.Values.ToArray());
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
                            OutputRightSchema = false,
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
                return rewrites[scalar].ToColumnReference();

            return null;
        }

        private bool UseMergeJoin(IDataExecutionPlanNodeInternal node, IDataExecutionPlanNodeInternal subqueryPlan, NodeCompilationContext context, Dictionary<string, string> outerReferences, string subqueryCol, string inPredicateCol, bool semiJoin, bool preserveTop, out string outputCol, out MergeJoinNode merge)
        {
            outputCol = null;
            merge = null;

            // We can use a merge join for a scalar subquery when the subquery is simply SELECT [TOP 1] <column> FROM <table> WHERE <table>.<key> = <outertable>.<column>
            // The filter must be on the inner table's primary key
            var subNode = subqueryPlan;

            var alias = subNode as AliasNode;
            if (alias != null)
                subNode = alias.Source;

            if (subNode is TopNode top && !preserveTop)
                subNode = top.Source;

            var filter = subNode as FilterNode;
            if (filter != null)
                subNode = filter.Source;
            else if (inPredicateCol == null)
                return false;

            var outerKey = (string)null;
            var innerKey = (string)null;

            if (inPredicateCol != null)
            {
                outerKey = inPredicateCol;
                innerKey = subqueryCol;

                if (filter != null)
                {
                    subNode = filter;
                    filter = null;
                }
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

            var outerSchema = node.GetSchema(new NodeCompilationContext(Session, Options, null, Log));
            var innerSchema = subNode.GetSchema(new NodeCompilationContext(Session, Options, null, Log));

            if (!outerSchema.ContainsColumn(outerKey, out outerKey) ||
                !innerSchema.ContainsColumn(innerKey, out innerKey))
                return false;

            if (!semiJoin && innerSchema.PrimaryKey != innerKey)
                return false;

            // Sort order could be changed by merge join, which would change results if combined with a TOP node
            if (subNode is TopNode && innerSchema.SortOrder.Any())
                return false;

            var rightAttribute = innerKey.ToColumnReference();

            // Give the inner fetch a unique alias and update the name of the inner key
            var subAlias = alias?.Alias;

            if (subNode is FetchXmlScan fetch)
            {
                if (alias != null)
                    alias.FoldToFetchXML(fetch);
                else
                    fetch.Alias = context.GetExpressionName();

                subAlias = fetch.Alias;
            }
            else if (semiJoin && alias == null)
            {
                var select = new SelectNode { Source = subNode };
                select.ColumnSet.Add(new SelectColumn { SourceColumn = innerKey, OutputColumn = innerKey.SplitMultiPartIdentifier().Last() });
                alias = new AliasNode(select, new Identifier { Value = context.GetExpressionName() }, context);
                subAlias = alias.Alias;
            }

            if (rightAttribute.MultiPartIdentifier.Identifiers.Count == 2)
                rightAttribute.MultiPartIdentifier.Identifiers[0].Value = subAlias;

            // Add the required column with the expected alias (used for scalar subqueries and IN predicates, not for CROSS/OUTER APPLY
            if (subqueryCol != null)
            {
                if (subAlias != null)
                {
                    var subqueryAttribute = subqueryCol.ToColumnReference();

                    if (subqueryAttribute.MultiPartIdentifier.Identifiers.Count == 2)
                        subqueryAttribute.MultiPartIdentifier.Identifiers[0].Value = subAlias;

                    subqueryCol = subqueryAttribute.GetColumnName();
                }

                outputCol = subqueryCol;
                //subNode.AddRequiredColumns(context, new List<string> { subqueryCol });
            }

            if (alias != null && !(subNode is FetchXmlScan))
            {
                alias.Source = subNode;
                subNode = alias;
            }

            merge = new MergeJoinNode
            {
                LeftSource = node,
                LeftAttribute = outerKey.ToColumnReference(),
                RightSource = subNode,
                RightAttribute = rightAttribute.Clone(),
                JoinType = QualifiedJoinType.LeftOuter
            };

            if (semiJoin)
            {
                // Regenerate the schema after changing the alias
                innerSchema = subNode.GetSchema(new NodeCompilationContext(Session, Options, null, Log));

                if (innerSchema.PrimaryKey != rightAttribute.GetColumnName() && !(merge.RightSource is DistinctNode))
                {
                    merge.RightSource = new DistinctNode
                    {
                        Source = merge.RightSource,
                        Columns = { rightAttribute.GetColumnName() }
                    };
                }

                merge.SemiJoin = true;
                merge.OutputRightSchema = false;
                var definedValue = context.GetExpressionName();
                merge.DefinedValues[definedValue] = outputCol ?? rightAttribute.GetColumnName();
                outputCol = definedValue;
            }

            return true;
        }
        
        private void InsertCorrelatedSubquerySpool(ISingleSourceExecutionPlanNode node, IDataExecutionPlanNode outerSource, IList<OptimizerHint> hints, NodeCompilationContext context, string[] outerReferences)
        {
            if (hints != null && hints.Any(hint => hint.HintKind == OptimizerHintKind.NoPerformanceSpool))
                return;

            // Look for a simple case where there is a reference to the outer table in a filter node. Extract the minimal
            // amount of that filter to a new filter node and place a table spool between the correlated filter and its source

            // Skip over simple leading nodes to try to find a Filter node
            var lastCorrelatedStep = node;
            ISingleSourceExecutionPlanNode parentNode = null;
            FilterNode filter = null;

            while (node != null)
            {
                if (node is FilterNode f)
                {
                    filter = f;
                    break;
                }

                if (node is FetchXmlScan)
                    break;

                parentNode = node;

                if (node is IDataExecutionPlanNodeInternal dataNode && dataNode.GetVariables(false).Intersect(outerReferences).Any())
                    lastCorrelatedStep = node;

                node = node.Source as ISingleSourceExecutionPlanNode;
            }

            // If anything in the filter's source uses the outer reference we can't spool ths results
            if (filter != null && filter.Source.GetVariables(true).Intersect(outerReferences).Any())
                return;

            if (filter != null && filter.Filter.GetVariables().Any())
            {
                // The filter is correlated. Check if there's any non-correlated criteria we can split out into a separate node
                // that could be folded into the data source first
                if (SplitCorrelatedCriteria(filter.Filter, out var correlatedFilter, out var nonCorrelatedFilter))
                {
                    filter.Filter = correlatedFilter.Clone();
                    filter.Source = new FilterNode
                    {
                        Filter = nonCorrelatedFilter.Clone(),
                        Source = filter.Source
                    };
                }

                lastCorrelatedStep = filter;
            }

            if (lastCorrelatedStep?.Source == null)
                return;

            // If the last correlated step has a source which we couldn't step into because it's not an ISingleSourceExecutionPlanNode
            // but it uses an outer reference we can't spool it
            if (lastCorrelatedStep.Source.GetVariables(true).Intersect(outerReferences).Any())
                return;

            // Check the estimated counts for the outer loop and the source at the point we'd insert the spool
            // If the outer loop is non-trivial (>= 100 rows) or the inner loop is small (<= 5000 records) then we want
            // to use the spool.
            var outerCount = EstimateRowsOut((IDataExecutionPlanNodeInternal) outerSource, context);
            var innerCount = outerCount >= 100 ? -1 : EstimateRowsOut(lastCorrelatedStep.Source, context);

            if (outerCount >= 100 || innerCount <= 5000)
            {
                var spool = new TableSpoolNode
                {
                    Source = lastCorrelatedStep.Source,
                    SpoolType = SpoolType.Lazy,
                    IsPerformanceSpool = true
                };

                lastCorrelatedStep.Source = spool;
            }
        }

        private int EstimateRowsOut(IExecutionPlanNode source, NodeCompilationContext context)
        {
            if (source is IDataExecutionPlanNodeInternal dataNode)
            {
                dataNode.EstimateRowsOut(context);
                return dataNode.EstimatedRowsOut;
            }
            else
            {
                foreach (var child in source.GetSources())
                {
                    EstimateRowsOut(child, context);
                }
            }

            return 0;
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
                    correlatedFilter = correlatedLhs.And(correlatedRhs);
                    nonCorrelatedFilter = nonCorrelatedLhs.And(nonCorrelatedRhs);

                    return true;
                }
            }

            correlatedFilter = filter;
            return false;
        }

        private IDataExecutionPlanNodeInternal ConvertFromClause(FromClause fromClause, IList<OptimizerHint> hints, TSqlFragment query, INodeSchema outerSchema, Dictionary<string, string> outerReferences, NodeCompilationContext context)
        {
            fromClause.Accept(new DuplicateTableNameValidatingVisitor());

            var tables = fromClause.TableReferences;
            var node = ConvertTableReference(tables[0], hints, query, outerSchema, outerReferences, context);

            for (var i = 1; i < tables.Count; i++)
            {
                var nextTable = ConvertTableReference(tables[i], hints, query, outerSchema, outerReferences, context);

                // Join predicates will be lifted from the WHERE clause during folding later. For now, just add a table spool
                // to cache the results of the second table and use a nested loop to join them.
                nextTable = new TableSpoolNode { Source = nextTable, SpoolType = SpoolType.Lazy };

                node = new NestedLoopNode { LeftSource = node, RightSource = nextTable };
            }

            return node;
        }

        private IDataExecutionPlanNodeInternal ConvertTableReference(TableReference reference, IList<OptimizerHint> hints, TSqlFragment query, INodeSchema outerSchema, Dictionary<string, string> outerReferences, NodeCompilationContext context)
        {
            if (reference is NamedTableReference table)
            {
                if (table.SchemaObject.Identifiers.Count == 1 && _cteSubplans != null && _cteSubplans.TryGetValue(table.SchemaObject.BaseIdentifier.Value, out var cteSubplan))
                {
                    var aliasNode = (AliasNode)cteSubplan.Clone();

                    if (table.Alias != null)
                        aliasNode.Alias = table.Alias.Value;

                    return aliasNode;
                }

                var dataSource = SelectDataSource(table.SchemaObject);
                var entityName = table.SchemaObject.BaseIdentifier.Value;

                if (table.SchemaObject.SchemaIdentifier?.Value?.Equals("metadata", StringComparison.OrdinalIgnoreCase) == true)
                {
                    // We're asking for metadata - check the type
                    if (entityName.Equals("entity", StringComparison.OrdinalIgnoreCase))
                    {
                        return new MetadataQueryNode
                        {
                            DataSource = dataSource.Name,
                            MetadataSource = MetadataSource.Entity,
                            EntityAlias = table.Alias?.Value ?? entityName
                        };
                    }

                    if (entityName.Equals("attribute", StringComparison.OrdinalIgnoreCase))
                    {
                        return new MetadataQueryNode
                        {
                            DataSource = dataSource.Name,
                            MetadataSource = MetadataSource.Attribute,
                            AttributeAlias = table.Alias?.Value ?? entityName
                        };
                    }

                    if (entityName.Equals("relationship_1_n", StringComparison.OrdinalIgnoreCase))
                    {
                        return new MetadataQueryNode
                        {
                            DataSource = dataSource.Name,
                            MetadataSource = MetadataSource.OneToManyRelationship,
                            OneToManyRelationshipAlias = table.Alias?.Value ?? entityName
                        };
                    }

                    if (entityName.Equals("relationship_n_1", StringComparison.OrdinalIgnoreCase))
                    {
                        return new MetadataQueryNode
                        {
                            DataSource = dataSource.Name,
                            MetadataSource = MetadataSource.ManyToOneRelationship,
                            ManyToOneRelationshipAlias = table.Alias?.Value ?? entityName
                        };
                    }

                    if (entityName.Equals("relationship_n_n", StringComparison.OrdinalIgnoreCase))
                    {
                        return new MetadataQueryNode
                        {
                            DataSource = dataSource.Name,
                            MetadataSource = MetadataSource.ManyToManyRelationship,
                            ManyToManyRelationshipAlias = table.Alias?.Value ?? entityName
                        };
                    }

                    if (entityName.Equals("alternate_key", StringComparison.OrdinalIgnoreCase))
                    {
                        return new MetadataQueryNode
                        {
                            DataSource = dataSource.Name,
                            MetadataSource = MetadataSource.Key,
                            KeyAlias = table.Alias?.Value ?? entityName
                        };
                    }

                    if (entityName.Equals("optionsetvalue", StringComparison.OrdinalIgnoreCase))
                    {
                        return new MetadataQueryNode
                        {
                            DataSource = dataSource.Name,
                            MetadataSource = MetadataSource.Value,
                            ValueAlias = table.Alias?.Value ?? entityName
                        };
                    }

                    if (entityName.Equals("globaloptionset", StringComparison.OrdinalIgnoreCase))
                    {
                        return new GlobalOptionSetQueryNode
                        {
                            DataSource = dataSource.Name,
                            MetadataSource = OptionSetSource.OptionSet,
                            OptionSetAlias = table.Alias?.Value ?? entityName
                        };
                    }

                    if (entityName.Equals("globaloptionsetvalue", StringComparison.OrdinalIgnoreCase))
                    {
                        return new GlobalOptionSetQueryNode
                        {
                            DataSource = dataSource.Name,
                            MetadataSource = OptionSetSource.Value,
                            ValueAlias = table.Alias?.Value ?? entityName
                        };
                    }

                    throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidObjectName(table.SchemaObject));
                }
                else if (table.SchemaObject.SchemaIdentifier?.Value.Equals("sys", StringComparison.OrdinalIgnoreCase) == true)
                {
                    if (!Enum.TryParse<SystemFunction>(table.SchemaObject.BaseIdentifier.Value, true, out var systemFunction))
                        throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidObjectName(table.SchemaObject));

                    if (typeof(SystemFunction).GetField(systemFunction.ToString()).GetCustomAttribute<SystemObjectTypeAttribute>().Type != SystemObjectType.View)
                        throw new NotSupportedQueryFragmentException(Sql4CdsError.FunctionCalledWithoutParameters(table.SchemaObject));

                    return new SystemFunctionNode
                    {
                        DataSource = dataSource.Name,
                        Alias = table.Alias?.Value ?? systemFunction.ToString(),
                        SystemFunction = systemFunction
                    };
                }

                if (!String.IsNullOrEmpty(table.SchemaObject.SchemaIdentifier?.Value) &&
                    !table.SchemaObject.SchemaIdentifier.Value.Equals("dbo", StringComparison.OrdinalIgnoreCase) &&
                    !table.SchemaObject.SchemaIdentifier.Value.Equals("archive", StringComparison.OrdinalIgnoreCase) &&
                    !(table.SchemaObject.SchemaIdentifier.Value.Equals("bin", StringComparison.OrdinalIgnoreCase) && dataSource.Metadata.RecycleBinEntities != null))
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidObjectName(table.SchemaObject));

                if (entityName.StartsWith("#") && String.IsNullOrEmpty(table.SchemaObject.SchemaIdentifier?.Value))
                {
                    var dataTable = Session.TempDb.Tables[entityName];

                    if (dataTable == null)
                        throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidObjectName(table.SchemaObject));

                    return new TableScanNode
                    {
                        TableName = dataTable.TableName,
                        Alias = table.Alias?.Value ?? dataTable.TableName
                    };
                }

                // Validate the entity name
                EntityMetadata meta;

                try
                {
                    meta = dataSource.Metadata[entityName];
                }
                catch (FaultException ex)
                {
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidObjectName(table.SchemaObject), ex);
                }

                var unsupportedHint = table.TableHints.FirstOrDefault(hint => hint.HintKind != TableHintKind.NoLock);
                if (unsupportedHint != null)
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.NotSupported(unsupportedHint, unsupportedHint.HintKind.ToString()));

                if (table.TableSampleClause != null)
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.NotSupported(table.TableSampleClause, "TABLESAMPLE"));

                if (table.TemporalClause != null)
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.NotSupported(table.TemporalClause, table.TemporalClause.TemporalClauseType.ToString()));

                // Convert to a simple FetchXML source
                var fetchXmlScan = new FetchXmlScan
                {
                    DataSource = dataSource.Name,
                    FetchXml = new FetchXml.FetchType
                    {
                        nolock = table.TableHints.Any(hint => hint.HintKind == TableHintKind.NoLock),
                        Items = new object[]
                        {
                            new FetchXml.FetchEntityType
                            {
                                name = meta.LogicalName
                            }
                        }
                    },
                    Alias = table.Alias?.Value ?? entityName,
                    ReturnFullSchema = true
                };

                // Check if this should be using the long-term retention table
                if (table.SchemaObject.SchemaIdentifier?.Value.Equals("archive", StringComparison.OrdinalIgnoreCase) == true)
                {
                    if (meta.IsRetentionEnabled != true && meta.IsArchivalEnabled != true)
                        throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidObjectName(table.SchemaObject)) { Suggestion = "Ensure long term retention is enabled for this table - see https://learn.microsoft.com/en-us/power-apps/maker/data-platform/data-retention-set?WT.mc_id=DX-MVP-5004203" };

                    fetchXmlScan.FetchXml.DataSource = "retained";
                }
                // Check if this should be using the recycle bin table
                else if (table.SchemaObject.SchemaIdentifier?.Value.Equals("bin", StringComparison.OrdinalIgnoreCase) == true)
                {
                    if (!dataSource.Metadata.RecycleBinEntities.Contains(meta.LogicalName))
                        throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidObjectName(table.SchemaObject)) { Suggestion = "Ensure restoring of deleted records is enabled for this table - see https://learn.microsoft.com/en-us/power-platform/admin/restore-deleted-table-records?WT.mc_id=DX-MVP-5004203" };

                    fetchXmlScan.FetchXml.DataSource = "bin";
                }

                return fetchXmlScan;
            }

            if (reference is QualifiedJoin join)
            {
                // If the join involves the primary key of one table we can safely use a merge join.
                // Otherwise use a nested loop join
                var lhs = ConvertTableReference(join.FirstTableReference, hints, query, outerSchema, outerReferences, context);
                var rhs = ConvertTableReference(join.SecondTableReference, hints, query, outerSchema, outerReferences, context);
                var lhsSchema = lhs.GetSchema(context);
                var rhsSchema = rhs.GetSchema(context);
                var fixedValueColumns = GetFixedValueColumnsFromWhereClause(query, lhsSchema, rhsSchema);

                // Capture any references to data from an outer query
                // Use a temporary NestedLoopNode to include the full schema available within this query so far to ensure columns are
                // used from this query in preference to the outer query.
                CaptureOuterReferences(outerSchema, new NestedLoopNode { LeftSource = lhs, RightSource = rhs }, join.SearchCondition, context, outerReferences);

                var joinConditionVisitor = new JoinConditionVisitor(lhsSchema, rhsSchema, fixedValueColumns);
                join.SearchCondition.Accept(joinConditionVisitor);

                // If we didn't find any join criteria equating two columns in the table, try again
                // but allowing computed columns instead. This lets us use more efficient join types (merge or hash join)
                // by pre-computing the values of the expressions to use as the join keys
                if (joinConditionVisitor.LhsKey == null || joinConditionVisitor.RhsKey == null)
                {
                    joinConditionVisitor = new JoinConditionVisitor(lhsSchema, rhsSchema, fixedValueColumns);
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

                            var lhsColumn = ComputeScalarExpression(joinConditionVisitor.LhsExpression, hints, query, lhsComputeScalar, null, context, ref lhs);
                            joinConditionVisitor.LhsKey = new ColumnReferenceExpression { MultiPartIdentifier = new MultiPartIdentifier { Identifiers = { new Identifier { Value = lhsColumn } } } };
                        }

                        if (joinConditionVisitor.RhsKey == null)
                        {
                            if (!(rhs is ComputeScalarNode rhsComputeScalar))
                            {
                                rhsComputeScalar = new ComputeScalarNode { Source = rhs };
                                rhs = rhsComputeScalar;
                            }

                            var rhsColumn = ComputeScalarExpression(joinConditionVisitor.RhsExpression, hints, query, rhsComputeScalar, null, context, ref lhs);
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
                        LeftAttribute = joinConditionVisitor.LhsKey.Clone(),
                        RightSource = rhs,
                        RightAttribute = joinConditionVisitor.RhsKey.Clone(),
                        Expressions = { joinConditionVisitor.JoinCondition },
                        JoinType = join.QualifiedJoinType,
                        AdditionalJoinCriteria = join.SearchCondition.RemoveCondition(joinConditionVisitor.JoinCondition).Clone()
                    };
                }
                else if (joinConditionVisitor.LhsKey != null && joinConditionVisitor.RhsKey != null && joinConditionVisitor.RhsKey.GetColumnName() == rhsSchema.PrimaryKey)
                {
                    joinNode = new MergeJoinNode
                    {
                        LeftSource = rhs,
                        LeftAttribute = joinConditionVisitor.RhsKey.Clone(),
                        RightSource = lhs,
                        RightAttribute = joinConditionVisitor.LhsKey.Clone(),
                        Expressions = { joinConditionVisitor.JoinCondition },
                        AdditionalJoinCriteria = join.SearchCondition.RemoveCondition(joinConditionVisitor.JoinCondition).Clone()
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
                        LeftAttribute = joinConditionVisitor.LhsKey.Clone(),
                        RightSource = rhs,
                        RightAttribute = joinConditionVisitor.RhsKey.Clone(),
                        Expressions = { joinConditionVisitor.JoinCondition },
                        JoinType = join.QualifiedJoinType,
                        AdditionalJoinCriteria = join.SearchCondition.RemoveCondition(joinConditionVisitor.JoinCondition).Clone()
                    };
                }
                else
                {
                    // Spool the inner table so the results can be reused by the nested loop
                    rhs = new TableSpoolNode { Source = rhs, SpoolType = SpoolType.Eager, IsPerformanceSpool = true };

                    joinNode = new NestedLoopNode
                    {
                        LeftSource = lhs,
                        RightSource = rhs,
                        JoinType = join.QualifiedJoinType,
                        JoinCondition = join.SearchCondition.Clone()
                    };
                }

                BooleanExpression additionalCriteria = (joinNode as FoldableJoinNode)?.AdditionalJoinCriteria ?? (joinNode as NestedLoopNode)?.JoinCondition;

                if (additionalCriteria != null)
                {
                    var where = new WhereClause { SearchCondition = additionalCriteria };

                    if (join.QualifiedJoinType == QualifiedJoinType.Inner)
                    {
                        // Move any additional criteria to a filter node on the join results
                        var result = (IDataExecutionPlanNodeInternal)joinNode;
                        result = ConvertInSubqueries(result, hints, where, context, outerSchema, outerReferences);
                        result = ConvertExistsSubqueries(result, hints, where, context, outerSchema, outerReferences);

                        if (where.SearchCondition != null)
                        {
                            var joinSchema = result.GetSchema(context);
                            where.SearchCondition.GetType(GetExpressionContext(joinSchema, context), out _);

                            result = new FilterNode
                            {
                                Source = result,
                                Filter = where.SearchCondition
                            };
                        }

                        if (joinNode is FoldableJoinNode foldable)
                            foldable.AdditionalJoinCriteria = null;
                        else if (joinNode is NestedLoopNode nestedLoop)
                            nestedLoop.JoinCondition = null;

                        return result;
                    }
                    else
                    {
                        // Convert any subqueries in the join criteria. There may be multiple subqueries, and each one could reference data from
                        // just the LHS, just the RHS, or both.
                        // Subqueries that only reference the LHS data should be added to that path. Any others should be added to the RHS path,
                        // including any required data from the LHS via an outer reference.
                        var inSubqueries = new InSubqueryVisitor();
                        where.Accept(inSubqueries);

                        var inSubqueryConversions = inSubqueries.InSubqueries
                            .Select(subquery =>
                            {
                                var cols = subquery.GetColumns();
                                var hasLhsCols = cols.Any(c => lhsSchema.ContainsColumn(c, out _));
                                var hasRhsCols = cols.Any(c => rhsSchema.ContainsColumn(c, out _));

                                return new
                                {
                                    Subquery = subquery,
                                    HasLHSCols = hasLhsCols,
                                    HasRHSCols = hasRhsCols
                                };
                            })
                            .ToList();

                        var existsSubqueries = new ExistsSubqueryVisitor();
                        where.Accept(existsSubqueries);

                        var existsSubqueryConversions = existsSubqueries.ExistsSubqueries
                            .Select(subquery =>
                            {
                                var cols = subquery.GetColumns();
                                var hasLhsCols = cols.Any(c => lhsSchema.ContainsColumn(c, out _));
                                var hasRhsCols = cols.Any(c => rhsSchema.ContainsColumn(c, out _));

                                return new
                                {
                                    Subquery = subquery,
                                    HasLHSCols = hasLhsCols,
                                    HasRHSCols = hasRhsCols
                                };
                            })
                            .ToList();

                        if (joinNode is FoldableJoinNode foldable && (inSubqueryConversions.Any(s => s.HasLHSCols && s.HasRHSCols) || existsSubqueryConversions.Any(s => s.HasLHSCols && s.HasRHSCols)))
                        {
                            // We're currently using a hash- or merge-join, but we need to apply a subquery that requires data from both
                            // sides of the join. Replace the join with a nested loop so we can apply the required outer references
                            joinNode = new NestedLoopNode
                            {
                                LeftSource = foldable.LeftSource,
                                RightSource = new TableSpoolNode { Source = foldable.RightSource },
                                JoinType = foldable.JoinType
                            };

                            // Need to add the original join condition back in
                            where.SearchCondition = new BooleanBinaryExpression
                            {
                                FirstExpression = new BooleanComparisonExpression
                                {
                                    FirstExpression = foldable.LeftAttribute,
                                    ComparisonType = BooleanComparisonType.Equals,
                                    SecondExpression = foldable.RightAttribute
                                },
                                BinaryExpressionType = BooleanBinaryExpressionType.And,
                                SecondExpression = where.SearchCondition
                            };
                        }

                        var nestedLoopJoinNode = joinNode as NestedLoopNode;

                        if (nestedLoopJoinNode != null && nestedLoopJoinNode.OuterReferences == null)
                            nestedLoopJoinNode.OuterReferences = new Dictionary<string, string>();

                        foreach (var subquery in inSubqueryConversions)
                        {
                            if (subquery.HasLHSCols && subquery.HasRHSCols)
                                CaptureOuterReferences(lhsSchema, joinNode.RightSource, subquery.Subquery, context, nestedLoopJoinNode.OuterReferences);

                            if (!subquery.HasRHSCols)
                                joinNode.LeftSource = ConvertInSubquery(joinNode.LeftSource, hints, where, subquery.Subquery, context, outerSchema, outerReferences);
                            else
                                joinNode.RightSource = ConvertInSubquery(joinNode.RightSource, hints, where, subquery.Subquery, context, outerSchema, outerReferences);
                        }

                        foreach (var subquery in existsSubqueryConversions)
                        {
                            if (subquery.HasLHSCols && subquery.HasRHSCols)
                                CaptureOuterReferences(lhsSchema, joinNode.RightSource, subquery.Subquery, context, nestedLoopJoinNode.OuterReferences);

                            if (!subquery.HasRHSCols)
                                joinNode.LeftSource = ConvertExistsSubquery(joinNode.LeftSource, hints, where, subquery.Subquery, context, outerSchema, outerReferences);
                            else
                                joinNode.RightSource = ConvertExistsSubquery(joinNode.RightSource, hints, where, subquery.Subquery, context, outerSchema, outerReferences);
                        }

                        // Validate the remaining join condition
                        if (where.SearchCondition != null)
                        {
                            var joinSchema = joinNode.GetSchema(context);
                            where.SearchCondition.GetType(GetExpressionContext(joinSchema, context), out _);
                        }

                        if (nestedLoopJoinNode != null)
                            nestedLoopJoinNode.JoinCondition = where.SearchCondition;
                        else
                            ((FoldableJoinNode)joinNode).AdditionalJoinCriteria = where.SearchCondition;
                    }
                }

                return joinNode;
            }

            if (reference is QueryDerivedTable queryDerivedTable)
            {
                if (queryDerivedTable.Columns.Count > 0)
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.NotSupported(queryDerivedTable, "query-derived table column list"));

                var select = ConvertSelectStatement(queryDerivedTable.QueryExpression, hints, outerSchema, outerReferences, context);
                var alias = new AliasNode(select, queryDerivedTable.Alias, context);

                return alias;
            }

            if (reference is InlineDerivedTable inlineDerivedTable)
                return ConvertInlineDerivedTable(inlineDerivedTable, hints, outerSchema, outerReferences, context);

            if (reference is UnqualifiedJoin unqualifiedJoin)
            {
                var lhs = ConvertTableReference(unqualifiedJoin.FirstTableReference, hints, query, outerSchema, outerReferences, context);
                IDataExecutionPlanNodeInternal rhs;
                Dictionary<string, string> lhsReferences;

                if (unqualifiedJoin.UnqualifiedJoinType == UnqualifiedJoinType.CrossJoin)
                {
                    rhs = ConvertTableReference(unqualifiedJoin.SecondTableReference, hints, query, outerSchema, outerReferences, context);
                    lhsReferences = null;
                }
                else
                {
                    // CROSS APPLY / OUTER APPLY - treat the second table as a correlated subquery
                    var lhsSchema = lhs.GetSchema(context);
                    lhsReferences = new Dictionary<string, string>();
                    var innerContext = context.CreateChildContext(null);
                    var subqueryPlan = ConvertTableReference(unqualifiedJoin.SecondTableReference, hints, query, lhsSchema, lhsReferences, innerContext);
                    rhs = subqueryPlan;

                    // If the subquery is uncorrelated, add a table spool to cache the results
                    // If it is correlated, add a spool where possible closer to the data source
                    if (lhsReferences.Count == 0)
                    {
                        var spool = new TableSpoolNode { Source = rhs, SpoolType = SpoolType.Lazy };
                        rhs = spool;
                    }
                    else if (UseMergeJoin(lhs, subqueryPlan, context, lhsReferences, null, null, false, true, out _, out var merge))
                    {
                        if (unqualifiedJoin.UnqualifiedJoinType == UnqualifiedJoinType.CrossApply)
                            merge.JoinType = QualifiedJoinType.Inner;

                        return merge;
                    }
                    else if (rhs is ISingleSourceExecutionPlanNode loopRightSourceSimple)
                    {
                        InsertCorrelatedSubquerySpool(loopRightSourceSimple, lhs, hints, context, lhsReferences.Values.ToArray());
                    }
                }

                // For cross joins there is no outer reference so the entire result can be spooled for reuse
                if (unqualifiedJoin.UnqualifiedJoinType == UnqualifiedJoinType.CrossJoin)
                    rhs = new TableSpoolNode { Source = rhs, SpoolType = SpoolType.Lazy };
                
                return new NestedLoopNode
                {
                    LeftSource = lhs,
                    RightSource = rhs,
                    JoinType = unqualifiedJoin.UnqualifiedJoinType == UnqualifiedJoinType.OuterApply ? QualifiedJoinType.LeftOuter : QualifiedJoinType.Inner,
                    OuterReferences = lhsReferences
                };
            }

            var tvf = reference as SchemaObjectFunctionTableReference;
            var gf = reference as GlobalFunctionTableReference;

            if (tvf != null || gf != null)
            {
                var parameters = tvf?.Parameters ?? gf.Parameters;

                // Capture any references to data from an outer query
                CaptureOuterReferences(outerSchema, null, reference, context, outerReferences);

                // Convert any scalar subqueries in the parameters to its own execution plan, and capture the references from those plans
                // as parameters to be passed to the function
                IDataExecutionPlanNodeInternal source = new ConstantScanNode { Values = { new Dictionary<string, ScalarExpression>() } };
                var computeScalar = new ComputeScalarNode { Source = source };

                foreach (var param in parameters.ToList())
                    ConvertScalarSubqueries(param, hints, ref source, computeScalar, context, reference);

                if (source is ConstantScanNode)
                    source = null;
                else if (computeScalar.Columns.Count > 0)
                    source = computeScalar;

                var scalarSubquerySchema = source?.GetSchema(context);
                var scalarSubqueryReferences = new Dictionary<string, string>();
                CaptureOuterReferences(scalarSubquerySchema, null, reference, context, scalarSubqueryReferences);

                IDataExecutionPlanNodeInternal execute;

                if (tvf != null)
                {
                    var dataSource = SelectDataSource(tvf.SchemaObject);

                    if (String.IsNullOrEmpty(tvf.SchemaObject.SchemaIdentifier?.Value) ||
                        tvf.SchemaObject.SchemaIdentifier.Value.Equals("dbo", StringComparison.OrdinalIgnoreCase))
                    {
                        execute = ExecuteMessageNode.FromMessage(tvf, dataSource, GetExpressionContext(null, context));
                    }
                    else if (tvf.SchemaObject.SchemaIdentifier.Value.Equals("sys", StringComparison.OrdinalIgnoreCase))
                    {
                        if (!Enum.TryParse<SystemFunction>(tvf.SchemaObject.BaseIdentifier.Value, true, out var systemFunction))
                            throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidObjectName(tvf.SchemaObject));

                        if (typeof(SystemFunction).GetField(systemFunction.ToString()).GetCustomAttribute<SystemObjectTypeAttribute>().Type != SystemObjectType.Function)
                            throw new NotSupportedQueryFragmentException(Sql4CdsError.NonFunctionCalledWithParameters(tvf.SchemaObject));

                        execute = new SystemFunctionNode
                        {
                            DataSource = dataSource.Name,
                            Alias = tvf.Alias?.Value,
                            SystemFunction = systemFunction
                        };
                    }
                    else
                    {
                        throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidObjectName(tvf.SchemaObject));
                    }
                }
                else
                {
                    if (gf.Name.Value.Equals("string_split", StringComparison.OrdinalIgnoreCase))
                        execute = new StringSplitNode(gf, context);
                    else
                        throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidObjectName(gf.Name));
                }

                if (source == null)
                    return execute;

                // If we've got any subquery parameters we need to use a loop to pass them to the function
                var loop = new NestedLoopNode
                {
                    LeftSource = source,
                    RightSource = execute,
                    JoinType = QualifiedJoinType.Inner,
                    OuterReferences = scalarSubqueryReferences,
                    OutputLeftSchema = false,
                };

                return loop;
            }

            if (reference is OpenJsonTableReference openJson)
            {
                // Capture any references to data from an outer query
                CaptureOuterReferences(outerSchema, null, openJson, context, outerReferences);

                // Convert any scalar subqueries in the parameters to its own execution plan, and capture the references from those plans
                // as parameters to be passed to the function
                IDataExecutionPlanNodeInternal source = new ConstantScanNode { Values = { new Dictionary<string, ScalarExpression>() } };
                var computeScalar = new ComputeScalarNode { Source = source };

                ConvertScalarSubqueries(openJson.Variable, hints, ref source, computeScalar, context, openJson);

                if (openJson.RowPattern != null)
                    ConvertScalarSubqueries(openJson.RowPattern, hints, ref source, computeScalar, context, openJson);

                if (source is ConstantScanNode)
                    source = null;
                else if (computeScalar.Columns.Count > 0)
                    source = computeScalar;

                var scalarSubquerySchema = source?.GetSchema(context);
                var scalarSubqueryReferences = new Dictionary<string, string>();
                CaptureOuterReferences(scalarSubquerySchema, null, openJson, context, scalarSubqueryReferences);

                var execute = new OpenJsonNode(openJson, context);

                if (source == null)
                    return execute;

                // If we've got any subquery parameters we need to use a loop to pass them to the function
                var loop = new NestedLoopNode
                {
                    LeftSource = source,
                    RightSource = execute,
                    JoinType = QualifiedJoinType.Inner,
                    OuterReferences = scalarSubqueryReferences,
                    OutputLeftSchema = false,
                };

                return loop;
            }

            throw new NotSupportedQueryFragmentException(Sql4CdsError.SyntaxError(reference)) { Suggestion = "Unhandled table reference" };
        }

        private HashSet<string> GetFixedValueColumnsFromWhereClause(TSqlFragment query, params INodeSchema[] schemas)
        {
            var columns = new HashSet<string>();

            if (query is QuerySpecification select && select.WhereClause != null)
                GetFixedValueColumnsFromWhereClause(columns, select.WhereClause.SearchCondition, schemas);

            return columns;
        }

        private void GetFixedValueColumnsFromWhereClause(HashSet<string> columns, BooleanExpression searchCondition, INodeSchema[] schemas)
        {
            if (searchCondition is BooleanComparisonExpression cmp &&
                cmp.ComparisonType == BooleanComparisonType.Equals)
            {
                var col = cmp.FirstExpression as ColumnReferenceExpression;
                var lit = cmp.SecondExpression as Literal;

                if (col == null && lit == null)
                {
                    col = cmp.SecondExpression as ColumnReferenceExpression;
                    lit = cmp.FirstExpression as Literal;
                }

                if (col != null && lit != null)
                {
                    foreach (var schema in schemas)
                    {
                        if (schema.ContainsColumn(col.GetColumnName(), out var colName))
                        {
                            columns.Add(colName);
                            break;
                        }
                    }
                }
            }

            if (searchCondition is BooleanBinaryExpression bin &&
                bin.BinaryExpressionType == BooleanBinaryExpressionType.And)
            {
                GetFixedValueColumnsFromWhereClause(columns, bin.FirstExpression, schemas);
                GetFixedValueColumnsFromWhereClause(columns, bin.SecondExpression, schemas);
            }
        }

        private IDataExecutionPlanNodeInternal ConvertInlineDerivedTable(InlineDerivedTable inlineDerivedTable, IList<OptimizerHint> hints, INodeSchema outerSchema, Dictionary<string, string> outerReferences, NodeCompilationContext context)
        {
            // Check all the rows have the same number of columns
            var expectedColumnCount = inlineDerivedTable.RowValues[0].ColumnValues.Count;
            var firstRowWithIncorrectNumberOfColumns = inlineDerivedTable.RowValues.FirstOrDefault(row => row.ColumnValues.Count != expectedColumnCount);
            if (firstRowWithIncorrectNumberOfColumns != null)
                throw new NotSupportedQueryFragmentException(Sql4CdsError.TableValueConstructorRequiresConsistentColumns(firstRowWithIncorrectNumberOfColumns));

            // Check all the rows have the expected number of values and column names are unique
            var columnNames = inlineDerivedTable.Columns.Select(col => col.Value).ToList();

            for (var i = 1; i < columnNames.Count; i++)
            {
                if (columnNames.Take(i).Any(prevCol => prevCol.Equals(columnNames[i], StringComparison.OrdinalIgnoreCase)))
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.DuplicateColumn(inlineDerivedTable.Alias, columnNames[i]));
            }

            if (expectedColumnCount > inlineDerivedTable.Columns.Count)
                throw new NotSupportedQueryFragmentException(Sql4CdsError.TableValueConstructorTooManyColumns(inlineDerivedTable.Alias));

            if (expectedColumnCount < inlineDerivedTable.Columns.Count)
                throw new NotSupportedQueryFragmentException(Sql4CdsError.TableValueConstructorTooFewColumns(inlineDerivedTable.Alias));

            var rows = inlineDerivedTable.RowValues.Select(row => ConvertSelectQuerySpec(CreateSelectRow(row, inlineDerivedTable.Columns), null, outerSchema, outerReferences, context));
            var concat = new ConcatenateNode();

            foreach (var row in rows)
            {
                if (concat.ColumnSet.Count == 0)
                {
                    for (var i = 0; i < inlineDerivedTable.Columns.Count; i++)
                        concat.ColumnSet.Add(new ConcatenateColumn { OutputColumn = inlineDerivedTable.Columns[i].Value.EscapeIdentifier() });
                }

                for (var i = 0; i < inlineDerivedTable.Columns.Count; i++)
                {
                    concat.ColumnSet[i].SourceColumns.Add(row.ColumnSet[i].SourceColumn);
                    concat.ColumnSet[i].SourceExpressions.Add(row.ColumnSet[i].SourceExpression);
                }

                concat.Sources.Add(row.Source);
            }

            var source = (IDataExecutionPlanNodeInternal) concat;

            if (concat.Sources.Count == 1)
            {
                // If there was only one source, no need to return the concatenate node but make sure all the column names line up
                source = concat.Sources[0];
                var sourceCompute = source as ComputeScalarNode;

                var rename = new ComputeScalarNode { Source = source };

                foreach (var col in concat.ColumnSet)
                {
                    if (col.SourceColumns[0] != col.OutputColumn)
                    {
                        if (sourceCompute != null && sourceCompute.Columns.TryGetValue(col.SourceColumns[0], out var colValue))
                        {
                            sourceCompute.Columns.Remove(col.SourceColumns[0]);
                            sourceCompute.Columns[col.OutputColumn] = colValue;
                        }
                        else
                        {
                            rename.Columns[col.OutputColumn] = col.SourceColumns[0].ToColumnReference();
                        }
                    }
                }

                if (rename.Columns.Count > 0)
                    source = rename;
            }

            // Make sure expected table name is used
            if (!String.IsNullOrEmpty(inlineDerivedTable.Alias?.Value))
            {
                var converted = new SelectNode { Source = source };

                foreach (var col in concat.ColumnSet)
                    converted.ColumnSet.Add(new SelectColumn { SourceColumn = col.OutputColumn, OutputColumn = col.OutputColumn, SourceExpression = col.OutputColumn.ToColumnReference() });

                source = new AliasNode(converted, inlineDerivedTable.Alias, context);
            }

            return source;
        }

        private QuerySpecification CreateSelectRow(RowValue row, IList<Identifier> columns)
        {
            var querySpec = new QuerySpecification();
            
            for (var i = 0; i < columns.Count; i++)
            {
                querySpec.SelectElements.Add(new SelectScalarExpression
                {
                    Expression = row.ColumnValues[i],
                    ColumnName = new IdentifierOrValueExpression { Identifier = columns[i] }
                });
            }

            return querySpec;
        }

        private ExpressionCompilationContext GetExpressionContext(INodeSchema schema, NodeCompilationContext context, INodeSchema nonAggregateSchema = null)
        {
            return new ExpressionCompilationContext(context, schema, nonAggregateSchema);
        }
    }
}
