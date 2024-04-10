using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Globalization;
using System.IO;
using System.Linq;
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

        public ExecutionPlanBuilder(IEnumerable<DataSource> dataSources, IQueryExecutionOptions options)
        {
            DataSources = dataSources.ToDictionary(ds => ds.Name, StringComparer.OrdinalIgnoreCase);
            Options = options;

            if (!DataSources.ContainsKey(Options.PrimaryDataSource))
                throw new ArgumentOutOfRangeException(nameof(options), "Primary data source " + options.PrimaryDataSource + " not found");

            EstimatedPlanOnly = true;
        }

        /// <summary>
        /// The connections that will be used by this conversion
        /// </summary>
        public IDictionary<string, DataSource> DataSources { get; }

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

        private DataSource PrimaryDataSource => DataSources[Options.PrimaryDataSource];

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
            var parameterTypes = new Dictionary<string, DataTypeReference>(StringComparer.OrdinalIgnoreCase);
            _staticContext = new ExpressionCompilationContext(DataSources, Options, parameterTypes, null, null);
            _nodeContext = new NodeCompilationContext(DataSources, Options, parameterTypes, Log);

            if (parameters != null)
            {
                foreach (var param in parameters)
                    parameterTypes[param.Key] = param.Value;
            }

            // Add in standard global variables
            parameterTypes["@@IDENTITY"] = DataTypeHelpers.EntityReference;
            parameterTypes["@@ROWCOUNT"] = DataTypeHelpers.Int;
            parameterTypes["@@SERVERNAME"] = DataTypeHelpers.NVarChar(100, DataSources[Options.PrimaryDataSource].DefaultCollation, CollationLabel.CoercibleDefault);
            parameterTypes["@@VERSION"] = DataTypeHelpers.NVarChar(Int32.MaxValue, DataSources[Options.PrimaryDataSource].DefaultCollation, CollationLabel.CoercibleDefault);
            parameterTypes["@@ERROR"] = DataTypeHelpers.Int;

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

            if (hintValidator.TdsCompatible && TDSEndpoint.CanUseTDSEndpoint(Options, DataSources[Options.PrimaryDataSource].Connection))
            {
                using (var con = DataSources[Options.PrimaryDataSource].Connection == null ? null : TDSEndpoint.Connect(DataSources[Options.PrimaryDataSource].Connection))
                {
                    var tdsEndpointCompatibilityVisitor = new TDSEndpointCompatibilityVisitor(con, DataSources[Options.PrimaryDataSource].Metadata);
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
            var optimizer = new ExecutionPlanOptimizer(DataSources, Options, parameterTypes, !EstimatedPlanOnly, _nodeContext.Log);

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
            {
                hints = stmtWithCtes.OptimizerHints;

                if (stmtWithCtes.WithCtesAndXmlNamespaces != null)
                {
                    foreach (var cte in stmtWithCtes.WithCtesAndXmlNamespaces.CommonTableExpressions)
                    {
                        if (_cteSubplans.ContainsKey(cte.ExpressionName.Value))
                            throw new NotSupportedQueryFragmentException(Sql4CdsError.CteDuplicateName(cte.ExpressionName));

                        var cteValidator = new CteValidatorVisitor();
                        cte.Accept(cteValidator);

                        // Start by converting the anchor query to a subquery
                        var plan = ConvertSelectStatement(cteValidator.AnchorQuery, hints, null, null, _nodeContext);

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
            }

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
            }

            return output.ToArray();
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
            var childContext = new NodeCompilationContext(_nodeContext, outerReferences.ToDictionary(kvp => kvp.Value, kvp => anchorSchema.Schema[cteValidator.Name.EscapeIdentifier() + "." + kvp.Key.EscapeIdentifier()].Type, StringComparer.OrdinalIgnoreCase));
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
            if (!(msgType is SqlDataTypeReference msgSqlType) || !msgSqlType.SqlDataTypeOption.IsStringType())
                throw new NotSupportedQueryFragmentException(Sql4CdsError.NotSupported(raiserror.FirstParameter, "predefined error number")) { Suggestion = "Define a message string instead of a number" };

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
                    ErrorMessage = raiserror.FirstParameter,
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
                throw new NotSupportedQueryFragmentException(Sql4CdsError.Create(1046, print.Expression));

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
                node.Source = source;
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

            IExecutionPlanNodeInternal source;

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
                source = select;
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

            columns = table.Columns.Select(col => col.Value).ToArray();
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
                columns = null;
                return sql;
            }

            throw new NotSupportedQueryFragmentException(Sql4CdsError.UnsupportedStatement(selectSource, "INSERT"));
        }

        private DataSource SelectDataSource(SchemaObjectName schemaObject)
        {
            var databaseName = schemaObject.DatabaseIdentifier?.Value ?? Options.PrimaryDataSource;
            
            if (!DataSources.TryGetValue(databaseName, out var dataSource))
                throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidObjectName(schemaObject)) { Suggestion = $"Available database names:\r\n* {String.Join("\r\n* ", DataSources.Keys.OrderBy(k => k))}" };

            return dataSource;
        }

        private InsertNode ConvertInsertSpecification(NamedTableReference target, IList<ColumnReferenceExpression> targetColumns, IExecutionPlanNodeInternal source, string[] sourceColumns, IList<OptimizerHint> queryHints, InsertStatement insertStatement)
        {
            var dataSource = SelectDataSource(target.SchemaObject);

            var node = new InsertNode
            {
                DataSource = dataSource.Name,
                LogicalName = target.SchemaObject.BaseIdentifier.Value,
                Source = source
            };

            ValidateDMLSchema(target);

            // Validate the entity name
            EntityMetadata metadata;

            try
            {
                metadata = dataSource.Metadata[node.LogicalName];
            }
            catch (FaultException ex)
            {
                throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidObjectName(target.SchemaObject), ex);
            }

            var attributes = metadata.Attributes.ToDictionary(attr => attr.LogicalName, StringComparer.OrdinalIgnoreCase);
            var attributeNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var virtualTypeAttributes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var schema = sourceColumns == null ? null : ((IDataExecutionPlanNodeInternal)source).GetSchema(_nodeContext);

            // Check all target columns are valid for create
            foreach (var col in targetColumns)
            {
                var colName = col.MultiPartIdentifier.Identifiers.Last().Value.ToLowerInvariant();

                // Could be a virtual ___type attribute where the "real" virtual attribute uses a different name, e.g.
                // entityid in listmember has an associated entitytypecode attribute
                if (colName.EndsWith("type", StringComparison.OrdinalIgnoreCase) &&
                    attributes.TryGetValue(colName.Substring(0, colName.Length - 4), out var attr) &&
                    attr is LookupAttributeMetadata lookupAttr &&
                    lookupAttr.Targets.Length > 1)
                {
                    if (!virtualTypeAttributes.Add(colName))
                        throw new NotSupportedQueryFragmentException(Sql4CdsError.DuplicateInsertUpdateColumn(col));

                    continue;
                }

                if (!attributes.TryGetValue(colName, out attr))
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidColumnName(col));

                if (!attributeNames.Add(colName))
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.DuplicateInsertUpdateColumn(col));

                if (metadata.LogicalName == "listmember")
                {
                    if (attr.LogicalName != "listid" && attr.LogicalName != "entityid")
                        throw new NotSupportedQueryFragmentException(Sql4CdsError.ReadOnlyColumn(col)) { Suggestion = "Only the listid and entityid columns can be used when inserting values into the listmember table" };
                }
                else if (metadata.IsIntersect == true)
                {
                    var relationship = metadata.ManyToManyRelationships.Single();

                    if (attr.LogicalName != relationship.Entity1IntersectAttribute && attr.LogicalName != relationship.Entity2IntersectAttribute)
                        throw new NotSupportedQueryFragmentException(Sql4CdsError.ReadOnlyColumn(col)) { Suggestion = $"Only the {relationship.Entity1IntersectAttribute} and {relationship.Entity2IntersectAttribute} columns can be used when inserting values into the {metadata.LogicalName} table" };
                }
                else
                {
                    if (attr.IsValidForCreate == false)
                        throw new NotSupportedQueryFragmentException(Sql4CdsError.ReadOnlyColumn(col)) { Suggestion = "Column is not valid for INSERT" };
                }
            }

            // Special case: inserting into listmember requires listid and entityid
            if (metadata.LogicalName == "listmember")
            {
                if (!attributeNames.Contains("listid"))
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.NotNullInsert(new Identifier { Value = "listid" }, new Identifier { Value = metadata.LogicalName }, "Insert", target)) { Suggestion = $"Inserting values into the {metadata.LogicalName} table requires the listid column to be set" };
                if (!attributeNames.Contains("entityid"))
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.NotNullInsert(new Identifier { Value = "entityid" }, new Identifier { Value = metadata.LogicalName }, "Insert", target)) { Suggestion = $"Inserting values into the {metadata.LogicalName} table requires the entityid column to be set" };
            }
            else if (metadata.IsIntersect == true)
            {
                var relationship = metadata.ManyToManyRelationships.Single();
                if (!attributeNames.Contains(relationship.Entity1IntersectAttribute))
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.NotNullInsert(new Identifier { Value = relationship.Entity1IntersectAttribute }, new Identifier { Value = metadata.LogicalName }, "Insert", target)) { Suggestion = $"Inserting values into the {metadata.LogicalName} table requires the {relationship.Entity1IntersectAttribute} column to be set" };
                if (!attributeNames.Contains(relationship.Entity2IntersectAttribute))
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.NotNullInsert(new Identifier { Value = relationship.Entity2IntersectAttribute }, new Identifier { Value = metadata.LogicalName }, "Insert", target)) { Suggestion = $"Inserting values into the {metadata.LogicalName} table requires the {relationship.Entity2IntersectAttribute} column to be set" };
            }

            if (sourceColumns == null)
            {
                // Source is TDS endpoint so can't validate the columns, assume they are correct
                for (var i = 0; i < targetColumns.Count; i++)
                    node.ColumnMappings[targetColumns[i].MultiPartIdentifier.Identifiers.Last().Value.ToLowerInvariant()] = i.ToString();
            }
            else
            {
                if (targetColumns.Count != sourceColumns.Length)
                {
                    if (targetColumns.Count > sourceColumns.Length)
                        throw new NotSupportedQueryFragmentException(Sql4CdsError.InsertTooManyColumns(insertStatement));
                    else
                        throw new NotSupportedQueryFragmentException(Sql4CdsError.InsertTooFewColumns(insertStatement));
                }

                for (var i = 0; i < targetColumns.Count; i++)
                {
                    string targetName;
                    DataTypeReference targetType;

                    var colName = targetColumns[i].MultiPartIdentifier.Identifiers.Last().Value.ToLowerInvariant();
                    if (virtualTypeAttributes.Contains(colName))
                    {
                        targetName = colName;
                        targetType = DataTypeHelpers.NVarChar(MetadataExtensions.EntityLogicalNameMaxLength, dataSource.DefaultCollation, CollationLabel.CoercibleDefault);
                    }
                    else
                    {
                        var attr = attributes[colName];
                        targetName = attr.LogicalName;
                        targetType = attr.GetAttributeSqlType(dataSource, true);

                        // If we're inserting into a lookup field, the field type will be a SqlEntityReference. Change this to
                        // a SqlGuid so we can accept any guid values, including from TDS endpoint where SqlEntityReference
                        // values will not be available
                        if (targetType.IsSameAs(DataTypeHelpers.EntityReference))
                            targetType = DataTypeHelpers.UniqueIdentifier;
                    }

                    if (!schema.ContainsColumn(sourceColumns[i], out var sourceColumn))
                        throw new NotSupportedQueryFragmentException("Invalid source column");

                    var sourceType = schema.Schema[sourceColumn].Type;

                    if (!SqlTypeConverter.CanChangeTypeImplicit(sourceType, targetType))
                        throw new NotSupportedQueryFragmentException(Sql4CdsError.TypeClash(targetColumns[i], sourceType, targetType));

                    node.ColumnMappings[targetName] = sourceColumn;
                }
            }

            // If any of the insert columns are a polymorphic lookup field, make sure we've got a value for the associated type field too
            foreach (var col in targetColumns)
            {
                var targetAttrName = col.MultiPartIdentifier.Identifiers.Last().Value.ToLowerInvariant();

                if (attributeNames.Contains(targetAttrName))
                {
                    var targetLookupAttribute = attributes[targetAttrName] as LookupAttributeMetadata;

                    if (targetLookupAttribute == null)
                        continue;

                    if (targetLookupAttribute.Targets.Length > 1 &&
                        !virtualTypeAttributes.Contains(targetAttrName + "type") &&
                        targetLookupAttribute.AttributeType != AttributeTypeCode.PartyList &&
                        (schema == null || (node.ColumnMappings[targetAttrName].ToColumnReference().GetType(GetExpressionContext(schema, _nodeContext), out var lookupType) != typeof(SqlEntityReference) && lookupType != DataTypeHelpers.ImplicitIntForNullLiteral)))
                    {
                        // Special case: not required for listmember.entityid
                        if (metadata.LogicalName == "listmember" && targetLookupAttribute.LogicalName == "entityid")
                            continue;

                        throw new NotSupportedQueryFragmentException(Sql4CdsError.ReadOnlyColumn(col))
                        {
                            Suggestion = $"Inserting values into a polymorphic lookup field requires setting the associated type column as well\r\nAdd a value for the {targetLookupAttribute.LogicalName}type column and set it to one of the following values:\r\n{String.Join("\r\n", targetLookupAttribute.Targets.Select(t => $"* {t}"))}"
                        };
                    }
                }
                else if (virtualTypeAttributes.Contains(targetAttrName))
                {
                    var idAttrName = targetAttrName.Substring(0, targetAttrName.Length - 4);

                    if (!attributeNames.Contains(idAttrName))
                    {
                        throw new NotSupportedQueryFragmentException(Sql4CdsError.ReadOnlyColumn(col))
                        {
                            Suggestion = $"Inserting values into a polymorphic type field requires setting the associated ID column as well\r\nAdd a value for the {idAttrName} column"
                        };
                    }
                }
            }

            return node;
        }

        private void ValidateDMLSchema(NamedTableReference target)
        {
            if (String.IsNullOrEmpty(target.SchemaObject.SchemaIdentifier?.Value))
                return;

            if (target.SchemaObject.SchemaIdentifier.Value.Equals("dbo", StringComparison.OrdinalIgnoreCase))
                return;

            if (target.SchemaObject.SchemaIdentifier.Value.Equals("archive", StringComparison.OrdinalIgnoreCase))
                throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidObjectName(target.SchemaObject)) { Suggestion = "Archive tables are read-only" };

            if (target.SchemaObject.SchemaIdentifier.Value.Equals("metadata", StringComparison.OrdinalIgnoreCase))
                throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidObjectName(target.SchemaObject)) { Suggestion = "Metadata tables are read-only" };

            throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidObjectName(target.SchemaObject)) { Suggestion = "All data tables are in the 'dbo' schema" };
        }

        private DeleteNode ConvertDeleteStatement(DeleteStatement delete)
        {
            if (delete.WithCtesAndXmlNamespaces != null)
                throw new NotSupportedQueryFragmentException(Sql4CdsError.NotSupported(delete.WithCtesAndXmlNamespaces, "WITH"));

            return ConvertDeleteStatement(delete.DeleteSpecification, delete.OptimizerHints);
        }

        private DeleteNode ConvertDeleteStatement(DeleteSpecification delete, IList<OptimizerHint> hints)
        {
            if (delete.OutputClause != null)
                throw new NotSupportedQueryFragmentException(Sql4CdsError.NotSupported(delete.OutputClause, "OUTPUT"));

            if (delete.OutputIntoClause != null)
                throw new NotSupportedQueryFragmentException(Sql4CdsError.NotSupported(delete.OutputIntoClause, "OUTPUT INTO"));

            if (!(delete.Target is NamedTableReference target))
                throw new NotSupportedQueryFragmentException(Sql4CdsError.NotSupported(delete.Target, "non-table DELETE target"));

            if (delete.WhereClause == null && Options.BlockDeleteWithoutWhere)
            {
                throw new NotSupportedQueryFragmentException("DELETE without WHERE is blocked by your settings", delete)
                {
                    Suggestion = "Add a WHERE clause to limit the records that will be deleted, or disable the \"Prevent DELETE without WHERE\" option in the settings window"
                };
            }

            ValidateDMLSchema(target);

            // Create the SELECT statement that generates the required information
            var queryExpression = new QuerySpecification
            {
                FromClause = delete.FromClause ?? new FromClause { TableReferences = { target } },
                WhereClause = delete.WhereClause,
                UniqueRowFilter = UniqueRowFilter.Distinct,
                TopRowFilter = delete.TopRowFilter
            };

            var deleteTarget = new UpdateTargetVisitor(target.SchemaObject, Options.PrimaryDataSource);
            queryExpression.FromClause.Accept(deleteTarget);

            if (String.IsNullOrEmpty(deleteTarget.TargetEntityName))
                throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidObjectName(target.SchemaObject)) { Suggestion = $"Target table '{target.ToSql()}' not found in FROM clause" };

            if (deleteTarget.Ambiguous)
                throw new NotSupportedQueryFragmentException(Sql4CdsError.AmbiguousTable(target));

            if (!DataSources.TryGetValue(deleteTarget.TargetDataSource, out var dataSource))
                throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidObjectName(target.SchemaObject)) { Suggestion = $"Available database names:\r\n* {String.Join("\r\n*", DataSources.Keys.OrderBy(k => k))}" };

            var targetAlias = deleteTarget.TargetAliasName ?? deleteTarget.TargetEntityName;
            var targetLogicalName = deleteTarget.TargetEntityName;

            EntityMetadata targetMetadata;

            try
            {
                targetMetadata = dataSource.Metadata[targetLogicalName];
            }
            catch (FaultException ex)
            {
                throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidObjectName(deleteTarget.Target.SchemaObject), ex);
            }

            var primaryKey = targetMetadata.PrimaryIdAttribute;
            string secondaryKey = null;

            if (targetMetadata.LogicalName == "listmember")
            {
                primaryKey = "listid";
                secondaryKey = "entityid";
            }
            else if (targetMetadata.IsIntersect == true)
            {
                var relationship = targetMetadata.ManyToManyRelationships.Single();
                primaryKey = relationship.Entity1IntersectAttribute;
                secondaryKey = relationship.Entity2IntersectAttribute;
            }
            else if (targetMetadata.DataProviderId == DataProviders.ElasticDataProvider)
            {
                // Elastic tables need the partitionid as part of the primary key
                secondaryKey = "partitionid";
            }
            
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

            if (secondaryKey != null)
            {
                queryExpression.SelectElements.Add(new SelectScalarExpression
                {
                    Expression = new ColumnReferenceExpression
                    {
                        MultiPartIdentifier = new MultiPartIdentifier
                        {
                            Identifiers =
                        {
                            new Identifier { Value = targetAlias },
                            new Identifier { Value = secondaryKey }
                        }
                        }
                    },
                    ColumnName = new IdentifierOrValueExpression
                    {
                        Identifier = new Identifier { Value = secondaryKey }
                    }
                });
            }
            
            var selectStatement = new SelectStatement { QueryExpression = queryExpression };
            CopyDmlHintsToSelectStatement(hints, selectStatement);

            var source = ConvertSelectStatement(selectStatement);

            // Add DELETE
            var deleteNode = new DeleteNode
            {
                LogicalName = targetMetadata.LogicalName,
                DataSource = dataSource.Name,
            };

            if (source is SelectNode select)
            {
                deleteNode.Source = select.Source;
                deleteNode.PrimaryIdSource = $"{targetAlias}.{primaryKey}";

                if (secondaryKey != null)
                    deleteNode.SecondaryIdSource = $"{targetAlias}.{secondaryKey}";
            }
            else
            {
                deleteNode.Source = source;
                deleteNode.PrimaryIdSource = primaryKey;
                deleteNode.SecondaryIdSource = secondaryKey;
            }

            return deleteNode;
        }

        private UpdateNode ConvertUpdateStatement(UpdateStatement update)
        {
            if (update.WithCtesAndXmlNamespaces != null)
                throw new NotSupportedQueryFragmentException(Sql4CdsError.NotSupported(update.WithCtesAndXmlNamespaces, "WITH"));

            return ConvertUpdateStatement(update.UpdateSpecification, update.OptimizerHints);
        }

        private UpdateNode ConvertUpdateStatement(UpdateSpecification update, IList<OptimizerHint> hints)
        {
            if (update.OutputClause != null)
                throw new NotSupportedQueryFragmentException(Sql4CdsError.NotSupported(update.OutputClause, "OUTPUT"));

            if (update.OutputIntoClause != null)
                throw new NotSupportedQueryFragmentException(Sql4CdsError.NotSupported(update.OutputIntoClause, "OUTPUT INTO"));

            if (!(update.Target is NamedTableReference target))
                throw new NotSupportedQueryFragmentException(Sql4CdsError.NotSupported(update.Target, "non-table UPDATE target"));

            if (update.WhereClause == null && Options.BlockUpdateWithoutWhere)
            {
                throw new NotSupportedQueryFragmentException("UPDATE without WHERE is blocked by your settings", update)
                {
                    Suggestion = "Add a WHERE clause to limit the records that will be affected by the update, or disable the \"Prevent UPDATE without WHERE\" option in the settings window"
                };
            }

            ValidateDMLSchema(target);

            // Create the SELECT statement that generates the required information
            var queryExpression = new QuerySpecification
            {
                FromClause = update.FromClause ?? new FromClause { TableReferences = { target } },
                WhereClause = update.WhereClause,
                UniqueRowFilter = UniqueRowFilter.Distinct,
                TopRowFilter = update.TopRowFilter
            };

            var updateTarget = new UpdateTargetVisitor(target.SchemaObject, Options.PrimaryDataSource);
            queryExpression.FromClause.Accept(updateTarget);

            if (String.IsNullOrEmpty(updateTarget.TargetEntityName))
                throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidObjectName(target.SchemaObject)) { Suggestion = $"Target table '{target.ToSql()}' not found in FROM clause" };

            if (updateTarget.Ambiguous)
                throw new NotSupportedQueryFragmentException(Sql4CdsError.AmbiguousTable(target));

            if (!DataSources.TryGetValue(updateTarget.TargetDataSource, out var dataSource))
                throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidObjectName(target.SchemaObject)) { Suggestion = $"Available database names:\r\n* {String.Join("\r\n*", DataSources.Keys.OrderBy(k => k))}" };

            var targetAlias = updateTarget.TargetAliasName ?? updateTarget.TargetEntityName;
            var targetLogicalName = updateTarget.TargetEntityName;

            EntityMetadata targetMetadata;

            try
            {
                targetMetadata = dataSource.Metadata[targetLogicalName];
            }
            catch (FaultException ex)
            {
                throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidObjectName(updateTarget.Target.SchemaObject), ex);
            }

            if (targetMetadata.IsIntersect != true)
            {
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

                if (targetMetadata.DataProviderId == DataProviders.ElasticDataProvider)
                {
                    // partitionid is required as part of the primary key for Elastic tables - included it as any
                    // other column in the update statement. Check first that the column isn't already being updated -
                    // the metadata shows it's valid for update but actually has no effect and is documented as not updateable
                    // https://learn.microsoft.com/en-us/power-apps/developer/data-platform/use-elastic-tables?tabs=sdk#update-a-record-in-an-elastic-table
                    var existingSet = update.SetClauses.OfType<AssignmentSetClause>().FirstOrDefault(set => set.Column.MultiPartIdentifier.Identifiers.Last().Value.Equals("partitionid", StringComparison.OrdinalIgnoreCase));

                    if (existingSet != null)
                        throw new NotSupportedQueryFragmentException(Sql4CdsError.ReadOnlyColumn(existingSet.Column)) { Suggestion = "The column \"partitionid\" cannot be modified" };

                    update.SetClauses.Add(new AssignmentSetClause
                    {
                        Column = new ColumnReferenceExpression
                        {
                            MultiPartIdentifier = new MultiPartIdentifier
                            {
                                Identifiers =
                                {
                                    new Identifier { Value = targetAlias },
                                    new Identifier { Value = "partitionid" }
                                }
                            }
                        },
                        NewValue = new ColumnReferenceExpression
                        {
                            MultiPartIdentifier = new MultiPartIdentifier
                            {
                                Identifiers =
                                {
                                    new Identifier { Value = targetAlias },
                                    new Identifier { Value = "partitionid" }
                                }
                            }
                        }
                    });
                }
            }

            var attributes = targetMetadata.Attributes.ToDictionary(attr => attr.LogicalName, StringComparer.OrdinalIgnoreCase);
            var attributeNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var virtualTypeAttributes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var existingAttributes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var useStateTransitions = !hints.OfType<UseHintList>().Any(h => h.Hints.Any(s => s.Value.Equals("DISABLE_STATE_TRANSITIONS", StringComparison.OrdinalIgnoreCase)));
            var stateTransitions = useStateTransitions ? StateTransitionLoader.LoadStateTransitions(targetMetadata) : null;
            var manyToManyRelationship = targetMetadata.IsIntersect == true && targetMetadata.LogicalName != "listmember" ? targetMetadata.ManyToManyRelationships.Single() : null;

            foreach (var set in update.SetClauses)
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

                // Could be a virtual ___type attribute where the "real" virtual attribute uses a different name, e.g.
                // entityid in listmember has an associated entitytypecode attribute
                if (targetAttrName.EndsWith("type", StringComparison.OrdinalIgnoreCase) &&
                    attributes.TryGetValue(targetAttrName.Substring(0, targetAttrName.Length - 4), out var attr) &&
                    attr is LookupAttributeMetadata lookupAttr &&
                    lookupAttr.Targets.Length > 1)
                {
                    if (!virtualTypeAttributes.Add(targetAttrName))
                        throw new NotSupportedQueryFragmentException(Sql4CdsError.DuplicateInsertUpdateColumn(assignment.Column));
                }
                else
                {
                    if (!attributes.TryGetValue(targetAttrName, out attr))
                        throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidColumnName(assignment.Column));

                    if (!attributeNames.Add(attr.LogicalName))
                        throw new NotSupportedQueryFragmentException(Sql4CdsError.DuplicateInsertUpdateColumn(assignment.Column));

                    if (manyToManyRelationship != null)
                    {
                        if (attr.LogicalName != manyToManyRelationship.Entity1IntersectAttribute & attr.LogicalName != manyToManyRelationship.Entity2IntersectAttribute)
                            throw new NotSupportedQueryFragmentException(Sql4CdsError.ReadOnlyColumn(assignment.Column)) { Suggestion = $"Only the {manyToManyRelationship.Entity1IntersectAttribute} and {manyToManyRelationship.Entity2IntersectAttribute} columns can be used when updating values in the {targetMetadata.LogicalName} table" };
                    }
                    else if (targetMetadata.LogicalName == "listmember")
                    {
                        if (attr.LogicalName != "listid" && attr.LogicalName != "entityid")
                            throw new NotSupportedQueryFragmentException(Sql4CdsError.ReadOnlyColumn(assignment.Column)) { Suggestion = "Only the listid and entityid columns can be used when updating values in the listmember table" };
                    }
                    else
                    {
                        if (attr.IsValidForUpdate == false)
                            throw new NotSupportedQueryFragmentException(Sql4CdsError.ReadOnlyColumn(assignment.Column)) { Suggestion = $"The column \"{targetAttrName}\" cannot be modified" };
                    }

                    targetAttrName = attr.LogicalName;
                }

                queryExpression.SelectElements.Add(new SelectScalarExpression { Expression = assignment.NewValue, ColumnName = new IdentifierOrValueExpression { Identifier = new Identifier { Value = "new_" + targetAttrName } } });

                // If we're changing the status of a record where only specific state transitions are allowed, we need to include the
                // current statecode and statuscode values
                if ((targetAttrName == "statecode" || targetAttrName == "statuscode") && stateTransitions != null)
                {
                    existingAttributes.Add("statecode");
                    existingAttributes.Add("statuscode");
                }
            }

            // quote/salesorder/invoice have custom logic for updating closed records, so load in the existing statecode & statuscode fields too
            if (useStateTransitions && (targetLogicalName == "quote" || targetLogicalName == "salesorder" || targetLogicalName == "invoice"))
            {
                existingAttributes.Add("statecode");
                existingAttributes.Add("statuscode");
            }

            // many-to-many intersect entities need both the existing IDs so we can remove the existing association and add the new one
            if (manyToManyRelationship != null)
            {
                existingAttributes.Add(manyToManyRelationship.Entity1IntersectAttribute);
                existingAttributes.Add(manyToManyRelationship.Entity2IntersectAttribute);
            }
            else if (targetLogicalName == "listmember")
            {
                existingAttributes.Add("listid");
                existingAttributes.Add("entityid");
            }

            foreach (var existingAttribute in existingAttributes)
            {
                queryExpression.SelectElements.Add(new SelectScalarExpression
                {
                    Expression = new ColumnReferenceExpression
                    {
                        MultiPartIdentifier = new MultiPartIdentifier
                        {
                            Identifiers =
                            {
                                new Identifier { Value = targetAlias },
                                new Identifier { Value = existingAttribute }
                            }
                        }
                    },
                    ColumnName = new IdentifierOrValueExpression
                    {
                        Identifier = new Identifier { Value = "existing_" + existingAttribute }
                    }
                });
            }

            var selectStatement = new SelectStatement { QueryExpression = queryExpression };
            CopyDmlHintsToSelectStatement(hints, selectStatement);

            var source = ConvertSelectStatement(selectStatement);

            // Add UPDATE
            var updateNode = ConvertSetClause(update.SetClauses, existingAttributes, dataSource, source, targetLogicalName, targetAlias, attributeNames, virtualTypeAttributes, hints);
            updateNode.StateTransitions = stateTransitions;

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

        private UpdateNode ConvertSetClause(IList<SetClause> setClauses, HashSet<string> existingAttributes, DataSource dataSource, IExecutionPlanNodeInternal node, string targetLogicalName, string targetAlias, HashSet<string> attributeNames, HashSet<string> virtualTypeAttributes, IList<OptimizerHint> queryHints)
        {
            var targetMetadata = dataSource.Metadata[targetLogicalName];
            var attributes = targetMetadata.Attributes.ToDictionary(attr => attr.LogicalName, StringComparer.OrdinalIgnoreCase);
            var sourceTypes = new Dictionary<string, DataTypeReference>();

            var update = new UpdateNode
            {
                LogicalName = targetMetadata.LogicalName,
                DataSource = dataSource.Name,
            };

            if (node is SelectNode select)
            {
                update.Source = select.Source;
                update.PrimaryIdSource = $"{targetAlias}.{targetMetadata.PrimaryIdAttribute}";

                var schema = select.Source.GetSchema(_nodeContext);
                var expressionContext = GetExpressionContext(schema, _nodeContext);

                foreach (var assignment in setClauses.Cast<AssignmentSetClause>())
                {
                    // Validate the type conversion
                    var targetAttrName = assignment.Column.MultiPartIdentifier.Identifiers.Last().Value;
                    DataTypeReference targetType;

                    // Could be a virtual ___type attribute where the "real" virtual attribute uses a different name, e.g.
                    // entityid in listmember has an associated entitytypecode attribute
                    if (virtualTypeAttributes.Contains(targetAttrName))
                    {
                        targetType = DataTypeHelpers.NVarChar(MetadataExtensions.EntityLogicalNameMaxLength, dataSource.DefaultCollation, CollationLabel.CoercibleDefault);

                        var targetAttribute = attributes[targetAttrName.Substring(0, targetAttrName.Length - 4)];
                        targetAttrName = targetAttribute.LogicalName + targetAttrName.Substring(targetAttrName.Length - 4, 4).ToLower();
                    }
                    else
                    {
                        var targetAttribute = attributes[targetAttrName];
                        targetType = targetAttribute.GetAttributeSqlType(dataSource, true);
                        targetAttrName = targetAttribute.LogicalName;

                        // If we're updating a lookup field, the field type will be a SqlEntityReference. Change this to
                        // a SqlGuid so we can accept any guid values, including from TDS endpoint where SqlEntityReference
                        // values will not be available
                        if (targetType.IsSameAs(DataTypeHelpers.EntityReference))
                            targetType = DataTypeHelpers.UniqueIdentifier;
                    }

                    var sourceColName = select.ColumnSet.Single(col => col.OutputColumn == "new_" + targetAttrName.ToLower()).SourceColumn;
                    var sourceCol = sourceColName.ToColumnReference();
                    sourceCol.GetType(expressionContext, out var sourceType);

                    if (!SqlTypeConverter.CanChangeTypeImplicit(sourceType, targetType))
                        throw new NotSupportedQueryFragmentException(Sql4CdsError.TypeClash(assignment, sourceType, targetType));

                    if (update.ColumnMappings.ContainsKey(targetAttrName))
                        throw new NotSupportedQueryFragmentException(Sql4CdsError.DuplicateInsertUpdateColumn(assignment.Column));

                    sourceTypes[targetAttrName] = sourceType;

                    // Normalize the column name
                    schema.ContainsColumn(sourceColName, out sourceColName);
                    update.ColumnMappings[targetAttrName] = new UpdateMapping { NewValueColumn = sourceColName };
                }

                foreach (var existingAttribute in existingAttributes)
                {
                    var sourceColName = select.ColumnSet.Single(col => col.OutputColumn == "existing_" + existingAttribute).SourceColumn;
                    schema.ContainsColumn(sourceColName, out sourceColName);

                    if (!update.ColumnMappings.TryGetValue(existingAttribute, out var mapping))
                    {
                        mapping = new UpdateMapping();
                        update.ColumnMappings[existingAttribute] = mapping;
                    }

                    mapping.OldValueColumn = sourceColName;
                }
            }
            else
            {
                update.Source = node;
                update.PrimaryIdSource = targetMetadata.PrimaryIdAttribute;

                foreach (var assignment in setClauses.Cast<AssignmentSetClause>())
                {
                    var targetAttrName = assignment.Column.MultiPartIdentifier.Identifiers.Last().Value;
                    update.ColumnMappings[targetAttrName] = new UpdateMapping { NewValueColumn = "new_" + targetAttrName };
                }

                foreach (var existingAttribute in existingAttributes)
                {
                    if (!update.ColumnMappings.TryGetValue(existingAttribute, out var mapping))
                    {
                        mapping = new UpdateMapping();
                        update.ColumnMappings[existingAttribute] = mapping;
                    }

                    mapping.OldValueColumn = "existing_" + existingAttribute;
                }
            }

            // If any of the updates are for a polymorphic lookup field, make sure we've got an update for the associated type field too
            foreach (var assignment in setClauses.Cast<AssignmentSetClause>())
            {
                var targetAttrName = assignment.Column.MultiPartIdentifier.Identifiers.Last().Value;

                if (attributeNames.Contains(targetAttrName))
                {
                    var targetLookupAttribute = attributes[targetAttrName] as LookupAttributeMetadata;

                    if (targetLookupAttribute == null)
                        continue;

                    if (targetLookupAttribute.Targets.Length > 1 &&
                        !virtualTypeAttributes.Contains(targetAttrName + "type") &&
                        targetLookupAttribute.AttributeType != AttributeTypeCode.PartyList &&
                        (!sourceTypes.TryGetValue(targetAttrName, out var sourceType) || (!sourceType.IsSameAs(DataTypeHelpers.EntityReference) && sourceType != DataTypeHelpers.ImplicitIntForNullLiteral)))
                    {
                        throw new NotSupportedQueryFragmentException("Updating a polymorphic lookup field requires setting the associated type column as well", assignment.Column)
                        {
                            Suggestion = $"Add a SET clause for the {targetLookupAttribute.LogicalName}type column and set it to one of the following values:\r\n{String.Join("\r\n", targetLookupAttribute.Targets.Select(t => $"* {t}"))}"
                        };
                    }
                }
                else if (virtualTypeAttributes.Contains(targetAttrName))
                {
                    var idAttrName = targetAttrName.Substring(0, targetAttrName.Length - 4);

                    if (!attributeNames.Contains(idAttrName))
                    {
                        throw new NotSupportedQueryFragmentException("Updating a polymorphic type field requires setting the associated ID column as well", assignment.Column)
                        {
                            Suggestion = $"Add a SET clause for the {idAttrName} column"
                        };
                    }
                }
            }

            return update;
        }

        private IRootExecutionPlanNodeInternal ConvertSelectStatement(SelectStatement select)
        {
            if (TDSEndpoint.CanUseTDSEndpoint(Options, DataSources[Options.PrimaryDataSource].Connection))
            {
                using (var con = DataSources[Options.PrimaryDataSource].Connection == null ? null : TDSEndpoint.Connect(DataSources[Options.PrimaryDataSource].Connection))
                {
                    var tdsEndpointCompatibilityVisitor = new TDSEndpointCompatibilityVisitor(con, DataSources[Options.PrimaryDataSource].Metadata, false);
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
                            Sql = select.ToSql()
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
            if (binary.BinaryQueryExpressionType != BinaryQueryExpressionType.Union)
                throw new NotSupportedQueryFragmentException(Sql4CdsError.NotSupported(binary, binary.BinaryQueryExpressionType.ToString()));

            var left = ConvertSelectStatement(binary.FirstQueryExpression, hints, outerSchema, outerReferences, context);
            var right = ConvertSelectStatement(binary.SecondQueryExpression, hints, outerSchema, outerReferences, context);

            var concat = left.Source as ConcatenateNode;

            if (concat == null)
            {
                concat = new ConcatenateNode();

                concat.Sources.Add(left.Source);

                left.ExpandWildcardColumns(context);

                foreach (var col in left.ColumnSet)
                {
                    concat.ColumnSet.Add(new ConcatenateColumn
                    {
                        OutputColumn = context.GetExpressionName(),
                        SourceColumns = { col.SourceColumn },
                        SourceExpressions = { col.SourceExpression }
                    });
                }
            }

            concat.Sources.Add(right.Source);

            right.ExpandWildcardColumns(context);

            if (concat.ColumnSet.Count != right.ColumnSet.Count)
                throw new NotSupportedQueryFragmentException(Sql4CdsError.SetOperationWithDifferentColumnCounts(binary));

            for (var i = 0; i < concat.ColumnSet.Count; i++)
            {
                concat.ColumnSet[i].SourceColumns.Add(right.ColumnSet[i].SourceColumn);
                concat.ColumnSet[i].SourceExpressions.Add(right.ColumnSet[i].SourceExpression);
            }

            var node = (IDataExecutionPlanNodeInternal)concat;

            if (!binary.All)
            {
                var distinct = new DistinctNode { Source = node };
                distinct.Columns.AddRange(concat.ColumnSet.Select(col => col.OutputColumn));
                node = distinct;
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

                    aliasedCols.Add(concat.ColumnSet[i].OutputColumn);
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

                        aliasedCols.Add(concat.ColumnSet[i].OutputColumn);
                    }
                }
            }

            node = ConvertOrderByClause(node, hints, binary.OrderByClause, concat.ColumnSet.Select(col => col.OutputColumn.ToColumnReference()).ToArray(), aliases, binary, context, outerSchema, outerReferences, null);
            node = ConvertOffsetClause(node, binary.OffsetClause, context);

            var select = new SelectNode { Source = node, LogicalSourceSchema = concat.GetSchema(context) };
            select.ColumnSet.AddRange(concat.ColumnSet.Select((col, i) => new SelectColumn { SourceColumn = col.OutputColumn, SourceExpression = col.SourceExpressions[0], OutputColumn = left.ColumnSet[i].OutputColumn }));

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

            // Each table in the FROM clause starts as a separate FetchXmlScan node. Add appropriate join nodes
            var node = querySpec.FromClause == null || querySpec.FromClause.TableReferences.Count == 0 ? new ConstantScanNode { Values = { new Dictionary<string, ScalarExpression>() } } : ConvertFromClause(querySpec.FromClause, hints, querySpec, outerSchema, outerReferences, context);
            var logicalSchema = node.GetSchema(context);

            // Rewrite ColumnReferenceExpressions to use the fully qualified column name. This simplifies rewriting the
            // query later on due to aggregates etc. We need to process the SELECT clause first to capture aliases which might
            // be used in the ORDER BY clause
            var normalizeColNamesVisitor = new NormalizeColNamesVisitor(logicalSchema);
            querySpec.Accept(normalizeColNamesVisitor);

            node = ConvertInSubqueries(node, hints, querySpec, context, outerSchema, outerReferences);
            node = ConvertExistsSubqueries(node, hints, querySpec, context, outerSchema, outerReferences);

            // Add filters from WHERE
            node = ConvertWhereClause(node, hints, querySpec.WhereClause, outerSchema, outerReferences, context, querySpec);

            // Add aggregates from GROUP BY/SELECT/HAVING/ORDER BY
            var preGroupByNode = node;
            node = ConvertGroupByAggregates(node, querySpec, context, outerSchema, outerReferences);
            var nonAggregateSchema = preGroupByNode == node ? null : preGroupByNode.GetSchema(context);

            // Add filters from HAVING
            node = ConvertHavingClause(node, hints, querySpec.HavingClause, context, outerSchema, outerReferences, querySpec, nonAggregateSchema);

            // Add DISTINCT
            var distinct = querySpec.UniqueRowFilter == UniqueRowFilter.Distinct ? new DistinctNode { Source = node } : null;
            node = distinct ?? node;

            // Add SELECT
            var selectNode = ConvertSelectClause(querySpec.SelectElements, hints, node, distinct, querySpec, context, outerSchema, outerReferences, nonAggregateSchema, logicalSchema);
            node = selectNode.Source;

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

            node = ConvertOrderByClause(node, hints, querySpec.OrderByClause, selectFields.ToArray(), aliases, querySpec, context, outerSchema, outerReferences, nonAggregateSchema);

            // Add TOP/OFFSET
            if (querySpec.TopRowFilter != null && querySpec.OffsetClause != null)
                throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidTopWithOffset(querySpec.TopRowFilter));

            node = ConvertTopClause(node, querySpec.TopRowFilter, querySpec.OrderByClause, context);
            node = ConvertOffsetClause(node, querySpec.OffsetClause, context);

            selectNode.Source = node;

            // Convert to XML
            if (querySpec.ForClause is XmlForClause forXml)
                ConvertForXmlClause(selectNode, forXml, context);
            else if (querySpec.ForClause != null)
                throw new NotSupportedQueryFragmentException(Sql4CdsError.NotSupported(querySpec.ForClause, "FOR"));

            return selectNode;
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

        private IDataExecutionPlanNodeInternal ConvertInSubqueries(IDataExecutionPlanNodeInternal source, IList<OptimizerHint> hints, TSqlFragment query, NodeCompilationContext context, INodeSchema outerSchema, IDictionary<string,string> outerReferences)
        {
            var visitor = new InSubqueryVisitor();
            query.Accept(visitor);

            if (visitor.InSubqueries.Count == 0)
                return source;

            var computeScalar = source as ComputeScalarNode;
            var rewrites = new Dictionary<BooleanExpression, BooleanExpression>();
            var schema = source.GetSchema(context);

            foreach (var inSubquery in visitor.InSubqueries)
            {
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

                var parameters = context.ParameterTypes == null ? new Dictionary<string, DataTypeReference>(StringComparer.OrdinalIgnoreCase) : new Dictionary<string, DataTypeReference>(context.ParameterTypes, StringComparer.OrdinalIgnoreCase);
                var innerContext = new NodeCompilationContext(context, parameters);
                var references = new Dictionary<string, string>();
                var innerQuery = ConvertSelectStatement(inSubquery.Subquery.QueryExpression, hints, schema, references, innerContext);

                // Scalar subquery must return exactly one column and one row
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
                        var innerSchema = innerQuery.Source.GetSchema(new NodeCompilationContext(DataSources, Options, parameters, Log));
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

        private IDataExecutionPlanNodeInternal ConvertExistsSubqueries(IDataExecutionPlanNodeInternal source, IList<OptimizerHint> hints, TSqlFragment query, NodeCompilationContext context, INodeSchema outerSchema, IDictionary<string, string> outerReferences)
        {
            var visitor = new ExistsSubqueryVisitor();
            query.Accept(visitor);

            if (visitor.ExistsSubqueries.Count == 0)
                return source;

            var rewrites = new Dictionary<BooleanExpression, BooleanExpression>();
            var schema = source.GetSchema(context);

            foreach (var existsSubquery in visitor.ExistsSubqueries)
            {
                // Each query of the format "EXISTS (SELECT * FROM source)" becomes a outer semi join
                var parameters = context.ParameterTypes == null ? new Dictionary<string, DataTypeReference>(StringComparer.OrdinalIgnoreCase) : new Dictionary<string, DataTypeReference>(context.ParameterTypes, StringComparer.OrdinalIgnoreCase);
                var innerContext = new NodeCompilationContext(context, parameters);
                var references = new Dictionary<string, string>();
                var innerQuery = ConvertSelectStatement(existsSubquery.Subquery.QueryExpression, hints, schema, references, innerContext);
                var innerSchema = innerQuery.Source.GetSchema(new NodeCompilationContext(DataSources, Options, parameters, Log));
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

                converted.SqlExpression = aggregate.Expression.Parameters[0].Clone();

                switch (aggregate.Expression.FunctionName.Value.ToUpper())
                {
                    case "AVG":
                        converted.AggregateType = AggregateType.Average;
                        break;

                    case "COUNT":
                        if ((converted.SqlExpression is ColumnReferenceExpression countCol && countCol.ColumnType == ColumnType.Wildcard) || (converted.SqlExpression is Literal && !(converted.SqlExpression is NullLiteral)))
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

            CaptureOuterReferences(outerSchema, source, orderByClause, context, outerReferences);

            var computeScalar = new ComputeScalarNode { Source = source };
            ConvertScalarSubqueries(orderByClause, hints, ref source, computeScalar, context, query);

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
                        throw new NotSupportedQueryFragmentException(Sql4CdsError.AmbiguousColumnName(orderByCol));

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
                        throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidOrderByColumnNumber(orderBy))
                        {
                            Suggestion = $"Must be between 1 and {selectList.Length}"
                        };
                    }

                    orderBy.Expression = selectList[index];
                    orderBy.ScriptTokenStream = null;
                }

                // Anything complex expression should be pre-calculated
                if (!(orderBy.Expression is ColumnReferenceExpression) &&
                    !(orderBy.Expression is VariableReference) &&
                    !(orderBy.Expression is Literal))
                {
                    var calculated = ComputeScalarExpression(orderBy.Expression, hints, query, computeScalar, nonAggregateSchema, context, ref source);
                    sort.Source = source;
                    schema = source.GetSchema(context);

                    calculationRewrites[orderBy.Expression] = calculated.ToColumnReference();
                }

                // Validate the expression
                orderBy.Expression.GetType(GetExpressionContext(schema, context, nonAggregateSchema), out _);

                sort.Sorts.Add(orderBy.Clone());
            }

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

            foreach (var element in selectElements)
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
                            throw new NotSupportedQueryFragmentException(colTypeColl.CollationConflictError);
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
                var innerParameterTypes = context.ParameterTypes == null ? new Dictionary<string, DataTypeReference>(StringComparer.OrdinalIgnoreCase) : new Dictionary<string, DataTypeReference>(context.ParameterTypes, StringComparer.OrdinalIgnoreCase);
                var innerContext = new NodeCompilationContext(context, innerParameterTypes);
                var subqueryPlan = ConvertSelectStatement(subquery.QueryExpression, hints, outerSchema, outerReferences, innerContext);

                // Scalar subquery must return exactly one column and one row
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
                return new ColumnReferenceExpression { MultiPartIdentifier = new MultiPartIdentifier { Identifiers = { new Identifier { Value = rewrites[scalar] } } } };

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

            var outerSchema = node.GetSchema(new NodeCompilationContext(DataSources, Options, null, Log));
            var innerSchema = subNode.GetSchema(new NodeCompilationContext(DataSources, Options, null, Log));

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
                select.ColumnSet.Add(new SelectColumn { SourceColumn = subqueryCol, OutputColumn = subqueryCol.SplitMultiPartIdentifier().Last() });
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
                innerSchema = subNode.GetSchema(new NodeCompilationContext(DataSources, Options, null, Log));

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
                if (table.SchemaObject.Identifiers.Count == 1 && _cteSubplans.TryGetValue(table.SchemaObject.BaseIdentifier.Value, out var cteSubplan))
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

                    if (entityName.Equals("globaloptionset", StringComparison.OrdinalIgnoreCase))
                    {
                        return new GlobalOptionSetQueryNode
                        {
                            DataSource = dataSource.Name,
                            Alias = table.Alias?.Value ?? entityName
                        };
                    }

                    throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidObjectName(table.SchemaObject));
                }

                if (!String.IsNullOrEmpty(table.SchemaObject.SchemaIdentifier?.Value) &&
                    !table.SchemaObject.SchemaIdentifier.Value.Equals("dbo", StringComparison.OrdinalIgnoreCase) &&
                    !table.SchemaObject.SchemaIdentifier.Value.Equals("archive", StringComparison.OrdinalIgnoreCase))
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidObjectName(table.SchemaObject));

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

                // Validate the join condition
                var joinSchema = joinNode.GetSchema(context);
                join.SearchCondition.GetType(GetExpressionContext(joinSchema, context), out _);

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
                    var innerParameterTypes = context.ParameterTypes == null ? new Dictionary<string, DataTypeReference>(StringComparer.OrdinalIgnoreCase) : new Dictionary<string, DataTypeReference>(context.ParameterTypes, StringComparer.OrdinalIgnoreCase);
                    var innerContext = new NodeCompilationContext(context, innerParameterTypes);
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

            if (reference is SchemaObjectFunctionTableReference tvf)
            {
                // Capture any references to data from an outer query
                CaptureOuterReferences(outerSchema, null, tvf, context, outerReferences);

                // Convert any scalar subqueries in the parameters to its own execution plan, and capture the references from those plans
                // as parameters to be passed to the function
                IDataExecutionPlanNodeInternal source = new ConstantScanNode { Values = { new Dictionary<string, ScalarExpression>() } };
                var computeScalar = new ComputeScalarNode { Source = source };

                foreach (var param in tvf.Parameters.ToList())
                    ConvertScalarSubqueries(param, hints, ref source, computeScalar, context, tvf);

                if (source is ConstantScanNode)
                    source = null;
                else if (computeScalar.Columns.Count > 0)
                    source = computeScalar;

                var scalarSubquerySchema = source?.GetSchema(context);
                var scalarSubqueryReferences = new Dictionary<string, string>();
                CaptureOuterReferences(scalarSubquerySchema, null, tvf, context, scalarSubqueryReferences);

                var dataSource = SelectDataSource(tvf.SchemaObject);
                IDataExecutionPlanNodeInternal execute;

                if (String.IsNullOrEmpty(tvf.SchemaObject.SchemaIdentifier?.Value) ||
                    tvf.SchemaObject.SchemaIdentifier.Value.Equals("dbo", StringComparison.OrdinalIgnoreCase))
                {
                    execute = ExecuteMessageNode.FromMessage(tvf, dataSource, GetExpressionContext(null, context));
                }
                else if (tvf.SchemaObject.SchemaIdentifier.Value.Equals("sys", StringComparison.OrdinalIgnoreCase))
                {
                    if (!Enum.TryParse<SystemFunction>(tvf.SchemaObject.BaseIdentifier.Value, true, out var systemFunction))
                        throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidObjectName(tvf.SchemaObject));

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
