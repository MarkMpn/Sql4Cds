using System;
using System.Collections.Generic;
using System.Linq;
using EnvDTE;
using EnvDTE80;
using MarkMpn.Sql4Cds.Engine;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using Microsoft.SqlServer.Management.QueryExecution;
using Microsoft.SqlServer.Management.UI.VSIntegration;
using Microsoft.VisualStudio.Shell;

namespace MarkMpn.Sql4Cds.SSMS
{
    class DmlExecute : CommandBase
    {
        private readonly IDictionary<TextDocument, QueryExecutionOptions> _options;
        
        public DmlExecute(Sql4CdsPackage package, DTE2 dte) : base(package, dte)
        {
            _options = new Dictionary<TextDocument, QueryExecutionOptions>();

            var execute = dte.Commands.Item("Query.Execute");
            QueryExecuteEvent = dte.Events.CommandEvents[execute.Guid, execute.ID];
            QueryExecuteEvent.BeforeExecute += OnExecuteQuery;

            var cancel = dte.Commands.Item("Query.CancelExecutingQuery");
            QueryCancelEvent = dte.Events.CommandEvents[cancel.Guid, cancel.ID];
            QueryCancelEvent.BeforeExecute += OnCancelQuery;
        }

        public CommandEvents QueryExecuteEvent { get; private set; }

        public CommandEvents QueryCancelEvent { get; private set; }

        /// <summary>
        /// Gets the instance of the command.
        /// </summary>
        public static DmlExecute Instance
        {
            get;
            private set;
        }

        public static void Initialize(Sql4CdsPackage package, DTE2 dte)
        {
            Instance = new DmlExecute(package, dte);
        }

        private void OnExecuteQuery(string Guid, int ID, object CustomIn, object CustomOut, ref bool CancelDefault)
        {
            ThreadHelper.ThrowIfNotOnUIThread();

            if (ActiveDocument == null)
                return;

            if (!IsDataverse())
                return;

            // We are running a query against the Dataverse TDS endpoint, so check if there are any DML statements in the query

            // Get the SQL editor object
            var scriptFactory = new ScriptFactoryWrapper(ServiceCache.ScriptFactory);
            var sqlScriptEditorControl = scriptFactory.GetCurrentlyActiveFrameDocView(ServiceCache.VSMonitorSelection, false, out _);
            var textSpan = sqlScriptEditorControl.GetSelectedTextSpan();
            var sql = textSpan.Text;

            // Quick check first so we don't spend a long time connecting to CDS just to find there's a simple SELECT query
            if (sql.IndexOf("INSERT", StringComparison.OrdinalIgnoreCase) == -1 &&
                sql.IndexOf("UPDATE", StringComparison.OrdinalIgnoreCase) == -1 &&
                sql.IndexOf("DELETE", StringComparison.OrdinalIgnoreCase) == -1)
                return;

            // Allow user to bypass SQL 4 CDS logic in case of problematic queries
            if (sql.IndexOf("Bypass SQL 4 CDS", StringComparison.OrdinalIgnoreCase) != -1 ||
                sql.IndexOf("Bypass SQL4CDS", StringComparison.OrdinalIgnoreCase) != -1)
                return;

            // Store the options being used for these queries so we can cancel them later
            var options = new QueryExecutionOptions(sqlScriptEditorControl, Package.Settings);
            var metadata = GetMetadataCache();
            var org = ConnectCDS();

            // We've possibly got a DML statement, so parse the query properly to get the details
            var converter = new ExecutionPlanBuilder(metadata, new TableSizeCache(org, metadata), options)
            {
                TDSEndpointAvailable = true,
                QuotedIdentifiers = sqlScriptEditorControl.QuotedIdentifiers
            };
            IRootExecutionPlanNode[] queries;

            try
            {
                queries = converter.Build(sql);
            }
            catch (Exception ex)
            {
                CancelDefault = true;
                ShowError(sqlScriptEditorControl, textSpan, ex);
                return;
            }

            var dmlQueries = queries.OfType<IDmlQueryExecutionPlanNode>().ToArray();
            var hasSelect = queries.Length > dmlQueries.Length;
            var hasDml = dmlQueries.Length > 0;

            if (hasSelect && hasDml)
            {
                // Can't mix SELECT and DML queries as we can't show results in the grid and SSMS can't execute the DML queries
                CancelDefault = true;
                ShowError(sqlScriptEditorControl, textSpan, new ApplicationException("Cannot mix SELECT queries with DML queries. Execute SELECT statements in a separate batch to INSERT/UPDATE/DELETE"));
                return;
            }

            if (hasSelect)
                return;

            // We need to execute the DML statements directly
            CancelDefault = true;

            // Show the queries starting to run
            sqlScriptEditorControl.StandardPrepareBeforeExecute();
            sqlScriptEditorControl.OnExecutionStarted(sqlScriptEditorControl, EventArgs.Empty);
            sqlScriptEditorControl.ToggleResultsControl(true);
            sqlScriptEditorControl.Results.StartExecution();

            _options[ActiveDocument] = options;
            var doc = ActiveDocument;

            // Run the queries in a background thread
            var task = new System.Threading.Tasks.Task(async () =>
            {
                var resultFlag = 0;

                foreach (var query in dmlQueries)
                {
                    if (options.Cancelled)
                        break;

                    try
                    {
                        _ai.TrackEvent("Execute", new Dictionary<string, string> { ["QueryType"] = query.GetType().Name, ["Source"] = "SSMS" });
                        var msg = query.Execute(org, metadata, options, null, null);

                        sqlScriptEditorControl.Results.AddStringToMessages(msg + "\r\n\r\n");

                        resultFlag |= 1; // Success
                    }
                    catch (Exception ex)
                    {
                        var error = ex;

                        if (ex is PartialSuccessException partial)
                        {
                            error = partial.InnerException;

                            if (partial.Result is string msg)
                            {
                                sqlScriptEditorControl.Results.AddStringToMessages(msg + "\r\n\r\n");
                                resultFlag |= 1; // Success
                            }
                        }

                        _ai.TrackException(error, new Dictionary<string, string> { ["Sql"] = sql, ["Source"] = "SSMS" });

                        AddException(sqlScriptEditorControl, textSpan, error);
                        resultFlag |= 2; // Failure
                    }
                }

                if (options.Cancelled)
                    resultFlag = 4; // Cancel

                await Package.JoinableTaskFactory.SwitchToMainThreadAsync();

                sqlScriptEditorControl.Results.OnSqlExecutionCompletedInt(resultFlag);
                
                _options.Remove(doc);
            });

            options.Task = task;
            task.Start();
        }

        private void OnCancelQuery(string Guid, int ID, object CustomIn, object CustomOut, ref bool CancelDefault)
        {
            if (!_options.TryGetValue(ActiveDocument, out var options))
                return;

            options.Cancel();
            CancelDefault = true;
        }

        private void ShowError(SqlScriptEditorControlWrapper sqlScriptEditorControl, ITextSpan textSpan, Exception ex)
        {
            // Show the results pane
            sqlScriptEditorControl.StandardPrepareBeforeExecute();
            sqlScriptEditorControl.OnExecutionStarted(sqlScriptEditorControl, EventArgs.Empty);
            sqlScriptEditorControl.ToggleResultsControl(true);
            sqlScriptEditorControl.Results.StartExecution();

            // Add the messages
            AddException(sqlScriptEditorControl, textSpan, ex);

            // Show that the query failed
            sqlScriptEditorControl.Results.OnSqlExecutionCompletedInt(2);
        }

        private void AddException(SqlScriptEditorControlWrapper sqlScriptEditorControl, ITextSpan textSpan, Exception ex)
        {
            if (ex is AggregateException aggregate)
            {
                foreach (var error in aggregate.InnerExceptions)
                    AddException(sqlScriptEditorControl, textSpan, error);

                return;
            }

            var line = 0;

            if (ex is NotSupportedQueryFragmentException err)
                line = err.Fragment.StartLine;
            else if (ex is QueryParseException parse)
                line = parse.Error.Line;

            if (line != 0)
                sqlScriptEditorControl.Results.AddStringToErrors(ex.Message, line, textSpan, true);
            else
                sqlScriptEditorControl.Results.AddStringToErrors(ex.Message, true);
        }
    }
}
