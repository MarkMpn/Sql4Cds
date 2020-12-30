using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using EnvDTE;
using EnvDTE80;
using MarkMpn.Sql4Cds.Engine;
using Microsoft.SqlServer.Management.UI.VSIntegration;
using Microsoft.SqlServer.Management.UI.VSIntegration.ObjectExplorer;
using Microsoft.VisualStudio.Shell;
using Microsoft.VisualStudio.Shell.Interop;

namespace MarkMpn.Sql4Cds.SSMS
{
    class DmlExecute : CommandBase
    {
        private readonly IDictionary<TextDocument, QueryExecutionOptions> _options;

        private const string Banner =
        @"
  ___  ___  _      _ _     ___ ___  ___ 
 / __|/ _ \| |    | | |   / __|   \/ __|
 \__ \ (_) | |__  |_  _| | (__| |) \__ \
 |___/\__\_\____|   |_|   \___|___/|___/

 INSERT/UPDATE/DELETE commands are implemented by SQL 4 CDS
 and not supported by Microsoft
 https://markcarrington.dev/sql-4-cds/

";

        public DmlExecute(AsyncPackage package, DTE2 dte, IObjectExplorerService objExp) : base(package, dte, objExp)
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

        public static void Initialize(AsyncPackage package, DTE2 dte, IObjectExplorerService objExp)
        {
            Instance = new DmlExecute(package, dte, objExp);
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
            var sqlScriptEditorControl = ServiceCache.ScriptFactory.InvokeMethod("GetCurrentlyActiveFrameDocView", ServiceCache.VSMonitorSelection, false, null);
            var textSpan = sqlScriptEditorControl.InvokeMethod("GetSelectedTextSpan");
            var sql = (string) textSpan.GetProperty("Text");

            // Quick check first so we don't spend a long time connecting to CDS just to find there's a simple SELECT query
            if (sql.IndexOf("INSERT", StringComparison.OrdinalIgnoreCase) == -1 &&
                sql.IndexOf("UPDATE", StringComparison.OrdinalIgnoreCase) == -1 &&
                sql.IndexOf("DELETE", StringComparison.OrdinalIgnoreCase) == -1)
                return;

            // Allow user to bypass SQL 4 CDS logic in case of problematic queries
            if (sql.IndexOf("Bypass SQL 4 CDS", StringComparison.OrdinalIgnoreCase) != -1 ||
                sql.IndexOf("Bypass SQL4CDS", StringComparison.OrdinalIgnoreCase) != -1)
                return;

            // We've possibly got a DML statement, so parse the query properly to get the details
            var quotedIdentifiers = sqlScriptEditorControl.GetProperty("MyOptions").GetProperty("ExecutionSettings").GetProperty("SetQuotedIdentifier");
            var sql2FetchXml = new Sql2FetchXml(GetMetadataCache(), (bool) quotedIdentifiers);
            sql2FetchXml.TDSEndpointAvailable = true;
            sql2FetchXml.ForceTDSEndpoint = true;
            Query[] queries;

            try
            {
                queries = sql2FetchXml.Convert(sql);
            }
            catch (Exception ex)
            {
                CancelDefault = true;
                ShowError(sqlScriptEditorControl, textSpan, ex);
                return;
            }

            var hasSelect = queries.OfType<SelectQuery>().Count();
            var hasDml = queries.Length - hasSelect;

            if (hasSelect > 0 && hasDml > 0)
            {
                // Can't mix SELECT and DML queries as we can't show results in the grid and SSMS can't execute the DML queries
                CancelDefault = true;
                ShowError(sqlScriptEditorControl, textSpan, new ApplicationException("Cannot mix SELECT queries with DML queries. Execute SELECT statements in a separate batch to INSERT/UPDATE/DELETE"));
                return;
            }

            if (hasSelect > 0)
                return;

            // We need to execute the DML statements directly
            CancelDefault = true;
            var org = ConnectCDS();
            var metadata = GetMetadataCache();

            // Show the queries starting to run
            sqlScriptEditorControl.SetProperty("IsExecuting", true);
            sqlScriptEditorControl.InvokeMethod("OnExecutionStarted", sqlScriptEditorControl, EventArgs.Empty);
            sqlScriptEditorControl.InvokeMethod("ToggleResultsControl", true);
            var sqlResultsControl = sqlScriptEditorControl.GetField("m_sqlResultsControl");
            sqlResultsControl.InvokeMethod("PrepareForExecution", true);

            // Cancel method expects execution options to have been set at the start - create some default ones now
            var m_sqlExec = sqlResultsControl.GetField("m_sqlExec");
            var execOptions = Activator.CreateInstance(Type.GetType("Microsoft.SqlServer.Management.QueryExecution.QESQLExecutionOptions, SQLEditors"));
            m_sqlExec.SetField("m_execOptions", execOptions);
            var batch = Activator.CreateInstance(Type.GetType("Microsoft.SqlServer.Management.QueryExecution.QESQLBatch, SQLEditors"));
            m_sqlExec.SetField("m_curBatch", batch);
            m_sqlExec.SetField("m_batchConsumer", sqlResultsControl.GetField("m_batchConsumer"));

            sqlResultsControl.InvokeMethod("AddStringToMessages", Banner, true);

            // Store the options being used for these queries so we can cancel them later
            var options = new QueryExecutionOptions(sqlScriptEditorControl, sqlResultsControl);
            _options[ActiveDocument] = options;
            var doc = ActiveDocument;

            // Run the queries in a background thread
            System.Threading.Tasks.Task.Run(async () =>
            {
                var resultFlag = 0;

                foreach (var query in queries)
                {
                    if (options.Cancelled)
                        break;

                    try
                    {
                        query.Execute(org, metadata, options);

                        if (query.Result is string msg)
                            sqlResultsControl.InvokeMethod("AddStringToMessages", msg + "\r\n\r\n", true);

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
                                sqlResultsControl.InvokeMethod("AddStringToMessages", msg + "\r\n\r\n", true);
                                resultFlag |= 1; // Success
                            }
                        }

                        AddException(sqlScriptEditorControl, sqlResultsControl, textSpan, error);
                        resultFlag |= 2; // Failure
                    }
                }

                if (options.Cancelled)
                    resultFlag = 4; // Cancel

                sqlResultsControl.InvokeMethod("AddStringToMessages", $"Completion time: {DateTime.Now:o}");

                await Package.JoinableTaskFactory.SwitchToMainThreadAsync();

                var result = Enum.ToObject(Type.GetType("Microsoft.SqlServer.Management.QueryExecution.ScriptExecutionResult, SQLEditors"), resultFlag);
                var resultsArg = Activator.CreateInstance(Type.GetType("Microsoft.SqlServer.Management.QueryExecution.ScriptExecutionCompletedEventArgs, SQLEditors"), BindingFlags.CreateInstance | BindingFlags.NonPublic | BindingFlags.Instance, null, new[] { result }, null);
                sqlResultsControl.InvokeMethod("OnSqlExecutionCompletedInt", sqlResultsControl, resultsArg);
                sqlScriptEditorControl.InvokeMethod("DoExecutionCompletedProcessing", result);

                _options.Remove(doc);
            });
        }

        private void OnCancelQuery(string Guid, int ID, object CustomIn, object CustomOut, ref bool CancelDefault)
        {
            if (!_options.TryGetValue(ActiveDocument, out var options))
                return;

            options.Cancel();
            CancelDefault = true;
        }

        private void ShowError(object sqlScriptEditorControl, object textSpan, Exception ex)
        {
            // Show the results pane
            sqlScriptEditorControl.SetProperty("IsExecuting", true);
            sqlScriptEditorControl.InvokeMethod("OnExecutionStarted", sqlScriptEditorControl, EventArgs.Empty);
            sqlScriptEditorControl.InvokeMethod("ToggleResultsControl", true);
            var sqlResultsControl = sqlScriptEditorControl.GetField("m_sqlResultsControl");
            sqlResultsControl.InvokeMethod("PrepareForExecution", true);

            // Add the messages
            sqlResultsControl.InvokeMethod("AddStringToMessages", Banner, true);
            AddException(sqlScriptEditorControl, sqlResultsControl, textSpan, ex);

            // Show that the query failed
            var failure = Enum.ToObject(Type.GetType("Microsoft.SqlServer.Management.QueryExecution.ScriptExecutionResult, SQLEditors"), 2);
            var resultsArg = Activator.CreateInstance(Type.GetType("Microsoft.SqlServer.Management.QueryExecution.ScriptExecutionCompletedEventArgs, SQLEditors"), BindingFlags.CreateInstance | BindingFlags.NonPublic | BindingFlags.Instance, null, new[] { failure }, null);
            sqlResultsControl.InvokeMethod("OnSqlExecutionCompletedInt", sqlResultsControl, resultsArg);
            sqlScriptEditorControl.InvokeMethod("DoExecutionCompletedProcessing", failure);
        }

        private void AddException(object sqlScriptEditorControl, object sqlResultsControl, object textSpan, Exception ex)
        {
            if (ex is AggregateException aggregate)
            {
                foreach (var error in aggregate.InnerExceptions)
                    AddException(sqlScriptEditorControl, sqlResultsControl, textSpan, error);

                return;
            }

            var line = 0;
            var msg = ex.Message;

            if (ex is NotSupportedQueryFragmentException err)
            {
                line = err.Fragment.StartLine;
                msg = err.Error;
            }
            else if (ex is QueryParseException parse)
            {
                line = parse.Error.Line;
            }

            if (line != 0)
                sqlResultsControl.InvokeMethod("AddStringToErrors", ex.Message, line - 1, textSpan, true);
            else
                sqlResultsControl.InvokeMethod("AddStringToErrors", ex.Message, true);
        }
    }
}
