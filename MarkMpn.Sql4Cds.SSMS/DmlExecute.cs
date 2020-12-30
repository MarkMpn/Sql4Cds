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
        public DmlExecute(AsyncPackage package, DTE2 dte, IObjectExplorerService objExp) : base(package, dte, objExp)
        {
            var execute = dte.Commands.Item("Query.Execute");
            QueryExecuteEvent = dte.Events.CommandEvents[execute.Guid, execute.ID];
            QueryExecuteEvent.BeforeExecute += OnExecuteQuery;
        }

        public CommandEvents QueryExecuteEvent { get; private set; }

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

            CancelDefault = true;
            var sqlScriptEditorControl = InvokeMethod(ServiceCache.ScriptFactory, "GetCurrentlyActiveFrameDocView", ServiceCache.VSMonitorSelection, false, null);
            SetProperty(sqlScriptEditorControl, "IsExecuting", true);
            InvokeMethod(sqlScriptEditorControl, "OnExecutionStarted", sqlScriptEditorControl, EventArgs.Empty);
            InvokeMethod(sqlScriptEditorControl, "ToggleResultsControl", true);
            var sqlResultsControl = GetField(sqlScriptEditorControl, "m_sqlResultsControl");
            InvokeMethod(sqlResultsControl, "PrepareForExecution", true);
            InvokeMethod(sqlResultsControl, "AddStringToMessages", "Hello World", true);
            var success = Enum.ToObject(Type.GetType("Microsoft.SqlServer.Management.QueryExecution.ScriptExecutionResult, SQLEditors"), 1);
            var resultsArg = Activator.CreateInstance(Type.GetType("Microsoft.SqlServer.Management.QueryExecution.ScriptExecutionCompletedEventArgs, SQLEditors"), BindingFlags.CreateInstance | BindingFlags.NonPublic | BindingFlags.Instance, null, new[] { success }, null);
            InvokeMethod(sqlResultsControl, "OnSqlExecutionCompletedInt", sqlResultsControl, resultsArg);
            InvokeMethod(sqlScriptEditorControl, "DoExecutionCompletedProcessing", success);

            /*
             * internal enum ScriptExecutionResult
  {
    Success = 1,
    Failure = 2,
    Cancel = 4,
    Timeout = 8,
    Halted = 16, // 0x00000010
    Mask = Halted | Timeout | Cancel | Failure | Success, // 0x0000001F
  }
  */

            return;
            /*
            // We are running a query against the Dataverse endpoint, so check if there are any DML statements in the query
            var sql = GetQuery();
            var sql2FetchXml = new Sql2FetchXml(GetMetadataCache(), false);
            sql2FetchXml.ColumnComparisonAvailable = true;
            sql2FetchXml.TDSEndpointAvailable = true;
            sql2FetchXml.ForceTDSEndpoint = true;
            Query[] queries;

            try
            {
                queries = sql2FetchXml.Convert(sql);
            }
            catch (Exception ex)
            {
                var execute = VsShellUtilities.PromptYesNo(ex.Message, "Error Converting Query. Execute normally?", OLEMSGICON.OLEMSGICON_WARNING, (IVsUIShell) Package.GetServiceAsync(typeof(IVsUIShell)).ConfigureAwait(false).GetAwaiter().GetResult());
                CancelDefault = !execute;
                windowFrame.Hide();
                return;
            }

            var hasSelect = queries.OfType<SelectQuery>().Count();
            var hasDml = queries.Length - hasSelect;

            if (hasSelect > 0 && hasDml > 0)
            {
                // Can't mix SELECT and DML queries as we can't show results in the grid and SSMS can't execute the DML queries
                CancelDefault = true;
                windowFrame.Hide();

                VsShellUtilities.ShowMessageBox(Package, "Cannot mix SELECT queries with DML queries. Execute SELECT statements in a separate batch to INSERT/UPDATE/DELETE", "Unsupported Query Mix", OLEMSGICON.OLEMSGICON_CRITICAL, OLEMSGBUTTON.OLEMSGBUTTON_OK, OLEMSGDEFBUTTON.OLEMSGDEFBUTTON_FIRST);
                return;
            }

            if (hasSelect > 0)
            {
                windowFrame.Hide();
                return;
            }

            // We need to execute the DML statements directly
            CancelDefault = true;
            
            var resultsWindow = (ResultsToolWindowControl)window.Content;

            var viewGuid = System.Guid.Empty;
            ErrorHandler.ThrowOnFailure(windowFrame.Show());
            windowFrame.SetProperty((int)__VSFPROPID.VSFPROPID_FrameMode, VSFRAMEMODE.VSFM_Dock);

            try
            {
                resultsWindow.ExecuteQueries(queries, ConnectCDS(), GetMetadataCache(), Package.JoinableTaskFactory);
            }
            catch (Exception ex)
            {
                VsShellUtilities.ShowMessageBox(Package, ex.Message, "Error Executing Query", OLEMSGICON.OLEMSGICON_CRITICAL, OLEMSGBUTTON.OLEMSGBUTTON_OK, OLEMSGDEFBUTTON.OLEMSGDEFBUTTON_FIRST);
            }*/
        }

        public object GetField(object obj, string field)
        {
            FieldInfo f = obj.GetType().GetField(field, BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance);

            return f.GetValue(obj);
        }

        private void SetProperty(object target, string propName, object value)
        {
            var prop = target.GetType().GetProperty(propName, BindingFlags.NonPublic  | BindingFlags.Public| BindingFlags.Instance);
            prop.SetValue(target, value);
        }

        private object InvokeMethod(object target, string methodName, params object[] args)
        {
            var type = target.GetType();

            while (type != null)
            {
                var method = type.GetMethod(methodName, BindingFlags.InvokeMethod | BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance);

                if (method != null)
                    return method.Invoke(target, args);

                type = type.BaseType;
            }

            throw new ArgumentOutOfRangeException(nameof(methodName));
        }
    }
}
