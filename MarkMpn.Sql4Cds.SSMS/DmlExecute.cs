using System;
using System.Collections.Generic;
using System.Drawing;
using System.Linq;
using System.Windows.Forms;
using EnvDTE;
using EnvDTE80;
using MarkMpn.Sql4Cds.Controls;
//using MarkMpn.Sql4Cds.Controls;
using MarkMpn.Sql4Cds.Engine;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using Microsoft.SqlServer.Management.QueryExecution;
using Microsoft.SqlServer.Management.UI.VSIntegration;
using Microsoft.VisualStudio.Shell;
using Microsoft.VisualStudio.Shell.Interop;

namespace MarkMpn.Sql4Cds.SSMS
{
    class DmlExecute : CommandBase
    {
        private readonly IDictionary<TextDocument, QueryExecutionOptions> _options;
        private bool _addingResult;
        
        public DmlExecute(Sql4CdsPackage package, DTE2 dte) : base(package, dte)
        {
            ThreadHelper.ThrowIfNotOnUIThread();

            _options = new Dictionary<TextDocument, QueryExecutionOptions>();

            var execute = dte.Commands.Item("Query.Execute");
            QueryExecuteEvent = dte.Events.CommandEvents[execute.Guid, execute.ID];
            QueryExecuteEvent.BeforeExecute += OnExecuteQuery;

            var cancel = dte.Commands.Item("Query.CancelExecutingQuery");
            QueryCancelEvent = dte.Events.CommandEvents[cancel.Guid, cancel.ID];
            QueryCancelEvent.BeforeExecute += OnCancelQuery;

            var estimatedPlan = dte.Commands.Item("Query.DisplayEstimatedExecutionPlan");
            EstimatedPlanEvent = dte.Events.CommandEvents[estimatedPlan.Guid, estimatedPlan.ID];
            EstimatedPlanEvent.BeforeExecute += OnShowEstimatedPlan;
        }

        public CommandEvents QueryExecuteEvent { get; private set; }

        public CommandEvents QueryCancelEvent { get; private set; }

        public CommandEvents EstimatedPlanEvent { get; private set; }

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

            try
            {
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

                // Allow user to bypass SQL 4 CDS logic in case of problematic queries
                if (sql.IndexOf("Bypass SQL 4 CDS", StringComparison.OrdinalIgnoreCase) != -1 ||
                    sql.IndexOf("Bypass SQL4CDS", StringComparison.OrdinalIgnoreCase) != -1)
                    return;

                // Store the options being used for these queries so we can cancel them later
                var dataSource = GetDataSource();

                using (var con = new Sql4CdsConnection(new Dictionary<string, DataSource>(StringComparer.OrdinalIgnoreCase) { [dataSource.Name] = dataSource }))
                using (var cmd = con.CreateCommand())
                {
                    con.ApplicationName = "SSMS";
                    cmd.CommandTimeout = 0;
                    var options = new QueryExecutionOptions(sqlScriptEditorControl, Package.Settings, true, cmd);
                    options.ApplySettings(con);
                    cmd.CommandText = sql;

                    try
                    {
                        cmd.Prepare();
                    }
                    catch (Exception ex)
                    {
                        CancelDefault = true;
                        ShowError(sqlScriptEditorControl, textSpan, ex);
                        return;
                    }

                    if (cmd.UseTDSEndpointDirectly)
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
                    var resultFlag = 0;
                    var tabPage = sqlScriptEditorControl.IsWithShowPlan ? AddShowPlanTab(sqlScriptEditorControl) : null;

                    con.InfoMessage += (s, msg) =>
                    {
                        sqlScriptEditorControl.Results.AddStringToMessages(msg.Message + "\r\n\r\n");
                    };

                    cmd.StatementCompleted += (s, stmt) =>
                    {
                        if (tabPage != null)
                            ShowPlan(sqlScriptEditorControl, tabPage, stmt.Statement, dataSource, true);

                        if (stmt.Message != null)
                            sqlScriptEditorControl.Results.AddStringToMessages(stmt.Message + "\r\n\r\n");

                        resultFlag |= 1; // Success
                    };

                    // Run the queries in a background thread
                    options.Task = System.Threading.Tasks.Task.Run(async () =>
                    {
                        try
                        {
                            // SSMS grid doesn't know how to show entity references, so show as simple guids
                            con.ReturnEntityReferenceAsGuid = true;

                            using (var reader = await cmd.ExecuteReaderAsync())
                            {
                                while (!reader.IsClosed)
                                {
                                    var resultSet = QEResultSetWrapper.Create(reader);
                                    resultSet.Initialize(false);

                                    var gridContainer = ResultSetAndGridContainerWrapper.Create(resultSet, true, 1024);
                                    sqlScriptEditorControl.Results.AddGridContainer(gridContainer);
                                    gridContainer.StartRetrievingData();
                                    gridContainer.UpdateGrid();

                                    if (!await reader.NextResultAsync())
                                        break;
                                }
                            }
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

                            AddException(sqlScriptEditorControl, textSpan, error);
                            resultFlag |= 2; // Failure
                        }

                        if (options.IsCancelled)
                            resultFlag = 4; // Cancel

                        await Package.JoinableTaskFactory.SwitchToMainThreadAsync();

                        sqlScriptEditorControl.Results.OnSqlExecutionCompletedInt(resultFlag);

                        _options.Remove(doc);
                    });
                }
            }
            catch (Exception ex)
            {
                VsShellUtilities.LogError("SQL 4 CDS", ex.ToString());
            }
        }

        private void OnCancelQuery(string Guid, int ID, object CustomIn, object CustomOut, ref bool CancelDefault)
        {
            ThreadHelper.ThrowIfNotOnUIThread();

            if (!_options.TryGetValue(ActiveDocument, out var options))
                return;

            options.Cancel();
            CancelDefault = true;
        }

        private void OnShowEstimatedPlan(string Guid, int ID, object CustomIn, object CustomOut, ref bool CancelDefault)
        {
            ThreadHelper.ThrowIfNotOnUIThread();

            try
            {
                if (ActiveDocument == null)
                    return;

                if (!IsDataverse())
                    return;

                // Get the SQL editor object
                var scriptFactory = new ScriptFactoryWrapper(ServiceCache.ScriptFactory);
                var sqlScriptEditorControl = scriptFactory.GetCurrentlyActiveFrameDocView(ServiceCache.VSMonitorSelection, false, out _);
                var textSpan = sqlScriptEditorControl.GetSelectedTextSpan();
                var sql = textSpan.Text;

                // Store the options being used for these queries so we can cancel them later
                var dataSource = GetDataSource();

                using (var con = new Sql4CdsConnection(new Dictionary<string, DataSource>(StringComparer.OrdinalIgnoreCase) { [dataSource.Name] = dataSource }))
                using (var cmd = con.CreateCommand())
                {
                    con.ApplicationName = "SSMS";
                    var options = new QueryExecutionOptions(sqlScriptEditorControl, Package.Settings, !Package.Settings.ShowFetchXMLInEstimatedExecutionPlans, cmd);
                    options.ApplySettings(con);
                    cmd.CommandText = sql;

                    IRootExecutionPlanNode[] plans;

                    try
                    {
                        plans = cmd.GeneratePlan(false);
                    }
                    catch (Exception ex)
                    {
                        CancelDefault = true;
                        ShowError(sqlScriptEditorControl, textSpan, ex);
                        return;
                    }

                    CancelDefault = true;

                    // Show the queries starting to run
                    sqlScriptEditorControl.StandardPrepareBeforeExecute();
                    sqlScriptEditorControl.OnExecutionStarted(sqlScriptEditorControl, EventArgs.Empty);
                    sqlScriptEditorControl.ToggleResultsControl(true);
                    sqlScriptEditorControl.Results.StartExecution();

                    var tabPage = AddShowPlanTab(sqlScriptEditorControl);
                    sqlScriptEditorControl.Results.ResultsTabCtrl.SelectedTab = tabPage;

                    foreach (var query in plans)
                        ShowPlan(sqlScriptEditorControl, tabPage, query, dataSource, false);

                    var resultFlag = 1; // Success

                    sqlScriptEditorControl.Results.OnSqlExecutionCompletedInt(resultFlag);
                }
            }
            catch (Exception ex)
            {
                VsShellUtilities.LogError("SQL 4 CDS", ex.ToString());
            }
        }

        private TabPage AddShowPlanTab(SqlScriptEditorControlWrapper sqlScriptEditorControl)
        {
            // Add a tab to the results
            var tabPage = new TabPage("SQL 4 CDS Execution Plan");
            tabPage.ImageIndex = 5; // EstimatedShowPlanImageIndex
            sqlScriptEditorControl.Results.ResultsTabCtrl.TabPages.Insert(1, tabPage);

            var flp = new FlowLayoutPanel
            {
                AutoScroll = true,
                Dock = DockStyle.Fill,
                FlowDirection = FlowDirection.TopDown,
                Margin = new Padding(0),
                WrapContents = false
            };
            flp.ClientSizeChanged += ResizeLayoutPanel;
            tabPage.Controls.Add(flp);

            return tabPage;
        }

        private void ShowPlan(SqlScriptEditorControlWrapper sqlScriptEditorControl, TabPage tabPage, IRootExecutionPlanNode query, DataSource dataSource, bool executed)
        {
            if (tabPage.InvokeRequired)
            {
                tabPage.Invoke((Action) (() =>
                {
                    ThreadHelper.ThrowIfNotOnUIThread();

                    ShowPlan(sqlScriptEditorControl, tabPage, query, dataSource, executed);
                }));
                return;
            }

            var plan = new Panel();
            var fetchLabel = new Label
            {
                Text = query.Sql,
                AutoSize = false,
                Dock = DockStyle.Top,
                Height = 32,
                BorderStyle = BorderStyle.FixedSingle,
                Padding = new Padding(4),
                BackColor = SystemColors.Info,
                ForeColor = SystemColors.InfoText,
                AutoEllipsis = true,
                UseMnemonic = false
            };
            var planView = new ExecutionPlanView { Dock = DockStyle.Fill, Executed = executed, DataSources = new Dictionary<string, DataSource> { [dataSource.Name] = dataSource } };
            planView.Plan = query;

            planView.NodeSelected += (s, e) =>
            {
                ThreadHelper.ThrowIfNotOnUIThread();

                ShowProperties(sqlScriptEditorControl, planView.Selected, executed);
            };
            planView.DoubleClick += (s, e) =>
            {
                ThreadHelper.ThrowIfNotOnUIThread();

                if (planView.Selected is IFetchXmlExecutionPlanNode fetchXml)
                    ShowFetchXML(fetchXml.FetchXmlString);
            };
            plan.Controls.Add(planView);
            plan.Controls.Add(fetchLabel);

            AddControl(plan, tabPage);
        }

        private void ShowFetchXML(string fetchXmlString)
        {
            ThreadHelper.ThrowIfNotOnUIThread();

            var window = Dte.ItemOperations.NewFile("General\\XML File");
            
            var editPoint = ActiveDocument.EndPoint.CreateEditPoint();
            editPoint.Insert(fetchXmlString);
        }

        private void ShowProperties(SqlScriptEditorControlWrapper sqlScriptEditorControl, IExecutionPlanNode selected, bool executed)
        {
            ThreadHelper.ThrowIfNotOnUIThread();

            var trackSelection = (Microsoft.SqlServer.Management.UI.VSIntegration.ITrackSelection) sqlScriptEditorControl.ServiceProvider.GetService(typeof(Microsoft.SqlServer.Management.UI.VSIntegration.ITrackSelection));

            if (trackSelection == null)
                return;

            var selection = new SelectionService();

            if (selected != null)
                selection.SelectObjects(1, new[] { new ExecutionPlanNodeTypeDescriptor(selected, !executed, null) }, 0);

            trackSelection.OnSelectChange(selection);
        }

        private void ResizeLayoutPanel(object sender, EventArgs e)
        {
            if (_addingResult)
                return;

            var flp = (FlowLayoutPanel)sender;
            var prevHeight = 0;

            foreach (Control control in flp.Controls)
            {
                control.Width = flp.ClientSize.Width;
                prevHeight += control.Height + control.Margin.Top + control.Margin.Bottom;
            }

            if (flp.Controls.Count > 0)
            {
                var lastControl = flp.Controls[flp.Controls.Count - 1];
                prevHeight -= lastControl.Height;
                var minHeight = GetMinHeight(lastControl, flp.ClientSize.Height * 2 / 3);
                if (prevHeight + minHeight > flp.ClientSize.Height)
                    lastControl.Height = minHeight;
                else
                    lastControl.Height = flp.ClientSize.Height - prevHeight;
            }
        }

        private void AddControl(Control control, TabPage tabPage)
        {
            _addingResult = true;

            var flp = (FlowLayoutPanel)tabPage.Controls[0];
            flp.HorizontalScroll.Enabled = false;

            if (flp.Controls.Count == 0)
            {
                control.Height = flp.Height;
                control.Margin = Padding.Empty;
            }
            else
            {
                control.Margin = new Padding(0, 0, 0, 3);

                if (flp.Controls.Count == 1)
                    flp.Controls[0].Margin = control.Margin;

                if (flp.Controls.Count > 0)
                    flp.Controls[flp.Controls.Count - 1].Height = GetMinHeight(flp.Controls[flp.Controls.Count - 1], flp.ClientSize.Height * 2 / 3);

                control.Height = GetMinHeight(control, flp.ClientSize.Height * 2 / 3);

                var prevHeight = flp.Controls.OfType<Control>().Sum(c => c.Height + c.Margin.Top + c.Margin.Bottom);
                if (prevHeight + control.Height < flp.ClientSize.Height)
                    control.Height = flp.ClientSize.Height - prevHeight;
            }

            control.Width = flp.ClientSize.Width;

            flp.Controls.Add(control);

            if (control.Width > flp.ClientSize.Width)
            {
                foreach (Control child in flp.Controls)
                    child.Width = flp.ClientSize.Width;
            }

            _addingResult = false;
        }

        private int GetMinHeight(Control control, int max)
        {
            if (control is Panel panel)
                return panel.Controls.OfType<Control>().Sum(child => GetMinHeight(child, max));

            if (control is ExecutionPlanView plan)
                return plan.AutoScrollMinSize.Height;

            return control.Height;
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

            if (ex is Sql4CdsException sql4CdsException && sql4CdsException.Errors != null)
            {
                foreach (var sql4CdsError in sql4CdsException.Errors)
                {
                    var parts = new List<string>
                                    {
                                        $"Msg {sql4CdsError.Number}",
                                        $"Level {sql4CdsError.Class}",
                                        $"State {sql4CdsError.State}"
                                    };

                    if (sql4CdsError.Procedure != null)
                    {
                        parts.Add($"Procedure {sql4CdsError.Procedure}");
                        parts.Add($"Line 0 [Batch Start Line {sql4CdsError.LineNumber}]");
                    }
                    else
                    {
                        parts.Add($"Line {sql4CdsError.LineNumber}");
                    }

                    if (line != 0)
                        sqlScriptEditorControl.Results.AddStringToErrors(String.Join(", ", parts), line, textSpan, true);
                    else
                        sqlScriptEditorControl.Results.AddStringToErrors(String.Join(", ", parts), true);
                }
            }

            if (line != 0)
                sqlScriptEditorControl.Results.AddStringToErrors(ex.Message, line, textSpan, true);
            else
                sqlScriptEditorControl.Results.AddStringToErrors(ex.Message, true);
        }
    }
}
