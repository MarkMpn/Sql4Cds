using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.SqlTypes;
using System.Diagnostics;
using System.Drawing;
using System.Globalization;
using System.IO;
using System.Linq;
using System.ServiceModel;
using System.Text;
using System.Text.RegularExpressions;
using System.Web;
using System.Windows.Forms;
using AutocompleteMenuNS;
using MarkMpn.Sql4Cds.Controls;
using MarkMpn.Sql4Cds.Engine;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using McTools.Xrm.Connection;
using Microsoft.ApplicationInsights;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using Microsoft.Xrm.Tooling.Connector;
using ScintillaNET;
using xrmtb.XrmToolBox.Controls.Controls;

namespace MarkMpn.Sql4Cds.XTB
{
    partial class SqlQueryControl : DocumentWindowBase, ISaveableDocumentWindow, IFormatableDocumentWindow
    {
        class ExecuteParams
        {
            public string Sql { get; set; }
            public bool Execute { get; set; }
            public bool IncludeFetchXml { get; set; }
            public int Offset { get; set; }
        }

        class QueryException : ApplicationException
        {
            public QueryException(IRootExecutionPlanNode query, Exception innerException) : base(innerException.Message, innerException)
            {
                Query = query;
            }

            public IRootExecutionPlanNode Query { get; }
        }

        class TextRange
        {
            public TextRange(int index, int length)
            {
                Index = index;
                Length = length;
            }

            public int Index { get; }
            public int Length { get; }
        }

        private ConnectionDetail _con;
        private readonly TelemetryClient _ai;
        private readonly Scintilla _editor;
        private readonly Action<string> _log;
        private readonly PropertiesWindow _properties;
        private int _maxLineNumberCharLength;
        private static int _queryCounter;
        private static ImageList _images;
        private static Icon _sqlIcon;
        private readonly AutocompleteMenu _autocomplete;
        private ToolTip _tooltip;
        private bool _cancellable;
        private bool _cancelled;
        private Stopwatch _stopwatch;
        private Sql4CdsCommand _command;
        private ExecuteParams _params;
        private int _rowCount;
        private ToolStripControlHost _progressHost;
        private bool _addingResult;
        private IDictionary<int, TextRange> _messageLocations;
        private Sql4CdsConnection _connection;
        private FindReplace _findReplace;
        private bool _ctrlK;
        private Font _linkFont;

        static SqlQueryControl()
        {
            _images = new ImageList();
            _images.Images.AddRange(new ObjectExplorer(null, null, null, null).GetImages().ToArray());

            _sqlIcon = Icon.FromHandle(Properties.Resources.SQLFile_16x.GetHicon());
        }

        public SqlQueryControl(ConnectionDetail con, IDictionary<string, DataSource> dataSources, TelemetryClient ai, Action<string> showFetchXml, Action<string> log, PropertiesWindow properties)
        {
            InitializeComponent();
            DisplayName = $"SQLQuery{++_queryCounter}.sql";
            ShowFetchXML = showFetchXml;
            DataSources = dataSources;
            _editor = CreateSqlEditor();
            _autocomplete = CreateAutocomplete();
            _ai = ai;
            _log = log;
            _properties = properties;
            _stopwatch = new Stopwatch();
            BusyChanged += (s, e) => SyncTitle();

            // Populate the status bar and add separators between each field
            for (var i = statusStrip.Items.Count - 1; i > 1; i--)
                statusStrip.Items.Insert(i, new ToolStripSeparator());

            var progressImage = new PictureBox { Image = Properties.Resources.progress, Height = 16, Width = 16 };
            _progressHost = new ToolStripControlHost(progressImage) { Visible = false };
            statusStrip.Items.Insert(0, _progressHost);

            splitContainer.Panel1.Controls.Add(_editor);
            splitContainer.Panel1.Controls.SetChildIndex(_editor, 0);
            Icon = _sqlIcon;

            Connect();

            ChangeConnection(con);
        }

        protected override string Type => "SQL";

        public override string Content
        {
            get => _editor.Text;
            set
            {
                _editor.Text = value;
                Modified = true;
            }
        }

        public IDictionary<string, DataSource> DataSources { get; }

        public Action<string> ShowFetchXML { get; }

        public ConnectionDetail Connection => _con;
        
        public string Sql => String.IsNullOrEmpty(_editor.SelectedText) ? _editor.Text : _editor.SelectedText;

        protected override void OnClosing(CancelEventArgs e)
        {
            if (Busy)
            {
                MessageBox.Show(this, "Query is still executing. Please wait for the query to finish before closing this tab.", "Query Running", MessageBoxButtons.OK, MessageBoxIcon.Error);
                e.Cancel = true;
                return;
            }

            base.OnClosing(e);
        }

        internal void ChangeConnection(ConnectionDetail con)
        {
            Connect();

            _con = con;

            if (con != null)
            {
                hostLabel.Text = new Uri(_con.OrganizationServiceUrl).Host;
                orgNameLabel.Text = _con.Organization;

                toolStripStatusLabel.Text = "Connected";
                toolStripStatusLabel.Image = Properties.Resources.ConnectFilled_grey_16x;

                environmentHighlightLabel.BackColor = statusStrip.BackColor = con.EnvironmentHighlightingInfo?.Color ?? Color.Khaki;
                environmentHighlightLabel.ForeColor = statusStrip.ForeColor = con.EnvironmentHighlightingInfo?.TextColor ?? SystemColors.WindowText;
                environmentHighlightLabel.Text = con.EnvironmentHighlightingInfo?.Text ?? "";

                environmentHighlightLabel.Visible = con.EnvironmentHighlightingInfo != null;
            }
            else
            {
                hostLabel.Text = "";
                orgNameLabel.Text = "";

                toolStripStatusLabel.Text = "Disconnected";
                toolStripStatusLabel.Image = Properties.Resources.Disconnect_Filled_16x;

                statusStrip.BackColor = Color.Khaki;
                statusStrip.ForeColor = SystemColors.WindowText;

                environmentHighlightLabel.Visible = false;
            }

            SyncUsername();
            SyncTitle();
        }

        private void Connect()
        {
            if (_connection != null)
                return;

            if (DataSources.Count == 0)
                return;

            _connection = new Sql4CdsConnection(DataSources);
            _connection.ApplicationName = "XrmToolBox";
            _connection.InfoMessage += (s, msg) =>
            {
                Execute(() => ShowResult(msg.Statement, null, null, msg.Message.Message, null));
            };
        }

        protected override string GetTitle()
        {
            var text = DisplayName;

            if (Busy)
                text += " Executing...";

            if (Modified)
                text += " *";

            if (_con != null)
                text += $" ({_con.ConnectionName})";
            else
                text += " (Disconnected)";

            return text;
        }

        public void SetFocus()
        {
            _editor.Focus();
        }

        string ISaveableDocumentWindow.Filter => "SQL Scripts (*.sql)|*.sql";

        public void InsertText(string text)
        {
            _editor.ReplaceSelection(text);
            _editor.Focus();
        }

        public void Format()
        {
            _ai.TrackEvent("Format SQL", new Dictionary<string, string> { ["Source"] = "XrmToolBox" });

            _editor.Text = Formatter.Format(_editor.Text);
        }

        private Scintilla CreateEditor()
        {
            var scintilla = new Scintilla();

            // Reset the styles
            scintilla.StyleResetDefault();
            scintilla.Styles[Style.Default].Font = "Courier New";
            scintilla.Styles[Style.Default].Size = 10;
            scintilla.StyleClearAll();

            return scintilla;
        }

        private Scintilla CreateMessageEditor()
        {
            var scintilla = CreateEditor();

            scintilla.Lexer = Lexer.Null;
            scintilla.StyleClearAll();
            scintilla.Styles[1].ForeColor = Color.Red;
            scintilla.Styles[2].ForeColor = Color.Black;

            return scintilla;
        }

        private Scintilla CreateSqlEditor()
        {
            var scintilla = CreateEditor();

            // Set the SQL Lexer
            scintilla.Lexer = Lexer.Sql;

            // Show line numbers
            CalcLineNumberWidth(scintilla);

            // Set the Styles
            scintilla.Styles[Style.LineNumber].ForeColor = Color.FromArgb(255, 128, 128, 128);  //Dark Gray
            scintilla.Styles[Style.LineNumber].BackColor = Color.FromArgb(255, 228, 228, 228);  //Light Gray
            scintilla.Styles[Style.Sql.Comment].ForeColor = Color.Green;
            scintilla.Styles[Style.Sql.CommentLine].ForeColor = Color.Green;
            scintilla.Styles[Style.Sql.CommentLineDoc].ForeColor = Color.Green;
            scintilla.Styles[Style.Sql.Number].ForeColor = Color.Maroon;
            scintilla.Styles[Style.Sql.Word].ForeColor = Color.Blue;
            scintilla.Styles[Style.Sql.Word2].ForeColor = Color.Fuchsia;
            scintilla.Styles[Style.Sql.User1].ForeColor = Color.Gray;
            scintilla.Styles[Style.Sql.User2].ForeColor = Color.FromArgb(255, 00, 128, 192);    //Medium Blue-Green
            scintilla.Styles[Style.Sql.String].ForeColor = Color.Red;
            scintilla.Styles[Style.Sql.Character].ForeColor = Color.Red;
            scintilla.Styles[Style.Sql.Operator].ForeColor = Color.Black;

            // Set keyword lists
            // Word = 0
            scintilla.SetKeywords(0, @"add alter as authorization backup begin bigint binary bit break browse bulk by cascade case catch check checkpoint close clustered column commit compute constraint containstable continue create current cursor cursor database date datetime datetime2 datetimeoffset dbcc deallocate decimal declare default delete deny desc disk distinct distributed double drop dump else end errlvl escape except exec execute exit external fetch file fillfactor float for foreign freetext freetexttable from full function goto grant group having hierarchyid holdlock identity identity_insert identitycol if image index insert int intersect into key kill lineno load merge money national nchar nocheck nocount nolock nonclustered ntext numeric nvarchar of off offsets on open opendatasource openquery openrowset openxml option order over percent plan precision primary print proc procedure public raiserror read readtext real reconfigure references replication restore restrict return revert revoke rollback rowcount rowguidcol rule save schema securityaudit select set setuser shutdown smalldatetime smallint smallmoney sql_variant statistics table table tablesample text textsize then time timestamp tinyint to top tran transaction trigger truncate try union unique uniqueidentifier update updatetext use user values varbinary varchar varying view waitfor when where while with writetext xml go ");
            // Word2 = 1
            scintilla.SetKeywords(1, @"ascii cast char charindex ceiling coalesce collate contains convert current_date current_time current_timestamp current_user floor isnull max min nullif object_id session_user substring system_user tsequal ");
            // User1 = 4
            scintilla.SetKeywords(4, @"all and any between cross exists in inner is join left like not null or outer pivot right some unpivot ( ) * ");
            // User2 = 5
            scintilla.SetKeywords(5, @"sys objects sysobjects ");

            scintilla.Dock = DockStyle.Fill;

            scintilla.KeyDown += (s, e) =>
            {
                if (e.KeyCode == Keys.Back && scintilla.SelectedText == String.Empty)
                {
                    var lineIndex = scintilla.LineFromPosition(scintilla.SelectionStart);
                    var line = scintilla.Lines[lineIndex];
                    if (scintilla.SelectionStart == line.Position + line.Length && line.Text.EndsWith("    "))
                    {
                        scintilla.SelectionStart -= 4;
                        scintilla.SelectionEnd = scintilla.SelectionStart + 4;
                        scintilla.ReplaceSelection("");
                        e.Handled = true;
                    }
                }
                else if (e.KeyCode == Keys.Space && e.Control)
                {
                    _autocomplete.Show(_editor, true);
                    e.Handled = true;
                    e.SuppressKeyPress = true;
                }
                else if (e.KeyCode == Keys.F && e.Control)
                {
                    ShowFindControl();
                    e.Handled = true;
                    e.SuppressKeyPress = true;
                }
                else if (e.KeyCode == Keys.H && e.Control)
                {
                    ShowFindControl();
                    _findReplace.ShowReplace = true;
                    e.Handled = true;
                    e.SuppressKeyPress = true;
                }
                else if (e.KeyCode == Keys.Escape && _findReplace != null)
                {
                    _findReplace.Hide();
                    e.Handled = true;
                    e.SuppressKeyPress = true;
                }
                else if (e.KeyCode == Keys.F3 && _findReplace != null)
                {
                    if (e.Shift)
                        _findReplace.FindPrevious();
                    else
                        _findReplace.FindNext();

                    e.Handled = true;
                    e.SuppressKeyPress = true;
                }
            };

            // Auto-indent new lines
            // https://github.com/jacobslusser/ScintillaNET/issues/137
            scintilla.InsertCheck += (s, e) =>
            {
                if (e.Text.EndsWith("\r") || e.Text.EndsWith("\n"))
                {
                    var startPos = scintilla.Lines[scintilla.LineFromPosition(scintilla.CurrentPosition)].Position;
                    var endPos = e.Position;
                    var curLineText = scintilla.GetTextRange(startPos, (endPos - startPos)); // Text until the caret.
                    var indent = Regex.Match(curLineText, "^[ \t]*");
                    e.Text = (e.Text + indent.Value);
                }
            };

            // Define an indicator
            scintilla.Indicators[8].Style = IndicatorStyle.Squiggle;
            scintilla.Indicators[8].ForeColor = Color.Red;

            // Get ready for fill
            scintilla.IndicatorCurrent = 8;

            // Handle changes
            scintilla.TextChanged += (s, e) =>
            {
                Modified = true;
                CalcLineNumberWidth((Scintilla)s);
            };

            // Rectangular selections
            scintilla.MultipleSelection = true;
            scintilla.MouseSelectionRectangularSwitch = true;
            scintilla.AdditionalSelectionTyping = true;
            scintilla.VirtualSpaceOptions = VirtualSpace.RectangularSelection;

            // Tooltips
            _tooltip = new ToolTip();
            scintilla.DwellStart += (s, e) =>
            {
                _tooltip.Hide(scintilla);

                if (_con == null)
                    return;

                if (!Settings.Instance.ShowIntellisenseTooltips)
                    return;

                var pos = scintilla.CharPositionFromPoint(e.X, e.Y);
                var text = scintilla.Text;
                var wordEnd = new Regex("\\b").Match(text, pos);

                if (!wordEnd.Success)
                    return;

                var autocompleteDataSources = DataSources.Values
                    .Cast<XtbDataSource>()
                    .Select(ds =>
                    {
                        EntityCache.TryGetEntities(ds.ConnectionDetail.MetadataCacheLoader, ds.Connection, out var entities);

                        var metaEntities = MetaMetadataCache.GetMetadata();

                        if (entities == null)
                            entities = metaEntities.ToArray();
                        else
                            entities = entities.Concat(metaEntities).ToArray();

                        return new AutocompleteDataSource { Name = ds.Name, Entities = entities, Metadata = new MetaMetadataCache(ds.Metadata), Messages = ds.MessageCache };
                    })
                    .ToDictionary(ds => ds.Name, StringComparer.OrdinalIgnoreCase);

                var suggestions = new Autocomplete(autocompleteDataSources, _con.ConnectionName).GetSuggestions(text, wordEnd.Index - 1).ToList();
                var exactSuggestions = suggestions.Where(suggestion => suggestion.Text.Length <= wordEnd.Index && text.Substring(wordEnd.Index - suggestion.CompareText.Length, suggestion.CompareText.Length).Equals(suggestion.CompareText, StringComparison.OrdinalIgnoreCase)).ToList();

                if (exactSuggestions.Count == 1)
                {
                    if (!String.IsNullOrEmpty(exactSuggestions[0].ToolTipTitle) && !String.IsNullOrEmpty(exactSuggestions[0].ToolTipText))
                    {
                        _tooltip.ToolTipTitle = exactSuggestions[0].ToolTipTitle;
                        _tooltip.Show(exactSuggestions[0].ToolTipText, scintilla);
                    }
                    else if (!String.IsNullOrEmpty(exactSuggestions[0].ToolTipTitle))
                    {
                        _tooltip.ToolTipTitle = "";
                        _tooltip.Show(exactSuggestions[0].ToolTipTitle, scintilla);
                    }
                    else if (!String.IsNullOrEmpty(exactSuggestions[0].ToolTipText))
                    {
                        _tooltip.ToolTipTitle = "";
                        _tooltip.Show(exactSuggestions[0].ToolTipText, scintilla);
                    }
                }
            };
            scintilla.MouseDwellTime = (int)TimeSpan.FromSeconds(1).TotalMilliseconds;

            scintilla.GotFocus += CheckForNewVersion;

            return scintilla;
        }

        private void ShowFindControl()
        {
            if (_findReplace != null)
            {
                _findReplace.Visible = true;
                _findReplace.ShowFind();
                return;
            }

            _findReplace = new FindReplace(_editor);
            _findReplace.Left = ClientSize.Width - _findReplace.Width - 2;
            _findReplace.Top = 2;
            _findReplace.Anchor = AnchorStyles.Top | AnchorStyles.Right;
            Controls.Add(_findReplace);
            _findReplace.ShowFind();
            _findReplace.BringToFront();
        }

        private void CalcLineNumberWidth(Scintilla scintilla)
        {
            // Did the number of characters in the line number display change?
            // i.e. nnn VS nn, or nnnn VS nn, etc...
            var maxLineNumberCharLength = scintilla.Lines.Count.ToString().Length;
            if (maxLineNumberCharLength == _maxLineNumberCharLength)
                return;

            // Calculate the width required to display the last line number
            // and include some padding for good measure.
            const int padding = 2;
            scintilla.Margins[0].Width = scintilla.TextWidth(Style.LineNumber, new string('9', maxLineNumberCharLength + 1)) + padding;
            _maxLineNumberCharLength = maxLineNumberCharLength;
        }

        private AutocompleteMenu CreateAutocomplete()
        {
            var menu = new AutocompleteMenu();
            menu.MinFragmentLength = 1;
            menu.AllowsTabKey = true;
            menu.AppearInterval = 100;
            menu.TargetControlWrapper = new ScintillaWrapper(_editor);
            menu.Font = new Font(_editor.Styles[Style.Default].Font, _editor.Styles[Style.Default].SizeF);
            menu.ImageList = _images;
            menu.MaximumSize = new Size(1000, menu.MaximumSize.Height);

            menu.SetAutocompleteItems(new AutocompleteMenuItems(this));

            return menu;
        }

        class AutocompleteMenuItems : IEnumerable<AutocompleteItem>
        {
            private readonly SqlQueryControl _control;

            public AutocompleteMenuItems(SqlQueryControl control)
            {
                _control = control;
            }

            public IEnumerator<AutocompleteItem> GetEnumerator()
            {
                if (_control._con == null)
                    yield break;

                var pos = _control._editor.CurrentPosition - 1;

                if (pos == 0)
                    yield break;

                var text = _control._editor.Text;

                var autocompleteDataSources = _control.DataSources.Values
                    .Cast<XtbDataSource>()
                    .Select(ds =>
                    {
                        EntityCache.TryGetEntities(ds.ConnectionDetail.MetadataCacheLoader, ds.Connection, out var entities);

                        var metaEntities = MetaMetadataCache.GetMetadata();

                        if (entities == null)
                            entities = metaEntities.ToArray();
                        else
                            entities = entities.Concat(metaEntities).ToArray();

                        return new AutocompleteDataSource { Name = ds.Name, Entities = entities, Metadata = new MetaMetadataCache(ds.Metadata), Messages = ds.MessageCache };
                    })
                    .ToDictionary(ds => ds.Name, StringComparer.OrdinalIgnoreCase);

                var suggestions = new Autocomplete(autocompleteDataSources, _control.Connection.ConnectionName).GetSuggestions(text, pos).ToList();

                if (suggestions.Count == 0)
                    yield break;

                foreach (var suggestion in suggestions)
                    yield return suggestion;
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return this.GetEnumerator();
            }
        }

        public void Execute(bool execute, bool includeFetchXml)
        {
            if (Connection == null)
                return;

            if (backgroundWorker.IsBusy)
                return;

            var offset = String.IsNullOrEmpty(_editor.SelectedText) ? 0 : _editor.SelectionStart;

            _editor.IndicatorClearRange(0, _editor.TextLength);

            var sql = _editor.SelectedText;

            if (String.IsNullOrEmpty(sql))
                sql = _editor.Text;

            _params = new ExecuteParams { Sql = sql, Execute = execute, IncludeFetchXml = includeFetchXml, Offset = offset };
            backgroundWorker.RunWorkerAsync(_params);
        }

        public bool Busy => backgroundWorker.IsBusy;

        public event EventHandler BusyChanged;

        public bool Cancellable
        {
            get { return _cancellable; }
            set
            {
                if (Cancellable == value)
                    return;

                _cancellable = value;
                CancellableChanged?.Invoke(this, EventArgs.Empty);
            }
        }

        public event EventHandler CancellableChanged;

        public void Cancel()
        {
            backgroundWorker.ReportProgress(0, "Cancelling query...");
            _cancelled = true;
            _command.Cancel();
        }

        private void AddResult(Control results, int rowCount)
        {
            if (!tabControl.TabPages.Contains(resultsTabPage))
            {
                tabControl.TabPages.Insert(0, resultsTabPage);
                tabControl.SelectedTab = resultsTabPage;
            }

            AddControl(results, resultsTabPage);

            _rowCount += rowCount;

            if (_rowCount == 1)
                rowsLabel.Text = "1 row";
            else
                rowsLabel.Text = $"{_rowCount:N0} rows";
        }

        private void AddExecutionPlan(Control executionPlan)
        {
            if (!tabControl.TabPages.Contains(fetchXmlTabPage))
                tabControl.TabPages.Insert(tabControl.TabPages.Count - 1, fetchXmlTabPage);

            AddControl(executionPlan, fetchXmlTabPage);
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

        private void copyToolStripMenuItem_Click(object sender, EventArgs e)
        {
            var grid = (DataGridView)gridContextMenuStrip.SourceControl;

            var content = CopyGrid(grid);

            if (content != null)
                Clipboard.SetDataObject(content);
        }

        private void copyWithHeadersToolStripMenuItem_Click(object sender, EventArgs e)
        {
            var grid = (DataGridView)gridContextMenuStrip.SourceControl;
            grid.ClipboardCopyMode = DataGridViewClipboardCopyMode.EnableAlwaysIncludeHeaderText;

            var content = CopyGrid(grid);

            if (content != null)
                Clipboard.SetDataObject(content);
            else
                Clipboard.SetDataObject(String.Join("\t", grid.Columns.Cast<DataGridViewColumn>().Select(col => col.HeaderText)));

            grid.ClipboardCopyMode = DataGridViewClipboardCopyMode.EnableWithoutHeaderText;
        }

        private void backgroundWorker_RunWorkerCompleted(object sender, RunWorkerCompletedEventArgs e)
        {
            var cancelled = e.Cancelled || (Cancellable && _cancelled && e.Error is Sql4CdsException);

            _progressHost.Visible = false;
            _stopwatch.Stop();
            timer.Enabled = false;
            Cancellable = false;
            _cancelled = false;
            _command = null;

            if (cancelled)
            {
                toolStripStatusLabel.Image = Properties.Resources.StatusStop_16x;
                toolStripStatusLabel.Text = "Query cancelled";
            }
            else if (e.Error != null)
            {
                toolStripStatusLabel.Image = Properties.Resources.StatusWarning_16x;
                toolStripStatusLabel.Text = "Query completed with errors";
            }
            else
            {
                toolStripStatusLabel.Image = Properties.Resources.StatusOK_16x;
                toolStripStatusLabel.Text = "Query executed successfully";
            }

            if (e.Error != null)
            {
                var error = e.Error;
                var index = -1;
                var length = 0;
                var messageSuffix = "";
                IRootExecutionPlanNode plan = null;

                if (error is Sql4CdsException sql4CdsException)
                {
                    if (sql4CdsException.Errors != null)
                    {
                        var lines = _params.Sql.Split('\n');

                        foreach (var sql4CdsError in sql4CdsException.Errors)
                        {
                            var errorIndex = lines.Take(sql4CdsError.LineNumber - 1).Sum(l => l.Length + 1) + _params.Offset;
                            var errorLength = lines[sql4CdsError.LineNumber - 1].Length;

                            var parts = new List<string>
                            {
                                $"Msg {sql4CdsError.Number}",
                                $"Level {sql4CdsError.Class}",
                                $"State {sql4CdsError.State}"
                            };

                            if (sql4CdsError.Procedure != null)
                            {
                                parts.Add($"Procedure {sql4CdsError.Procedure}");
                                parts.Add($"Line 0 [Batch Start Line {sql4CdsError.LineNumber + _editor.LineFromPosition(_params.Offset)}]");
                            }
                            else
                            {
                                parts.Add($"Line {sql4CdsError.LineNumber + _editor.LineFromPosition(_params.Offset)}");
                            }

                            AddMessage(errorIndex, errorLength, String.Join(", ", parts), MessageType.ErrorPrefix);
                        }
                    }

                    if (sql4CdsException.InnerException != null)
                        error = sql4CdsException.InnerException;
                }

                if (error is QueryException queryException)
                {
                    plan = queryException.Query;
                    index = _params.Offset + queryException.Query.Index;
                    length = queryException.Query.Length;
                    error = queryException.InnerException;
                }

                if (error is QueryExecutionException queryExecution)
                {
                    if (plan == null)
                        plan = GetRootNode(queryExecution.Node);

                    messageSuffix = "\r\nSee the Execution Plan tab for details of where this error occurred";
                    ShowResult(plan, new ExecuteParams { Execute = true, IncludeFetchXml = true, Sql = plan?.Sql }, null, null, queryExecution);
                    index = _params.Offset + plan.Index;
                    length = plan.Length;

                    if (queryExecution.InnerException != null)
                        error = queryExecution.InnerException;
                }

                if (error is NotSupportedQueryFragmentException err && err.Fragment != null)
                {
                    _editor.IndicatorFillRange(_params.Offset + err.Fragment.StartOffset, err.Fragment.FragmentLength);
                    index = _params.Offset + err.Fragment.StartOffset;
                    length = err.Fragment.FragmentLength;

                    if (!String.IsNullOrEmpty(err.Suggestion))
                        messageSuffix = "\r\n" + err.Suggestion;
                }
                else if (error is QueryParseException parseErr)
                {
                    index = _params.Offset + parseErr.Error.Offset;
                    length = new Regex("\\b").Matches(_params.Sql.Substring(parseErr.Error.Offset)).Cast<Match>().Skip(1).FirstOrDefault()?.Index ?? 0;
                    _editor.IndicatorFillRange(_params.Offset + parseErr.Error.Offset, length);
                }
                else if (error is PartialSuccessException partialSuccess)
                {
                    if (partialSuccess.Result is string msg)
                        AddMessage(index, length, msg, MessageType.Info);

                    error = partialSuccess.InnerException;
                }

                _log(e.Error.ToString());

                AddMessage(index, length, GetErrorMessage(error) + messageSuffix, MessageType.Error);

                tabControl.SelectedTab = messagesTabPage;
            }
            else if (!_params.Execute)
            {
                tabControl.SelectedTab = fetchXmlTabPage;
            }

            AddMessage(-1, -1, $"Completion time: {DateTimeOffset.Now:O}", MessageType.Info);

            BusyChanged?.Invoke(this, EventArgs.Empty);

            _editor.Focus();
        }

        private IRootExecutionPlanNode GetRootNode(IExecutionPlanNode node)
        {
            while (node != null)
            {
                if (node is IRootExecutionPlanNode root)
                    return root;

                node = node.Parent;
            }

            return null;
        }

        private string GetErrorMessage(Exception error)
        {
            string msg;

            if (error is AggregateException aggregateException)
                msg = String.Join("\r\n", aggregateException.InnerExceptions.Select(ex => GetErrorMessage(ex)));
            else
                msg = error.Message;

            while (error.InnerException != null)
            {
                if (error.InnerException.Message != error.Message)
                    msg += "\r\n" + error.InnerException.Message;

                error = error.InnerException;
            }

            if (error is FaultException<OrganizationServiceFault> faultEx)
            {
                var fault = faultEx.Detail;

                if (fault.Message != error.Message)
                    msg += "\r\n" + fault.Message;

                if (fault.ErrorDetails.TryGetValue("Plugin.ExceptionFromPluginExecute", out var plugin))
                    msg += "\r\nError from plugin: " + plugin;

                if (!String.IsNullOrEmpty(fault.TraceText))
                    msg += "\r\nTrace log: " + fault.TraceText;

                while (fault.InnerFault != null)
                {
                    if (fault.InnerFault.Message != fault.Message)
                        msg += "\r\n" + fault.InnerFault.Message;

                    fault = fault.InnerFault;

                    if (fault.ErrorDetails.TryGetValue("Plugin.ExceptionFromPluginExecute", out plugin))
                        msg += "\r\nError from plugin: " + plugin;

                    if (!String.IsNullOrEmpty(fault.TraceText))
                        msg += "\r\nTrace log: " + fault.TraceText;
                }
            }

            return msg;
        }

        private void AddMessage(int index, int length, string message, MessageType messageType)
        {
            var scintilla = (Scintilla)messagesTabPage.Controls[0];
            var line = scintilla.Lines.Count - 1;
            scintilla.ReadOnly = false;
            scintilla.AppendText(message + (messageType == MessageType.ErrorPrefix ? "\r\n" : "\r\n\r\n"));
            scintilla.StartStyling(scintilla.Text.Length - message.Length - 4);
            scintilla.SetStyling(message.Length, messageType == MessageType.Info ? 2 : 1);
            scintilla.ReadOnly = true;

            if (index != -1)
            {
                foreach (var l in message.Split('\n'))
                    _messageLocations[line++] = new TextRange(index, length);
            }
        }

        enum MessageType
        {
            Info,
            Error,
            ErrorPrefix,
        }

        private void NavigateToMessage(object sender, DoubleClickEventArgs e)
        {
            if (_messageLocations.TryGetValue(e.Line, out var textRange))
            {
                _editor.SelectionStart = textRange.Index;
                _editor.SelectionEnd = textRange.Index + textRange.Length;
                _editor.Focus();
                _editor.ScrollCaret();
            }
        }

        private void Execute(Action action)
        {
            if (InvokeRequired)
                Invoke(action);
            else
                action();
        }

        private void backgroundWorker_DoWork(object sender, DoWorkEventArgs e)
        {
            BusyChanged?.Invoke(this, EventArgs.Empty);

            var args = (ExecuteParams)e.Argument;

            Execute(() =>
            {
                _progressHost.Visible = true;
                timerLabel.Text = "00:00:00";
                _stopwatch.Restart();
                timer.Enabled = true;
                _rowCount = 0;
                rowsLabel.Text = "0 rows";

                tabControl.TabPages.Remove(resultsTabPage);
                tabControl.TabPages.Remove(fetchXmlTabPage);

                resultsFlowLayoutPanel.Controls.Clear();
                fetchXMLFlowLayoutPanel.Controls.Clear();

                if (messagesTabPage.Controls.Count == 0)
                {
                    messagesTabPage.Controls.Add(CreateMessageEditor());
                    ((Scintilla)messagesTabPage.Controls[0]).Dock = DockStyle.Fill;
                    ((Scintilla)messagesTabPage.Controls[0]).DoubleClick += NavigateToMessage;
                }

                ((Scintilla)messagesTabPage.Controls[0]).ReadOnly = false;
                ((Scintilla)messagesTabPage.Controls[0]).Text = "";
                ((Scintilla)messagesTabPage.Controls[0]).StartStyling(0);
                ((Scintilla)messagesTabPage.Controls[0]).ReadOnly = true;
                _messageLocations = new Dictionary<int, TextRange>();

                splitContainer.Panel2Collapsed = false;
            });

            backgroundWorker.ReportProgress(0, "Executing query...");

            using (var cmd = _connection.CreateCommand())
            using (var options = new QueryExecutionOptions(this, backgroundWorker, _connection, cmd))
            {
                _connection.ChangeDatabase(Connection.ConnectionName);
                options.ApplySettings(args.Execute);
                cmd.CommandTimeout = 0;

                cmd.CommandText = args.Sql;

                if (args.Execute)
                {
                    _command = cmd;
                    Execute(() => Cancellable = true);

                    cmd.StatementCompleted += (s, stmt) =>
                    {
                        Execute(() => ShowResult(stmt.Statement, args, null, null, null));

                        if (stmt.Message != null)
                            Execute(() => ShowResult(stmt.Statement, args, null, stmt.Message, null));

                        if (stmt.Statement is IImpersonateRevertExecutionPlanNode)
                            Execute(() => SyncUsername());
                    };

                    using (var reader = cmd.ExecuteReader())
                    {
                        while (!reader.IsClosed)
                        {
                            var columnNames = new List<string>();
                            for (var i = 0; i < reader.FieldCount; i++)
                                columnNames.Add(reader.GetName(i));

                            var dataTable = new DataTable();
                            var schemaTable = reader.GetSchemaTable();
                            var constraintError = false;

                            try
                            {
                                dataTable.Load(reader);
                            }
                            catch (ConstraintException ex)
                            {
                                constraintError = true;
                                foreach (DataRow row in dataTable.Rows)
                                {
                                    if (row.RowError != null)
                                        throw new ConstraintException(row.RowError, ex);
                                }

                                throw;
                            }
                            finally
                            {
                                if (!constraintError)
                                {
                                    for (var i = 0; i < schemaTable.Rows.Count; i++)
                                        dataTable.Columns[i].ExtendedProperties["Schema"] = schemaTable.Rows[i];

                                    for (var i = 0; i < columnNames.Count; i++)
                                        dataTable.Columns[i].Caption = String.IsNullOrEmpty(columnNames[i]) ? "(No column name)" : columnNames[i];

                                    Execute(() => ShowResult(null, args, dataTable, null, null));
                                }
                            }
                        }
                    }
                }
                else
                {
                    var plan = cmd.GeneratePlan(false);

                    foreach (var query in plan)
                        Execute(() => ShowResult(query, args, null, null, null));
                }
            }
        }

        private void ShowResult(IRootExecutionPlanNode query, ExecuteParams args, DataTable results, string msg, QueryExecutionException ex)
        {
            if (results != null)
            {
                var grid = new DataGridView();

                grid.AllowUserToAddRows = false;
                grid.AllowUserToDeleteRows = false;
                grid.AllowUserToOrderColumns = true;
                grid.AllowUserToResizeRows = false;
                grid.AlternatingRowsDefaultCellStyle = new DataGridViewCellStyle { BackColor = Color.WhiteSmoke };
                grid.AutoGenerateColumns = false;
                grid.BackgroundColor = SystemColors.Window;
                grid.BorderStyle = BorderStyle.None;
                grid.CellBorderStyle = DataGridViewCellBorderStyle.None;
                grid.ClipboardCopyMode = DataGridViewClipboardCopyMode.EnableWithoutHeaderText;
                grid.ColumnHeadersHeightSizeMode = DataGridViewColumnHeadersHeightSizeMode.DisableResizing;
                grid.EnableHeadersVisualStyles = false;
                grid.ReadOnly = true;
                grid.RowHeadersWidth = 24;
                grid.ShowEditingIcon = false;
                grid.ContextMenuStrip = gridContextMenuStrip;
                grid.DataSource = results;

                foreach (DataColumn col in results.Columns)
                {
                    grid.Columns.Add(new DataGridViewTextBoxColumn
                    {
                        DataPropertyName = col.ColumnName,
                        HeaderText = col.Caption,
                        ValueType = col.DataType,
                        FillWeight = 1,
                    });
                }

                if (_linkFont == null)
                    _linkFont = new Font(grid.Font, grid.Font.Style | FontStyle.Underline);

                grid.CellFormatting += FormatCell;

                var specialPaintingChars = new[] { '\r', '\n', '\t' };
                grid.CellPainting += (s, e) =>
                {
                    if (e.Value is string str && str.IndexOfAny(specialPaintingChars) != -1)
                    {
                        e.PaintBackground(e.ClipBounds, true);

                        var gv = (DataGridView)s;
                        var text = str.Replace('\r', ' ').Replace('\n', ' ').Replace('\t', ' ');
                        var rect = e.CellBounds;
                        rect.X += BorderThickness(e.AdvancedBorderStyle.Left);
                        rect.Y += BorderThickness(e.AdvancedBorderStyle.Top);
                        rect.Width -= BorderThickness(e.AdvancedBorderStyle.Right) + BorderThickness(e.AdvancedBorderStyle.Left) + gv.Columns[e.ColumnIndex].DividerWidth;
                        rect.Height -= BorderThickness(e.AdvancedBorderStyle.Bottom) + BorderThickness(e.AdvancedBorderStyle.Top) + gv.Rows[e.RowIndex].DividerHeight;
                        rect.X += e.CellStyle.Padding.Left;
                        rect.Y += e.CellStyle.Padding.Top;
                        rect.Width -= e.CellStyle.Padding.Horizontal + e.CellStyle.Padding.Left;
                        rect.Height -= e.CellStyle.Padding.Vertical + e.CellStyle.Padding.Top;
                        var selected = (e.State & DataGridViewElementStates.Selected) != 0;
                        var textFormatFlags = TextFormatFlags.VerticalCenter | TextFormatFlags.SingleLine | TextFormatFlags.NoPrefix | TextFormatFlags.PreserveGraphicsClipping | TextFormatFlags.EndEllipsis;
                        TextRenderer.DrawText(e.Graphics, text, e.CellStyle.Font, rect, selected ? e.CellStyle.SelectionForeColor : e.CellStyle.ForeColor, textFormatFlags);
                        e.Handled = true;
                    }
                };

                grid.CellMouseEnter += (s, e) =>
                {
                    if (e.RowIndex < 0 || e.ColumnIndex < 0)
                        return;

                    var gv = (DataGridView)s;
                    var cell = gv.Rows[e.RowIndex].Cells[e.ColumnIndex];

                    if (cell.Value is SqlEntityReference er && !er.IsNull ||
                        cell.Value is SqlXml xml && !xml.IsNull)
                        gv.Cursor = Cursors.Hand;
                    else
                        gv.Cursor = Cursors.Default;
                };

                grid.CellMouseLeave += (s, e) =>
                {
                    var gv = (DataGridView)s;
                    gv.Cursor = Cursors.Default;
                };

                grid.CellMouseDown += (s, e) =>
                {
                    if (e.RowIndex < 0 || e.ColumnIndex < 0)
                        return;

                    if (e.Button != MouseButtons.Right)
                        return;

                    var gv = (DataGridView)s;
                    var cell = gv.Rows[e.RowIndex].Cells[e.ColumnIndex];

                    if (cell.Selected)
                        return;

                    gv.CurrentCell = cell;
                };

                grid.CellContentDoubleClick += (s, e) =>
                {
                    if (e.RowIndex < 0 || e.ColumnIndex < 0)
                        return;

                    var gv = (DataGridView)s;
                    var cell = gv.Rows[e.RowIndex].Cells[e.ColumnIndex];

                    if (cell.Value is SqlEntityReference er && !er.IsNull)
                        OpenRecord(er);
                    else if (cell.Value is SqlXml xml && !xml.IsNull)
                        ShowFetchXML(xml.Value);
                };

                if (Settings.Instance.AutoSizeColumns)
                    grid.DataBindingComplete += (s, e) => ((DataGridView)s).AutoResizeColumns();

                grid.RowPostPaint += (s, e) =>
                {
                    var rowIdx = (e.RowIndex + 1).ToString();

                    var centerFormat = new System.Drawing.StringFormat()
                    {
                        Alignment = StringAlignment.Far,
                        LineAlignment = StringAlignment.Center,
                        FormatFlags = StringFormatFlags.NoWrap | StringFormatFlags.NoClip,
                        Trimming = StringTrimming.EllipsisCharacter
                    };

                    var headerBounds = new Rectangle(e.RowBounds.Left, e.RowBounds.Top, grid.RowHeadersWidth - 2, e.RowBounds.Height);
                    e.Graphics.DrawString(rowIdx, this.Font, SystemBrushes.ControlText, headerBounds, centerFormat);
                };

                var rowCount = results.Rows.Count;

                AddResult(grid, rowCount);
            }
            else if (msg != null)
            {
                AddMessage(query?.Index ?? -1, query?.Length ?? 0, msg, MessageType.Info);
            }
            else if (args.IncludeFetchXml)
            {
                var plan = new Panel();
                var fetchLabel = new System.Windows.Forms.Label
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
                var planView = new ExecutionPlanView { Dock = DockStyle.Fill, Executed = args.Execute, Exception = ex, DataSources = DataSources.ToDictionary(kvp => kvp.Key, kvp => (Engine.DataSource)kvp.Value) };
                planView.Plan = query;
                planView.NodeSelected += (s, e) => _properties.SelectObject(planView.Selected, !args.Execute);
                planView.DoubleClick += (s, e) =>
                {
                    if (planView.Selected is IFetchXmlExecutionPlanNode fetchXml)
                        ShowFetchXML(fetchXml.FetchXmlString);
                };
                plan.Controls.Add(planView);
                plan.Controls.Add(fetchLabel);

                AddExecutionPlan(plan);
            }
        }

        private void FormatCell(object sender, DataGridViewCellFormattingEventArgs e)
        {
            var grid = (DataGridView)sender;
            var results = (DataTable)grid.DataSource;

            if (e.Value is DBNull || (e.Value is INullable nullable && nullable.IsNull))
            {
                e.Value = "NULL";

                if (e.CellStyle != null)
                    e.CellStyle.BackColor = Color.FromArgb(0xff, 0xff, 0xe1);
                
                e.FormattingApplied = true;
            }
            else if (e.Value is bool b)
            {
                e.Value = b ? "1" : "0";
                e.FormattingApplied = true;
            }
            else if (e.Value is DateTime dt)
            {
                var schema = (DataRow)results.Columns[e.ColumnIndex].ExtendedProperties["Schema"];
                var type = (string)schema["DataTypeName"];

                if (type == "date")
                {
                    if (Settings.Instance.LocalFormatDates)
                        e.Value = dt.ToShortDateString();
                    else
                        e.Value = dt.ToString("yyyy-MM-dd");

                    e.FormattingApplied = true;
                }
                else if (type == "smalldatetime")
                {
                    if (Settings.Instance.LocalFormatDates)
                        e.Value = dt.ToShortDateString() + " " + dt.ToString("HH:mm");
                    else
                        e.Value = dt.ToString("yyyy-MM-dd HH:mm");

                    e.FormattingApplied = true;
                }
                else if (!Settings.Instance.LocalFormatDates)
                {
                    var scale = (short)schema["NumericScale"];
                    e.Value = dt.ToString("yyyy-MM-dd HH:mm:ss" + (scale == 0 ? "" : ("." + new string('f', scale))));
                    e.FormattingApplied = true;
                }
            }
            else if (e.Value is TimeSpan ts && !Settings.Instance.LocalFormatDates)
            {
                var schema = (DataRow)results.Columns[e.ColumnIndex].ExtendedProperties["Schema"];
                var scale = (short)schema["NumericScale"];
                e.Value = ts.ToString("hh\\:mm\\:ss" + (scale == 0 ? "" : ("\\." + new string('f', scale))));
                e.FormattingApplied = true;
            }
            else if (e.Value is decimal dec)
            {
                var schema = (DataRow)results.Columns[e.ColumnIndex].ExtendedProperties["Schema"];
                var scale = (short)schema["NumericScale"];
                e.Value = dec.ToString("0" + (scale == 0 ? "" : ("." + new string('0', scale))));
                e.FormattingApplied = true;
            }
            else if (e.Value is SqlEntityReference)
            {
                if (e.CellStyle != null)
                {
                    e.CellStyle.ForeColor = SystemColors.HotTrack;
                    e.CellStyle.Font = _linkFont;
                }
            }
            else if (e.Value is SqlXml xml)
            {
                if (e.CellStyle != null)
                {
                    e.CellStyle.ForeColor = SystemColors.HotTrack;
                    e.CellStyle.Font = _linkFont;
                }

                e.Value = xml.Value;
                e.FormattingApplied = true;
            }
        }

        private int BorderThickness(DataGridViewAdvancedCellBorderStyle style)
        {
            switch (style)
            {
                case DataGridViewAdvancedCellBorderStyle.None:
                    return 0;
                case DataGridViewAdvancedCellBorderStyle.InsetDouble:
                case DataGridViewAdvancedCellBorderStyle.OutsetDouble:
                    return 2;
                default:
                    return 1;
            }
        }

        private string GetRecordUrl(SqlEntityReference entityReference, out XtbDataSource dataSource)
        {
            dataSource = null;

            if (!DataSources.TryGetValue(entityReference.DataSource, out var ds))
                return null;

            dataSource = (XtbDataSource)ds;
            return dataSource.ConnectionDetail.GetEntityReferenceUrl(entityReference);
        }

        private void OpenRecord(SqlEntityReference entityReference)
        {
            var url = GetRecordUrl(entityReference, out var dataSource);

            if (url == null)
                return;

            dataSource.ConnectionDetail.OpenUrlWithBrowserProfile(new Uri(url));
        }

        private void backgroundWorker_ProgressChanged(object sender, ProgressChangedEventArgs e)
        {
            toolStripStatusLabel.Image = null;
            toolStripStatusLabel.Text = (string) e.UserState;
        }

        private void timer_Tick(object sender, EventArgs e)
        {
            timerLabel.Text = _stopwatch.Elapsed.ToString(@"hh\:mm\:ss");
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

        private int GetMinHeight(Control control, int max)
        {
            if (control is DataGridView grid)
            {
                var rowCount = grid.Rows.Count;

                if (rowCount == 0 && grid.DataSource is EntityCollection entities)
                    rowCount = entities.Entities.Count;
                else if (rowCount == 0 && grid.DataSource is DataTable table)
                    rowCount = table.Rows.Count;
                else if (rowCount == 0 && grid.DataSource == null)
                    grid.DataBindingComplete += (sender, args) => grid.Height = Math.Min(Math.Max(grid.Height, GetMinHeight(grid, max)), max);

                return Math.Min(Math.Max(2, rowCount + 1) * grid.ColumnHeadersHeight + SystemInformation.HorizontalScrollBarHeight, max);
            }

            if (control is Scintilla scintilla)
                return (int) ((scintilla.Lines.Count + 1) * scintilla.Styles[Style.Default].Size * 1.6) + 20;

            if (control is Panel panel)
                return panel.Controls.OfType<Control>().Sum(child => GetMinHeight(child, max));

            if (control is ExecutionPlanView plan)
                return plan.AutoScrollMinSize.Height;

            return control.Height;
        }

        private void SyncUsername()
        {
            if (Connection == null)
            {
                usernameDropDownButton.Text = "";
                usernameDropDownButton.Image = null;
                revertToolStripMenuItem.Enabled = false;
            }
            else
            {
                var service = (CrmServiceClient)DataSources[Connection.ConnectionName].Connection;
                
                if (service.CallerId == Guid.Empty)
                {
                    usernameDropDownButton.Text = _con.UserName;
                    usernameDropDownButton.Image = null;
                    revertToolStripMenuItem.Enabled = false;
                }
                else
                {
                    var user = service.Retrieve("systemuser", service.CallerId, new ColumnSet("domainname"));

                    usernameDropDownButton.Text = user.GetAttributeValue<string>("domainname");
                    usernameDropDownButton.Image = Properties.Resources.StatusWarning_16x;
                    revertToolStripMenuItem.Enabled = true;
                }
            }
        }

        private void impersonateMenuItem_Click(object sender, EventArgs e)
        {
            using (var dlg = new CDSLookupDialog())
            {
                var service = (CrmServiceClient)DataSources[Connection.ConnectionName].Connection;

                dlg.Service = service;
                dlg.Metadata = DataSources[Connection.ConnectionName].Metadata;
                dlg.LogicalName = "systemuser";

                if (dlg.ShowDialog(this) == DialogResult.OK)
                {
                    _ai.TrackEvent("Execute", new Dictionary<string, string> { ["QueryType"] = "ExecuteAsNode", ["Source"] = "XrmToolBox" });
                    service.CallerId = dlg.Entity.Id;
                    SyncUsername();
                }
            }
        }

        private void revertToolStripMenuItem_Click(object sender, EventArgs e)
        {
            var service = (CrmServiceClient)DataSources[Connection.ConnectionName].Connection;

            _ai.TrackEvent("Execute", new Dictionary<string, string> { ["QueryType"] = "RevertNode", ["Source"] = "XrmToolBox" });
            service.CallerId = Guid.Empty;
            SyncUsername();
        }

        private void gridContextMenuStrip_Opening(object sender, CancelEventArgs e)
        {
            var grid = (DataGridView)gridContextMenuStrip.SourceControl;

            openRecordToolStripMenuItem.Enabled = false;
            copyRecordUrlToolStripMenuItem.Enabled = false;
            createSELECTStatementToolStripMenuItem.Enabled = false;

            if (grid.CurrentCell?.Value is SqlEntityReference er && !er.IsNull)
            {
                openRecordToolStripMenuItem.Enabled = true;
                copyRecordUrlToolStripMenuItem.Enabled = true;
                createSELECTStatementToolStripMenuItem.Enabled = true;
            }

            copyToolStripMenuItem.Enabled = grid.Rows.Count > 0;
        }

        private void openRecordToolStripMenuItem_Click(object sender, EventArgs e)
        {
            var grid = (DataGridView)gridContextMenuStrip.SourceControl;

            if (grid.CurrentCell?.Value is SqlEntityReference er && !er.IsNull)
                OpenRecord(er);
        }

        private void copyRecordUrlToolStripMenuItem_Click(object sender, EventArgs e)
        {
            var grid = (DataGridView)gridContextMenuStrip.SourceControl;

            if (grid.CurrentCell?.Value is SqlEntityReference er && !er.IsNull)
            {
                var url = GetRecordUrl(er, out _);

                if (url != null)
                    Clipboard.SetText(url);
            }
        }

        private void createSELECTStatementToolStripMenuItem_Click(object sender, EventArgs e)
        {
            var grid = (DataGridView)gridContextMenuStrip.SourceControl;

            if (grid.CurrentCell?.Value is SqlEntityReference er && !er.IsNull && DataSources.TryGetValue(er.DataSource, out var dataSource))
            {
                var select = "\r\n\r\nSELECT * FROM ";

                if (er.DataSource != _con.ConnectionName)
                    select += $"{Autocomplete.SqlAutocompleteItem.EscapeIdentifier(er.DataSource)}.dbo.";

                select += Autocomplete.SqlAutocompleteItem.EscapeIdentifier(er.LogicalName);

                var metadata = dataSource.Metadata[er.LogicalName];

                select += $" WHERE {Autocomplete.SqlAutocompleteItem.EscapeIdentifier(metadata.PrimaryIdAttribute)} = '{er.Id}'";

                _editor.AppendText(select);
            }
        }

        class CellCoordinate : IComparable<CellCoordinate>, IEquatable<CellCoordinate>
        {
            public int RowIndex { get; set; }
            public int ColumnIndex { get; set; }

            public int CompareTo(CellCoordinate other)
            {
                var result = RowIndex.CompareTo(other.RowIndex);

                if (result != 0)
                    return result;

                return ColumnIndex.CompareTo(other.ColumnIndex);
            }

            public bool Equals(CellCoordinate other)
            {
                return RowIndex == other.RowIndex && ColumnIndex == other.ColumnIndex;
            }
        }

        protected override bool ProcessCmdKey(ref System.Windows.Forms.Message msg, Keys keyData)
        {
            if (keyData == (Keys.Control | Keys.K))
            {
                _ctrlK = true;
                return true;
            }
            else if (keyData == (Keys.Control | Keys.C) && GetFocusedControl(this) is DataGridView grid && grid.DataSource is DataTable table)
            {
                var data = CopyGrid(grid);

                if (data != null)
                    Clipboard.SetDataObject(data);

                return true;
            }
            else if (_ctrlK)
            {
                _ctrlK = false;

                if (keyData == (Keys.Control | Keys.C))
                {
                    // Comment
                    var startLine = _editor.LineFromPosition(_editor.SelectionStart);
                    var endLine = _editor.LineFromPosition(_editor.SelectionEnd);

                    for (var line = startLine; line <= endLine; line++)
                    {
                        _editor.TargetStart = _editor.Lines[line].Position;
                        _editor.TargetEnd = _editor.TargetStart;
                        _editor.ReplaceTarget("--");
                    }

                    return true;
                }
                else if (keyData == (Keys.Control | Keys.U))
                {
                    // Uncomment
                    var startLine = _editor.LineFromPosition(_editor.SelectionStart);
                    var endLine = _editor.LineFromPosition(_editor.SelectionEnd);

                    for (var line = startLine; line <= endLine; line++)
                    {
                        if (_editor.Lines[line].Text.StartsWith("--"))
                        {
                            _editor.TargetStart = _editor.Lines[line].Position;
                            _editor.TargetEnd = _editor.TargetStart + 2;
                            _editor.ReplaceTarget(string.Empty);
                        }
                    }

                    return true;
                }
            }

            return base.ProcessCmdKey(ref msg, keyData);
        }

        private DataObject CopyGrid(DataGridView grid)
        {
            if (!(grid.DataSource is DataTable table) ||
                grid.SortedColumn != null)
            {
                // The data source should be a data table. If not, use the default logic
                return grid.GetClipboardContent();
            }
            else
            {
                // The standard ways for getting the selection are very slow for large data sets as the DataGridView copies a lot of
                // settings on each property access and spends a lot of time setting up formatting etc. that we don't care about for
                // accessing the raw data. Use reflection to access the internal state of the grid instead. This is made up of two
                // parts - selectedBandIndexes is a list of row indexes for rows that are completely selected, and individualSelectedCells
                // are the additional cells that are selected not as part of a full row. Combine these to work out if each individual
                // cell is selected
                var allSelected = grid.AreAllCellsSelected(true);
                var selectedRows = allSelected ? null : (IEnumerable)grid.GetType().GetField("selectedBandIndexes", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance).GetValue(grid);
                var rowIndexes = selectedRows
                    ?.Cast<int>()
                    ?.ToList();
                rowIndexes?.Sort();
                var individualSelectedCells = allSelected ? null : (IEnumerable)grid.GetType().GetField("individualSelectedCells", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance).GetValue(grid);
                var cellCoords = individualSelectedCells
                    ?.Cast<DataGridViewCell>()
                    ?.Select(c => new CellCoordinate { RowIndex = c.RowIndex, ColumnIndex = c.ColumnIndex })
                    ?.ToList();
                cellCoords?.Sort();

                // Work out the min and max column and row indexes for the bounds of the selection
                var minRow = allSelected ? 0 : Math.Min(cellCoords == null || cellCoords.Count == 0 ? Int32.MaxValue : cellCoords.Min(c => c.RowIndex), rowIndexes == null || rowIndexes.Count == 0 ? Int32.MaxValue : rowIndexes[0]);
                var maxRow = allSelected ? table.Rows.Count - 1 : Math.Max(cellCoords == null || cellCoords.Count == 0 ? Int32.MinValue : cellCoords.Max(c => c.RowIndex), rowIndexes == null || rowIndexes.Count == 0 ? Int32.MinValue : rowIndexes[rowIndexes.Count - 1]);
                var minCol = allSelected || rowIndexes.Count > 0 ? 0 : cellCoords == null || cellCoords.Count == 0 ? Int32.MaxValue : cellCoords.Min(c => c.ColumnIndex);
                var maxCol = allSelected || rowIndexes.Count > 0 ? table.Columns.Count - 1 : cellCoords == null || cellCoords.Count == 0 ? Int32.MinValue : cellCoords.Max(c => c.ColumnIndex);

                // If nothing is selected, exit early
                if (minRow > maxRow || minCol > maxCol)
                    return null;

                // Build up the data in three different formats for pasting into different apps
                var txt = new StringBuilder();
                var csv = new StringBuilder();
                var html = new StringBuilder();
                var specialCsvChars = new[] { '"', ',', '\r', '\n' };

                html.Append("<TABLE>");
                var firstRow = true;

                if (grid.ClipboardCopyMode == DataGridViewClipboardCopyMode.EnableAlwaysIncludeHeaderText)
                {
                    // Add the column headers
                    html.Append("<TR>");

                    for (var col = minCol; col <= maxCol; col++)
                    {
                        if (col > minCol)
                        {
                            txt.Append('\t');
                            csv.Append(',');
                        }

                        html.Append("<TH>");

                        var isSelected = allSelected;
                        if (!isSelected)
                            isSelected = rowIndexes.Count > 0;
                        if (!isSelected)
                            isSelected = cellCoords.Any(c => c.ColumnIndex == col);

                        if (isSelected)
                        {
                            var str = grid.Columns[col].HeaderText;
                            txt.Append(str.Replace('\t', ' '));

                            var requiresCsvEncoding = str.IndexOfAny(specialCsvChars) != -1;

                            if (requiresCsvEncoding)
                            {
                                csv.Append('"');
                                csv.Append(str.Replace("\"", "\"\""));
                                csv.Append('"');
                            }
                            else
                            {
                                csv.Append(str);
                            }

                            html.Append(HttpUtility.HtmlEncode(str));
                        }

                        html.Append("</TH>");
                    }

                    html.Append("</TR>");
                    firstRow = false;
                }

                for (var i = minRow; i <= maxRow; i++)
                {
                    var row = table.Rows[i];
                    html.Append("<TR>");

                    for (var j = minCol; j <= maxCol; j++)
                    {
                        if (j == minCol && !firstRow)
                        {
                            txt.Append(Environment.NewLine);
                            csv.Append(Environment.NewLine);
                        }
                        else if (j > minCol)
                        {
                            txt.Append('\t');
                            csv.Append(',');
                        }

                        html.Append("<TD>");

                        var isSelected = allSelected;
                        if (!isSelected)
                            isSelected = rowIndexes.BinarySearch(i) >= 0;
                        if (!isSelected)
                            isSelected = cellCoords.BinarySearch(new CellCoordinate { RowIndex = i, ColumnIndex = j }) >= 0;

                        if (isSelected)
                        {
                            var args = new DataGridViewCellFormattingEventArgs(j, i, row[j], typeof(string), null);
                            FormatCell(grid, args);

                            var str = args.Value.ToString();

                            // Copying a single cell - don't replace any special characters, just extract the raw text
                            if (minCol == maxCol && minRow == maxRow)
                                txt.Append(str);
                            else
                                txt.Append(str.Replace('\t', ' ').Replace('\r', ' ').Replace('\n', ' '));

                            var requiresCsvEncoding = str.IndexOfAny(specialCsvChars) != -1;

                            if (requiresCsvEncoding)
                            {
                                csv.Append('"');
                                csv.Append(str.Replace("\"", "\"\""));
                                csv.Append('"');
                            }
                            else
                            {
                                csv.Append(str);
                            }

                            var isLink = false;

                            if (row[j] is SqlEntityReference entityReference && !entityReference.IsNull)
                            {
                                var url = GetRecordUrl(entityReference, out _);
                                html.Append($"<A HREF=\"{url}\">");
                                isLink = true;
                            }

                            html.Append(HttpUtility.HtmlEncode(str));

                            if (isLink)
                                html.Append("</A>");
                        }

                        html.Append("</TD>");
                    }

                    html.Append("</TR>");
                    firstRow = false;
                }

                html.Append("</TABLE>");

                // Add the magic header for copying HTML
                var htmlBytes = Encoding.UTF8.GetByteCount(html.ToString());
                var header = "Version:1.0\r\nStartHTML:aaaaaaaaa\r\nEndHTML:bbbbbbbbb\r\nStartFragment:ccccccccc\r\nEndFragment:ddddddddd\r\n";
                var fragmentHeader = "<HTML>\r\n<BODY>\r\n<!--StartFragment-->";
                var trailer = "\r\n<!--EndFragment-->\r\n</BODY>\r\n</HTML>";

                header = header.Replace("aaaaaaaaa", header.Length.ToString("000000000", CultureInfo.InvariantCulture));
                header = header.Replace("bbbbbbbbb", (header.Length + fragmentHeader.Length + htmlBytes + trailer.Length).ToString("000000000", CultureInfo.InvariantCulture));
                header = header.Replace("ccccccccc", (header.Length + fragmentHeader.Length).ToString("000000000", CultureInfo.InvariantCulture));
                header = header.Replace("ddddddddd", (header.Length + fragmentHeader.Length + htmlBytes).ToString("000000000", CultureInfo.InvariantCulture));

                html.Insert(0, fragmentHeader);
                html.Insert(0, header);
                html.Append(trailer);

                var data = new DataObject();
                data.SetText(txt.ToString(), TextDataFormat.Text);
                data.SetText(txt.ToString(), TextDataFormat.UnicodeText);
                data.SetText(csv.ToString(), TextDataFormat.CommaSeparatedValue);
                data.SetText(html.ToString(), TextDataFormat.Html);

                return data;
            }
        }

        public static Control GetFocusedControl(Control control)
        {
            var container = control as IContainerControl;
            while (container != null)
            {
                control = container.ActiveControl;
                container = control as IContainerControl;
            }
            return control;
        }
    }
}
