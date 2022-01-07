using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.SqlTypes;
using System.Diagnostics;
using System.Drawing;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using System.ServiceModel;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Windows.Forms;
using System.Xml;
using System.Xml.Serialization;
using AutocompleteMenuNS;
using MarkMpn.Sql4Cds.Engine;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using McTools.Xrm.Connection;
using Microsoft.ApplicationInsights;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using Microsoft.Xrm.Tooling.Connector;
using ScintillaNET;
using xrmtb.XrmToolBox.Controls;
using xrmtb.XrmToolBox.Controls.Controls;
using XrmToolBox.Extensibility;

namespace MarkMpn.Sql4Cds
{
    partial class SqlQueryControl : WeifenLuo.WinFormsUI.Docking.DockContent, IDocumentWindow
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
        private string _displayName;
        private string _filename;
        private bool _modified;
        private static int _queryCounter;
        private static ImageList _images;
        private static Icon _sqlIcon;
        private readonly AutocompleteMenu _autocomplete;
        private ToolTip _tooltip;
        private bool _cancellable;
        private Stopwatch _stopwatch;
        private ExecuteParams _params;
        private int _rowCount;
        private ToolStripControlHost _progressHost;

        private bool _addingResult;
        private IDictionary<int, TextRange> _messageLocations;

        static SqlQueryControl()
        {
            _images = new ImageList();
            _images.Images.AddRange(new ObjectExplorer(null, null, null, null).GetImages().ToArray());

            _sqlIcon = Icon.FromHandle(Properties.Resources.SQLFile_16x.GetHicon());
        }

        public SqlQueryControl(ConnectionDetail con, IDictionary<string, DataSource> dataSources, TelemetryClient ai, Action<string> showFetchXml, Action<string> log, PropertiesWindow properties)
        {
            InitializeComponent();
            _displayName = $"Query {++_queryCounter}";
            _modified = true;
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
            Icon = _sqlIcon;

            ChangeConnection(con);
        }

        public IDictionary<string, DataSource> DataSources { get; private set; }
        public Action<string> ShowFetchXML { get; }
        public string Filename
        {
            get { return _filename; }
            set
            {
                _filename = value;
                _displayName = Path.GetFileName(value);
                _modified = false;
                SyncTitle();
            }
        }
        public ConnectionDetail Connection => _con;
        public string Sql => String.IsNullOrEmpty(_editor.SelectedText) ? _editor.Text : _editor.SelectedText;

        internal void ChangeConnection(ConnectionDetail con)
        {
            _con = con;

            if (con != null)
            {
                hostLabel.Text = new Uri(_con.OrganizationServiceUrl).Host;
                orgNameLabel.Text = _con.Organization;

                toolStripStatusLabel.Text = "Connected";
                toolStripStatusLabel.Image = Properties.Resources.ConnectFilled_grey_16x;
            }
            else
            {
                hostLabel.Text = "";
                orgNameLabel.Text = "";

                toolStripStatusLabel.Text = "Disconnected";
                toolStripStatusLabel.Image = Properties.Resources.Disconnect_Filled_16x;
            }

            SyncUsername();
            SyncTitle();
        }

        private void SyncTitle()
        {
            var text = _displayName;

            if (Busy)
                text += " Executing...";

            if (_modified)
                text += " *";

            if (_con != null)
                text += $" ({_con.ConnectionName})";
            else
                text += " (Disconnected)";

            Text = text;
        }

        public void SetFocus()
        {
            _editor.Focus();
        }

        public void Save()
        {
            if (Filename == null)
            {
                using (var save = new SaveFileDialog())
                {
                    save.Filter = "SQL Scripts (*.sql)|*.sql";

                    if (save.ShowDialog() != DialogResult.OK)
                        return;

                    Filename = save.FileName;
                }
            }

            File.WriteAllText(Filename, _editor.Text);
            _modified = false;
            SyncTitle();
        }

        public void InsertText(string text)
        {
            _editor.ReplaceSelection(text);
            _editor.Focus();
        }

        public void Format()
        {
            _ai.TrackEvent("Format SQL", new Dictionary<string, string> { ["Source"] = "XrmToolBox" });

            var dom = new TSql150Parser(true);
            var fragment = dom.Parse(new StringReader(_editor.Text), out var errors);

            if (errors.Count != 0)
                return;

            new Sql150ScriptGenerator().GenerateScript(fragment, out var sql);
            _editor.Text = sql;
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
                if (e.KeyCode == Keys.Space && e.Control)
                {
                    _autocomplete.Show(_editor, true);
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
                if (!_modified)
                {
                    _modified = true;
                    SyncTitle();
                }

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
                    .Select(ds =>
                    {
                        EntityCache.TryGetEntities(ds.ConnectionDetail.MetadataCacheLoader, ds.Connection, out var entities);

                        var metaEntities = MetaMetadataCache.GetMetadata();

                        if (entities == null)
                            entities = metaEntities.ToArray();
                        else
                            entities = entities.Concat(metaEntities).ToArray();

                        return new AutocompleteDataSource { Name = ds.Name, Entities = entities, Metadata = new MetaMetadataCache(ds.Metadata) };
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

            return scintilla;
        }

        TabContent IDocumentWindow.GetSessionDetails()
        {
            return new TabContent
            {
                Type = "SQL",
                Filename = Filename,
                Query = _modified ? _editor.Text : null
            };
        }

        void IDocumentWindow.RestoreSessionDetails(TabContent tab)
        {
            var content = tab.Query;

            if (!String.IsNullOrEmpty(tab.Filename))
            {
                Filename = tab.Filename;

                if (content == null && !String.IsNullOrEmpty(tab.Filename))
                    content = File.ReadAllText(tab.Filename);
            }

            _editor.Text = content;
            _modified = tab.Query != null;
            SyncTitle();
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
                    .Select(ds =>
                    {
                        EntityCache.TryGetEntities(ds.ConnectionDetail.MetadataCacheLoader, ds.Connection, out var entities);

                        var metaEntities = MetaMetadataCache.GetMetadata();

                        if (entities == null)
                            entities = metaEntities.ToArray();
                        else
                            entities = entities.Concat(metaEntities).ToArray();

                        return new AutocompleteDataSource { Name = ds.Name, Entities = entities, Metadata = new MetaMetadataCache(ds.Metadata) };
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
                if (_cancellable == value)
                    return;

                _cancellable = value;
                CancellableChanged?.Invoke(this, EventArgs.Empty);
            }
        }

        public event EventHandler CancellableChanged;

        public void Cancel()
        {
            backgroundWorker.ReportProgress(0, "Cancelling query...");
            backgroundWorker.CancelAsync();
        }

        private void AddResult(Control results, Control fetchXml, int rowCount)
        {
            if (results != null)
            {
                if (!tabControl.TabPages.Contains(resultsTabPage))
                {
                    tabControl.TabPages.Insert(0, resultsTabPage);
                    tabControl.SelectedTab = resultsTabPage;
                }

                AddControl(results, resultsTabPage);
            }

            if (fetchXml != null)
            {
                if (!tabControl.TabPages.Contains(fetchXmlTabPage))
                    tabControl.TabPages.Insert(tabControl.TabPages.Count - 1, fetchXmlTabPage);

                AddControl(fetchXml, fetchXmlTabPage);
            }

            _rowCount += rowCount;

            if (_rowCount == 1)
                rowsLabel.Text = "1 row";
            else
                rowsLabel.Text = $"{_rowCount:N0} rows";
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
            Clipboard.SetDataObject(grid.GetClipboardContent());
        }

        private void copyWithHeadersToolStripMenuItem_Click(object sender, EventArgs e)
        {
            var grid = (DataGridView)gridContextMenuStrip.SourceControl;
            grid.ClipboardCopyMode = DataGridViewClipboardCopyMode.EnableAlwaysIncludeHeaderText;
            Clipboard.SetDataObject(grid.GetClipboardContent());
            grid.ClipboardCopyMode = DataGridViewClipboardCopyMode.EnableWithoutHeaderText;
        }

        private void backgroundWorker_RunWorkerCompleted(object sender, RunWorkerCompletedEventArgs e)
        {
            _progressHost.Visible = false;
            _stopwatch.Stop();
            timer.Enabled = false;
            Cancellable = false;

            if (e.Cancelled)
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

                if (e.Error is QueryException queryException)
                {
                    plan = queryException.Query;
                    index = _params.Offset + queryException.Query.Index;
                    length = queryException.Query.Length;
                    error = queryException.InnerException;
                }

                if (error is QueryExecutionException queryExecution)
                {
                    messageSuffix = "\r\nSee the Execution Plan tab for details of where this error occurred";
                    ShowResult(plan, new ExecuteParams { Execute = true, IncludeFetchXml = true, Sql = plan.Sql }, null, null, queryExecution);

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
                    _editor.IndicatorFillRange(_params.Offset + parseErr.Error.Offset, 1);
                    index = _params.Offset + parseErr.Error.Offset;
                    length = 0;
                }
                else if (error is PartialSuccessException partialSuccess)
                {
                    if (partialSuccess.Result is string msg)
                        AddMessage(index, length, msg, false);

                    error = partialSuccess.InnerException;
                }

                _ai.TrackException(error, new Dictionary<string, string> { ["Sql"] = _params.Sql, ["Source"] = "XrmToolBox" });
                _log(e.Error.ToString());

                AddMessage(index, length, GetErrorMessage(error) + messageSuffix, true);

                tabControl.SelectedTab = messagesTabPage;
            }
            else if (!_params.Execute)
            {
                tabControl.SelectedTab = fetchXmlTabPage;
            }

            BusyChanged?.Invoke(this, EventArgs.Empty);

            _editor.Focus();
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

                while (fault.InnerFault != null)
                {
                    if (fault.InnerFault.Message != fault.Message)
                        msg += "\r\n" + fault.InnerFault.Message;

                    fault = fault.InnerFault;
                }
            }

            return msg;
        }

        private void AddMessage(int index, int length, string message, bool error)
        {
            var scintilla = (Scintilla)messagesTabPage.Controls[0];
            var line = scintilla.Lines.Count - 1;
            scintilla.ReadOnly = false;
            scintilla.Text += message + "\r\n\r\n";
            scintilla.StartStyling(scintilla.Text.Length - message.Length - 4);
            scintilla.SetStyling(message.Length, error ? 1 : 2);
            scintilla.ReadOnly = true;

            if (index != -1)
            {
                foreach (var l in message.Split('\n'))
                    _messageLocations[line++] = new TextRange(index, length);
            }
        }

        private void NavigateToMessage(object sender, DoubleClickEventArgs e)
        {
            if (_messageLocations.TryGetValue(e.Line, out var textRange))
            {
                _editor.SelectionStart = textRange.Index;
                _editor.SelectionEnd = textRange.Index + textRange.Length;
                _editor.Focus();
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
                Cancellable = args.Execute;
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

            var options = new QueryExecutionOptions(_con, DataSources[_con.ConnectionName].Connection, backgroundWorker, this);
            var converter = new ExecutionPlanBuilder(DataSources.Values, options);
            converter.QuotedIdentifiers = Settings.Instance.QuotedIdentifiers;

            if (Settings.Instance.UseTSQLEndpoint &&
                args.Execute &&
                !String.IsNullOrEmpty(((CrmServiceClient)DataSources[_con.ConnectionName].Connection).CurrentAccessToken))
                converter.TDSEndpointAvailable = true;

            var queries = converter.Build(args.Sql);
            var parameterTypes = new Dictionary<string, Type>(StringComparer.OrdinalIgnoreCase);
            var parameterValues = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);

            if (args.Execute)
            {
                foreach (var query in queries)
                {
                    _ai.TrackEvent("Execute", new Dictionary<string, string> { ["QueryType"] = query.GetType().Name, ["Source"] = "XrmToolBox" });

                    try
                    {
                        if (query is IDataSetExecutionPlanNode dataQuery)
                        {
                            var result = dataQuery.Execute(DataSources.Values.Cast<Engine.DataSource>().ToDictionary(ds => ds.Name, StringComparer.OrdinalIgnoreCase), options, parameterTypes, parameterValues);

                            Execute(() => ShowResult(query, args, result, null, null));
                        }
                        else if (query is IDmlQueryExecutionPlanNode dmlQuery)
                        {
                            var result = dmlQuery.Execute(DataSources.Values.Cast<Engine.DataSource>().ToDictionary(ds => ds.Name, StringComparer.OrdinalIgnoreCase), options, parameterTypes, parameterValues);

                            Execute(() => ShowResult(query, args, null, result, null));
                        }
                    }
                    catch (Exception ex)
                    {
                        throw new QueryException(query, ex);
                    }

                    if (query is IImpersonateRevertExecutionPlanNode)
                    {
                        options.SyncUserId();
                        Execute(() => SyncUsername());
                    }
                }
            }
            else
            {
                foreach (var query in queries)
                {
                    _ai.TrackEvent("Convert", new Dictionary<string, string> { ["QueryType"] = query.GetType().Name, ["Source"] = "XrmToolBox" });
                    Execute(() => ShowResult(query, args, null, null, null));
                }
            }

            if (options.Cancelled)
            {
                e.Cancel = true;
                AddMessage(-1, 0, "Query was cancelled by user", true);
            }
        }

        private void ShowResult(IRootExecutionPlanNode query, ExecuteParams args, DataTable results, string msg, QueryExecutionException ex)
        {
            Control result = null;
            Control plan = null;
            var rowCount = 0;

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
                        FillWeight = 1
                    });
                }

                var linkFont = new Font(grid.Font, grid.Font.Style | FontStyle.Underline);

                grid.CellFormatting += (s, e) =>
                {
                    if (e.Value is INullable nullable && nullable.IsNull || e.Value is DBNull)
                    {
                        e.Value = "NULL";
                        e.CellStyle.BackColor = Color.FromArgb(0xff, 0xff, 0xe1);
                        e.FormattingApplied = true;
                    }
                    else if (e.Value is SqlBoolean b)
                    {
                        e.Value = b.Value ? "1" : "0";
                    }
                    else if (!Settings.Instance.LocalFormatDates && e.Value is SqlDateTime dt)
                    {
                        e.Value = dt.Value.ToString("yyyy-MM-dd HH:mm:ss.fff");
                    }
                    else if (e.Value is SqlEntityReference)
                    {
                        e.CellStyle.ForeColor = SystemColors.HotTrack;
                        e.CellStyle.Font = linkFont;
                    }
                };

                grid.CellMouseEnter += (s, e) =>
                {
                    if (e.RowIndex < 0 || e.ColumnIndex < 0)
                        return;

                    var gv = (DataGridView)s;
                    var cell = gv.Rows[e.RowIndex].Cells[e.ColumnIndex];

                    if (cell.Value is SqlEntityReference er && !er.IsNull)
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
                };

                grid.HandleCreated += (s, e) =>
                {
                    if (Settings.Instance.AutoSizeColumns)
                        grid.AutoResizeColumns();
                };

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

                result = grid;

                rowCount = results.Rows.Count;

                AddMessage(query.Index, query.Length, $"({rowCount} row{(rowCount == 1 ? "" : "s")} affected)", false);
            }
            else if (msg != null)
            {
                AddMessage(query.Index, query.Length, msg, false);
            }

            if (args.IncludeFetchXml)
            {
                plan = new Panel();
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
                var options = new QueryExecutionOptions(_con, DataSources[_con.ConnectionName].Connection, backgroundWorker, this);
                var planView = new ExecutionPlanView { Dock = DockStyle.Fill, Executed = args.Execute, Exception = ex, DataSources = DataSources, Options = options };
                planView.Plan = query;
                planView.NodeSelected += (s, e) => _properties.SelectedObject = planView.Selected;
                planView.DoubleClick += (s, e) =>
                {
                    if (planView.Selected is IFetchXmlExecutionPlanNode fetchXml)
                        ShowFetchXML(fetchXml.FetchXmlString);
                };
                plan.Controls.Add(planView);
                plan.Controls.Add(fetchLabel);
            }

            AddResult(result, plan, rowCount);
        }

        private void OpenRecord(SqlEntityReference entityReference)
        {
            if (!DataSources.TryGetValue(entityReference.DataSource, out var dataSource))
                return;

            var url = dataSource.ConnectionDetail.GetEntityReferenceUrl(entityReference);
            Process.Start(url);
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

                return Math.Max(2, rowCount + 1) * grid.ColumnHeadersHeight + SystemInformation.HorizontalScrollBarHeight;
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
                dlg.LogicalName = "systemuser";
                dlg.Multiselect = false;

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
            createSELECTStatementToolStripMenuItem.Enabled = false;

            if (grid.CurrentCell?.Value is SqlEntityReference er && !er.IsNull)
            {
                openRecordToolStripMenuItem.Enabled = true;
                createSELECTStatementToolStripMenuItem.Enabled = true;
            }
        }

        private void openRecordToolStripMenuItem_Click(object sender, EventArgs e)
        {
            var grid = (DataGridView)gridContextMenuStrip.SourceControl;

            if (grid.CurrentCell?.Value is SqlEntityReference er && !er.IsNull)
                OpenRecord(er);
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
    }
}
