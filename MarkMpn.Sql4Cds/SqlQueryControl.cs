using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Drawing;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Windows.Forms;
using System.Xml;
using System.Xml.Serialization;
using AutocompleteMenuNS;
using MarkMpn.Sql4Cds.Engine;
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
    public partial class SqlQueryControl : WeifenLuo.WinFormsUI.Docking.DockContent
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
            public QueryException(Query query, Exception innerException) : base(innerException.Message, innerException)
            {
                Query = query;
            }

            public Query Query { get; }
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

        private readonly ConnectionDetail _con;
        private readonly TelemetryClient _ai;
        private readonly Scintilla _editor;
        private readonly string _sourcePlugin;
        private readonly Action<string> _log;
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
        private int _metadataLoadingTasks;
        private string _preMetadataLoadingStatus;
        private Image _preMetadataLoadingImage;
        private bool _addingResult;
        private IDictionary<int, TextRange> _messageLocations;

        static SqlQueryControl()
        {
            _images = new ImageList();
            _images.Images.AddRange(new ObjectExplorer(null, null, null).GetImages().ToArray());

            _sqlIcon = Icon.FromHandle(Properties.Resources.SQLFile_16x.GetHicon());
        }

        public SqlQueryControl(ConnectionDetail con, AttributeMetadataCache metadata, TelemetryClient ai, Action<MessageBusEventArgs> outgoingMessageHandler, string sourcePlugin, Action<string> log)
        {
            InitializeComponent();
            _displayName = $"Query {++_queryCounter}";
            _modified = true;
            Service = con.ServiceClient;
            Metadata = new MetaMetadataCache(metadata);
            OutgoingMessageHandler = outgoingMessageHandler;
            _editor = CreateSqlEditor();
            _autocomplete = CreateAutocomplete();
            _sourcePlugin = sourcePlugin;
            _ai = ai;
            _con = con;
            _log = log;
            _stopwatch = new Stopwatch();
            SyncTitle();
            BusyChanged += (s, e) => SyncTitle();

            // Populate the status bar and add separators between each field
            hostLabel.Text = new Uri(_con.OrganizationServiceUrl).Host;
            SyncUsername();
            orgNameLabel.Text = _con.Organization;
            for (var i = statusStrip.Items.Count - 1; i > 1; i--)
                statusStrip.Items.Insert(i, new ToolStripSeparator());

            var progressImage = new PictureBox { Image = Properties.Resources.progress, Height = 16, Width = 16 };
            _progressHost = new ToolStripControlHost(progressImage) { Visible = false };
            statusStrip.Items.Insert(0, _progressHost);

            metadata.MetadataLoading += MetadataLoading;

            splitContainer.Panel1.Controls.Add(_editor);
            Icon = _sqlIcon;
        }

        public IOrganizationService Service { get; }
        public IAttributeMetadataCache Metadata { get; }
        public Action<MessageBusEventArgs> OutgoingMessageHandler { get; }
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

        private void SyncTitle()
        {
            var busySuffix = Busy ? " Executing..." : "";

            if (_modified)
                Text = $"{_displayName}{busySuffix} * ({_con.ConnectionName})";
            else
                Text = $"{_displayName}{busySuffix} ({_con.ConnectionName})";
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
            scintilla.Margins[0].Width = 20;

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

                if (!Settings.Instance.ShowIntellisenseTooltips)
                    return;

                var pos = scintilla.CharPositionFromPoint(e.X, e.Y);
                var text = scintilla.Text;
                var wordEnd = new Regex("\\b").Match(text, pos);

                if (!wordEnd.Success)
                    return;

                EntityCache.TryGetEntities(_con.ServiceClient, out var entities);

                var metaEntities = MetaMetadata.GetMetadata().Select(m => m.GetEntityMetadata());

                if (entities == null)
                    entities = metaEntities.ToArray();
                else
                    entities = entities.Concat(metaEntities).ToArray();

                var suggestions = new Autocomplete(entities, Metadata).GetSuggestions(text, wordEnd.Index - 1).ToList();
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
                var pos = _control._editor.CurrentPosition - 1;

                if (pos == 0)
                    yield break;

                var text = _control._editor.Text;
                EntityCache.TryGetEntities(_control._con.ServiceClient, out var entities);

                var metaEntities = MetaMetadata.GetMetadata().Select(m => m.GetEntityMetadata());

                if (entities == null)
                    entities = metaEntities.ToArray();
                else
                    entities = entities.Concat(metaEntities).ToArray();

                var suggestions = new Autocomplete(entities, _control.Metadata).GetSuggestions(text, pos).ToList();

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

        private Scintilla CreateXmlEditor()
        {
            var scintilla = CreateEditor();

            scintilla.Lexer = Lexer.Xml;

            // Show line numbers
            scintilla.Margins[0].Width = 20;

            scintilla.StyleClearAll();
            scintilla.Styles[Style.LineNumber].ForeColor = Color.FromArgb(255, 128, 128, 128);  //Dark Gray
            scintilla.Styles[Style.LineNumber].BackColor = Color.FromArgb(255, 228, 228, 228);  //Light Gray
            scintilla.Styles[Style.Xml.Attribute].ForeColor = Color.Red;
            scintilla.Styles[Style.Xml.Entity].ForeColor = Color.Red;
            scintilla.Styles[Style.Xml.Comment].ForeColor = Color.Green;
            scintilla.Styles[Style.Xml.Tag].ForeColor = Color.Blue;
            scintilla.Styles[Style.Xml.TagEnd].ForeColor = Color.Blue;
            scintilla.Styles[Style.Xml.DoubleString].ForeColor = Color.DeepPink;
            scintilla.Styles[Style.Xml.SingleString].ForeColor = Color.DeepPink;
            return scintilla;
        }

        public void Execute(bool execute, bool includeFetchXml)
        {
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

        private string SerializeRequest(object request)
        {
            using (var stream = new MemoryStream())
            {
                using (var writer = XmlWriter.Create(stream, new XmlWriterSettings { Indent = true }))
                {
                    var serializer = new DataContractSerializer(request.GetType());
                    serializer.WriteObject(writer, request);
                }

                return Encoding.UTF8.GetString(stream.ToArray());
            }
        }

        private void Grid_CellMouseUp(object sender, DataGridViewCellMouseEventArgs e)
        {
            if (e.Button == MouseButtons.Right)
            {
                if (e.ColumnIndex < 0 || e.RowIndex < 0)
                    return;

                var grid = (DataGridView)sender;
                var cell = grid[e.ColumnIndex, e.RowIndex];

                if (!cell.Selected)
                {
                    grid.CurrentCell = cell;
                    grid.ContextMenuStrip.Show(grid, grid.PointToClient(Cursor.Position));
                }
            }
        }

        private void Grid_RecordClick(object sender, CRMRecordEventArgs e)
        {
            // Store the details of what's been clicked
            // Show context menu with Open & Create SELECT options enabled
            if (e.Entity != null && e.Entity.Contains(e.Attribute) && e.Entity[e.Attribute] is EntityReference)
            {
                var grid = (Control)sender;
                gridContextMenuStrip.Show(grid, grid.PointToClient(Cursor.Position));
            }
        }

        private ToolStrip CreateFXBToolbar(Scintilla xmlEditor)
        {
            var toolbar = new ToolStrip();
            toolbar.ImageScalingSize = new Size(24, 24);
            var btn = new ToolStripButton
            {
                Text = "Edit in",
                Image = Properties.Resources.FXB,
                ImageAlign = ContentAlignment.MiddleRight,
                TextAlign = ContentAlignment.MiddleLeft,
                TextImageRelation = TextImageRelation.TextBeforeImage,
                ToolTipText = "Edit in FetchXML Builder"
            };

            btn.Click += (sender, e) =>
            {
                OutgoingMessageHandler(new MessageBusEventArgs("FetchXML Builder")
                {
                    TargetArgument = xmlEditor.Text
                });
            };
            toolbar.Items.Add(btn);

            if (_sourcePlugin != null)
            {
                var srcBtn = new ToolStripButton
                {
                    Text = "Return to " + _sourcePlugin
                };
                srcBtn.Click += (sender, e) =>
                {
                    OutgoingMessageHandler(new MessageBusEventArgs(_sourcePlugin)
                    {
                        TargetArgument = xmlEditor.Text
                    });
                };
                toolbar.Items.Add(srcBtn);
            }

            return toolbar;
        }

        private Panel CreatePostProcessingWarning(FetchXmlQuery fxq, bool metadata)
        {
            if (!metadata && fxq.Extensions.Count == 0)
                return null;

            return CreateWarning(
                $"This query required additional processing. This {(metadata ? "metadata request" : "FetchXML")} gives the required data, but will not give the final results when run outside SQL 4 CDS.",
                "Learn more",
                "https://markcarrington.dev/sql-4-cds/additional-processing/");
        }

        private Panel CreateDistinctWithoutSortWarning(FetchXmlQuery fxq)
        {
            if (!fxq.DistinctWithoutSort)
                return null;

            return CreateWarning(
                "This DISTINCT query does not have a sort order applied. Unexpected results may be returned when the results are split over multiple pages. Add a sort order to retrieve the correct results.",
                "Learn more",
                "https://docs.microsoft.com/powerapps/developer/common-data-service/org-service/paging-behaviors-and-ordering#ordering-with-a-paging-cookie");
        }

        private Panel CreateWarning(string message, string link, string url)
        {
            var panel = new Panel
            {
                BackColor = SystemColors.Info,
                BorderStyle = BorderStyle.None,
                Dock = DockStyle.Top,
                Padding = new Padding(4),
                Height = 24
            };
            var label = new LinkLabel
            {
                Text = message,
                ForeColor = SystemColors.InfoText,
                AutoSize = false,
                Dock = DockStyle.Fill
            };

            if (!String.IsNullOrEmpty(link))
            {
                label.Text += " " + link;
                label.LinkArea = new LinkArea(label.Text.Length - link.Length, link.Length);
                label.LinkClicked += (s, e) => Process.Start(url);
            }

            panel.Controls.Add(label);
            panel.Controls.Add(new PictureBox
            {
                Image = Properties.Resources.StatusWarning_16x,
                Height = 16,
                Width = 16,
                Dock = DockStyle.Left
            });

            return panel;
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

        private void gridContextMenuStrip_Opening(object sender, CancelEventArgs e)
        {
            var grid = gridContextMenuStrip.SourceControl as CRMGridView;

            if (grid == null)
            {
                openRecordToolStripMenuItem.Enabled = false;
                createSELECTQueryToolStripMenuItem.Enabled = false;
            }
            else
            {
                var entity = grid.SelectedCells.Count == 1 ? grid.SelectedCellRecords.Single() : null;
                var isEntityReference = false;

                if (entity != null)
                {
                    var attr = grid.SelectedCells[0].OwningColumn.DataPropertyName;

                    if (entity.Contains(attr) && entity[attr] is EntityReference)
                        isEntityReference = true;
                }

                openRecordToolStripMenuItem.Enabled = isEntityReference;
                createSELECTQueryToolStripMenuItem.Enabled = isEntityReference;
            }
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

        private void openRecordToolStripMenuItem_Click(object sender, EventArgs e)
        {
            var grid = (CRMGridView)gridContextMenuStrip.SourceControl;
            var entity = grid.SelectedCells.Count == 1 ? grid.SelectedCellRecords.Single() : null;
            var attr = grid.SelectedCells[0].OwningColumn.DataPropertyName;
            var entityReference = entity.GetAttributeValue<EntityReference>(attr);

            // Open record
            var url = new Uri(new Uri(_con.WebApplicationUrl), $"main.aspx?etn={entityReference.LogicalName}&id={entityReference.Id}&pagetype=entityrecord");
            Process.Start(url.ToString());
        }

        private void createSELECTQueryToolStripMenuItem_Click(object sender, EventArgs e)
        {
            var grid = (CRMGridView)gridContextMenuStrip.SourceControl;
            var entity = grid.SelectedCells.Count == 1 ? grid.SelectedCellRecords.Single() : null;
            var attr = grid.SelectedCells[0].OwningColumn.DataPropertyName;
            var entityReference = entity.GetAttributeValue<EntityReference>(attr);

            // Create SELECT query
            var metadata = Metadata[entityReference.LogicalName];
            _editor.AppendText("\r\n\r\n");
            var end = _editor.TextLength;
            _editor.AppendText($"SELECT * FROM {entityReference.LogicalName} WHERE {metadata.PrimaryIdAttribute} = '{entityReference.Id}'");
            _editor.SetSelection(_editor.TextLength, end);
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

                if (e.Error is QueryException queryException)
                {
                    index = _params.Offset + queryException.Query.Index;
                    length = queryException.Query.Length;
                    error = queryException.InnerException;
                }

                if (error is NotSupportedQueryFragmentException err)
                {
                    _editor.IndicatorFillRange(_params.Offset + err.Fragment.StartOffset, err.Fragment.FragmentLength);
                    index = _params.Offset + err.Fragment.StartOffset;
                    length = err.Fragment.FragmentLength;
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

                if (error is AggregateException aggregateException)
                    AddMessage(index, length, String.Join("\r\n", aggregateException.InnerExceptions.Select(ex => ex.Message)), true);
                else
                    AddMessage(index, length, error.Message, true);

                tabControl.SelectedTab = messagesTabPage;
            }
            else if (!_params.Execute)
            {
                tabControl.SelectedTab = fetchXmlTabPage;
            }

            BusyChanged?.Invoke(this, EventArgs.Empty);
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

            var converter = new Sql2FetchXml(Metadata, Settings.Instance.QuotedIdentifiers);

            if (Settings.Instance.UseTSQLEndpoint &&
                args.Execute &&
                !String.IsNullOrEmpty(((CrmServiceClient)Service).CurrentAccessToken))
                converter.TDSEndpointAvailable = true;

            converter.ColumnComparisonAvailable = new Version(_con.OrganizationVersion) >= new Version("9.1.0.19251");

            var queries = converter.Convert(args.Sql);

            var options = new QueryExecutionOptions(Service, backgroundWorker, this);

            if (args.Execute)
            {
                foreach (var query in queries)
                {
                    _ai.TrackEvent("Execute", new Dictionary<string, string> { ["QueryType"] = query.GetType().Name, ["Source"] = "XrmToolBox" });
                    query.Execute(Service, Metadata, options);

                    Execute(() => ShowResult(query, args));

                    if (query.Result is Exception ex)
                        throw new QueryException(query, ex);

                    if (query is ImpersonateQuery || query is RevertQuery)
                        Execute(() => SyncUsername());
                }
            }
            else
            {
                foreach (var query in queries)
                {
                    _ai.TrackEvent("Convert", new Dictionary<string, string> { ["QueryType"] = query.GetType().Name, ["Source"] = "XrmToolBox" });

                    if (query is IQueryRequiresFinalization finalize)
                        finalize.FinalizeRequest(Service, options);

                    Execute(() => ShowResult(query, args));
                }
            }

            if (options.Cancelled)
            {
                e.Cancel = true;
                AddMessage(-1, 0, "Query was cancelled by user", true);
            }
        }

        private void ShowResult(Query query, ExecuteParams args)
        {
            Control result = null;
            Control fetchXml = null;
            var rowCount = 0;

            var isMetadata = query.GetType().BaseType.IsGenericType && query.GetType().BaseType.GetGenericTypeDefinition() == typeof(MetadataQuery<,>);

            if (query.Result is EntityCollection || query.Result is DataTable)
            {
                var entityCollection = query.Result as EntityCollection;
                var dataTable = query.Result as DataTable;

                var grid = entityCollection != null ? new CRMGridView() : new DataGridView();

                grid.AllowUserToAddRows = false;
                grid.AllowUserToDeleteRows = false;
                grid.AllowUserToOrderColumns = true;
                grid.AllowUserToResizeRows = false;
                grid.AlternatingRowsDefaultCellStyle = new DataGridViewCellStyle { BackColor = Color.WhiteSmoke };
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

                if (entityCollection != null)
                {
                    var crmGrid = (CRMGridView)grid;

                    crmGrid.EntityReferenceClickable = true;
                    crmGrid.OrganizationService = Service;
                    crmGrid.ShowFriendlyNames = Settings.Instance.ShowEntityReferenceNames;
                    crmGrid.ShowIdColumn = false;
                    crmGrid.ShowIndexColumn = false;
                    crmGrid.ShowLocalTimes = Settings.Instance.ShowLocalTimes;
                    crmGrid.RecordClick += Grid_RecordClick;
                    crmGrid.CellMouseUp += Grid_CellMouseUp;
                }

                if (query is SelectQuery select && (entityCollection != null || isMetadata))
                {
                    foreach (var col in select.ColumnSet)
                    {
                        var colName = col;

                        if (grid.Columns.Contains(col))
                        {
                            var suffix = 1;
                            while (grid.Columns.Contains($"{col}_{suffix}"))
                                suffix++;

                            var newCol = $"{col}_{suffix}";

                            if (entityCollection != null)
                            {
                                foreach (var entity in entityCollection.Entities)
                                {
                                    if (entity.Contains(col))
                                        entity[newCol] = entity[col];
                                }
                            }

                            colName = newCol;
                        }

                        grid.Columns.Add(colName, colName);
                        grid.Columns[colName].FillWeight = 1;

                        if (entityCollection == null)
                            grid.Columns[colName].DataPropertyName = col;
                    }

                    if (isMetadata)
                        grid.AutoGenerateColumns = false;
                }

                grid.HandleCreated += (s, e) =>
                {
                    if (grid is CRMGridView crmGrid)
                        crmGrid.DataSource = query.Result;
                    else
                        grid.DataSource = query.Result;

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

                if (entityCollection != null)
                    rowCount = entityCollection.Entities.Count;
                else
                    rowCount = dataTable.Rows.Count;

                AddMessage(query.Index, query.Length, $"({rowCount} row{(rowCount == 1 ? "" : "s")} affected)", false);
            }
            else if (query.Result is string msg)
            {
                AddMessage(query.Index, query.Length, msg, false);
            }

            if (args.IncludeFetchXml)
            {
                if (isMetadata)
                {
                    var queryDisplay = CreateXmlEditor();
                    queryDisplay.Text = SerializeRequest(query.GetType().GetProperty("Request").GetValue(query));
                    queryDisplay.ReadOnly = true;

                    var metadataInfo = CreatePostProcessingWarning(null, true);

                    fetchXml = new Panel();
                    fetchXml.Controls.Add(queryDisplay);
                    fetchXml.Controls.Add(metadataInfo);
                }
                else if (query is FetchXmlQuery fxq && fxq.FetchXml != null)
                {
                    var xmlDisplay = CreateXmlEditor();
                    xmlDisplay.Text = fxq.FetchXmlString;
                    xmlDisplay.ReadOnly = true;
                    xmlDisplay.Dock = DockStyle.Fill;

                    var postWarning = CreatePostProcessingWarning(fxq, false);
                    var distinctWithoutSortWarning = CreateDistinctWithoutSortWarning(fxq);
                    var toolbar = CreateFXBToolbar(xmlDisplay);

                    fetchXml = new Panel();
                    fetchXml.Controls.Add(xmlDisplay);

                    if (postWarning != null)
                        fetchXml.Controls.Add(postWarning);

                    if (distinctWithoutSortWarning != null)
                        fetchXml.Controls.Add(distinctWithoutSortWarning);

                    fetchXml.Controls.Add(toolbar);
                }
            }

            AddResult(result, fetchXml, rowCount);
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

                if (rowCount == 0)
                    return 2 * grid.ColumnHeadersHeight;

                return Math.Min(rowCount * grid.GetRowDisplayRectangle(0, false).Height + grid.ColumnHeadersHeight, max);
            }

            if (control is Scintilla scintilla)
                return (int) ((scintilla.Lines.Count + 1) * scintilla.Styles[Style.Default].Size * 1.6) + 20;

            if (control is Panel panel)
                return panel.Controls.OfType<Control>().Sum(child => GetMinHeight(child, max));

            return control.Height;
        }

        private void MetadataLoading(object sender, MetadataLoadingEventArgs e)
        {
            if (!Busy)
            {
                if (Interlocked.Increment(ref _metadataLoadingTasks) == 1)
                {
                    Execute(() =>
                    {
                        _preMetadataLoadingStatus = toolStripStatusLabel.Text;
                        _preMetadataLoadingImage = toolStripStatusLabel.Image;
                        toolStripStatusLabel.Text = "Loading metadata for " + e.LogicalName;
                        toolStripStatusLabel.Image = null;
                        _progressHost.Visible = true;
                    });
                }

                e.Task.ContinueWith(t =>
                {
                    if (Interlocked.Decrement(ref _metadataLoadingTasks) == 0 && !Busy)
                    {
                        Execute(() =>
                        {
                            toolStripStatusLabel.Text = _preMetadataLoadingStatus;
                            toolStripStatusLabel.Image = _preMetadataLoadingImage;
                            _progressHost.Visible = false;
                        });
                    }
                });
            }
        }

        private void SyncUsername()
        {
            if (_con.ServiceClient.CallerId == Guid.Empty)
            {
                usernameDropDownButton.Text = _con.UserName;
                usernameDropDownButton.Image = null;
                revertToolStripMenuItem.Enabled = false;
            }
            else
            {
                var user = Service.Retrieve("systemuser", _con.ServiceClient.CallerId, new ColumnSet("domainname"));

                usernameDropDownButton.Text = user.GetAttributeValue<string>("domainname");
                usernameDropDownButton.Image = Properties.Resources.StatusWarning_16x;
                revertToolStripMenuItem.Enabled = true;
            }
        }

        private void impersonateMenuItem_Click(object sender, EventArgs e)
        {
            using (var dlg = new CDSLookupDialog())
            {
                dlg.Service = Service;
                dlg.LogicalName = "systemuser";
                dlg.Multiselect = false;

                if (dlg.ShowDialog(this) == DialogResult.OK)
                {
                    _ai.TrackEvent("Execute", new Dictionary<string, string> { ["QueryType"] = typeof(ImpersonateQuery).Name, ["Source"] = "XrmToolBox" });
                    _con.ServiceClient.CallerId = dlg.Entity.Id;
                    SyncUsername();
                }
            }
        }

        private void revertToolStripMenuItem_Click(object sender, EventArgs e)
        {
            _ai.TrackEvent("Execute", new Dictionary<string, string> { ["QueryType"] = typeof(RevertQuery).Name, ["Source"] = "XrmToolBox" });
            _con.ServiceClient.CallerId = Guid.Empty;
            SyncUsername();
        }
    }
}
