using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Drawing;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using Microsoft.Xrm.Sdk;
using ScintillaNET;
using XrmToolBox.Extensibility;
using System.Text.RegularExpressions;
using Microsoft.Xrm.Sdk.Metadata;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using System.IO;
using McTools.Xrm.Connection;
using Microsoft.ApplicationInsights;
using xrmtb.XrmToolBox.Controls;
using MarkMpn.Sql4Cds.Engine;
using System.Diagnostics;

namespace MarkMpn.Sql4Cds
{
    public partial class SqlQueryControl : WeifenLuo.WinFormsUI.Docking.DockContent
    {
        private readonly ConnectionDetail _con;
        private readonly TelemetryClient _ai;
        private readonly Scintilla _editor;
        private readonly string _sourcePlugin;
        private string _displayName;
        private string _filename;
        private bool _modified;
        private static int _queryCounter;

        public SqlQueryControl(ConnectionDetail con, IAttributeMetadataCache metadata, TelemetryClient ai, Action<WorkAsyncInfo> workAsync, Action<string> setWorkingMessage, Action<Action> executeMethod, Action<MessageBusEventArgs> outgoingMessageHandler, string sourcePlugin)
        {
            InitializeComponent();
            _displayName = $"Query {++_queryCounter}";
            _modified = true;
            Service = con.ServiceClient;
            Metadata = metadata;
            WorkAsync = workAsync;
            SetWorkingMessage = setWorkingMessage;
            ExecuteMethod = executeMethod;
            OutgoingMessageHandler = outgoingMessageHandler;
            _editor = CreateSqlEditor();
            _sourcePlugin = sourcePlugin;
            _ai = ai;
            _con = con;
            SyncTitle();

            splitContainer.Panel1.Controls.Add(_editor);
        }

        public IOrganizationService Service { get; }
        public IAttributeMetadataCache Metadata { get; }
        public Action<WorkAsyncInfo> WorkAsync { get; }
        public Action<string> SetWorkingMessage { get; }
        public Action<Action> ExecuteMethod { get; }
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
            if (_modified)
                Text = $"{_displayName} * ({_con.ConnectionName})";
            else
                Text = $"{_displayName} ({_con.ConnectionName})";
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
            _ai.TrackEvent("Format SQL");

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

        private Scintilla CreateTextEditor(bool error)
        {
            var scintilla = CreateEditor();

            scintilla.Lexer = Lexer.Null;
            
            if (error)
                scintilla.Styles[Style.Default].ForeColor = Color.Red;
            
            scintilla.StyleClearAll();

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
                if (e.KeyCode == Keys.F5)
                    Execute(true);

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
                    ShowIntellisense(true);
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

            // Intellisense
            scintilla.CharAdded += ShowIntellisense;
            scintilla.AutoCIgnoreCase = true;

            return scintilla;
        }

        private void ShowIntellisense(object sender, EventArgs e)
        {
            ShowIntellisense(false);
        }

        private void ShowIntellisense(bool force)
        {
            var pos = _editor.CurrentPosition - 1;

            if (pos == 0)
                return;

            var text = _editor.Text;
            EntityCache.TryGetEntities(_con.ServiceClient, out var entities);

            var suggestions = new Autocomplete(entities, Metadata).GetSuggestions(text, pos, out var currentLength).ToList();

            if (suggestions.Count == 0)
                return;

            if (force || currentLength > 0 || text[pos] == '.')
                _editor.AutoCShow(currentLength, String.Join(_editor.AutoCSeparator.ToString(), suggestions));
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

        public void Execute(bool execute)
        {
            var offset = String.IsNullOrEmpty(_editor.SelectedText) ? 0 : _editor.SelectionStart;

            _editor.IndicatorClearRange(0, _editor.TextLength);

            var sql = _editor.SelectedText;

            if (String.IsNullOrEmpty(sql))
                sql = _editor.Text;

            WorkAsync(new WorkAsyncInfo
            {
                Message = "Executing...",
                IsCancelable = execute,
                Work = (worker, args) =>
                {
                    var queries = new Sql2FetchXml(Metadata, Settings.Instance.QuotedIdentifiers).Convert(sql);

                    if (execute)
                    {
                        var options = new QueryExecutionOptions(worker);

                        foreach (var query in queries)
                        {
                            _ai.TrackEvent("Execute", new Dictionary<string, string> { ["QueryType"] = query.GetType().Name });
                            query.Execute(Service, Metadata, options);
                        }
                    }

                    args.Result = queries;
                },
                ProgressChanged = e =>
                {
                    SetWorkingMessage(e.UserState.ToString());
                },
                PostWorkCallBack = (args) =>
                {
                    splitContainer.Panel2.Controls.Clear();

                    if (args.Cancelled)
                        return;

                    if (args.Error != null)
                        _ai.TrackException(args.Error, new Dictionary<string, string> { ["Sql"] = sql });

                    if (args.Error is NotSupportedQueryFragmentException err)
                        _editor.IndicatorFillRange(offset + err.Fragment.StartOffset, err.Fragment.FragmentLength);
                    else if (args.Error is QueryParseException parseErr)
                        _editor.IndicatorFillRange(offset + parseErr.Error.Offset, 1);

                    if (args.Error != null)
                    {
                        var error = CreateTextEditor(true);
                        error.Text = args.Error.Message;
                        error.ReadOnly = true;
                        AddResult(error, false);
                        return;
                    }

                    var queries = (Query[])args.Result;

                    foreach (var query in queries.Reverse())
                    {
                        if (execute)
                        {
                            Control display = null;

                            if (query.Result is EntityCollection queryResults)
                            {
                                var grid = new CRMGridView();

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
                                grid.Dock = DockStyle.Fill;
                                grid.EnableHeadersVisualStyles = false;
                                grid.EntityReferenceClickable = true;
                                grid.OrganizationService = Service;
                                grid.ReadOnly = true;
                                grid.RowHeadersWidth = 24;
                                grid.ShowEditingIcon = false;
                                grid.ShowFriendlyNames = Settings.Instance.ShowEntityReferenceNames;
                                grid.ShowIdColumn = false;
                                grid.ShowIndexColumn = false;
                                grid.ShowLocalTimes = Settings.Instance.ShowLocalTimes;
                                grid.ContextMenuStrip = gridContextMenuStrip;
                                grid.RecordClick += Grid_RecordClick;
                                grid.CellMouseUp += Grid_CellMouseUp;

                                if (query is SelectQuery select)
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

                                            foreach (var entity in queryResults.Entities)
                                            {
                                                if (entity.Contains(col))
                                                    entity[newCol] = entity[col];
                                            }

                                            colName = newCol;
                                        }

                                        grid.Columns.Add(colName, colName);
                                    }
                                }

                                grid.HandleCreated += (s, e) =>
                                {
                                    grid.DataSource = queryResults;
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

                                var panel = new Panel();
                                panel.Controls.Add(grid);

                                var statusBar = new StatusBar();
                                statusBar.Text = $"{queryResults.Entities.Count:N0} record(s) returned";
                                statusBar.SizingGrip = false;
                                panel.Controls.Add(statusBar);
                                display = panel;
                            }
                            else if (query.Result is string msg)
                            {
                                var msgDisplay = CreateTextEditor(false);
                                msgDisplay.Text = msg;
                                msgDisplay.ReadOnly = true;
                                display = msgDisplay;
                            }
                            else if (query.Result is Exception ex)
                            {
                                var msgDisplay = CreateTextEditor(true);
                                msgDisplay.Text = ex.Message;
                                msgDisplay.ReadOnly = true;
                                display = msgDisplay;
                            }

                            if (query is FetchXmlQuery fxq)
                            {
                                var xmlDisplay = CreateXmlEditor();
                                xmlDisplay.Text = fxq.FetchXmlString;
                                xmlDisplay.ReadOnly = true;

                                var postWarning = CreatePostProcessingWarning(fxq);
                                var toolbar = CreateFXBToolbar(xmlDisplay);

                                if (display == null)
                                {
                                    var panel = new Panel();
                                    panel.Controls.Add(xmlDisplay);

                                    if (postWarning != null)
                                        panel.Controls.Add(postWarning);

                                    panel.Controls.Add(toolbar);
                                    display = panel;
                                }
                                else
                                {
                                    var tab = new TabControl();
                                    tab.TabPages.Add("Results");
                                    tab.TabPages.Add("FetchXML");
                                    tab.TabPages[0].Controls.Add(display);
                                    tab.TabPages[1].Controls.Add(xmlDisplay);

                                    if (postWarning != null)
                                        tab.TabPages[1].Controls.Add(postWarning);

                                    tab.TabPages[1].Controls.Add(toolbar);

                                    display.Dock = DockStyle.Fill;
                                    xmlDisplay.Dock = DockStyle.Fill;

                                    display = tab;
                                }
                            }

                            AddResult(display, queries.Length > 1);
                        }
                        else if (query is FetchXmlQuery fxq)
                        {
                            var xmlDisplay = CreateXmlEditor();
                            xmlDisplay.Text = fxq.FetchXmlString;
                            xmlDisplay.ReadOnly = true;
                            xmlDisplay.Dock = DockStyle.Fill;
                            var postWarning = CreatePostProcessingWarning(fxq);
                            var toolbar = CreateFXBToolbar(xmlDisplay);
                            var container = new Panel();
                            container.Controls.Add(xmlDisplay);

                            if (postWarning != null)
                                container.Controls.Add(postWarning);

                            container.Controls.Add(toolbar);
                            AddResult(container, queries.Length > 1);
                        }
                    }
                }
            });
        }

        private void Grid_CellMouseUp(object sender, DataGridViewCellMouseEventArgs e)
        {
            if (e.Button == MouseButtons.Right)
            {
                if (e.ColumnIndex < 0 || e.RowIndex < 0)
                    return;

                var grid = (CRMGridView)sender;
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
            if (e.Entity.Contains(e.Attribute) && e.Entity[e.Attribute] is EntityReference)
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

        private Panel CreatePostProcessingWarning(FetchXmlQuery fxq)
        {
            if (fxq.Extensions.Count == 0)
                return null;

            var postWarning = new Panel
            {
                BackColor = SystemColors.Info,
                BorderStyle = BorderStyle.None,
                Dock = DockStyle.Top,
                Padding = new Padding(4),
                Height = 24
            };
            postWarning.Controls.Add(new System.Windows.Forms.Label
            {
                Text = "This query required additional processing. This FetchXML gives the required data, but will not give the final results when run outside SQL 4 CDS",
                ForeColor = SystemColors.InfoText,
                AutoSize = false,
                Dock = DockStyle.Fill
            });

            var image = new Bitmap(16, 16);

            using (var g = Graphics.FromImage(image))
            {
                g.InterpolationMode = System.Drawing.Drawing2D.InterpolationMode.HighQualityBicubic;
                g.DrawImage(SystemIcons.Warning.ToBitmap(), new Rectangle(Point.Empty, image.Size));
            }
            postWarning.Controls.Add(new PictureBox
            {
                Image = image,
                Height = 16,
                Width = 16,
                Dock = DockStyle.Left
            });
            
            return postWarning;
        }

        private void AddResult(Control control, bool multi)
        {
            control.Height = splitContainer.Panel2.Height * 2 / 3;
            control.Dock = multi ? DockStyle.Top : DockStyle.Fill;
            splitContainer.Panel2.Controls.Add(control);
        }

        private void gridContextMenuStrip_Opening(object sender, CancelEventArgs e)
        {
            var grid = (CRMGridView) gridContextMenuStrip.SourceControl;
            var entity = grid.SelectedCells.Count == 1 ? grid.SelectedCellRecords.Entities[0] : null;
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

        private void copyToolStripMenuItem_Click(object sender, EventArgs e)
        {
            var grid = (CRMGridView)gridContextMenuStrip.SourceControl;
            Clipboard.SetDataObject(grid.GetClipboardContent());
        }

        private void copyWithHeadersToolStripMenuItem_Click(object sender, EventArgs e)
        {
            var grid = (CRMGridView)gridContextMenuStrip.SourceControl;
            grid.ClipboardCopyMode = DataGridViewClipboardCopyMode.EnableAlwaysIncludeHeaderText;
            Clipboard.SetDataObject(grid.GetClipboardContent());
            grid.ClipboardCopyMode = DataGridViewClipboardCopyMode.EnableWithoutHeaderText;
        }

        private void openRecordToolStripMenuItem_Click(object sender, EventArgs e)
        {
            var grid = (CRMGridView)gridContextMenuStrip.SourceControl;
            var entity = grid.SelectedCells.Count == 1 ? grid.SelectedCellRecords.Entities[0] : null;
            var attr = grid.SelectedCells[0].OwningColumn.DataPropertyName;
            var entityReference = entity.GetAttributeValue<EntityReference>(attr);

            // Open record
            var url = new Uri(new Uri(_con.WebApplicationUrl), $"main.aspx?etn={entityReference.LogicalName}&id={entityReference.Id}&pagetype=entityrecord");
            Process.Start(url.ToString());
        }

        private void createSELECTQueryToolStripMenuItem_Click(object sender, EventArgs e)
        {
            var grid = (CRMGridView)gridContextMenuStrip.SourceControl;
            var entity = grid.SelectedCells.Count == 1 ? grid.SelectedCellRecords.Entities[0] : null;
            var attr = grid.SelectedCells[0].OwningColumn.DataPropertyName;
            var entityReference = entity.GetAttributeValue<EntityReference>(attr);

            // Create SELECT query
            var metadata = Metadata[entityReference.LogicalName];
            _editor.AppendText("\r\n\r\n");
            var end = _editor.TextLength;
            _editor.AppendText($"SELECT * FROM {entityReference.LogicalName} WHERE {metadata.PrimaryIdAttribute} = '{entityReference.Id}'");
            _editor.SetSelection(_editor.TextLength, end);
        }
    }
}
