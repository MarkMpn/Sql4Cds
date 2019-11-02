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
using Cinteros.Xrm.CRMWinForm;
using System.Text.RegularExpressions;
using Microsoft.Xrm.Sdk.Metadata;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using System.IO;

namespace MarkMpn.Sql4Cds
{
    public partial class SqlQueryControl : UserControl
    {
        public SqlQueryControl(IOrganizationService org, AttributeMetadataCache metadata, Action<WorkAsyncInfo> workAsync, Action<Action> executeMethod, Action<MessageBusEventArgs> outgoingMessageHandler)
        {
            InitializeComponent();
            Service = org;
            Metadata = metadata;
            WorkAsync = workAsync;
            ExecuteMethod = executeMethod;
            OutgoingMessageHandler = outgoingMessageHandler;
            _editor = CreateSqlEditor();

            splitContainer.Panel1.Controls.Add(_editor);
        }

        public IOrganizationService Service { get; }
        public AttributeMetadataCache Metadata { get; }
        public Action<WorkAsyncInfo> WorkAsync { get; }
        public Action<Action> ExecuteMethod { get; }
        public Action<MessageBusEventArgs> OutgoingMessageHandler { get; }

        public void SetFocus()
        {
            _editor.Focus();
        }

        public void InsertText(string text)
        {
            _editor.ReplaceSelection(text);
            _editor.Focus();
        }

        public void Format()
        {
            var dom = new TSql150Parser(true);
            var fragment = dom.Parse(new StringReader(_editor.Text), out var errors);

            if (errors.Count != 0)
                return;

            new Sql150ScriptGenerator().GenerateScript(fragment, out var sql);
            _editor.Text = sql;
        }

        private readonly Scintilla _editor;

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

        private Scintilla CreateErrorEditor()
        {
            var scintilla = CreateEditor();

            scintilla.Lexer = Lexer.Null;
            
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

            return scintilla;
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
                Work = (worker, args) =>
                {
                    var queries = new Sql2FetchXml().Convert(sql, Metadata);

                    if (execute)
                    {
                        foreach (var query in queries)
                            query.Execute(Service, msg => worker.ReportProgress(-1, msg));
                    }

                    args.Result = queries;
                },
                PostWorkCallBack = (args) =>
                {
                    splitContainer.Panel2.Controls.Clear();

                    if (args.Error is NotSupportedQueryFragmentException err)
                        _editor.IndicatorFillRange(offset + err.Fragment.StartOffset, err.Fragment.FragmentLength);
                    else if (args.Error is QueryParseException parseErr)
                        _editor.IndicatorFillRange(offset + parseErr.Error.Offset, 1);

                    if (args.Error != null)
                    {
                        var error = CreateErrorEditor();
                        error.Text = args.Error.Message;
                        error.ReadOnly = true;
                        AddResult(error, false);
                        return;
                    }

                    var queries = (Query[])args.Result;

                    foreach (var query in queries)
                    {
                        if (execute)
                        {
                            Control display = null;

                            if (query.Result is EntityCollection queryResults)
                            {
                                var grid = new CRMGridView();

                                if (query is SelectQuery select)
                                {
                                    foreach (var col in select.ColumnSet)
                                        grid.Columns.Add(col, col);
                                }

                                grid.DataSource = queryResults;
                                display = grid;
                            }
                            else if (query.Result is string msg)
                            {
                                var msgDisplay = CreateErrorEditor();
                                msgDisplay.Text = msg;
                                msgDisplay.ReadOnly = true;
                                display = msgDisplay;
                            }

                            if (query is FetchXmlQuery fxq)
                            {
                                var xmlDisplay = CreateXmlEditor();
                                xmlDisplay.Text = FetchXmlQuery.Serialize(fxq.FetchXml);
                                xmlDisplay.ReadOnly = true;

                                var toolbar = CreateFXBToolbar(xmlDisplay);

                                if (display == null)
                                {
                                    var panel = new Panel();
                                    panel.Controls.Add(xmlDisplay);
                                    panel.Controls.Add(toolbar);
                                    display = panel;
                                }
                                else
                                {
                                    var tab = new TabControl();
                                    tab.TabPages.Add("Results");
                                    tab.TabPages.Add("FetchXml");
                                    tab.TabPages[0].Controls.Add(display);
                                    tab.TabPages[1].Controls.Add(toolbar);
                                    tab.TabPages[1].Controls.Add(xmlDisplay);

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
                            xmlDisplay.Text = FetchXmlQuery.Serialize(fxq.FetchXml);
                            xmlDisplay.ReadOnly = true;
                            var toolbar = CreateFXBToolbar(xmlDisplay);
                            AddResult(xmlDisplay, queries.Length > 1);
                            AddResult(toolbar, true);
                        }
                    }
                }
            });
        }

        private ToolStrip CreateFXBToolbar(Scintilla xmlEditor)
        {
            var toolbar = new ToolStrip();
            toolbar.ImageScalingSize = new Size(24, 24);
            var btn = new ToolStripButton();
            btn.Text = "Edit in";
            btn.Image = Properties.Resources.FXB;
            btn.ImageAlign = ContentAlignment.MiddleRight;
            btn.TextAlign = ContentAlignment.MiddleLeft;
            btn.TextImageRelation = TextImageRelation.TextBeforeImage;

            btn.ToolTipText = "Edit in FetchXML Builder";
            btn.Click += (sender, e) =>
            {
                OutgoingMessageHandler(new MessageBusEventArgs("FetchXML Builder")
                {
                    TargetArgument = xmlEditor.Text
                });
            };
            toolbar.Items.Add(btn);
            return toolbar;
        }

        private void AddResult(Control control, bool multi)
        {
            control.Height = splitContainer.Panel2.Height;
            control.Dock = multi ? DockStyle.Top : DockStyle.Fill;
            splitContainer.Panel2.Controls.Add(control);
        }
    }
}
