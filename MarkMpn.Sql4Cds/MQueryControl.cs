using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using System.Xml;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using McTools.Xrm.Connection;
using ScintillaNET;

namespace MarkMpn.Sql4Cds
{
    public partial class MQueryControl : WeifenLuo.WinFormsUI.Docking.DockContent, IDocumentWindow
    {
        private static int _queryCounter;

        public MQueryControl()
        {
            InitializeComponent();

            Text = $"M Query (Power BI) {++_queryCounter} *";

            // Ref: https://github.com/jacobslusser/ScintillaNET/wiki/Automatic-Syntax-Highlighting#complete-recipe

            // Configuring the default style with properties
            // we have common to every lexer style saves time.
            scintilla.StyleResetDefault();
            scintilla.Styles[Style.Default].Font = "Consolas";
            scintilla.Styles[Style.Default].Size = 10;
            scintilla.StyleClearAll();

            // Configure the CPP (C#) lexer styles
            scintilla.Styles[Style.Cpp.Default].ForeColor = Color.Silver;
            scintilla.Styles[Style.Cpp.Comment].ForeColor = Color.FromArgb(0, 128, 0); // Green
            scintilla.Styles[Style.Cpp.CommentLine].ForeColor = Color.FromArgb(0, 128, 0); // Green
            scintilla.Styles[Style.Cpp.CommentLineDoc].ForeColor = Color.FromArgb(128, 128, 128); // Gray
            scintilla.Styles[Style.Cpp.Number].ForeColor = Color.Olive;
            scintilla.Styles[Style.Cpp.Word].ForeColor = Color.Blue;
            scintilla.Styles[Style.Cpp.Word2].ForeColor = Color.Blue;
            scintilla.Styles[Style.Cpp.String].ForeColor = Color.FromArgb(163, 21, 21); // Red
            scintilla.Styles[Style.Cpp.Character].ForeColor = Color.FromArgb(163, 21, 21); // Red
            scintilla.Styles[Style.Cpp.Verbatim].ForeColor = Color.FromArgb(163, 21, 21); // Red
            scintilla.Styles[Style.Cpp.StringEol].BackColor = Color.Pink;
            scintilla.Styles[Style.Cpp.Operator].ForeColor = Color.Purple;
            scintilla.Styles[Style.Cpp.Preprocessor].ForeColor = Color.Maroon;
            scintilla.Lexer = Lexer.Cpp;

            // Set the keywords
            scintilla.SetKeywords(0, "let in");
        }

        public string M
        {
            get { return scintilla.Text; }
            set
            {
                scintilla.ReadOnly = false;
                scintilla.Text = value;
                scintilla.ReadOnly = true;
                scintilla.Focus();
            }
        }

        public void SetFocus()
        {
            scintilla.Focus();
        }

        void IDocumentWindow.Format()
        {

        }

        void IDocumentWindow.Save()
        {
            throw new NotImplementedException();
        }

        TabContent IDocumentWindow.GetSessionDetails()
        {
            return new TabContent
            {
                Type = "M",
                Query = scintilla.Text
            };
        }

        void IDocumentWindow.RestoreSessionDetails(TabContent tab)
        {
            scintilla.ReadOnly = false;
            scintilla.Text = tab.Query;
            scintilla.ReadOnly = true;
        }
    }
}
