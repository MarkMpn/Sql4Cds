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

namespace MarkMpn.Sql4Cds.XTB
{
    public partial class FetchXmlControl : DocumentWindowBase
    {
        class XmlFragmentWriter : XmlTextWriter
        {
            public XmlFragmentWriter(TextWriter writer) : base(writer)
            {
            }

            public override void WriteStartDocument()
            {
                // Do nothing (omit the declaration)
            }
        }

        private static int _queryCounter;

        public FetchXmlControl()
        {
            InitializeComponent();

            DisplayName = $"FetchXML {++_queryCounter}";
            Modified = true;

            // Ref: https://gist.github.com/anonymous/63036aa8c1cefcfcb013

            // Reset the styles
            scintilla.StyleResetDefault();
            scintilla.Styles[Style.Default].Font = Settings.Instance.EditorFontName;
            scintilla.Styles[Style.Default].Size = Settings.Instance.EditorFontSize;
            scintilla.StyleClearAll();

            // Set the XML Lexer
            scintilla.Lexer = Lexer.Xml;

            // Show line numbers
            scintilla.Margins[0].Width = 20;

            // Enable folding
            scintilla.SetProperty("fold", "1");
            scintilla.SetProperty("fold.compact", "1");
            scintilla.SetProperty("fold.html", "1");

            // Use Margin 2 for fold markers
            scintilla.Margins[2].Type = MarginType.Symbol;
            scintilla.Margins[2].Mask = Marker.MaskFolders;
            scintilla.Margins[2].Sensitive = true;
            scintilla.Margins[2].Width = 20;

            // Reset folder markers
            for (int i = Marker.FolderEnd; i <= Marker.FolderOpen; i++)
            {
                scintilla.Markers[i].SetForeColor(SystemColors.ControlLightLight);
                scintilla.Markers[i].SetBackColor(SystemColors.ControlDark);
            }

            // Style the folder markers
            scintilla.Markers[Marker.Folder].Symbol = MarkerSymbol.BoxPlus;
            scintilla.Markers[Marker.Folder].SetBackColor(SystemColors.ControlText);
            scintilla.Markers[Marker.FolderOpen].Symbol = MarkerSymbol.BoxMinus;
            scintilla.Markers[Marker.FolderEnd].Symbol = MarkerSymbol.BoxPlusConnected;
            scintilla.Markers[Marker.FolderEnd].SetBackColor(SystemColors.ControlText);
            scintilla.Markers[Marker.FolderMidTail].Symbol = MarkerSymbol.TCorner;
            scintilla.Markers[Marker.FolderOpenMid].Symbol = MarkerSymbol.BoxMinusConnected;
            scintilla.Markers[Marker.FolderSub].Symbol = MarkerSymbol.VLine;
            scintilla.Markers[Marker.FolderTail].Symbol = MarkerSymbol.LCorner;

            // Enable automatic folding
            scintilla.AutomaticFold = AutomaticFold.Show | AutomaticFold.Click | AutomaticFold.Change;

            // Set the Styles
            scintilla.StyleResetDefault();
            scintilla.Styles[Style.Default].Font = Settings.Instance.EditorFontName;
            scintilla.Styles[Style.Default].Size = Settings.Instance.EditorFontSize;
            scintilla.StyleClearAll();
            scintilla.Styles[Style.Xml.Attribute].ForeColor = Color.Red;
            scintilla.Styles[Style.Xml.Entity].ForeColor = Color.Red;
            scintilla.Styles[Style.Xml.Comment].ForeColor = Color.Green;
            scintilla.Styles[Style.Xml.Tag].ForeColor = Color.Blue;
            scintilla.Styles[Style.Xml.TagEnd].ForeColor = Color.Blue;
            scintilla.Styles[Style.Xml.DoubleString].ForeColor = Color.DeepPink;
            scintilla.Styles[Style.Xml.SingleString].ForeColor = Color.DeepPink;
        }

        public void SetFocus()
        {
            scintilla.Focus();
        }

        public override void SettingsChanged()
        {
            base.SettingsChanged();

            // Update all styles on the editor to use the new font
            foreach (var style in scintilla.Styles)
            {
                style.Font = Settings.Instance.EditorFontName;
                style.Size = Settings.Instance.EditorFontSize;
            }
        }

        protected override string Type => "FetchXML";

        public override string Content
        {
            get { return scintilla.Text; }
            set
            {
                // Reformat XML to use single quotes for attributes so it can be copied into C# string literals easily
                if (!String.IsNullOrEmpty(value))
                {
                    try
                    {
                        using (var src = new StringReader(value))
                        using (var reader = XmlReader.Create(src, new XmlReaderSettings { ConformanceLevel = ConformanceLevel.Fragment }))
                        using (var writer = new StringWriter())
                        using (var xmlWriter = new XmlFragmentWriter(writer))
                        {
                            xmlWriter.QuoteChar = '\'';
                            xmlWriter.Formatting = Formatting.Indented;

                            if (reader.Read())
                            {
                                while (reader.ReadState != ReadState.EndOfFile)
                                    xmlWriter.WriteNode(reader, true);
                            }

                            value = writer.ToString();
                        }
                    }
                    catch
                    {
                        // Use the original value
                    }
                }

                scintilla.ReadOnly = false;
                scintilla.Text = value;
                scintilla.ReadOnly = true;
                scintilla.Focus();
            }
        }
    }
}
