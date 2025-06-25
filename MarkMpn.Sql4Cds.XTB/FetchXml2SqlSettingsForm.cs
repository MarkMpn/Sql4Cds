using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using MarkMpn.Sql4Cds.Engine;
using Microsoft.Xrm.Sdk.Metadata;
using ScintillaNET;

namespace MarkMpn.Sql4Cds.XTB
{
    public partial class FetchXml2SqlSettingsForm : Form
    {
        private readonly FetchXml2SqlOptions _options;

        public FetchXml2SqlSettingsForm(FetchXml2SqlOptions options)
        {
            InitializeComponent();

            SetFetchXmlStyle(fetchXmlScintilla);
            SetSqlStyle(sqlScintilla);

            _options = options;

            operatorConversionComboBox.SelectedIndex = (int)options.ConvertFetchXmlOperatorsTo;
            useParametersCheckBox.Checked = options.UseParametersForLiterals;
            useUTCCheckBox.Checked = options.ConvertDateTimeToUtc;
            UpdateOptions();
        }

        protected override void OnFormClosed(FormClosedEventArgs e)
        {
            base.OnFormClosed(e);

            if (DialogResult == DialogResult.OK)
                UpdateOptions(_options);
        }

        private void SetFetchXmlStyle(Scintilla scintilla)
        {
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
            // I like fixed font for XML
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

            scintilla.ReadOnly = true;
        }

        private void SetSqlStyle(Scintilla scintilla)
        {
            scintilla.StyleResetDefault();
            scintilla.Styles[Style.Default].Font = Settings.Instance.EditorFontName;
            scintilla.Styles[Style.Default].Size = Settings.Instance.EditorFontSize;
            scintilla.StyleClearAll();

            scintilla.Lexer = Lexer.Sql;

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

            scintilla.ReadOnly = true;
        }

        private void UpdateOptions()
        {
            var options = new FetchXml2SqlOptions();
            UpdateOptions(options);

            sqlScintilla.ReadOnly = false;
            sqlScintilla.Text = FetchXml2Sql.Convert(null, new DemoMetadataCache(), fetchXmlScintilla.Text, options, out _);
            sqlScintilla.ReadOnly = true;
        }

        private void UpdateOptions(FetchXml2SqlOptions options)
        {
            options.ConvertFetchXmlOperatorsTo = (FetchXmlOperatorConversion)operatorConversionComboBox.SelectedIndex;
            options.UseParametersForLiterals = useParametersCheckBox.Checked;
            options.ConvertDateTimeToUtc = useUTCCheckBox.Checked;
        }

        private void operatorConversionComboBox_SelectedIndexChanged(object sender, EventArgs e)
        {
            UpdateOptions();
        }

        private void useParametersCheckBox_CheckedChanged(object sender, EventArgs e)
        {
            UpdateOptions();
        }

        private void useUTCCheckBox_CheckedChanged(object sender, EventArgs e)
        {
            UpdateOptions();
        }

        class DemoMetadataCache : IAttributeMetadataCache
        {
            private Dictionary<string, EntityMetadata> _cache;

            public DemoMetadataCache()
            {
                var contact = new EntityMetadata
                {
                    LogicalName = "contact"
                };

                var firstname = new StringAttributeMetadata { LogicalName = "firstname" };
                var fullname = new StringAttributeMetadata { LogicalName = "fullname" };
                var contactid = new UniqueIdentifierAttributeMetadata { LogicalName = "contactid" };
                var createdon = new DateTimeAttributeMetadata { LogicalName = "createdon", DateTimeBehavior = DateTimeBehavior.UserLocal };
                typeof(EntityMetadata).GetProperty(nameof(EntityMetadata.Attributes)).SetValue(contact, new AttributeMetadata[] { contactid, firstname, fullname, createdon });

                _cache = new Dictionary<string, EntityMetadata>
                {
                    [contact.LogicalName] = contact
                };
            }

            public EntityMetadata this[string name] => _cache[name];

            public EntityMetadata this[int otc] => throw new NotImplementedException();

            public bool TryGetMinimalData(string logicalName, out EntityMetadata metadata)
            {
                return TryGetValue(logicalName, out metadata);
            }

            public bool TryGetValue(string logicalName, out EntityMetadata metadata)
            {
                return _cache.TryGetValue(logicalName, out metadata);
            }

            public string[] RecycleBinEntities => throw new NotImplementedException();

            public IEnumerable<EntityMetadata> GetAllEntities() => throw new NotImplementedException();

            public string[] TryGetRecycleBinEntities() => throw new NotImplementedException();
        }
    }
}
