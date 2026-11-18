using AutocompleteMenuNS;
using MarkMpn.Sql4Cds.Engine;
using Microsoft.Xrm.Sdk.Metadata;

namespace MarkMpn.Sql4Cds.XTB
{
    /// <summary>
    /// Adapts a shared <see cref="Autocomplete.SqlAutocompleteItem"/> to the XTB autocomplete menu
    /// </summary>
    class SqlAutocompleteMenuItem : AutocompleteItem, IAutoCompleteMenuItemCustomReplace
    {
        private readonly Autocomplete.SqlAutocompleteItem _item;

        public SqlAutocompleteMenuItem(Autocomplete.SqlAutocompleteItem item) : base(item.Text, GetImageIndex(item), item.MenuText)
        {
            _item = item;
        }

        public int ReplaceLength => _item.ReplaceLength;

        public override string ToolTipTitle
        {
            get => _item.ToolTipTitle;
            set { }
        }

        public override string ToolTipText
        {
            get => _item.ToolTipText;
            set { }
        }

        public override CompareResult Compare(string fragmentText)
        {
            return CompareResult.VisibleAndSelected;
        }

        public override string GetTextForReplace()
        {
            return _item.GetTextForReplace();
        }

        private static int GetImageIndex(Autocomplete.SqlAutocompleteItem item)
        {
            switch (item.Kind)
            {
                case AutocompleteItemKind.Instance:
                    return 5;

                case AutocompleteItemKind.Schema:
                    return 15;

                case AutocompleteItemKind.Table:
                    return 4;

                case AutocompleteItemKind.TableValuedFunction:
                    return 25;

                case AutocompleteItemKind.StoredProcedure:
                case AutocompleteItemKind.StoredProcedureParameter:
                    return 26;

                case AutocompleteItemKind.OneToManyJoin:
                    return 19;

                case AutocompleteItemKind.ManyToOneJoin:
                    return 18;

                case AutocompleteItemKind.Attribute:
                    return GetImageIndex(item.AttributeType);

                case AutocompleteItemKind.Function:
                    return 23;

                case AutocompleteItemKind.AggregateFunction:
                    return 24;

                case AutocompleteItemKind.OptionSetValue:
                case AutocompleteItemKind.EntityName:
                    return 11;

                case AutocompleteItemKind.VirtualAttribute:
                case AutocompleteItemKind.Collation:
                case AutocompleteItemKind.Variable:
                    return 13;

                default:
                    return -1;
            }
        }

        private static int GetImageIndex(AttributeTypeCode? attributeType)
        {
            switch (attributeType)
            {
                case AttributeTypeCode.BigInt:
                case AttributeTypeCode.Integer:
                    return 8;

                case AttributeTypeCode.Boolean:
                case AttributeTypeCode.Picklist:
                case AttributeTypeCode.State:
                case AttributeTypeCode.Status:
                    return 11;

                case AttributeTypeCode.Customer:
                case AttributeTypeCode.Owner:
                case AttributeTypeCode.PartyList:
                    return 12;

                case AttributeTypeCode.DateTime:
                    return 2;

                case AttributeTypeCode.Decimal:
                    return 3;

                case AttributeTypeCode.Lookup:
                    return 9;

                case AttributeTypeCode.Memo:
                    return 10;

                case AttributeTypeCode.Money:
                    return 0;

                case AttributeTypeCode.String:
                case AttributeTypeCode.Virtual:
                case AttributeTypeCode.EntityName:
                    return 13;

                case AttributeTypeCode.Uniqueidentifier:
                    return 14;

                default:
                    return -1;
            }
        }
    }
}
