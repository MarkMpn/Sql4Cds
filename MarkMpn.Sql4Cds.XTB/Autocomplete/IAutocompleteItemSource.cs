using System.Collections.Generic;

namespace AutocompleteMenuNS
{
    public interface IAutocompleteItemSource
    {
        IEnumerable<AutocompleteItem> GetItems(bool forced);
    }
}
