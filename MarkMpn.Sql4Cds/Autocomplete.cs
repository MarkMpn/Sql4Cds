using System;
using System.Collections.Generic;
using System.Linq;
using AutocompleteMenuNS;
using MarkMpn.Sql4Cds.Engine;
using Microsoft.Xrm.Sdk.Metadata;

namespace MarkMpn.Sql4Cds
{
    /// <summary>
    /// Produces Intellisense suggestions
    /// </summary>
    public class Autocomplete
    {
        private readonly EntityMetadata[] _entities;
        private readonly IAttributeMetadataCache _metadata;

        /// <summary>
        /// Creates a new <see cref="Autocomplete"/>
        /// </summary>
        /// <param name="entities">The list of entities available to use in the query</param>
        /// <param name="metadata">The cache of metadata about each entity</param>
        public Autocomplete(EntityMetadata[] entities, IAttributeMetadataCache metadata)
        {
            _entities = entities;
            _metadata = metadata;
        }

        /// <summary>
        /// Gets the list of Intellisense suggestions to show
        /// </summary>
        /// <param name="text">The current query text</param>
        /// <param name="pos">The index of the character in the <paramref name="text"/> that has just been entered</param>
        /// <param name="currentLength">The length of the current word that is being auto-completed</param>
        /// <returns>A sequence of suggestions to be shown to the user</returns>
        public IEnumerable<AutocompleteItem> GetSuggestions(string text, int pos, out int currentLength)
        {
            // If we're in the first word after a FROM or JOIN, show a list of table names
            string currentWord = null;
            string prevWord = null;
            string prevPrevWord = null;

            foreach (var word in ReverseWords(text, pos))
            {
                if (currentWord == null)
                {
                    currentWord = word;
                }
                else if (prevWord == null)
                {
                    prevWord = word;
                }
                else if (prevPrevWord == null)
                {
                    prevPrevWord = word;
                    break;
                }
            }

            currentLength = currentWord.Length;

            if (prevWord == null)
                return Array.Empty<AutocompleteItem>();

            switch (prevWord.ToLower())
            {
                case "from":
                case "insert":
                case "into":
                    // Show table list
                    if (_entities != null)
                        return FilterList(_entities.Select(x => new EntityAutocompleteItem(x, _metadata)).OrderBy(x => x), currentWord);
                    break;

                default:
                    // Find the FROM clause
                    var words = new List<string>();
                    var foundFrom = false;
                    var foundQueryStart = false;
                    var foundPossibleFrom = false;
                    string clause = null;

                    foreach (var word in ReverseWords(text, pos))
                    {
                        switch (word.ToLower())
                        {
                            case "from":
                                foundFrom = true;
                                clause = clause ?? "from";
                                break;

                            case "select":
                                foundQueryStart = true;
                                break;

                            case "update":
                            case "delete":
                                foundPossibleFrom = true;
                                foundQueryStart = true;
                                break;

                            case "join":
                                clause = clause ?? "join";
                                words.Insert(0, word);
                                break;

                            case "on":
                                clause = clause ?? "on";
                                words.Insert(0, word);
                                break;

                            case "where":
                            case "(":
                                words.Clear();
                                break;

                            case "order":
                            case "group":
                                break;

                            case "set":
                                words.Clear();
                                clause = clause ?? "set";
                                break;

                            case "insert":
                            case "into":
                                clause = clause ?? "insert";
                                foundQueryStart = true;
                                foundFrom = true;
                                break;

                            default:
                                if (!String.IsNullOrEmpty(word))
                                    words.Insert(0, word);
                                break;
                        }

                        if (foundFrom || foundQueryStart)
                            break;
                    }

                    if (!foundFrom)
                    {
                        var nextWords = new List<string>();
                        var foundQueryEnd = false;

                        foreach (var word in Words(text, pos))
                        {
                            switch (word.ToLower())
                            {
                                case "from":
                                    foundFrom = true;
                                    break;

                                case "group":
                                case "order":
                                case "select":
                                case "insert":
                                case "update":
                                case "delete":
                                    foundQueryEnd = true;
                                    break;

                                default:
                                    if (foundFrom)
                                        nextWords.Add(word);
                                    break;
                            }

                            if (foundQueryEnd)
                                break;
                        }

                        if (foundFrom)
                            words = nextWords;
                    }

                    IDictionary<string, string> tables = null;

                    if (foundFrom || (foundPossibleFrom && words.Count > 0))
                    {
                        // Try to get the table & alias names from the words in the possible FROM clause
                        tables = new Dictionary<string, string>();

                        for (var i = 0; i < words.Count; i++)
                        {
                            var tableName = words[i];
                            var alias = tableName;

                            if (i < words.Count - 1)
                            {
                                if (words[i + 1].ToLower() == "as" && i < words.Count - 2)
                                {
                                    alias = words[i + 2];
                                    i += 2;
                                }
                                else if (words[i + 1].ToLower() != "on" && words[i + 1].ToLower() != "left" && words[i + 1].ToLower() != "inner" && words[i + 1].ToLower() != "right" && words[i + 1].ToLower() != "join" && words[i + 1].ToLower() != "full")
                                {
                                    alias = words[i + 1];
                                    i++;
                                }
                            }

                            tables[alias] = tableName;

                            while (i < words.Count && words[i].ToLower() != "join")
                                i++;
                        }

                        // Start loading all the appropriate metadata in the background
                        foreach (var table in tables.Values)
                        {
                            if (_entities.Any(e => e.LogicalName.Equals(table, StringComparison.OrdinalIgnoreCase)))
                                _metadata.TryGetMinimalData(table, out _);
                        }
                    }

                    if (!prevWord.EndsWith(".") &&
                        !prevWord.EndsWith(",") &&
                        !prevWord.EndsWith("(") &&
                        !prevWord.EndsWith("+") &&
                        !prevWord.EndsWith("-") &&
                        !prevWord.EndsWith("*") &&
                        !prevWord.EndsWith("/") &&
                        !prevWord.EndsWith("=") &&
                        !prevWord.EndsWith(">") &&
                        !prevWord.EndsWith("<") &&
                        !prevWord.Equals("and", StringComparison.OrdinalIgnoreCase) &&
                        !prevWord.Equals("or", StringComparison.OrdinalIgnoreCase) &&
                        !prevWord.Equals("select", StringComparison.OrdinalIgnoreCase) &&
                        !prevWord.Equals("case", StringComparison.OrdinalIgnoreCase) &&
                        !prevWord.Equals("when", StringComparison.OrdinalIgnoreCase) &&
                        !prevWord.Equals("like", StringComparison.OrdinalIgnoreCase) &&
                        !prevWord.Equals("where", StringComparison.OrdinalIgnoreCase) &&
                        !prevWord.Equals("on", StringComparison.OrdinalIgnoreCase) &&
                        !prevWord.Equals("by", StringComparison.OrdinalIgnoreCase) &&
                        !prevWord.Equals("having", StringComparison.OrdinalIgnoreCase) &&
                        !prevWord.Equals("update", StringComparison.OrdinalIgnoreCase) &&
                        !prevWord.Equals("delete", StringComparison.OrdinalIgnoreCase) &&
                        !prevWord.Equals("insert", StringComparison.OrdinalIgnoreCase) &&
                        !prevWord.Equals("into", StringComparison.OrdinalIgnoreCase) &&
                        !prevWord.Equals("set", StringComparison.OrdinalIgnoreCase) &&
                        !prevWord.Equals("join", StringComparison.OrdinalIgnoreCase) &&
                        (prevPrevWord == null || !prevPrevWord.Equals("top", StringComparison.OrdinalIgnoreCase)))
                        return Array.Empty<AutocompleteItem>();

                    if (tables != null)
                    {
                        if (prevWord.Equals("join", StringComparison.OrdinalIgnoreCase))
                        {
                            // Suggest known relationships from the entities already in the FROM clause, followed by a list of all entities
                            // Exclude the table that's currently being entered from the suggestion sources
                            tables.Remove(currentWord);
                            var joinSuggestions = new List<AutocompleteItem>();

                            foreach (var table in tables)
                            {
                                if (_metadata.TryGetMinimalData(table.Value, out var metadata))
                                {
                                    if (metadata.OneToManyRelationships != null)
                                        joinSuggestions.AddRange(metadata.OneToManyRelationships.Select(rel => new JoinAutocompleteItem(rel, $"{rel.ReferencingEntity}{GetUniqueTableAlias(rel.ReferencingEntity, tables)} ON {table.Key}.{rel.ReferencedAttribute} = {GetUniqueTableName(rel.ReferencingEntity, tables)}.{rel.ReferencingAttribute}", true, _entities, _metadata)));
                                    
                                    if (metadata.ManyToOneRelationships != null)
                                        joinSuggestions.AddRange(metadata.ManyToOneRelationships.Select(rel => new JoinAutocompleteItem(rel, $"{rel.ReferencedEntity}{GetUniqueTableAlias(rel.ReferencedEntity, tables)} ON {table.Key}.{rel.ReferencingAttribute} = {GetUniqueTableName(rel.ReferencedEntity, tables)}.{rel.ReferencedAttribute}", false, _entities, _metadata)));
                                }
                            }

                            joinSuggestions.Sort();

                            joinSuggestions.AddRange(_entities.Select(e => new EntityAutocompleteItem(e, _metadata)).OrderBy(name => name));
                            return FilterList(joinSuggestions, currentWord);
                        }

                        var additionalSuggestions = (IEnumerable<AutocompleteItem>) Array.Empty<AutocompleteItem>();

                        if (prevWord.Equals("on", StringComparison.OrdinalIgnoreCase) && _metadata.TryGetMinimalData(tables[prevPrevWord], out var newTableMetadata))
                        {
                            // Suggest known relationships from the other entities in the FROM clause, followed by the normal list of attributes
                            additionalSuggestions = new List<AutocompleteItem>();

                            if (newTableMetadata.OneToManyRelationships != null)
                                ((List<AutocompleteItem>)additionalSuggestions).AddRange(newTableMetadata.OneToManyRelationships.SelectMany(rel => tables.Where(table => table.Key != prevPrevWord && table.Value == rel.ReferencingEntity).Select(table => new JoinAutocompleteItem(rel, $"{table.Key}.{rel.ReferencingAttribute} = {prevPrevWord}.{rel.ReferencedAttribute}", false, _entities, _metadata))));

                            if (newTableMetadata.ManyToOneRelationships != null)
                                ((List<AutocompleteItem>)additionalSuggestions).AddRange(newTableMetadata.ManyToOneRelationships.SelectMany(rel => tables.Where(table => table.Key != prevPrevWord && table.Value == rel.ReferencedEntity).Select(table => new JoinAutocompleteItem(rel, $"{table.Key}.{rel.ReferencedAttribute} = {prevPrevWord}.{rel.ReferencingAttribute}", true, _entities, _metadata))));

                            ((List<AutocompleteItem>)additionalSuggestions).Sort();
                        }

                        if (prevWord.Equals("update", StringComparison.OrdinalIgnoreCase) ||
                            prevWord.Equals("delete", StringComparison.OrdinalIgnoreCase))
                            return FilterList(tables.Select(kvp => new { Entity = _entities.SingleOrDefault(e => e.LogicalName == kvp.Value), Alias = kvp.Key }).Where(e => e.Entity != null).Select(x => new EntityAutocompleteItem(x.Entity, x.Alias, _metadata)).OrderBy(x => x), currentWord);

                        if (clause == "set" && (prevWord.Equals("set", StringComparison.OrdinalIgnoreCase) || prevWord == ","))
                        {
                            var targetTable = "";

                            foreach (var word in ReverseWords(text, pos))
                            {
                                if (word.Equals("update", StringComparison.OrdinalIgnoreCase))
                                    break;

                                targetTable = word;
                            }

                            if (tables.TryGetValue(targetTable, out var tableName) && _metadata.TryGetMinimalData(tableName, out var metadata))
                                return FilterList(metadata.Attributes.Where(a => a.IsValidForUpdate != false).Select(a => new AttributeAutocompleteItem(a, _metadata)).OrderBy(a => a), currentWord);
                        }

                        if (currentWord.Contains("."))
                        {
                            // Autocomplete list is attributes in the current table
                            var alias = currentWord.Substring(0, currentWord.IndexOf('.'));
                            currentWord = currentWord.Substring(currentWord.IndexOf('.') + 1);
                            currentLength = currentWord.Length;

                            if (tables.TryGetValue(alias, out var tableName))
                            {
                                if (_metadata.TryGetMinimalData(tableName, out var metadata))
                                    return FilterList(metadata.Attributes.Select(a => new AttributeAutocompleteItem(a, _metadata)).OrderBy(a => a), currentWord);
                            }
                        }
                        else if (clause == "join")
                        {
                            // Entering a table alias, nothing useful to auto-complete
                        }
                        else
                        {
                            // Autocomplete list is:
                            // * table/alias names
                            // * attribute names unique across tables
                            // * functions
                            var items = new List<AutocompleteItem>();

                            if (clause != "insert")
                                items.AddRange(tables.Select(kvp => new { Entity = _entities.SingleOrDefault(e => e.LogicalName == kvp.Value), Alias = kvp.Key }).Where(x => x.Entity != null).Select(x => new EntityAutocompleteItem(x.Entity, x.Alias, _metadata)));

                            var attributes = new List<AttributeMetadata>();

                            foreach (var table in tables)
                            {
                                if (_metadata.TryGetMinimalData(table.Value, out var metadata))
                                    attributes.AddRange(metadata.Attributes);
                            }

                            items.AddRange(attributes.GroupBy(x => x.LogicalName).Where(g => g.Count() == 1).Select(g => new AttributeAutocompleteItem(g.Single(), _metadata)));
                            items.Sort();

                            return additionalSuggestions.Concat(FilterList(items, currentWord)).OrderBy(x => x);
                        }
                    }
                    else if (prevWord.Equals("update", StringComparison.OrdinalIgnoreCase))
                    {
                        return FilterList(_entities.Select(e => new EntityAutocompleteItem(e, _metadata)), currentWord).OrderBy(x => x);
                    }

                    break;
            }

            return Array.Empty<AutocompleteItem>();
        }

        private IEnumerable<AutocompleteItem> FilterList(IEnumerable<AutocompleteItem> list, string currentWord)
        {
            var startsWith = list.Where(obj => obj.Text.StartsWith(currentWord, StringComparison.OrdinalIgnoreCase)).ToList();

            if (startsWith.Any())
                return startsWith;

            return list.Where(obj => obj.Text.IndexOf(currentWord, StringComparison.OrdinalIgnoreCase) != -1);
        }

        private IEnumerable<string> Words(string text, int start)
        {
            var inWord = false;

            for (var i = start; i < text.Length; i++)
            {
                if (Char.IsWhiteSpace(text[i]))
                {
                    if (inWord)
                    {
                        var wordEnd = i - 1;
                        var word = text.Substring(start, wordEnd - start + 1);

                        yield return word;
                        inWord = false;
                    }
                }
                else if (!inWord)
                {
                    start = i;
                    inWord = true;
                }
            }

            if (inWord)
                yield return text.Substring(start, text.Length - start);
        }

        private IEnumerable<string> ReverseWords(string text, int end)
        {
            var inWord = true;

            for (var i = end; i >= 0; i--)
            {
                if (Char.IsWhiteSpace(text[i]) || (Char.IsPunctuation(text[i]) && text[i] != '.' && text[i] != '_'))
                {
                    if (inWord)
                    {
                        var wordStart = i + 1;
                        var word = text.Substring(wordStart, end - wordStart + 1);

                        yield return word;
                        inWord = false;
                    }

                    if (Char.IsPunctuation(text[i]))
                    {
                        yield return text[i].ToString();
                        //i--;
                    }
                }
                else if (!inWord)
                {
                    end = i;
                    inWord = true;
                }
            }

            if (inWord)
                yield return text.Substring(0, end + 1);
        }

        private static int GetIconIndex(AttributeMetadata a)
        {
            switch (a.AttributeType.Value)
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

                case AttributeTypeCode.Double:
                    return -1;

                case AttributeTypeCode.Lookup:
                    return 9;

                case AttributeTypeCode.Memo:
                    return 10;

                case AttributeTypeCode.Money:
                    return 0;

                case AttributeTypeCode.String:
                case AttributeTypeCode.Virtual:
                    return 13;

                case AttributeTypeCode.Uniqueidentifier:
                    return 14;

                default:
                    return -1;
            }
        }

        private string GetUniqueTableAlias(string name, IDictionary<string,string> tables)
        {
            var alias = GetUniqueTableName(name, tables);

            if (name == alias)
                return null;

            return " AS " + alias;
        }

        private string GetUniqueTableName(string name, IDictionary<string,string> tables)
        {
            if (!tables.ContainsKey(name))
                return name;

            var suffix = 2;

            while (tables.ContainsKey(name + suffix))
                suffix++;

            return name + suffix;
        }

        class SqlAutocompleteItem : AutocompleteItem, IComparable
        {
            public SqlAutocompleteItem(string text, int imageIndex) : base(text, imageIndex)
            {
            }

            public override CompareResult Compare(string fragmentText)
            {
                return CompareResult.VisibleAndSelected;
            }

            public int CompareTo(object obj)
            {
                var other = (AutocompleteItem)obj;

                return Text.CompareTo(other.Text);
            }
        }

        class EntityAutocompleteItem : SqlAutocompleteItem
        {
            private readonly EntityMetadata _entity;
            private readonly IAttributeMetadataCache _metadata;

            public EntityAutocompleteItem(EntityMetadata entity, IAttributeMetadataCache metadata) : this(entity, entity.LogicalName, metadata)
            {
            }

            public EntityAutocompleteItem(EntityMetadata entity, string alias, IAttributeMetadataCache metadata) : base(alias, 4)
            {
                _entity = entity;
                _metadata = metadata;
            }

            public override string ToolTipTitle
            {
                get => _entity.DisplayName?.UserLocalizedLabel?.Label;
                set => base.ToolTipTitle = value;
            }

            public override string ToolTipText
            {
                get => _entity.Description?.UserLocalizedLabel?.Label;
                set => base.ToolTipText = value;
            }

            public override string GetTextForReplace()
            {
                _metadata.TryGetMinimalData(_entity.LogicalName, out _);
                return base.GetTextForReplace();
            }
        }

        class JoinAutocompleteItem : SqlAutocompleteItem
        {
            private readonly EntityMetadata _rhs;
            private readonly AttributeMetadata _attribute;

            public JoinAutocompleteItem(OneToManyRelationshipMetadata relationship, string join, bool oneToMany, EntityMetadata[] entities, IAttributeMetadataCache metadata) : base(join, oneToMany ? 19 : 18)
            {
                _rhs = entities.SingleOrDefault(e => e.LogicalName == relationship.ReferencingEntity);

                if (!oneToMany && metadata.TryGetMinimalData(relationship.ReferencingEntity, out _rhs))
                    _attribute = _rhs.Attributes.Single(a => a.LogicalName == relationship.ReferencingAttribute);
            }

            public override string ToolTipTitle
            {
                get
                {
                    if (_attribute != null)
                        return _attribute.DisplayName?.UserLocalizedLabel?.Label;

                    return _rhs?.DisplayName?.UserLocalizedLabel?.Label;
                }
                set => base.ToolTipTitle = value;
            }

            public override string ToolTipText
            {
                get
                {
                    if (_attribute != null)
                        return _attribute.Description?.UserLocalizedLabel?.Label;

                    return _rhs?.Description?.UserLocalizedLabel?.Label;
                }
                set => base.ToolTipText = value;
            }
        }

        class AttributeAutocompleteItem : SqlAutocompleteItem
        {
            private readonly AttributeMetadata _attribute;
            private readonly AttributeMetadata _attributeOf;

            public AttributeAutocompleteItem(AttributeMetadata attribute, IAttributeMetadataCache metadata) : base(attribute.LogicalName, GetIconIndex(attribute))
            {
                _attribute = attribute;

                if (!String.IsNullOrEmpty(_attribute.AttributeOf) && metadata.TryGetMinimalData(attribute.EntityLogicalName, out var entity))
                    _attributeOf = entity.Attributes.Single(a => a.LogicalName == _attribute.AttributeOf);
            }

            public override string ToolTipTitle
            {
                get => (_attributeOf ?? _attribute).DisplayName?.UserLocalizedLabel?.Label;
                set => base.ToolTipTitle = value;
            }

            public override string ToolTipText
            {
                get
                {
                    var description = (_attributeOf ?? _attribute).Description?.UserLocalizedLabel?.Label;

                    if (_attribute.AttributeType == AttributeTypeCode.Picklist)
                        description += "\r\n\r\nThis attribute holds the underlying integer value of the field";
                    else if (_attribute.AttributeType == AttributeTypeCode.Lookup || _attribute.AttributeType == AttributeTypeCode.Customer || _attribute.AttributeType == AttributeTypeCode.Owner)
                        description += "\r\n\r\nThis attribute holds the underlying unique identifier value of the field";
                    else if (_attributeOf != null && _attribute.LogicalName == _attribute.AttributeOf + "name")
                        description += $"\r\n\r\nThis attribute holds the display name of the {_attributeOf.LogicalName} field";
                    else if (_attributeOf != null && _attribute.LogicalName == _attribute.AttributeOf + "yominame")
                        description += $"\r\n\r\nThis attribute holds the phonetic name of the {_attributeOf.LogicalName} field";
                    else if (_attributeOf != null && _attribute.LogicalName == _attribute.AttributeOf + "type")
                        description += $"\r\n\r\nThis attribute holds the logical name of the type of record referenced by the {_attributeOf.LogicalName} field";

                    return description;
                }
                set => base.ToolTipText = value;
            }
        }
    }
}
