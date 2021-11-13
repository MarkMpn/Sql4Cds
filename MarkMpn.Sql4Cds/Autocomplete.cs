using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlTypes;
using System.Linq;
using System.Reflection;
using System.Text;
using AutocompleteMenuNS;
using MarkMpn.Sql4Cds.Engine;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Metadata;
using static MarkMpn.Sql4Cds.FunctionMetadata;

namespace MarkMpn.Sql4Cds
{
    /// <summary>
    /// Produces Intellisense suggestions
    /// </summary>
    public class Autocomplete
    {
        private readonly IDictionary<string, AutocompleteDataSource> _dataSources;
        private readonly string _primaryDataSource;

        /// <summary>
        /// Creates a new <see cref="Autocomplete"/>
        /// </summary>
        /// <param name="entities">The list of entities available to use in the query</param>
        /// <param name="metadata">The cache of metadata about each entity</param>
        public Autocomplete(IDictionary<string, AutocompleteDataSource> dataSources, string primaryDataSource)
        {
            _dataSources = dataSources;
            _primaryDataSource = primaryDataSource;
        }

        /// <summary>
        /// Gets the list of Intellisense suggestions to show
        /// </summary>
        /// <param name="text">The current query text</param>
        /// <param name="pos">The index of the character in the <paramref name="text"/> that has just been entered</param>
        /// <returns>A sequence of suggestions to be shown to the user</returns>
        public IEnumerable<SqlAutocompleteItem> GetSuggestions(string text, int pos)
        {
            // Don't try to auto-complete inside string literals
            if (InStringLiteral(text, pos))
                return Array.Empty<SqlAutocompleteItem>();

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

            // Don't try to auto-complete numbers
            if (Decimal.TryParse(currentWord, out _))
                return Array.Empty<SqlAutocompleteItem>();

            var currentLength = currentWord.Length;

            if (prevWord == null)
                return Array.Empty<SqlAutocompleteItem>();

            switch (prevWord.ToLower())
            {
                case "from":
                case "insert":
                case "into":
                    return AutocompleteTableName(currentWord);

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
                                words.Clear();
                                clause = clause ?? "where";
                                break;

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
                            if (!TryParseTableName(tableName, out _, out _, out var alias))
                                alias = tableName;

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
                            if (TryParseTableName(table, out var instanceName, out _, out var tableName) && _dataSources.TryGetValue(instanceName, out var instance) && instance.Entities.Any(e => e.LogicalName.Equals(tableName, StringComparison.OrdinalIgnoreCase) && e.DataProviderId != MetaMetadataCache.ProviderId))
                                instance.Metadata.TryGetMinimalData(table, out _);
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
                        return Array.Empty<SqlAutocompleteItem>();

                    if (tables != null)
                    {
                        if (prevWord.Equals("join", StringComparison.OrdinalIgnoreCase))
                        {
                            // Suggest known relationships from the entities already in the FROM clause, followed by a list of all entities
                            // Exclude the table that's currently being entered from the suggestion sources
                            tables.Remove(currentWord);
                            var joinSuggestions = new List<SqlAutocompleteItem>();

                            foreach (var table in tables)
                            {
                                if (TryParseTableName(table.Value, out var instanceName, out _, out var tableName) && _dataSources.TryGetValue(instanceName, out var instance) && instance.Metadata.TryGetMinimalData(tableName, out var metadata))
                                {
                                    if (metadata.OneToManyRelationships != null)
                                        joinSuggestions.AddRange(metadata.OneToManyRelationships.Select(rel => new JoinAutocompleteItem(rel, $"{rel.ReferencingEntity}{GetUniqueTableAlias(rel.ReferencingEntity, tables)} ON {table.Key}.{rel.ReferencedAttribute} = {GetUniqueTableName(rel.ReferencingEntity, tables)}.{rel.ReferencingAttribute}", true, instance.Entities, instance.Metadata, currentLength)));
                                    
                                    if (metadata.ManyToOneRelationships != null)
                                        joinSuggestions.AddRange(metadata.ManyToOneRelationships.Select(rel => new JoinAutocompleteItem(rel, $"{rel.ReferencedEntity}{GetUniqueTableAlias(rel.ReferencedEntity, tables)} ON {table.Key}.{rel.ReferencingAttribute} = {GetUniqueTableName(rel.ReferencedEntity, tables)}.{rel.ReferencedAttribute}", false, instance.Entities, instance.Metadata, currentLength)));
                                }
                            }

                            joinSuggestions.Sort();

                            return FilterList(joinSuggestions, currentWord).Concat(AutocompleteTableName(currentWord));
                        }

                        var additionalSuggestions = (IEnumerable<SqlAutocompleteItem>) Array.Empty<SqlAutocompleteItem>();

                        if (prevWord.Equals("on", StringComparison.OrdinalIgnoreCase) && tables.TryGetValue(prevPrevWord, out var joinTableName))
                        {
                            if (TryParseTableName(joinTableName, out var instanceName, out _, out joinTableName) && _dataSources.TryGetValue(instanceName, out var instance) && instance.Metadata.TryGetMinimalData(joinTableName, out var newTableMetadata))
                            {
                                // Suggest known relationships from the other entities in the FROM clause, followed by the normal list of attributes
                                additionalSuggestions = new List<SqlAutocompleteItem>();

                                if (newTableMetadata.OneToManyRelationships != null)
                                    ((List<SqlAutocompleteItem>)additionalSuggestions).AddRange(newTableMetadata.OneToManyRelationships.SelectMany(rel => tables.Where(table => table.Key != prevPrevWord && table.Value == rel.ReferencingEntity).Select(table => new JoinAutocompleteItem(rel, $"{table.Key}.{rel.ReferencingAttribute} = {prevPrevWord}.{rel.ReferencedAttribute}", false, instance.Entities, instance.Metadata, currentLength))));

                                if (newTableMetadata.ManyToOneRelationships != null)
                                    ((List<SqlAutocompleteItem>)additionalSuggestions).AddRange(newTableMetadata.ManyToOneRelationships.SelectMany(rel => tables.Where(table => table.Key != prevPrevWord && table.Value == rel.ReferencedEntity).Select(table => new JoinAutocompleteItem(rel, $"{table.Key}.{rel.ReferencedAttribute} = {prevPrevWord}.{rel.ReferencingAttribute}", true, instance.Entities, instance.Metadata, currentLength))));

                                ((List<SqlAutocompleteItem>)additionalSuggestions).Sort();
                            }
                        }

                        if (prevWord.Equals("update", StringComparison.OrdinalIgnoreCase) ||
                            prevWord.Equals("delete", StringComparison.OrdinalIgnoreCase))
                        {
                            if (foundFrom)
                            {
                                var suggestions = new List<SqlAutocompleteItem>();

                                foreach (var table in tables)
                                {
                                    if (TryParseTableName(table.Value, out var instanceName, out _, out var tableName) && _dataSources.TryGetValue(instanceName, out var instance))
                                    {
                                        var entity = instance.Entities.SingleOrDefault(e => e.LogicalName == tableName);

                                        if (entity != null)
                                            suggestions.Add(new EntityAutocompleteItem(entity, table.Key, instance.Metadata, currentLength));
                                    }
                                }

                                suggestions.Sort();
                                return FilterList(suggestions, currentWord);
                            }

                            return AutocompleteTableName(currentWord);
                        }

                        if (clause == "set" && (prevWord.Equals("set", StringComparison.OrdinalIgnoreCase) || prevWord == ","))
                        {
                            var targetTable = "";

                            foreach (var word in ReverseWords(text, pos))
                            {
                                if (word.Equals("update", StringComparison.OrdinalIgnoreCase))
                                    break;

                                targetTable = word;
                            }

                            if (tables.TryGetValue(targetTable, out var tableName))
                            {
                                if (TryParseTableName(tableName, out var instanceName, out _, out tableName) && _dataSources.TryGetValue(instanceName, out var instance) && instance.Metadata.TryGetMinimalData(tableName, out var metadata))
                                    return FilterList(metadata.Attributes.Where(a => a.IsValidForUpdate != false && a.AttributeOf == null).SelectMany(a => AttributeAutocompleteItem.CreateList(a, currentLength, true)).OrderBy(a => a), currentWord);
                            }
                        }

                        if (currentWord.Contains("."))
                        {
                            // Autocomplete list is attributes in the current table
                            // TODO: Could also be schemas in the current instance or tables in the current schema when in the FROM clause
                            var alias = currentWord.Substring(0, currentWord.IndexOf('.'));
                            currentWord = currentWord.Substring(currentWord.IndexOf('.') + 1);
                            currentLength = currentWord.Length;

                            if (tables.TryGetValue(alias, out var tableName))
                            {
                                if (TryParseTableName(tableName, out var instanceName, out _, out tableName) && _dataSources.TryGetValue(instanceName, out var instance) && instance.Metadata.TryGetMinimalData(tableName, out var metadata))
                                    return FilterList(metadata.Attributes.Where(a => a.IsValidForRead != false && a.AttributeOf == null).SelectMany(a => AttributeAutocompleteItem.CreateList(a, currentLength, false)).OrderBy(a => a), currentWord);
                            }
                        }
                        else if (clause == "join")
                        {
                            // Entering a table alias, nothing useful to auto-complete
                        }
                        else if (clause == "insert" && tables.Count == 1)
                        {
                            var tableName = tables.Single().Value;
                            
                            if (TryParseTableName(tableName, out var instanceName, out _, out tableName) && _dataSources.TryGetValue(instanceName, out var instance) && instance.Metadata.TryGetMinimalData(tableName, out var metadata))
                            {
                                Func<AttributeMetadata, bool> attributeFilter;

                                if (metadata.LogicalName == "listmember")
                                {
                                    attributeFilter = a => a.LogicalName == "listid" || a.LogicalName == "entityid";
                                }
                                else if (metadata.IsIntersect == true)
                                {
                                    var relationship = metadata.ManyToManyRelationships.Single();
                                    attributeFilter = a => a.LogicalName == relationship.Entity1IntersectAttribute || a.LogicalName == relationship.Entity2IntersectAttribute;
                                }
                                else
                                {
                                    attributeFilter = a => a.IsValidForCreate != false && a.AttributeOf == null;
                                }
                                return FilterList(metadata.Attributes.Where(attributeFilter).SelectMany(a => AttributeAutocompleteItem.CreateList(a, currentLength, true)).OrderBy(a => a), currentWord);
                            }
                        }
                        else
                        {
                            // Autocomplete list is:
                            // * table/alias names
                            // * attribute names unique across tables
                            // * functions
                            var items = new List<SqlAutocompleteItem>();
                            var attributes = new List<AttributeMetadata>();

                            foreach (var table in tables)
                            {
                                if (TryParseTableName(table.Value, out var instanceName, out _, out var tableName) && _dataSources.TryGetValue(instanceName, out var instance))
                                {
                                    var entity = instance.Entities.SingleOrDefault(e => e.LogicalName == tableName && e.DataProviderId != MetaMetadataCache.ProviderId || ("metadata." + e.LogicalName) == table.Value && e.DataProviderId == MetaMetadataCache.ProviderId);

                                    if (entity != null)
                                        items.Add(new EntityAutocompleteItem(entity, table.Key, instance.Metadata, currentLength));

                                    if (instance.Metadata.TryGetMinimalData(tableName, out var metadata))
                                        attributes.AddRange(metadata.Attributes);
                                }
                            }

                            items.AddRange(attributes.Where(a => a.IsValidForRead != false && a.AttributeOf == null).GroupBy(x => x.LogicalName).Where(g => g.Count() == 1).SelectMany(g => AttributeAutocompleteItem.CreateList(g.Single(), currentLength, false)));

                            items.AddRange(typeof(FunctionMetadata.SqlFunctions).GetMethods(BindingFlags.DeclaredOnly | BindingFlags.Instance | BindingFlags.Public).Select(m => new FunctionAutocompleteItem(m, currentLength)));

                            if (clause == "where" && prevWord == "=")
                            {
                                // Check if there are any applicable filter operator functions that match the type of the current attribute
                                var identifiers = prevPrevWord.Split('.');
                                var attribute = default(AttributeMetadata);
                                
                                if (identifiers.Length == 2)
                                {
                                    if (tables.TryGetValue(identifiers[0], out var tableName))
                                    {
                                        if (TryParseTableName(tableName, out var instanceName, out _, out tableName) && _dataSources.TryGetValue(instanceName, out var instance) && instance.Metadata.TryGetMinimalData(tableName, out var entity))
                                        {
                                            attribute = entity.Attributes.SingleOrDefault(a => a.LogicalName.Equals(identifiers[1], StringComparison.OrdinalIgnoreCase));
                                        }
                                    }
                                }
                                else
                                {
                                    foreach (var table in tables.Values)
                                    {
                                        if (TryParseTableName(table, out var instanceName, out _, out var tableName) && _dataSources.TryGetValue(instanceName, out var instance) && instance.Metadata.TryGetMinimalData(tableName, out var entity))
                                        {
                                            var tableAttribute = entity.Attributes.SingleOrDefault(a => a.LogicalName.Equals(identifiers[0], StringComparison.OrdinalIgnoreCase));

                                            if (tableAttribute != null)
                                            {
                                                if (attribute == null)
                                                {
                                                    attribute = tableAttribute;
                                                }
                                                else
                                                {
                                                    attribute = null;
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                }

                                if (attribute != null)
                                {
                                    var expectedType = default(Type);

                                    if (attribute.AttributeType == AttributeTypeCode.String || attribute.AttributeType == AttributeTypeCode.Memo)
                                        expectedType = typeof(SqlString);
                                    else if (attribute.AttributeType == AttributeTypeCode.DateTime)
                                        expectedType = typeof(SqlDateTime);
                                    else if (attribute.AttributeType == AttributeTypeCode.Uniqueidentifier || attribute.AttributeType == AttributeTypeCode.Lookup || attribute.AttributeType == AttributeTypeCode.Owner || attribute.AttributeType == AttributeTypeCode.Customer)
                                        expectedType = typeof(SqlGuid);
                                    else if (attribute.AttributeTypeName == "MultiSelectPicklistType")
                                        expectedType = typeof(SqlString);

                                    if (expectedType != null)
                                        items.AddRange(typeof(FetchXmlConditionMethods).GetMethods(BindingFlags.Public | BindingFlags.Static).Where(m => m.GetParameters()[0].ParameterType == expectedType).Select(m => new FunctionAutocompleteItem(m, currentLength)));
                                }
                            }

                            items.Sort();

                            return additionalSuggestions.Concat(FilterList(items, currentWord)).OrderBy(x => x);
                        }
                    }
                    else if (prevWord.Equals("update", StringComparison.OrdinalIgnoreCase) ||
                        prevWord.Equals("insert", StringComparison.OrdinalIgnoreCase) ||
                        prevPrevWord != null && prevPrevWord.Equals("insert", StringComparison.OrdinalIgnoreCase) && prevWord.Equals("into", StringComparison.OrdinalIgnoreCase))
                    {
                        return AutocompleteTableName(currentWord);
                    }

                    break;
            }

            return Array.Empty<SqlAutocompleteItem>();
        }

        private IEnumerable<SqlAutocompleteItem> AutocompleteTableName(string currentWord)
        {
            var currentLength = currentWord.Length;
            var list = new List<SqlAutocompleteItem>();

            if (String.IsNullOrEmpty(currentWord))
            {
                // If there's multiple instances, show them
                if (_dataSources.Count > 1)
                    list.AddRange(_dataSources.Values.Select(x => new InstanceAutocompleteItem(x, currentLength)));

                // Show table list
                if (_dataSources.TryGetValue(_primaryDataSource, out var ds) && ds.Entities != null)
                    list.AddRange(ds.Entities.Select(x => new EntityAutocompleteItem(x, ds.Metadata, currentLength, false)));
            }
            else if (TryParseTableName(currentWord, out var instanceName, out var schemaName, out var tableName, out var parts, out var lastPartLength))
            {
                var lastPart = tableName;

                if (parts == 1)
                {
                    // Could be an instance name
                    list.AddRange(_dataSources.Values.Where(x => x.Name.StartsWith(lastPart, StringComparison.OrdinalIgnoreCase)).Select(x => new InstanceAutocompleteItem(x, lastPartLength)));
                }

                if (parts == 1 || parts == 2)
                {
                    // Could be a schema name
                    if ("dbo".StartsWith(lastPart, StringComparison.OrdinalIgnoreCase))
                        list.Add(new SchemaAutocompleteItem("dbo", lastPartLength));

                    if ("metadata".StartsWith(lastPart, StringComparison.OrdinalIgnoreCase))
                        list.Add(new SchemaAutocompleteItem("metadata", lastPartLength));
                }

                // Could be a table name
                if (_dataSources.TryGetValue(instanceName, out var instance) && instance.Entities != null)
                {
                    IEnumerable<EntityMetadata> entities;

                    if (schemaName.Equals("metadata", StringComparison.OrdinalIgnoreCase))
                    {
                        // Suggest metadata tables
                        entities = instance.Entities.Where(e => e.DataProviderId == MetaMetadataCache.ProviderId);
                    }
                    else if (String.IsNullOrEmpty(schemaName) || schemaName.Equals("dbo", StringComparison.OrdinalIgnoreCase))
                    {
                        // Suggest entity tables
                        entities = instance.Entities.Where(e => e.DataProviderId != MetaMetadataCache.ProviderId);
                    }
                    else
                    {
                        entities = Array.Empty<EntityMetadata>();
                    }

                    entities = entities.Where(e => e.LogicalName.StartsWith(lastPart, StringComparison.OrdinalIgnoreCase));

                    list.AddRange(entities.Select(e => new EntityAutocompleteItem(e, instance.Metadata, lastPartLength, true)));
                }
            }

            list.Sort();
            return list;
        }

        private bool TryParseTableName(string input, out string instanceName, out string schemaName, out string tableName)
        {
            return TryParseTableName(input, out instanceName, out schemaName, out tableName, out _, out _);
        }

        private bool TryParseTableName(string input, out string instanceName, out string schemaName, out string tableName, out int parts, out int lastPartLength)
        {
            var identifierRegex = new System.Text.RegularExpressions.Regex("(\\[(([^\\]])|(\\]\\]))*\\]?)|(([^\\[.])+)|(\\.)");
            var matches = identifierRegex.Matches(input);
            var identifiers = new List<string>();
            var lastBlank = false;
            lastPartLength = 0;

            foreach (System.Text.RegularExpressions.Match match in matches)
            {
                var identifier = match.Value;

                if (identifier == ".")
                {
                    if (lastBlank)
                    {
                        identifiers.Add("");
                        lastPartLength = 0;
                    }

                    lastBlank = true;
                }
                else
                {
                    lastPartLength = identifier.Length;

                    if (identifier.StartsWith("["))
                    {
                        identifier = identifier.Substring(1);

                        if (identifier.EndsWith("]"))
                            identifier = identifier.Substring(0, identifier.Length - 1);
                        
                        identifier = identifier.Replace("]]", "]");
                    }

                    identifiers.Add(identifier);
                    lastBlank = false;
                }
            }

            if (lastBlank)
            {
                identifiers.Add("");
                lastPartLength = 0;
            }

            parts = identifiers.Count;

            if (identifiers.Count == 1)
            {
                instanceName = _primaryDataSource;
                schemaName = "dbo";
                tableName = identifiers[0];
                return true;
            }
            
            if (identifiers.Count == 2)
            {
                instanceName = _primaryDataSource;
                schemaName = identifiers[0];
                tableName = identifiers[1];
                return true;
            }

            if (identifiers.Count == 3)
            {
                instanceName = identifiers[0];
                schemaName = identifiers[1];
                tableName = identifiers[2];
                return true;
            }

            instanceName = null;
            schemaName = null;
            tableName = null;
            return false;
        }

        private bool InStringLiteral(string text, int pos)
        {
            var i = -1;
            var quotes = 0;

            while ((i = text.IndexOf('\'', i + 1)) != -1)
            {
                if (i > pos)
                    break;

                quotes++;
            }

            return (quotes % 2) == 1;
        }

        private IEnumerable<SqlAutocompleteItem> FilterList(IEnumerable<SqlAutocompleteItem> list, string currentWord)
        {
            var startsWith = list.Where(obj => obj.Text.StartsWith(currentWord.Substring(currentWord.Length - obj.ReplaceLength, obj.ReplaceLength), StringComparison.OrdinalIgnoreCase)).ToList();

            if (startsWith.Any())
                return startsWith;

            return list.Where(obj => obj.Text.IndexOf(currentWord, StringComparison.OrdinalIgnoreCase) != -1);
        }

        private IEnumerable<string> Words(string text, int start)
        {
            var inWord = false;
            var inQuote = false;

            for (var i = start; i < text.Length; i++)
            {
                if (!inQuote && Char.IsWhiteSpace(text[i]))
                {
                    if (inWord)
                    {
                        var wordEnd = i - 1;
                        var word = text.Substring(start, wordEnd - start + 1);

                        yield return word;
                        inWord = false;
                    }
                }
                else if (inQuote && text[i] == ']')
                {
                    inQuote = false;
                }
                else if (!inWord)
                {
                    start = i;
                    inWord = true;
                    inQuote = text[i] == '[';
                }
                else if (!inQuote&& text[i] == '[')
                {
                    inQuote = true;
                }
            }

            if (inWord)
                yield return text.Substring(start, text.Length - start);
        }

        private IEnumerable<string> ReverseWords(string text, int end)
        {
            var inWord = true;
            var inQuote = false;

            for (var i = end; i >= 0; i--)
            {
                if (!inQuote && (Char.IsWhiteSpace(text[i]) || (Char.IsPunctuation(text[i]) && text[i] != '.' && text[i] != '_' && text[i] != '[' && text[i] != ']')))
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
                else if (inWord && text[i] == ']')
                {
                    inQuote = true;
                }
                else if (inQuote && text[i] == '[')
                {
                    inQuote = false;
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

        public class SqlAutocompleteItem : AutocompleteItem, IComparable, IAutoCompleteMenuItemCustomReplace
        {
            public SqlAutocompleteItem(string text, int replaceLength, int imageIndex) : base(text, imageIndex)
            {
                ReplaceLength = replaceLength;
            }

            public override CompareResult Compare(string fragmentText)
            {
                return CompareResult.VisibleAndSelected;
            }

            public virtual string CompareText => Text;

            public int ReplaceLength { get; }

            public int CompareTo(object obj)
            {
                var other = (SqlAutocompleteItem) obj;

                return CompareText.CompareTo(other.CompareText);
            }

            protected static string EscapeIdentifier(string identifier)
            {
                var id = new Microsoft.SqlServer.TransactSql.ScriptDom.Identifier { Value = identifier };
                id.QuoteType = RequiresQuote(id.Value) ? Microsoft.SqlServer.TransactSql.ScriptDom.QuoteType.SquareBracket : Microsoft.SqlServer.TransactSql.ScriptDom.QuoteType.NotQuoted;
                return id.ToSql();
            }

            private static readonly System.Text.RegularExpressions.Regex LegalIdentifier = new System.Text.RegularExpressions.Regex(@"^[\p{L}_@#][\p{L}\p{Nd}@$#_]*$", System.Text.RegularExpressions.RegexOptions.Compiled);
            private static readonly string[] ReservedWords = new[]
            {
                "ADD",
                "ALL",
                "ALTER",
                "AND",
                "ANY",
                "AS",
                "ASC",
                "AUTHORIZATION",
                "BACKUP",
                "BEGIN",
                "BETWEEN",
                "BREAK",
                "BROWSE",
                "BULK",
                "BY",
                "CASCADE",
                "CASE",
                "CHECK",
                "CHECKPOINT",
                "CLOSE",
                "CLUSTERED",
                "COALESCE",
                "COLLATE",
                "COLUMN",
                "COMMIT",
                "COMPUTE",
                "CONSTRAINT",
                "CONTAINS",
                "CONTAINSTABLE",
                "CONTINUE",
                "CONVERT",
                "CREATE",
                "CROSS",
                "CURRENT",
                "CURRENT_DATE",
                "CURRENT_TIME",
                "CURRENT_TIMESTAMP",
                "CURRENT_USER",
                "CURSOR",
                "DATABASE",
                "DBCC",
                "DEALLOCATE",
                "DECLARE",
                "DEFAULT",
                "DELETE",
                "DENY",
                "DESC",
                "DISK",
                "DISTINCT",
                "DISTRIBUTED",
                "DOUBLE",
                "DROP",
                "DUMP",
                "ELSE",
                "END",
                "ERRLVL",
                "ESCAPE",
                "EXCEPT",
                "EXEC",
                "EXECUTE",
                "EXISTS",
                "EXIT",
                "EXTERNAL",
                "FETCH",
                "FILE",
                "FILLFACTOR",
                "FOR",
                "FOREIGN",
                "FREETEXT",
                "FREETEXTTABLE",
                "FROM",
                "FULL",
                "FUNCTION",
                "GOTO",
                "GRANT",
                "GROUP",
                "HAVING",
                "HOLDLOCK",
                "IDENTITY",
                "IDENTITY_INSERT",
                "IDENTITYCOL",
                "IF",
                "IN",
                "INDEX",
                "INNER",
                "INSERT",
                "INTERSECT",
                "INTO",
                "IS",
                "JOIN",
                "KEY",
                "KILL",
                "LEFT",
                "LIKE",
                "LINENO",
                "LOAD",
                "MERGE",
                "NATIONAL",
                "NOCHECK",
                "NONCLUSTERED",
                "NOT",
                "NULL",
                "NULLIF",
                "OF",
                "OFF",
                "OFFSETS",
                "ON",
                "OPEN",
                "OPENDATASOURCE",
                "OPENQUERY",
                "OPENROWSET",
                "OPENXML",
                "OPTION",
                "OR",
                "ORDER",
                "OUTER",
                "OVER",
                "PERCENT",
                "PIVOT",
                "PLAN",
                "PRECISION",
                "PRIMARY",
                "PRINT",
                "PROC",
                "PROCEDURE",
                "PUBLIC",
                "RAISERROR",
                "READ",
                "READTEXT",
                "RECONFIGURE",
                "REFERENCES",
                "REPLICATION",
                "RESTORE",
                "RESTRICT",
                "RETURN",
                "REVERT",
                "REVOKE",
                "RIGHT",
                "ROLLBACK",
                "ROWCOUNT",
                "ROWGUIDCOL",
                "RULE",
                "SAVE",
                "SCHEMA",
                "SECURITYAUDIT",
                "SELECT",
                "SEMANTICKEYPHRASETABLE",
                "SEMANTICSIMILARITYDETAILSTABLE",
                "SEMANTICSIMILARITYTABLE",
                "SESSION_USER",
                "SET",
                "SETUSER",
                "SHUTDOWN",
                "SOME",
                "STATISTICS",
                "SYSTEM_USER",
                "TABLE",
                "TABLESAMPLE",
                "TEXTSIZE",
                "THEN",
                "TO",
                "TOP",
                "TRAN",
                "TRANSACTION",
                "TRIGGER",
                "TRUNCATE",
                "TRY_CONVERT",
                "TSEQUAL",
                "UNION",
                "UNIQUE",
                "UNPIVOT",
                "UPDATE",
                "UPDATETEXT",
                "USE",
                "USER",
                "VALUES",
                "VARYING",
                "VIEW",
                "WAITFOR",
                "WHEN",
                "WHERE",
                "WHILE",
                "WITH",
                "WITHIN GROUP",
                "WRITETEXT"
            };

            private static bool RequiresQuote(string identifier)
            {
                // Ref. https://msdn.microsoft.com/en-us/library/ms175874.aspx
                var permittedUnquoted = LegalIdentifier.IsMatch(identifier) && Array.BinarySearch(ReservedWords, identifier, StringComparer.OrdinalIgnoreCase) < 0;

                return !permittedUnquoted;
            }
        }

        class InstanceAutocompleteItem : SqlAutocompleteItem
        {
            private readonly AutocompleteDataSource _dataSource;

            public InstanceAutocompleteItem(AutocompleteDataSource dataSource, int replaceLength) : base(EscapeIdentifier(dataSource.Name), replaceLength, 5)
            {
                _dataSource = dataSource;
            }

            public override string ToolTipTitle
            {
                get => _dataSource.Name;
                set => base.ToolTipTitle = value;
            }

            public override string ToolTipText
            {
                get => $"Access data from the {_dataSource.Name} instance";
                set => base.ToolTipText = value;
            }
        }

        class SchemaAutocompleteItem : SqlAutocompleteItem
        {
            public SchemaAutocompleteItem(string schema, int replaceLength) : base(schema, replaceLength, 15)
            {
            }

            public override string ToolTipTitle
            {
                get => Text + " Schema";
                set => base.ToolTipTitle = value;
            }

            public override string ToolTipText
            {
                get => Text == "metadata" ? "Schema containing the metadata information" : "Schema containing the data tables";
                set => base.ToolTipText = value;
            }
        }

        class EntityAutocompleteItem : SqlAutocompleteItem
        {
            private readonly EntityMetadata _entity;
            private readonly IAttributeMetadataCache _metadata;

            public EntityAutocompleteItem(EntityMetadata entity, IAttributeMetadataCache metadata, int replaceLength, bool tableNameOnly) : this(entity, (!tableNameOnly && entity.DataProviderId == MetaMetadataCache.ProviderId ? "metadata." : "") + entity.LogicalName, metadata, replaceLength)
            {
            }

            public EntityAutocompleteItem(EntityMetadata entity, string alias, IAttributeMetadataCache metadata, int replaceLength) : base(alias, replaceLength, 4)
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
            private readonly string _lhs;
            private readonly IAttributeMetadataCache _metadata;

            public JoinAutocompleteItem(OneToManyRelationshipMetadata relationship, string join, bool oneToMany, EntityMetadata[] entities, IAttributeMetadataCache metadata, int replaceLength) : base(join, replaceLength, oneToMany ? 19 : 18)
            {
                _rhs = entities.SingleOrDefault(e => e.LogicalName == relationship.ReferencingEntity);
                _lhs = relationship.ReferencedEntity;

                if (!oneToMany && metadata.TryGetMinimalData(relationship.ReferencingEntity, out _rhs))
                    _attribute = _rhs.Attributes.SingleOrDefault(a => a.LogicalName == relationship.ReferencingAttribute);

                _metadata = metadata;
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

            public override string GetTextForReplace()
            {
                _metadata.TryGetMinimalData(_rhs.LogicalName, out _);
                _metadata.TryGetMinimalData(_lhs, out _);
                return base.GetTextForReplace();
            }
        }

        class AttributeAutocompleteItem : SqlAutocompleteItem
        {
            private readonly AttributeMetadata _attribute;
            private readonly string _virtualSuffix;

            public AttributeAutocompleteItem(AttributeMetadata attribute, int replaceLength, string virtualSuffix = null) : base(attribute.LogicalName + virtualSuffix, replaceLength, virtualSuffix == null ? GetIconIndex(attribute) : 13)
            {
                _attribute = attribute;
                _virtualSuffix = virtualSuffix;
            }

            public static IEnumerable<AttributeAutocompleteItem> CreateList(AttributeMetadata attribute, int replaceLength, bool writeable)
            {
                yield return new AttributeAutocompleteItem(attribute, replaceLength);

                if (!writeable && (attribute is EnumAttributeMetadata || attribute is BooleanAttributeMetadata || attribute is LookupAttributeMetadata))
                    yield return new AttributeAutocompleteItem(attribute, replaceLength, "name");

                if (attribute is LookupAttributeMetadata lookup && lookup.Targets?.Length > 1 && lookup.AttributeType != AttributeTypeCode.PartyList && (lookup.EntityLogicalName != "listmember" || lookup.LogicalName != "entityid"))
                    yield return new AttributeAutocompleteItem(attribute, replaceLength, "type");
            }

            public override string ToolTipTitle
            {
                get => _attribute.DisplayName?.UserLocalizedLabel?.Label;
                set => base.ToolTipTitle = value;
            }

            public override string ToolTipText
            {
                get
                {
                    var description = _attribute.Description?.UserLocalizedLabel?.Label;

                    if (_virtualSuffix == "name")
                        description += $"\r\n\r\nThis attribute holds the display name of the {_attribute.LogicalName} field";
                    else if (_virtualSuffix == "type")
                        description += $"\r\n\r\nThis attribute holds the logical name of the type of record referenced by the {_attribute.LogicalName} field";
                    else if (_attribute.AttributeType == AttributeTypeCode.Picklist)
                        description += "\r\n\r\nThis attribute holds the underlying integer value of the field";
                    else if (_attribute.AttributeType == AttributeTypeCode.Lookup || _attribute.AttributeType == AttributeTypeCode.Customer || _attribute.AttributeType == AttributeTypeCode.Owner)
                        description += "\r\n\r\nThis attribute holds the underlying unique identifier value of the field";

                    return description;
                }
                set => base.ToolTipText = value;
            }
        }

        class FunctionAutocompleteItem : SqlAutocompleteItem
        {
            private readonly MethodInfo _method;

            public FunctionAutocompleteItem(MethodInfo method, int replaceLength) : base(GetInsertText(method), replaceLength, GetIconIndex(method))
            {
                _method = method;
                MenuText = GetSignature(method);
            }

            private static int GetIconIndex(MethodInfo method)
            {
                var aggregate = method.GetCustomAttribute<AggregateAttribute>();

                if (aggregate != null)
                    return 24;

                return 23;
            }

            private static string GetInsertText(MethodInfo method)
            {
                var text = new StringBuilder();
                text.Append(method.DeclaringType == typeof(FetchXmlConditionMethods) ? method.Name.ToLowerInvariant() : method.Name.ToUpperInvariant());

                if (method.GetCustomAttribute<ParameterlessCallAttribute>() == null)
                {
                    text.Append("(");

                    if (method.GetParameters().Length == (method.DeclaringType == typeof(FetchXmlConditionMethods) ? 1 : 0))
                        text.Append(")");
                }

                return text.ToString();
            }

            private static string GetSignature(MethodInfo method)
            {
                var sig = new StringBuilder();
                sig.Append(method.DeclaringType == typeof(FetchXmlConditionMethods) ? method.Name.ToLowerInvariant() : method.Name.ToUpperInvariant());

                if (method.GetCustomAttribute<ParameterlessCallAttribute>() == null)
                {
                    sig.Append("(");

                    var firstParam = true;
                    foreach (var param in method.GetParameters().Skip(method.DeclaringType == typeof(FetchXmlConditionMethods) ? 1 : 0))
                    {
                        if (firstParam)
                            firstParam = false;
                        else
                            sig.Append(", ");

                        sig.Append(param.Name.ToLowerInvariant());
                    }

                    sig.Append(")");
                }

                return sig.ToString();
            }

            public override string ToolTipTitle
            {
                get => MenuText;
                set => base.ToolTipTitle = value;
            }

            public override string ToolTipText
            {
                get
                {
                    var description = _method.GetCustomAttribute<DescriptionAttribute>();
                    return description?.Description ?? "";
                }
                set
                {
                    base.ToolTipText = value;
                }
            }

            public override string CompareText => _method.DeclaringType == typeof(FetchXmlConditionMethods) ? _method.Name.ToLowerInvariant() : _method.Name.ToUpperInvariant();
        }
    }

    public class AutocompleteDataSource
    {
        public string Name { get; set; }
        public EntityMetadata[] Entities { get; set; }
        public IAttributeMetadataCache Metadata { get; set; }
    }
}
