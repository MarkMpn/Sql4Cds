using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlTypes;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Text;
using MarkMpn.Sql4Cds.Engine;
using Microsoft.Crm.Sdk.Messages;
using Microsoft.VisualStudio.LanguageServer.Protocol;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Metadata;
using Microsoft.Xrm.Sdk.Query;
using static MarkMpn.Sql4Cds.LanguageServer.Autocomplete.FunctionMetadata;

namespace MarkMpn.Sql4Cds.LanguageServer.Autocomplete
{
    /// <summary>
    /// Produces Intellisense suggestions
    /// </summary>
    public class Autocomplete
    {
        private readonly IDictionary<string, DataSource> _dataSources;
        private readonly string _primaryDataSource;
        private readonly ColumnOrdering _columnOrdering;

        /// <summary>
        /// Creates a new <see cref="Autocomplete"/>
        /// </summary>
        /// <param name="entities">The list of entities available to use in the query</param>
        /// <param name="metadata">The cache of metadata about each entity</param>
        /// <param name="columnOrdering">The order that columns are passed to table-valued functions or stored procedures</param>
        public Autocomplete(IDictionary<string, DataSource> dataSources, string primaryDataSource, ColumnOrdering columnOrdering)
        {
            _dataSources = dataSources;
            _primaryDataSource = primaryDataSource;
            _columnOrdering = columnOrdering;
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
            if (InCommentOrStringLiteral(text, pos))
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
            if (decimal.TryParse(currentWord, out _))
                return Array.Empty<SqlAutocompleteItem>();

            var currentLength = currentWord.Length;

            if (prevWord == null)
                return Array.Empty<SqlAutocompleteItem>();

            switch (prevWord.ToLower())
            {
                case "from":
                case "insert":
                case "into":
                    return AutocompleteTableName(currentWord, prevWord.Equals("from", StringComparison.OrdinalIgnoreCase));

                case "exec":
                case "execute":
                    return FilterList(AutocompleteSprocName(currentWord), currentWord);

                case "collate":
                    return FilterList(Collation.GetAllCollations().OrderBy(c => c.Name).Select(c => new CollationAutocompleteItem(c, currentLength)), currentWord);

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
                                clause = clause ?? "select";
                                break;

                            case "update":
                            case "delete":
                                foundPossibleFrom = true;
                                foundQueryStart = true;
                                clause = clause ?? word.ToLower();
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

                            //case "(":
                            //    words.Clear();
                            //    break;

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

                            case "values":
                                clause = clause ?? "values";
                                break;

                            case "exec":
                            case "execute":
                                clause = clause ?? "exec";
                                foundQueryStart = true;
                                break;

                            default:
                                if (!string.IsNullOrEmpty(word))
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

                    // Don't confuse the open bracket in "INSERT INTO account (" to mean account is a TVF
                    if (words.Count >= 2 && words[1] == "(" && clause == "insert")
                        words.RemoveRange(1, words.Count - 1);

                    IDictionary<string, string> tables = null;

                    if (foundFrom || foundPossibleFrom && words.Count > 0)
                    {
                        // Try to get the table & alias names from the words in the possible FROM clause
                        tables = new Dictionary<string, string>();

                        for (var i = 0; i < words.Count; i++)
                        {
                            // If the following word is "(", this is a TVF
                            if (i < words.Count - 1 && words[i + 1] == "(")
                            {
                                var functionName = words[i];
                                var alias = Guid.NewGuid().ToString();

                                // Skip the parameters to the function
                                var parenDepth = 1;
                                i += 2;
                                while (i < words.Count && parenDepth > 0)
                                {
                                    if (words[i] == "(")
                                        parenDepth++;
                                    else if (words[i] == ")")
                                        parenDepth--;

                                    i++;
                                }

                                // Check if there is an alias for this function
                                if (i < words.Count)
                                {
                                    if (words[i].ToLower() == "as" && i < words.Count - 1)
                                    {
                                        alias = words[i + 1];
                                        i += 2;
                                    }
                                    else if (words[i].ToLower() != "left" && words[i].ToLower() != "inner" && words[i].ToLower() != "right" && words[i].ToLower() != "join" && words[i].ToLower() != "full" && words[i] != ",")
                                    {
                                        alias = words[i];
                                        i++;
                                    }
                                }

                                tables[alias] = functionName + "(";
                            }
                            else
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
                                    else if (words[i + 1].ToLower() != "on" && words[i + 1].ToLower() != "left" && words[i + 1].ToLower() != "inner" && words[i + 1].ToLower() != "right" && words[i + 1].ToLower() != "join" && words[i + 1].ToLower() != "full" && words[i + 1] != ",")
                                    {
                                        alias = words[i + 1];
                                        i++;
                                    }
                                }

                                tables[alias] = tableName;
                            }

                            while (i < words.Count && words[i].ToLower() != "join" && words[i] != ",")
                                i++;
                        }

                        // Start loading all the appropriate metadata in the background
                        foreach (var table in tables.Values)
                        {
                            if (TryParseTableName(table, out var instanceName, out _, out var tableName) && _dataSources.TryGetValue(instanceName, out var instance) && instance.Metadata.GetAllEntities().Any(e => e.LogicalName.Equals(tableName, StringComparison.OrdinalIgnoreCase) && e.DataProviderId != MetaMetadataCache.ProviderId))
                                instance.Metadata.TryGetMinimalData(table, out _);
                        }
                    }

                    if ((clause == "exec" || clause == null) && (words.Count == 2 || words.Count > 2 && words[words.Count - 1].EndsWith(",")))
                    {
                        // Suggest parameter names
                        var sprocName = words[0];

                        if (TryParseTableName(sprocName, out var instanceName, out var schemaName, out sprocName) &&
                            _dataSources.TryGetValue(instanceName, out var instance) &&
                            schemaName.Equals("dbo", StringComparison.OrdinalIgnoreCase) &&
                            instance.MessageCache.TryGetValue(sprocName, out var message) &&
                            message.IsValidAsStoredProcedure())
                        {
                            var availableParameters = message.InputParameters
                                .Concat(message.OutputParameters)
                                .OrderBy(p => p.Name)
                                .Select(p => new SprocParameterAutocompleteItem(message, p, instance, currentLength));

                            return FilterList(availableParameters, currentWord);
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
                                if (TryParseTableName(table.Value, out var instanceName, out var schemaName, out var tableName) && _dataSources.TryGetValue(instanceName, out var instance) && instance.Metadata.TryGetMinimalData((schemaName == "metadata" ? "metadata." : "") + tableName, out var metadata))
                                {
                                    if (metadata.OneToManyRelationships != null)
                                        joinSuggestions.AddRange(metadata.OneToManyRelationships.Select(rel => new JoinAutocompleteItem(rel, $"{(instanceName == _primaryDataSource ? "" : SqlAutocompleteItem.EscapeIdentifier(instance.Name) + "." + (schemaName == "dbo" ? "dbo." : ""))}{rel.ReferencingEntity}{GetUniqueTableAlias(rel.ReferencingEntity, tables)} ON {table.Key}.{rel.ReferencedAttribute} = {GetUniqueTableName(rel.ReferencingEntity, tables)}.{rel.ReferencingAttribute}", true, instance.Metadata, currentLength)));

                                    if (metadata.ManyToOneRelationships != null)
                                        joinSuggestions.AddRange(metadata.ManyToOneRelationships.Select(rel => new JoinAutocompleteItem(rel, $"{(instanceName == _primaryDataSource ? "" : SqlAutocompleteItem.EscapeIdentifier(instance.Name) + "." + (schemaName == "dbo" ? "dbo." : ""))}{rel.ReferencedEntity}{GetUniqueTableAlias(rel.ReferencedEntity, tables)} ON {table.Key}.{rel.ReferencingAttribute} = {GetUniqueTableName(rel.ReferencedEntity, tables)}.{rel.ReferencedAttribute}", false, instance.Metadata, currentLength)));
                                }
                            }

                            joinSuggestions.Sort();

                            return FilterList(joinSuggestions, currentWord).Concat(AutocompleteTableName(currentWord, true));
                        }

                        var additionalSuggestions = (IEnumerable<SqlAutocompleteItem>)Array.Empty<SqlAutocompleteItem>();

                        if (prevWord.Equals("on", StringComparison.OrdinalIgnoreCase) && tables.TryGetValue(prevPrevWord, out var joinTableName))
                        {
                            if (TryParseTableName(joinTableName, out var instanceName, out var schemaName, out joinTableName) && _dataSources.TryGetValue(instanceName, out var instance) && instance.Metadata.TryGetMinimalData((schemaName == "metadata" ? "metadata." : "") + joinTableName, out var newTableMetadata))
                            {
                                // Suggest known relationships from the other entities in the FROM clause, followed by the normal list of attributes
                                additionalSuggestions = new List<SqlAutocompleteItem>();

                                if (newTableMetadata.OneToManyRelationships != null)
                                    ((List<SqlAutocompleteItem>)additionalSuggestions).AddRange(newTableMetadata.OneToManyRelationships.SelectMany(rel => tables.Where(table => table.Key != prevPrevWord && table.Value == rel.ReferencingEntity).Select(table => new JoinAutocompleteItem(rel, $"{table.Key}.{rel.ReferencingAttribute} = {prevPrevWord}.{rel.ReferencedAttribute}", false, instance.Metadata, currentLength))));

                                if (newTableMetadata.ManyToOneRelationships != null)
                                    ((List<SqlAutocompleteItem>)additionalSuggestions).AddRange(newTableMetadata.ManyToOneRelationships.SelectMany(rel => tables.Where(table => table.Key != prevPrevWord && table.Value == rel.ReferencedEntity).Select(table => new JoinAutocompleteItem(rel, $"{table.Key}.{rel.ReferencedAttribute} = {prevPrevWord}.{rel.ReferencingAttribute}", true, instance.Metadata, currentLength))));

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
                                        var entity = instance.Metadata.GetAllEntities().SingleOrDefault(e => e.LogicalName == tableName);

                                        if (entity != null)
                                            suggestions.Add(new EntityAutocompleteItem(entity, table.Key, instance.Metadata, currentLength));
                                    }
                                }

                                suggestions.Sort();
                                return FilterList(suggestions, currentWord);
                            }

                            return AutocompleteTableName(currentWord, false);
                        }

                        if (clause == "from" && prevWord == ",")
                            return AutocompleteTableName(currentWord, true);

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
                                {
                                    var attributes = metadata.Attributes.Where(a => a.IsValidForUpdate != false && a.AttributeOf == null);

                                    if (tableName == "solutioncomponent")
                                        attributes = metadata.Attributes.Where(a => a.LogicalName == "objectid" || a.LogicalName == "componenttype" || a.LogicalName == "solutionid" || a.LogicalName == "rootcomponentbehavior");

                                    return FilterList(attributes.SelectMany(a => AttributeAutocompleteItem.CreateList(a, currentLength, true, instance)).OrderBy(a => a), currentWord);
                                }
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
                                if (TryParseTableName(tableName, out var instanceName, out var schemaName, out tableName) && _dataSources.TryGetValue(instanceName, out var instance))
                                {
                                    if (tableName.EndsWith("("))
                                    {
                                        // TVF
                                        var messageName = tableName.Substring(0, tableName.Length - 1);
                                        if (instance.MessageCache.TryGetValue(messageName, out var message) &&
                                            message.IsValidAsTableValuedFunction())
                                        {
                                            return FilterList(GetMessageOutputAttributes(message, instance).SelectMany(a => AttributeAutocompleteItem.CreateList(a, currentLength, false, instance)).OrderBy(a => a), currentWord);
                                        }
                                    }
                                    else
                                    {
                                        // Table
                                        if (instance.Metadata.TryGetMinimalData((schemaName == "metadata" ? "metadata." : "") + tableName, out var metadata))
                                            return FilterList(metadata.Attributes.Where(a => a.IsValidForRead != false && a.AttributeOf == null).SelectMany(a => AttributeAutocompleteItem.CreateList(a, currentLength, false, instance)).OrderBy(a => a), currentWord);
                                    }
                                }
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
                                else if (metadata.LogicalName == "principalobjectaccess")
                                {
                                    attributeFilter = a => a.LogicalName == "objectid" || a.LogicalName == "objecttypecode" || a.LogicalName == "principalid" || a.LogicalName == "principaltypecode" || a.LogicalName == "accessrightsmask";
                                }
                                else if (metadata.LogicalName == "solutioncomponent")
                                {
                                    attributeFilter = a => a.LogicalName == "objectid" || a.LogicalName == "componenttype" || a.LogicalName == "solutionid" || a.LogicalName == "rootcomponentbehavior";
                                }
                                else
                                {
                                    attributeFilter = a => a.IsValidForCreate != false && a.AttributeOf == null;
                                }
                                return FilterList(metadata.Attributes.Where(attributeFilter).SelectMany(a => AttributeAutocompleteItem.CreateList(a, currentLength, true, instance)).OrderBy(a => a), currentWord);
                            }
                        }
                        else
                        {
                            // Autocomplete list is:
                            // * table/alias names
                            // * attribute names unique across tables
                            // * functions
                            // * variables
                            var items = new List<SqlAutocompleteItem>();
                            var attributes = new List<AttributeMetadata>();
                            var instance = default(DataSource);

                            foreach (var table in tables)
                            {
                                if (table.Value.EndsWith("("))
                                {
                                    // TVF
                                    var messageName = table.Value.Substring(0, table.Value.Length - 1);

                                    if (TryParseTableName(messageName, out var instanceName, out var schemaName, out var tableName) &&
                                        _dataSources.TryGetValue(instanceName, out instance) &&
                                        (string.IsNullOrEmpty(schemaName) || schemaName == "dbo") &&
                                        instance.MessageCache.TryGetValue(messageName, out var message) &&
                                        message.IsValidAsTableValuedFunction())
                                    {
                                        if (!Guid.TryParse(table.Key, out _))
                                            items.Add(new TVFAutocompleteItem(message, instance, table.Key, currentLength));

                                        attributes.AddRange(GetMessageOutputAttributes(message, instance));
                                    }
                                }
                                else
                                {
                                    // Table
                                    if (TryParseTableName(table.Value, out var instanceName, out var schemaName, out var tableName) && _dataSources.TryGetValue(instanceName, out instance))
                                    {
                                        var entity = instance.Metadata.GetAllEntities().SingleOrDefault(e =>
                                            e.LogicalName == tableName &&
                                            (
                                                (
                                                    (schemaName == "dbo" || schemaName == "") &&
                                                    e.DataProviderId != MetaMetadataCache.ProviderId
                                                )
                                                ||
                                                (
                                                    schemaName == "metadata" &&
                                                    e.DataProviderId == MetaMetadataCache.ProviderId
                                                )
                                                ||
                                                (
                                                    schemaName == "archive" &&
                                                    (e.IsArchivalEnabled == true || e.IsRetentionEnabled == true)
                                                )
                                                ||
                                                (
                                                    schemaName == "bin" &&
                                                    instance.Metadata.TryGetRecycleBinEntities()?.Contains(e.LogicalName) == true
                                                )
                                            )
                                        );

                                        if (entity != null)
                                            items.Add(new EntityAutocompleteItem(entity, table.Key, instance.Metadata, currentLength));

                                        if (instance.Metadata.TryGetMinimalData((schemaName == "metadata" ? "metadata." : "") + tableName, out var metadata))
                                            attributes.AddRange(metadata.Attributes);
                                    }
                                }
                            }

                            items.AddRange(attributes.Where(a => a.IsValidForRead != false && a.AttributeOf == null).GroupBy(x => x.LogicalName).Where(g => g.Count() == 1).SelectMany(g => AttributeAutocompleteItem.CreateList(g.Single(), currentLength, false, instance)));

                            items.AddRange(typeof(SqlFunctions).GetMethods(BindingFlags.DeclaredOnly | BindingFlags.Instance | BindingFlags.Public).Select(m => new FunctionAutocompleteItem(m, currentLength)));

                            items.AddRange(AutocompleteVariableName(currentWord, text.Substring(0, pos)));

                            if ((clause == "where" || clause == "set") && prevWord == "=")
                            {
                                // Check if there are any applicable filter operator functions that match the type of the current attribute
                                var identifiers = prevPrevWord.Split('.');
                                var entity = default(EntityMetadata);
                                var attribute = default(AttributeMetadata);

                                if (identifiers.Length == 2)
                                {
                                    if (tables.TryGetValue(identifiers[0], out var tableName))
                                    {
                                        if (TryParseTableName(tableName, out var instanceName, out _, out tableName) && _dataSources.TryGetValue(instanceName, out instance) && instance.Metadata.TryGetMinimalData(tableName, out entity))
                                        {
                                            attribute = entity.Attributes.SingleOrDefault(a => a.LogicalName.Equals(identifiers[1], StringComparison.OrdinalIgnoreCase));
                                        }
                                    }
                                }
                                else
                                {
                                    foreach (var table in tables.Values)
                                    {
                                        if (TryParseTableName(table, out var instanceName, out _, out var tableName) && _dataSources.TryGetValue(instanceName, out instance) && instance.Metadata.TryGetMinimalData(tableName, out entity))
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
                                    if (clause == "where")
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

                                    if (attribute is EntityNameAttributeMetadata entityNameAttr)
                                    {
                                        var lookupAttr = entity.Attributes.SingleOrDefault(a => a.LogicalName == entityNameAttr.AttributeOf) as LookupAttributeMetadata;

                                        if (lookupAttr?.Targets == null || lookupAttr.Targets.Length == 0)
                                        {
                                            // Could be any entity name, show them all
                                            items.AddRange(instance.Metadata.GetAllEntities().Select(e => new EntityNameAutocompleteItem(e, currentLength)));
                                        }
                                        else
                                        {
                                            // Can only be one of the allowed entity types
                                            items.AddRange(instance.Metadata.GetAllEntities().Where(e => lookupAttr.Targets.Contains(e.LogicalName)).Select(e => new EntityNameAutocompleteItem(e, currentLength)));
                                        }
                                    }
                                    else if (attribute is EnumAttributeMetadata enumAttr)
                                    {
                                        items.AddRange(enumAttr.OptionSet.Options.Select(o => new OptionSetAutocompleteItem(o, currentLength)));
                                    }
                                }
                            }

                            items.Sort();

                            return additionalSuggestions.Concat(FilterList(items, currentWord)).OrderBy(x => x);
                        }
                    }
                    else if (clause == "select")
                    {
                        // In the SELECT clause with no tables, just offer known functions
                        var items = new List<SqlAutocompleteItem>();
                        items.AddRange(typeof(FunctionMetadata.SqlFunctions).GetMethods(BindingFlags.DeclaredOnly | BindingFlags.Instance | BindingFlags.Public).Select(m => new FunctionAutocompleteItem(m, currentLength)));
                        items.AddRange(AutocompleteVariableName(currentWord, text.Substring(0, pos)));
                        return FilterList(items, currentWord).OrderBy(x => x);
                    }
                    else if (prevWord.Equals("update", StringComparison.OrdinalIgnoreCase) ||
                        prevWord.Equals("insert", StringComparison.OrdinalIgnoreCase) ||
                        prevPrevWord != null && prevPrevWord.Equals("insert", StringComparison.OrdinalIgnoreCase) && prevWord.Equals("into", StringComparison.OrdinalIgnoreCase))
                    {
                        return AutocompleteTableName(currentWord, false);
                    }

                    break;
            }

            return Array.Empty<SqlAutocompleteItem>();
        }

        private List<AttributeMetadata> GetMessageOutputAttributes(Message message, DataSource instance)
        {
            var attributes = new List<AttributeMetadata>();

            if (message.OutputParameters.All(p => p.IsScalarType()))
            {
                foreach (var param in message.OutputParameters)
                    attributes.Add(CreateParameterAttribute(param));
            }
            else
            {
                var otc = message.OutputParameters[0].OTC;
                var audit = false;

                if (message.OutputParameters[0].Type == typeof(AuditDetail) || message.OutputParameters[0].Type == typeof(AuditDetailCollection))
                {
                    otc = instance.Metadata.GetAllEntities().SingleOrDefault(e => e.LogicalName == "audit")?.ObjectTypeCode;
                    audit = true;
                }

                if (otc != null)
                {
                    var entity = instance.Metadata.GetAllEntities().SingleOrDefault(e => e.ObjectTypeCode == otc);

                    if (entity != null)
                    {
                        attributes.AddRange(entity.Attributes.Where(a => a.IsValidForRead != false && a.AttributeOf == null));

                        if (audit)
                        {
                            attributes.Add(new StringAttributeMetadata { LogicalName = "newvalues" });
                            attributes.Add(new StringAttributeMetadata { LogicalName = "oldvalues" });
                        }
                    }
                }
            }

            return attributes;
        }

        private AttributeMetadata CreateParameterAttribute(MessageParameter param)
        {
            // Create an attribute metadata of the correct type for the message parameter
            AttributeMetadata attribute;

            if (param.Type == typeof(string))
                attribute = new StringAttributeMetadata();
            else if (param.Type == typeof(Guid))
                attribute = new UniqueIdentifierAttributeMetadata();
            else if (param.Type == typeof(bool))
                attribute = new BooleanAttributeMetadata();
            else if (param.Type == typeof(int))
                attribute = new IntegerAttributeMetadata();
            else if (param.Type == typeof(EntityReference))
                attribute = new LookupAttributeMetadata();
            else if (param.Type == typeof(DateTime))
                attribute = new DateTimeAttributeMetadata();
            else if (param.Type == typeof(long))
                attribute = new BigIntAttributeMetadata();
            else if (param.Type == typeof(OptionSetValue))
                attribute = new PicklistAttributeMetadata();
            else
                attribute = new StringAttributeMetadata();

            attribute.LogicalName = param.Name;
            return attribute;
        }

        private IEnumerable<SqlAutocompleteItem> AutocompleteTableName(string currentWord, bool fromClause)
        {
            var currentLength = currentWord.Length;
            var list = new List<SqlAutocompleteItem>();

            if (string.IsNullOrEmpty(currentWord))
            {
                // If there's multiple instances, show them
                if (_dataSources.Count > 1)
                    list.AddRange(_dataSources.Values.Select(x => new InstanceAutocompleteItem(x, currentLength)));

                if (_dataSources.TryGetValue(_primaryDataSource, out var ds))
                {
                    // Show table list
                    if (ds.Metadata != null)
                        list.AddRange(ds.Metadata.GetAllEntities().Select(x => new EntityAutocompleteItem(x, ds.Metadata, currentLength, false)));

                    // Show TVF list
                    if (fromClause && ds.Metadata != null)
                        list.AddRange(ds.MessageCache.GetAllMessages(true).Where(x => x.IsValidAsTableValuedFunction()).Select(x => new TVFAutocompleteItem(x, _columnOrdering, ds, currentLength)));
                }
            }
            else if (TryParseTableName(currentWord, out var instanceName, out var schemaName, out var tableName, out var parts, out var lastPartLength))
            {
                _dataSources.TryGetValue(instanceName, out var instance);
                var lastPart = tableName;

                if (parts == 1)
                {
                    // Could be an instance name
                    list.AddRange(_dataSources.Values.Where(x => x.Name.StartsWith(lastPart, StringComparison.OrdinalIgnoreCase)).Select(x => new InstanceAutocompleteItem(x, lastPartLength)));
                }

                if (parts == 1 || (parts == 2 && _dataSources.ContainsKey(schemaName)))
                {
                    // Could be a schema name
                    var schemaNames = (IEnumerable<string>)new[] { "dbo", "archive", "metadata" };
                    if (instance?.Metadata?.TryGetRecycleBinEntities()?.Length > 0)
                        schemaNames = schemaNames.Append("bin");

                    schemaNames = schemaNames.Where(s => s.StartsWith(lastPart, StringComparison.OrdinalIgnoreCase));

                    foreach (var schema in schemaNames)
                        list.Add(new SchemaAutocompleteItem(schema, lastPartLength));
                }

                // Could be a table name
                if (instance?.Metadata != null)
                {
                    IEnumerable<EntityMetadata> entities;
                    IEnumerable<Message> messages = Array.Empty<Message>();

                    if (schemaName.Equals("metadata", StringComparison.OrdinalIgnoreCase))
                    {
                        // Suggest metadata tables
                        entities = instance.Metadata.GetAllEntities().Where(e => e.DataProviderId == MetaMetadataCache.ProviderId);
                    }
                    else if (String.IsNullOrEmpty(schemaName) || schemaName.Equals("dbo", StringComparison.OrdinalIgnoreCase) || schemaName.Equals("archive", StringComparison.OrdinalIgnoreCase))
                    {
                        // Suggest entity tables
                        entities = instance.Metadata.GetAllEntities().Where(e => e.DataProviderId != MetaMetadataCache.ProviderId);

                        if (schemaName.Equals("archive", StringComparison.OrdinalIgnoreCase))
                        {
                            // Filter tables
                            entities = entities.Where(e => e.IsRetentionEnabled == true || e.IsArchivalEnabled == true);
                        }
                        else
                        {
                            // Suggest TVFs
                            messages = instance.MessageCache.GetAllMessages(true);
                        }
                    }
                    else if (schemaName.Equals("bin", StringComparison.OrdinalIgnoreCase))
                    {
                        // Suggest tables that are enabled for the recycle bin
                        var recycleBinEntities = instance.Metadata.TryGetRecycleBinEntities();

                        if (recycleBinEntities?.Length > 0)
                            entities = instance.Metadata.GetAllEntities().Where(e => recycleBinEntities.Contains(e.LogicalName));
                        else
                            entities = Array.Empty<EntityMetadata>();
                    }
                    else
                    {
                        entities = Array.Empty<EntityMetadata>();
                    }

                    entities = entities.Where(e => e.LogicalName.StartsWith(lastPart, StringComparison.OrdinalIgnoreCase));
                    list.AddRange(entities.Select(e => new EntityAutocompleteItem(e, instance.Metadata, lastPartLength, true)));

                    if (fromClause)
                    {
                        messages = messages.Where(e => e.IsValidAsTableValuedFunction() && e.Name.StartsWith(lastPart, StringComparison.OrdinalIgnoreCase));
                        list.AddRange(messages.Select(e => new TVFAutocompleteItem(e, _columnOrdering, instance, lastPartLength)));
                    }
                }
            }

            list.Sort();
            return list;
        }

        private IEnumerable<SqlAutocompleteItem> AutocompleteSprocName(string currentWord)
        {
            var currentLength = currentWord.Length;
            var list = new List<SqlAutocompleteItem>();

            if (string.IsNullOrEmpty(currentWord))
            {
                // If there's multiple instances, show them
                if (_dataSources.Count > 1)
                    list.AddRange(_dataSources.Values.Select(x => new InstanceAutocompleteItem(x, currentLength)));

                if (_dataSources.TryGetValue(_primaryDataSource, out var ds) && ds.MessageCache != null)
                    list.AddRange(ds.MessageCache.GetAllMessages(true).Where(x => x.IsValidAsStoredProcedure()).Select(x => new SprocAutocompleteItem(x, _columnOrdering, ds, currentLength)));
            }
            else if (TryParseTableName(currentWord, out var instanceName, out var schemaName, out var tableName, out var parts, out var lastPartLength))
            {
                _dataSources.TryGetValue(instanceName, out var instance);
                var lastPart = tableName;

                if (parts == 1)
                {
                    // Could be an instance name
                    list.AddRange(_dataSources.Values.Where(x => x.Name.StartsWith(lastPart, StringComparison.OrdinalIgnoreCase)).Select(x => new InstanceAutocompleteItem(x, lastPartLength)));
                }

                if (parts == 1 || parts == 2)
                {
                    // Could be a schema name
                    var schemaNames = (IEnumerable<string>)new[] { "dbo", "archive", "metadata" };
                    if (instance?.Metadata?.TryGetRecycleBinEntities()?.Length > 0)
                        schemaNames = schemaNames.Append("bin");

                    schemaNames = schemaNames.Where(s => s.StartsWith(lastPart, StringComparison.OrdinalIgnoreCase));

                    foreach (var schema in schemaNames)
                        list.Add(new SchemaAutocompleteItem(schema, lastPartLength));
                }

                // Could be a sproc name
                if (schemaName.Equals("dbo", StringComparison.OrdinalIgnoreCase) && instance?.MessageCache != null)
                    list.AddRange(instance.MessageCache.GetAllMessages(true).Where(x => x.IsValidAsStoredProcedure()).Select(e => new SprocAutocompleteItem(e, _columnOrdering, instance, lastPartLength)));
            }

            list.Sort();
            return list;
        }

        private IEnumerable<SqlAutocompleteItem> AutocompleteVariableName(string currentWord, string text)
        {
            var list = new List<SqlAutocompleteItem>();

            // Add the known global variables
            list.Add(new VariableAutocompleteItem("@@IDENTITY", currentWord.Length));
            list.Add(new VariableAutocompleteItem("@@ROWCOUNT", currentWord.Length));
            list.Add(new VariableAutocompleteItem("@@SERVERNAME", currentWord.Length));
            list.Add(new VariableAutocompleteItem("@@VERSION", currentWord.Length));
            list.Add(new VariableAutocompleteItem("@@ERROR", currentWord.Length));
            list.Add(new VariableAutocompleteItem("@@FETCH_STATUS", currentWord.Length));

            // Find any other variable declarations in the preceding SQL
            var regex = new System.Text.RegularExpressions.Regex(@"\bdeclare\s+(@[a-z0-9_]+)", System.Text.RegularExpressions.RegexOptions.IgnoreCase, TimeSpan.FromSeconds(1));
            var matches = regex.Matches(text);

            foreach (System.Text.RegularExpressions.Match match in matches)
            {
                var variableName = match.Groups[1].Value;
                list.Add(new VariableAutocompleteItem(variableName, currentWord.Length));
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
            var identifierRegex = new System.Text.RegularExpressions.Regex("(\\[(([^\\]])|(\\]\\]))*\\]?)|(([^\\[.])+)|(\\.)", System.Text.RegularExpressions.RegexOptions.None, TimeSpan.FromSeconds(1));
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

                if (string.IsNullOrEmpty(schemaName))
                    schemaName = "dbo";

                return true;
            }

            instanceName = null;
            schemaName = null;
            tableName = null;
            return false;
        }

        private bool InCommentOrStringLiteral(string text, int pos)
        {
            var i = -1;
            var inSingleLineComment = false;
            var commentDepth = 0;
            var inQuotes = false;

            while ((i = text.IndexOfAny(new[] { '\n', '-', '\'', '/' }, i + 1)) != -1)
            {
                if (i > pos)
                    break;

                if (text[i] == '\n')
                    inSingleLineComment = false;
                else if (i > 0 && !inQuotes && text[i - 1] == '-' && text[i] == '-')
                    inSingleLineComment = true;
                else if (i < text.Length - 1 && !inQuotes && text[i] == '/' && text[i + 1] == '*')
                    commentDepth++;
                else if (i > 0 && !inQuotes && text[i - 1] == '*' && text[i] == '/')
                    commentDepth--;
                else if (text[i] == '\'' && !inSingleLineComment && commentDepth == 0)
                    inQuotes = !inQuotes;
            }

            return inSingleLineComment || commentDepth > 0 || inQuotes;
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
                if (!inQuote && (char.IsWhiteSpace(text[i]) || char.IsPunctuation(text[i]) && text[i] != '.' && text[i] != '_' && text[i] != '[' && text[i] != ']' && text[i] != '@'))
                {
                    if (inWord)
                    {
                        var wordEnd = i - 1;
                        var word = text.Substring(start, wordEnd - start + 1);

                        yield return word;
                        inWord = false;
                    }

                    if (char.IsPunctuation(text[i]))
                    {
                        yield return text[i].ToString();
                        //i--;
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
                else if (!inQuote && text[i] == '[')
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
                if (!inQuote && (char.IsWhiteSpace(text[i]) || char.IsPunctuation(text[i]) && text[i] != '.' && text[i] != '_' && text[i] != '[' && text[i] != ']' && text[i] != '@'))
                {
                    if (inWord)
                    {
                        var wordStart = i + 1;
                        var word = text.Substring(wordStart, end - wordStart + 1);
                        yield return word;

                        inWord = false;
                    }

                    if (char.IsPunctuation(text[i]))
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

        private string GetUniqueTableAlias(string name, IDictionary<string, string> tables)
        {
            var alias = GetUniqueTableName(name, tables);

            if (name.Split('.').Last() == alias)
                return null;

            return " AS " + alias;
        }

        private string GetUniqueTableName(string name, IDictionary<string, string> tables)
        {
            name = name.Split('.').Last();

            if (!tables.ContainsKey(name))
                return name;

            var suffix = 2;

            while (tables.ContainsKey(name + suffix))
                suffix++;

            return name + suffix;
        }

        public class SqlAutocompleteItem : IComparable
        {
            public SqlAutocompleteItem(string text, int replaceLength, CompletionItemKind imageIndex)
            {
                Text = text;
                ImageIndex = imageIndex;
                ReplaceLength = replaceLength;
                MenuText = text;
            }

            public string Text { get; }

            public CompletionItemKind ImageIndex { get; }

            public virtual string ToolTipText
            {
                get => Text;
                set { }
            }

            public virtual string ToolTipTitle
            {
                get => Text;
                set { }
            }

            public virtual string GetTextForReplace() => Text;

            public virtual string MenuText { get; set; }

            public virtual string CompareText => Text;

            public int ReplaceLength { get; }

            public int CompareTo(object obj)
            {
                var other = (SqlAutocompleteItem)obj;

                return CompareText.CompareTo(other.CompareText);
            }

            public static string EscapeIdentifier(string identifier)
            {
                var id = new Microsoft.SqlServer.TransactSql.ScriptDom.Identifier { Value = identifier };
                id.QuoteType = RequiresQuote(id.Value) ? Microsoft.SqlServer.TransactSql.ScriptDom.QuoteType.SquareBracket : Microsoft.SqlServer.TransactSql.ScriptDom.QuoteType.NotQuoted;
                return id.ToSql();
            }

            private static readonly System.Text.RegularExpressions.Regex LegalIdentifier = new System.Text.RegularExpressions.Regex(@"^[\p{L}_@#][\p{L}\p{Nd}@$#_]*$", System.Text.RegularExpressions.RegexOptions.Compiled, TimeSpan.FromSeconds(1));
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
            private readonly DataSource _dataSource;

            public InstanceAutocompleteItem(DataSource dataSource, int replaceLength) : base(EscapeIdentifier(dataSource.Name), replaceLength, CompletionItemKind.Reference)
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
            public SchemaAutocompleteItem(string schema, int replaceLength) : base(schema, replaceLength, CompletionItemKind.Module)
            {
            }

            public override string ToolTipTitle
            {
                get => Text + " Schema";
                set => base.ToolTipTitle = value;
            }

            public override string ToolTipText
            {
                get => Text == "metadata" ? "Schema containing the metadata information" :
                       Text == "archive" ? "Schema containing long-term retention tables" :
                       Text == "bin" ? "Schema containing recycle bin tables" :
                       "Schema containing the data tables";
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

            public EntityAutocompleteItem(EntityMetadata entity, string alias, IAttributeMetadataCache metadata, int replaceLength) : base(alias, replaceLength, CompletionItemKind.Field)
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

        class TVFAutocompleteItem : SqlAutocompleteItem
        {
            private readonly Message _message;
            private readonly ColumnOrdering _columnOrdering;
            private readonly DataSource _dataSource;

            public TVFAutocompleteItem(Message message, ColumnOrdering columnOrdering, DataSource dataSource, int replaceLength) : base(message.Name, replaceLength, CompletionItemKind.Function)
            {
                _message = message;
                _columnOrdering = columnOrdering;
                _dataSource = dataSource;
            }

            public TVFAutocompleteItem(Message message, DataSource dataSource, string alias, int replaceLength) : base(alias, replaceLength, CompletionItemKind.Function)
            {
                _message = message;
                _dataSource = dataSource;
            }

            public override string ToolTipTitle
            {
                get => _message.Name + " SDK Message";
                set => base.ToolTipTitle = value;
            }

            public override string ToolTipText
            {
                get
                {
                    var parameters = _message.InputParameters
                        .Where(p => p.Type != typeof(PagingInfo));

                    if (_columnOrdering == ColumnOrdering.Alphabetical)
                        parameters = parameters.OrderBy(p => p.Name, StringComparer.OrdinalIgnoreCase);
                    else
                        parameters = parameters.OrderBy(p => p.Position);

                    return _message.Name + "(" + String.Join(", ", parameters.Select(p => p.Name + " " + p.GetSqlDataType(_dataSource).ToSql())) + ")";
                }
                set => base.ToolTipText = value;
            }

            public override string GetTextForReplace()
            {
                return _message.Name + "(" + (_message.InputParameters.Count == 0 ? ")" : "");
            }
        }

        class SprocAutocompleteItem : SqlAutocompleteItem
        {
            private readonly Message _message;
            private readonly ColumnOrdering _columnOrdering;
            private readonly DataSource _dataSource;

            public SprocAutocompleteItem(Message message, ColumnOrdering columnOrdering, DataSource dataSource, int replaceLength) : base(message.Name, replaceLength, CompletionItemKind.Method)
            {
                _message = message;
                _columnOrdering = columnOrdering;
                _dataSource = dataSource;
            }

            public override string ToolTipTitle
            {
                get => _message.Name + " SDK Message";
                set => base.ToolTipTitle = value;
            }

            public override string ToolTipText
            {
                get
                {
                    var parameters = _message.InputParameters
                        .Where(p => p.Type != typeof(PagingInfo));

                    if (_columnOrdering == ColumnOrdering.Alphabetical)
                        parameters = parameters.OrderBy(p => p.Name, StringComparer.OrdinalIgnoreCase);
                    else
                        parameters = parameters.OrderBy(p => p.Position);

                    return _message.Name + " " + String.Join(", ", parameters.Select(p => (p.Optional ? "[" : "") + "@" + p.Name + " = " + p.GetSqlDataType(_dataSource).ToSql() + (p.Optional ? "]" : ""))) + (_message.OutputParameters.Count == 0 ? "" : ((_message.InputParameters.Count == 0 ? "" : ",") + " " + String.Join(", ", _message.OutputParameters.Select(p => "[@" + p.Name + " = " + p.GetSqlDataType(_dataSource).ToSql() + " OUTPUT]"))));
                }
                set => base.ToolTipText = value;
            }
        }

        class SprocParameterAutocompleteItem : SqlAutocompleteItem
        {
            private readonly Message _message;
            private readonly MessageParameter _parameter;
            private readonly DataSource _dataSource;

            public SprocParameterAutocompleteItem(Message message, MessageParameter parameter, DataSource dataSource, int replaceLength) : base("@" + parameter.Name, replaceLength, CompletionItemKind.Variable)
            {
                _message = message;
                _parameter = parameter;
                _dataSource = dataSource;
            }

            public override string ToolTipTitle
            {
                get => _parameter.Name + (_message.OutputParameters.Contains(_parameter) ? " output" : " input") + " parameter (" + _parameter.GetSqlDataType(_dataSource).ToSql() + ")";
                set => base.ToolTipTitle = value;
            }

            public override string ToolTipText
            {
                get => _message.Name + " " + string.Join(", ", _message.InputParameters.Select(p => (p.Optional ? "[" : "") + "@" + p.Name + " = " + p.GetSqlDataType(_dataSource).ToSql() + (p.Optional ? "]" : ""))) + (_message.OutputParameters.Count == 0 ? "" : (_message.InputParameters.Count == 0 ? "" : ",") + " " + string.Join(", ", _message.OutputParameters.Select(p => "[@" + p.Name + " = " + p.GetSqlDataType(_dataSource).ToSql() + " OUTPUT]")));
                set => base.ToolTipText = value;
            }
        }

        class JoinAutocompleteItem : SqlAutocompleteItem
        {
            private readonly EntityMetadata _rhs;
            private readonly AttributeMetadata _attribute;
            private readonly string _lhs;
            private readonly IAttributeMetadataCache _metadata;

            public JoinAutocompleteItem(OneToManyRelationshipMetadata relationship, string join, bool oneToMany, IAttributeMetadataCache metadata, int replaceLength) : base(join, replaceLength, CompletionItemKind.Snippet)
            {
                _rhs = metadata.GetAllEntities().SingleOrDefault(e => e.LogicalName == relationship.ReferencingEntity);
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
                if (_rhs != null)
                    _metadata.TryGetMinimalData(_rhs.LogicalName, out _);

                _metadata.TryGetMinimalData(_lhs, out _);
                return base.GetTextForReplace();
            }
        }

        class AttributeAutocompleteItem : SqlAutocompleteItem
        {
            private readonly AttributeMetadata _attribute;
            private readonly string _virtualSuffix;

            public AttributeAutocompleteItem(AttributeMetadata attribute, int replaceLength, string virtualSuffix = null) : base(attribute.LogicalName + virtualSuffix, replaceLength, CompletionItemKind.Field)
            {
                _attribute = attribute;
                _virtualSuffix = virtualSuffix;
            }

            public static IEnumerable<AttributeAutocompleteItem> CreateList(AttributeMetadata attribute, int replaceLength, bool writeable, DataSource dataSource)
            {
                yield return new AttributeAutocompleteItem(attribute, replaceLength);

                foreach (var virtualAttr in attribute.GetVirtualAttributes(dataSource, writeable))
                    yield return new AttributeAutocompleteItem(attribute, replaceLength, virtualAttr.Suffix);
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
                    else if (_virtualSuffix == "pid")
                        description += $"\r\n\r\nThis attribute holds the partition id of the record referenced by the {_attribute.LogicalName} field";
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

            public FunctionAutocompleteItem(MethodInfo method, int replaceLength) : base(GetInsertText(method), replaceLength, CompletionItemKind.Function)
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

        class OptionSetAutocompleteItem : SqlAutocompleteItem
        {
            private readonly OptionMetadata _option;

            public OptionSetAutocompleteItem(OptionMetadata option, int replaceLength) : base(option.Value.Value.ToString(CultureInfo.InvariantCulture), replaceLength, CompletionItemKind.Field)
            {
                _option = option;

                if (!String.IsNullOrEmpty(option.Label?.UserLocalizedLabel?.Label))
                    MenuText = $"{_option.Label.UserLocalizedLabel.Label} ({_option.Value.Value.ToString(CultureInfo.InvariantCulture)})";
            }

            public override string ToolTipTitle
            {
                get => _option.Label?.UserLocalizedLabel?.Label ?? Text;
                set => base.ToolTipTitle = value;
            }

            public override string ToolTipText
            {
                get => _option.Label?.UserLocalizedLabel?.Label ?? Text;
                set => base.ToolTipText = value;
            }
        }

        class EntityNameAutocompleteItem : SqlAutocompleteItem
        {
            private readonly EntityMetadata _entity;

            public EntityNameAutocompleteItem(EntityMetadata entity, int replaceLength) : base(entity.LogicalName, replaceLength, CompletionItemKind.Text)
            {
                _entity = entity;
            }

            public override string ToolTipTitle
            {
                get => _entity.DisplayName?.UserLocalizedLabel?.Label ?? _entity.LogicalName;
                set => base.ToolTipTitle = value;
            }

            public override string ToolTipText
            {
                get => _entity.Description?.UserLocalizedLabel?.Label;
                set => base.ToolTipText = value;
            }

            public override string GetTextForReplace()
            {
                return "'" + _entity.LogicalName + "'";
            }
        }

        class CollationAutocompleteItem : SqlAutocompleteItem
        {
            private readonly Collation _collation;

            public CollationAutocompleteItem(Collation collation, int replaceLength) : base(collation.Name, replaceLength, CompletionItemKind.Field)
            {
                _collation = collation;
            }

            public override string ToolTipTitle
            {
                get => _collation.Name;
                set { }
            }

            public override string ToolTipText
            {
                get => _collation.Description;
                set { }
            }
        }

        class VariableAutocompleteItem : SqlAutocompleteItem
        {
            public VariableAutocompleteItem(string name, int replaceLength) : base(name, replaceLength, CompletionItemKind.Variable)
            {
            }

            public override string ToolTipTitle
            {
                get => Text;
                set { }
            }

            public override string ToolTipText
            {
                get => Text.StartsWith("@@") ? "Global variable" : "User-defined variable";
                set { }
            }
        }
    }
}
