using MarkMpn.Sql4Cds.Engine;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Metadata;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
        public IEnumerable<string> GetSuggestions(string text, int pos, out int currentLength)
        {
            // If we're in the first word after a FROM or JOIN, show a list of table names
            string currentWord = null;
            string prevWord = null;

            foreach (var word in ReverseWords(text, pos))
            {
                if (currentWord == null)
                {
                    currentWord = word;
                }
                else if (prevWord == null)
                {
                    prevWord = word;
                    break;
                }
            }

            currentLength = currentWord.Length;

            if (prevWord == null)
                return Array.Empty<string>();

            switch (prevWord.ToLower())
            {
                case "from":
                case "join":
                case "into":
                    // Show table list
                    if (_entities != null)
                        return _entities.Select(x => x.LogicalName).Where(x => x.StartsWith(currentWord)).OrderBy(x => x);
                    break;

                default:
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
                        !prevWord.Equals("set", StringComparison.OrdinalIgnoreCase))
                        return Array.Empty<string>();

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
                            case "insert":
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
                                break;

                            case "order":
                            case "group":
                                break;

                            case "set":
                                words.Clear();
                                clause = clause ?? "set";
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

                    if (foundFrom || (foundPossibleFrom && words.Count > 0))
                    {
                        // Try to get the table & alias names from the words in the possible FROM clause
                        var tables = new Dictionary<string, string>();

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

                        if (prevWord.Equals("update", StringComparison.OrdinalIgnoreCase) ||
                            prevWord.Equals("delete", StringComparison.OrdinalIgnoreCase))
                            return tables.Keys.Where(x => x.StartsWith(currentWord)).OrderBy(x => x);

                        if (clause == "set" && (prevWord.Equals("set", StringComparison.OrdinalIgnoreCase) || prevWord == ","))
                        {
                            var targetTable = "";

                            foreach (var word in ReverseWords(text, pos))
                            {
                                if (word.Equals("update", StringComparison.OrdinalIgnoreCase))
                                    break;

                                targetTable = word;
                            }

                            if (tables.TryGetValue(targetTable, out var tableName) && _metadata.TryGetValue(tableName, out var metadata))
                                return metadata.Attributes.Where(a => a.IsValidForUpdate != false && a.LogicalName.StartsWith(currentWord, StringComparison.OrdinalIgnoreCase)).Select(a => a.LogicalName).OrderBy(a => a);
                        }

                        // Start loading all the appropriate metadata in the background
                        foreach (var table in tables.Values)
                            _metadata.TryGetValue(table, out _);

                        if (currentWord.Contains("."))
                        {
                            // Autocomplete list is attributes in the current table
                            var alias = currentWord.Substring(0, currentWord.IndexOf('.'));
                            currentWord = currentWord.Substring(currentWord.IndexOf('.') + 1);
                            currentLength = currentWord.Length;

                            if (tables.TryGetValue(alias, out var tableName))
                            {
                                if (_metadata.TryGetValue(tableName, out var metadata))
                                    return metadata.Attributes.Select(x => x.LogicalName).Where(x => x.StartsWith(currentWord)).OrderBy(x => x);
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
                            var items = new List<string>();

                            items.AddRange(tables.Keys);

                            var attributes = new List<string>();

                            foreach (var table in tables)
                            {
                                if (_metadata.TryGetValue(table.Value, out var metadata))
                                    attributes.AddRange(metadata.Attributes.Select(x => x.LogicalName));
                            }

                            items.AddRange(attributes.GroupBy(x => x).Where(g => g.Count() == 1).Select(g => g.Key));
                            items.Sort();

                            return items.Where(x => x.StartsWith(currentWord)).OrderBy(x => x);
                        }
                    }
                    else if (prevWord.Equals("update", StringComparison.OrdinalIgnoreCase))
                    {
                        return _entities.Select(x => x.LogicalName).Where(x => x.StartsWith(currentWord)).OrderBy(x => x);
                    }

                    break;
            }

            return Array.Empty<string>();
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
                if (Char.IsWhiteSpace(text[i]) || (Char.IsPunctuation(text[i]) && text[i] != '.'))
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
                        i--;
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
    }
}
