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
    public class Autocomplete
    {
        private readonly EntityMetadata[] _entities;
        private readonly IAttributeMetadataCache _metadata;

        public Autocomplete(EntityMetadata[] entities, IAttributeMetadataCache metadata)
        {
            _entities = entities;
            _metadata = metadata;
        }

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
                        !prevWord.Equals("having", StringComparison.OrdinalIgnoreCase))
                        return Array.Empty<string>();

                    // Find the FROM clause
                    var words = new List<string>();
                    var foundFrom = false;
                    var foundQueryStart = false;
                    var foundPossibleFrom = false;
                    var inFrom = false;
                    var inJoin = false;
                    var inOn = false;
                    var afterFrom = false;

                    foreach (var word in ReverseWords(text, pos))
                    {
                        switch (word.ToLower())
                        {
                            case "from":
                                foundFrom = true;
                                if (!afterFrom)
                                    inFrom = true;
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
                                inJoin = true;
                                words.Insert(0, word);
                                break;

                            case "on":
                                if (!inJoin)
                                    inOn = true;
                                words.Insert(0, word);
                                break;

                            case "where":
                                words.Clear();
                                afterFrom = true;
                                break;

                            case "order":
                            case "group":
                                afterFrom = true;
                                break;

                            default:
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

                    if (foundFrom || foundPossibleFrom)
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
                        else if (inFrom && !inOn)
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
                if (Char.IsWhiteSpace(text[i]))
                {
                    if (inWord)
                    {
                        var wordStart = i + 1;
                        var word = text.Substring(wordStart, end - wordStart + 1);

                        yield return word;
                        inWord = false;
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
