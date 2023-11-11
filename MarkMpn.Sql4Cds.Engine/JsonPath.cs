using System;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json.Linq;

namespace MarkMpn.Sql4Cds.Engine
{
    /// <summary>
    /// Handles navigating JSON documents using a JSON Path expression
    /// </summary>
    /// <remarks>
    /// The JSON Path syntax supported by SQL Server is subtely different to that implemented by
    /// JSON.NET, so to keep compatibility with the various T-SQL samples we need to use this class
    /// instead of the built-in <see cref="JToken.SelectToken(string)"/> method.
    /// https://learn.microsoft.com/en-us/sql/relational-databases/json/json-path-expressions-sql-server?view=sql-server-ver16
    /// </remarks>
    class JsonPath
    {
        private readonly JsonPathPart[] _parts;
        private readonly JsonPathMode _mode;

        public JsonPath(string expression)
        {
            _parts = Parse(expression, out _mode);
        }

        /// <summary>
        /// Returns the mode the path should be evaluated in
        /// </summary>
        public JsonPathMode Mode { get; }

        /// <summary>
        /// Finds the token matching the path
        /// </summary>
        /// <param name="token">The token to start matching from</param>
        /// <returns>The token matching the path, or <c>null</c> if no match is found</returns>
        public JToken Evaluate(JToken token)
        {
            foreach (var part in _parts)
            {
                token = part.Match(token);

                if (token == null)
                    return null;
            }

            return token;
        }

        private static JsonPathPart[] Parse(string expression, out JsonPathMode mode)
        {
            mode = JsonPathMode.Lax;

            if (expression.StartsWith("lax "))
            {
                expression = expression.Substring(4);
            }
            else if (expression.StartsWith("strict "))
            {
                expression = expression.Substring(7);
                mode = JsonPathMode.Strict;
            }

            var parts = new List<JsonPathPart>();

            for (var i = 0; i < expression.Length; i++)
            {
                if (i == 0 && expression[i] == '$')
                {
                    parts.Add(new ContextJsonPathPart());
                }
                else if (expression[i] == '.')
                {
                    // Start of a property key
                    i++;

                    if (i == expression.Length)
                        throw new Newtonsoft.Json.JsonException($"Invalid JSON path - missing property name after '.' at end of '{expression}'");

                    if (expression[i] == '"')
                    {
                        // Start of a quoted property key
                        i++;
                        var start = i;
                        
                        while (i < expression.Length)
                        {
                            if (expression[i] == '\\')
                                i += 2;
                            else if (expression[i] == '"')
                                break;
                            else
                                i++;
                        }

                        if (i < expression.Length && expression[i] == '"')
                        {
                            var propertyName = expression.Substring(start, i - start).Replace("\\\"", "\"").Replace("\\\\", "\\");
                            parts.Add(new PropertyJsonPathPart(propertyName));
                        }
                    }
                    else if (expression[i] >= 'a' && expression[i] <= 'z' ||
                        expression[i] >= 'A' && expression[i] <= 'Z')
                    {
                        // Start of an unquoted property key
                        var end = expression.IndexOfAny(new[] { '.', '[' }, i);

                        if (end == -1)
                            end = expression.Length;

                        var propertyName = expression.Substring(i, end - i);

                        if (propertyName == "sql:identity()")
                        {
                            if (end == expression.Length)
                                parts.Add(new ArrayIndexJsonPathPart());
                            else
                                throw new Newtonsoft.Json.JsonException($"Invalid JSON path - sql:identity() function must be the final token of the path'");
                        }

                        parts.Add(new PropertyJsonPathPart(propertyName));
                        i = end - 1;
                    }
                    else
                    {
                        // Error
                        throw new Newtonsoft.Json.JsonException($"Invalid JSON path - invalid property name at index {i} of '{expression}'");
                    }
                }
                else if (expression[i] == '[')
                {
                    // Start of an array indexer
                    var end = expression.IndexOf(']', i);

                    if (end == -1)
                        throw new Newtonsoft.Json.JsonException($"Invalid JSON path - missing closing bracket for indexer at index {i} of '{expression}'");

                    var indexStr = expression.Substring(i + 1, end - i - 1);

                    if (!UInt32.TryParse(indexStr, out var index))
                        throw new Newtonsoft.Json.JsonException($"Invalid JSON path - invalid indexer at index {i} of '{expression}'");

                    parts.Add(new ArrayElementJsonPathPart(index));
                    i = end;
                }
                else
                {
                    // Error
                    throw new Newtonsoft.Json.JsonException($"Invalid JSON path - invalid token at index {i} of '{expression}'");
                }
            }

            return parts.ToArray();
        }

        public override string ToString()
        {
            return _mode.ToString().ToLowerInvariant() + " " + String.Join("", _parts.Select(p => p.ToString()));
        }

        /// <summary>
        /// Represents a single token within the path
        /// </summary>
        abstract class JsonPathPart
        {
            /// <summary>
            /// Extracts the required child token from the current context token
            /// </summary>
            /// <param name="token">The current context token</param>
            /// <returns>The child token that is matched by this part of the path</returns>
            public abstract JToken Match(JToken token);
        }

        /// <summary>
        /// Handles the $ sign representing the context item
        /// </summary>
        class ContextJsonPathPart : JsonPathPart
        {
            public override JToken Match(JToken token)
            {
                return token;
            }

            public override string ToString()
            {
                return "$";
            }
        }

        /// <summary>
        /// Handles a property (key) name
        /// </summary>
        class PropertyJsonPathPart : JsonPathPart
        {
            /// <summary>
            /// Creates a new <see cref="PropertyJsonPathPart"/>
            /// </summary>
            /// <param name="propertyName">The key of the property to extract</param>
            public PropertyJsonPathPart(string propertyName)
            {
                PropertyName = propertyName;
            }

            /// <summary>
            /// Returns the key of the property to extract
            /// </summary>
            public string PropertyName { get; }

            public override JToken Match(JToken token)
            {
                if (!(token is JObject obj))
                    return null;

                var prop = obj.Property(PropertyName);
                return prop?.Value;
            }

            public override string ToString()
            {
                if ((PropertyName[0] >= 'a' && PropertyName[0] <= 'z' || PropertyName[0] >= 'A' && PropertyName[0] <= 'Z') 
                    && PropertyName.All(ch => ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9'))
                    return "." + PropertyName;

                return ".\"" + PropertyName.Replace("\\", "\\\\").Replace("\"", "\\\"") + "\"";
            }
        }

        /// <summary>
        /// Handles an array element indexer
        /// </summary>
        class ArrayElementJsonPathPart : JsonPathPart
        {
            /// <summary>
            /// Creates a new <see cref="ArrayElementJsonPathPart"/>
            /// </summary>
            /// <param name="index">The index of the array to extract</param>
            public ArrayElementJsonPathPart(uint index)
            {
                Index = (int)index;
            }

            /// <summary>
            /// The index of the array to extract
            /// </summary>
            public int Index { get; }

            public override JToken Match(JToken token)
            {
                if (!(token is JArray arr) || arr.Count <= Index)
                    return null;

                return arr[Index];
            }

            public override string ToString()
            {
                return $"[{Index}]";
            }
        }

        /// <summary>
        /// Handles the sql:identity() function to return the index of an element within it's containing array
        /// </summary>
        class ArrayIndexJsonPathPart : JsonPathPart
        {
            public override JToken Match(JToken token)
            {
                if (!(token.Parent is JArray arr))
                    return null;

                var index = arr.IndexOf(token);
                return new JValue(index);
            }

            public override string ToString()
            {
                return ".sql:identity()";
            }
        }
    }

    enum JsonPathMode
    {
        Lax,
        Strict
    }
}
