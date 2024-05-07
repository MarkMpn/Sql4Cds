using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;

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
        public JsonPathMode Mode { get => _mode; }

        /// <summary>
        /// Finds the token matching the path
        /// </summary>
        /// <param name="token">The token to start matching from</param>
        /// <returns>The token matching the path, or <c>null</c> if no match is found</returns>
        public JsonElement? Evaluate(JsonElement token)
        {
            foreach (var part in _parts)
            {
                var match = part.Match(token);

                if (match == null)
                    return null;

                token = match.Value;
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
                else if (i > 0 && expression[i] == '.')
                {
                    // Start of a property key
                    i++;

                    if (i == expression.Length)
                        throw new JsonPathException(Sql4CdsError.JsonPathFormatError(expression[i - 1], i));

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
                        var start = i;

                        while (i < expression.Length)
                        {
                            if (expression[i] >= 'a' && expression[i] <= 'z' ||
                                expression[i] >= 'A' && expression[i] <= 'Z' ||
                                expression[i] >= '0' && expression[i] <= '9')
                            {
                                i++;
                            }
                            else
                            {
                                break;
                            }
                        }

                        var propertyName = expression.Substring(start, i - start);
                        parts.Add(new PropertyJsonPathPart(propertyName));

                        if (i < expression.Length)
                            i--;
                    }
                    else
                    {
                        // Error
                        throw new JsonPathException(Sql4CdsError.JsonPathFormatError(expression[i], i + 1));
                    }
                }
                else if (expression[i] == '[')
                {
                    // Start of an array indexer
                    var end = expression.IndexOf(']', i);

                    if (end == -1)
                        throw new JsonPathException(Sql4CdsError.JsonPathFormatError('.', expression.Length));

                    var indexStr = expression.Substring(i + 1, end - i - 1);

                    if (!UInt32.TryParse(indexStr, out var index))
                        throw new JsonPathException(Sql4CdsError.JsonPathFormatError(expression[i+1], i + 2));

                    parts.Add(new ArrayElementJsonPathPart(index));
                    i = end ;
                }
                else
                {
                    // Error
                    throw new JsonPathException(Sql4CdsError.JsonPathFormatError(expression[i], i + 1));
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
            public abstract JsonElement? Match(JsonElement token);
        }

        /// <summary>
        /// Handles the $ sign representing the context item
        /// </summary>
        class ContextJsonPathPart : JsonPathPart
        {
            public override JsonElement? Match(JsonElement token)
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

            public override JsonElement? Match(JsonElement token)
            {
                if (token.ValueKind != JsonValueKind.Object)
                    return null;

                foreach (var prop in token.EnumerateObject())
                {
                    if (prop.NameEquals(PropertyName))
                        return prop.Value;
                }

                return null;
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

            public override JsonElement? Match(JsonElement token)
            {
                if (token.ValueKind != JsonValueKind.Array || token.GetArrayLength() < Index)
                    return null;

                return token[Index];
            }

            public override string ToString()
            {
                return $"[{Index}]";
            }
        }
    }

    enum JsonPathMode
    {
        Lax,
        Strict
    }

    class JsonPathException : ApplicationException, ISql4CdsErrorException
    {
        private readonly Sql4CdsError[] _errors;

        public JsonPathException(Sql4CdsError error) : base(error.Message)
        {
            _errors = new[] { error };
        }

        public IReadOnlyList<Sql4CdsError> Errors => _errors;
    }
}
