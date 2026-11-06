using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace MarkMpn.Sql4Cds.Engine
{
    /// <summary>
    /// Formats SQL queries while preserving comments, optionally adding display name aliases to columns
    /// </summary>
    public static class QueryFormatter
    {
        /// <summary>
        /// Formats a SQL query
        /// </summary>
        /// <param name="sql">The SQL to format</param>
        /// <param name="options">The options to control the formatting</param>
        /// <returns>The formatted SQL. If the query cannot be parsed and <see cref="QueryFormatterOptions.ThrowOnError"/>
        /// is not set, the original SQL is returned unchanged.</returns>
        public static string Format(string sql, QueryFormatterOptions options = null)
        {
            var dom = new TSql160Parser(true);
            var fragment = dom.Parse(new StringReader(sql), out var errors);

            if (errors.Count != 0)
            {
                if (options?.ThrowOnError == true)
                    throw new QueryParseException(String.Join("\n", errors.Select(error => $"Line {error.Line}: {error.Message}")));

                return sql;
            }

            if (options?.AddDisplayNames == true)
                AddDisplayNames(sql, (TSqlScript)fragment, options);

            return Format(fragment);
        }

        private static void AddDisplayNames(string sql, TSqlScript script, QueryFormatterOptions options)
        {
            if (options.Connection == null)
                throw new ArgumentNullException(nameof(options.Connection));

            var selectStatements = script.Batches.SelectMany(batch => batch.Statements).OfType<SelectStatement>().ToList();

            if (selectStatements.Count == 0)
                return;

            using (var cmd = options.Connection.CreateCommand())
            {
                cmd.CommandText = sql;

                try
                {
                    using (var reader = cmd.ExecuteReader(System.Data.CommandBehavior.SchemaOnly))
                    {
                        foreach (var select in selectStatements)
                        {
                            if (select.QueryExpression is QuerySpecification querySpec)
                                AddDisplayNames(querySpec, reader.GetSchemaTable(), options);

                            reader.NextResult();
                        }
                    }
                }
                catch (Sql4CdsException) when (options.ThrowOnError != true)
                {
                }
            }
        }

        private static void AddDisplayNames(QuerySpecification querySpec, System.Data.DataTable schema, QueryFormatterOptions options)
        {
            // Check if columns are from multiple tables
            var tables = new HashSet<string>();

            for (var colIndex = 0; colIndex < querySpec.SelectElements.Count; colIndex++)
            {
                var table = schema.Rows[colIndex]["BaseTableName"] as string;
                tables.Add(table);
            }

            for (var colIndex = 0; colIndex < querySpec.SelectElements.Count; colIndex++)
            {
                if (!(querySpec.SelectElements[colIndex] is SelectScalarExpression scalar))
                    break;

                if (scalar.ColumnName != null)
                    continue; // Already has an alias

                var server = schema.Rows[colIndex]["BaseServerName"] as string;
                var table = schema.Rows[colIndex]["BaseTableName"] as string;
                var column = schema.Rows[colIndex]["BaseColumnName"] as string;

                if (!string.IsNullOrEmpty(server) && !string.IsNullOrEmpty(table) && !string.IsNullOrEmpty(column) &&
                    options.DataSources != null &&
                    options.DataSources.TryGetValue(server, out var dataSource) &&
                    dataSource.Metadata.TryGetValue(table, out var metadata))
                {
                    var attribute = metadata.Attributes.SingleOrDefault(a => a.LogicalName.Equals(column, StringComparison.OrdinalIgnoreCase));

                    if (attribute?.DisplayName?.UserLocalizedLabel?.Label != null)
                    {
                        var alias = attribute.DisplayName.UserLocalizedLabel.Label;

                        if (tables.Count > 1 && metadata.DisplayName?.UserLocalizedLabel?.Label != null)
                            alias += $" ({metadata.DisplayName.UserLocalizedLabel.Label})";

                        scalar.ColumnName = new IdentifierOrValueExpression { Identifier = new Identifier { Value = alias, QuoteType = QuoteType.SquareBracket } };
                    }
                }
            }
        }

        /// <summary>
        /// Formats a parsed SQL fragment, preserving any comments from the original token stream
        /// </summary>
        /// <param name="fragment">The fragment to format</param>
        /// <returns>The formatted SQL</returns>
        public static string Format(TSqlFragment fragment)
        {
            var tokens = new Sql160ScriptGenerator().GenerateTokens(fragment);

            // Remove any trailing whitespace tokens
            while (tokens.Count > 0 && tokens[tokens.Count - 1].TokenType == TSqlTokenType.WhiteSpace)
                tokens.RemoveAt(tokens.Count - 1);

            // Insert any comments from the original tokens. Ignore whitespace tokens.
            var dstIndex = -1;

            for (var srcIndex = 0; srcIndex < fragment.ScriptTokenStream.Count; srcIndex++)
            {
                var token = fragment.ScriptTokenStream[srcIndex];

                if (token.TokenType == TSqlTokenType.WhiteSpace)
                    continue;

                if (token.TokenType == TSqlTokenType.MultilineComment || token.TokenType == TSqlTokenType.SingleLineComment)
                {
                    dstIndex = FindCommentInsertPoint(fragment, srcIndex, tokens, dstIndex);

                    dstIndex += CopyComment(fragment.ScriptTokenStream, ref srcIndex, tokens, dstIndex);

                    // Move back one so this is pointing to the last inserted token
                    dstIndex--;
                }
                else
                {
                    dstIndex++;

                    while (dstIndex < tokens.Count && !IsSameType(token, tokens[dstIndex]))
                        dstIndex++;
                }
            }

            return TokensToString(tokens);
        }

        private static int FindCommentInsertPoint(TSqlFragment fragment, int srcIndex, IList<TSqlParserToken> tokens, int dstIndex)
        {
            // dstIndex currently points to the previously matched token. Move forward one so we insert the comment after that token
            dstIndex++;

            // We may well have added a semicolon at the end of the statement - move after that too
            if ((srcIndex == fragment.ScriptTokenStream.Count - 1 || fragment.ScriptTokenStream[srcIndex + 1].TokenType != TSqlTokenType.Semicolon) &&
                dstIndex <= tokens.Count - 1 &&
                tokens[dstIndex].TokenType == TSqlTokenType.Semicolon)
                dstIndex++;

            return dstIndex;
        }

        private static string TokensToString(IList<TSqlParserToken> tokens)
        {
            using (var writer = new StringWriter())
            {
                foreach (var token in tokens)
                    writer.Write(token.Text);

                writer.Flush();

                return writer.ToString().Trim();
            }
        }

        private static bool IsSameType(TSqlParserToken srcToken, TSqlParserToken dstToken)
        {
            if (srcToken.TokenType == dstToken.TokenType)
                return true;

            if (srcToken.TokenType == TSqlTokenType.Variable && dstToken.TokenType == TSqlTokenType.Identifier && dstToken.Text.StartsWith("@"))
                return true;

            if (srcToken.TokenType == TSqlTokenType.QuotedIdentifier && dstToken.TokenType == TSqlTokenType.Identifier && dstToken.Text.StartsWith("["))
                return true;

            return false;
        }

        private static int CopyComment(IList<TSqlParserToken> src, ref int srcIndex, IList<TSqlParserToken> dst, int dstIndex)
        {
            var insertedTokenCount = 0;

            if (dstIndex >= dst.Count)
                dst.Add(src[srcIndex]);
            else
                dst.Insert(dstIndex, src[srcIndex]);

            insertedTokenCount++;

            // Also add any leading or trailing whitespace
            CopyLeadingWhitespace(src, srcIndex, dst, ref dstIndex, ref insertedTokenCount);
            CopyTrailingWhitespace(src, ref srcIndex, dst, dstIndex, ref insertedTokenCount);

            return insertedTokenCount;
        }

        private static void CopyLeadingWhitespace(IList<TSqlParserToken> src, int srcIndex, IList<TSqlParserToken> dst, ref int dstIndex, ref int insertedTokenCount)
        {
            var leadingSrcIndex = srcIndex - 1;
            var leadingDstIndex = dstIndex - 1;
            var insertPoint = dstIndex;

            while (leadingDstIndex < dst.Count && leadingSrcIndex >= 0 && src[leadingSrcIndex].TokenType == TSqlTokenType.WhiteSpace)
            {
                if (leadingDstIndex >= 0 && IsMatchingWhitespace(dst[leadingDstIndex], src[leadingSrcIndex]))
                    break;

                dst.Insert(insertPoint, src[leadingSrcIndex]);
                leadingSrcIndex--;
                leadingDstIndex--;
                dstIndex++;
                insertedTokenCount++;
            }
        }

        private static void CopyTrailingWhitespace(IList<TSqlParserToken> src, ref int srcIndex, IList<TSqlParserToken> dst, int dstIndex, ref int insertedTokenCount)
        {
            var trailingSrcIndex = srcIndex + 1;
            var trailingDstIndex = dstIndex + 1;

            while (trailingSrcIndex < src.Count && (src[trailingSrcIndex].TokenType == TSqlTokenType.WhiteSpace || src[trailingSrcIndex].TokenType == TSqlTokenType.SingleLineComment || src[trailingSrcIndex].TokenType == TSqlTokenType.MultilineComment))
            {
                if (trailingDstIndex < dst.Count && IsMatchingWhitespace(dst[trailingDstIndex], src[trailingSrcIndex]))
                    break;

                if (trailingDstIndex >= dst.Count)
                    dst.Add(src[trailingSrcIndex]);
                else
                    dst.Insert(trailingDstIndex, src[trailingSrcIndex]);

                srcIndex = trailingSrcIndex;
                trailingSrcIndex++;
                trailingDstIndex++;
                insertedTokenCount++;
            }

            if (dst[trailingDstIndex - 1].TokenType == TSqlTokenType.WhiteSpace)
            {
                // There might be some whitespace in the generated version straight after the whitespace we've just re-inserted - remove it
                while (trailingDstIndex < dst.Count && dst[trailingDstIndex].TokenType == TSqlTokenType.WhiteSpace)
                {
                    dst.RemoveAt(trailingDstIndex);
                    insertedTokenCount--;
                }
            }
        }

        private static bool IsMatchingWhitespace(TSqlParserToken x, TSqlParserToken y)
        {
            if (x.TokenType != TSqlTokenType.WhiteSpace)
                return false;

            if (y.TokenType != TSqlTokenType.WhiteSpace)
                return false;

            if (x.Text == y.Text)
                return true;

            if (x.Text == "\n" && y.Text == "\r\n")
                return true;

            if (x.Text == "\r\n" && y.Text == "\n")
                return true;

            return false;
        }
    }

    /// <summary>
    /// Options to control how <see cref="QueryFormatter"/> formats a query
    /// </summary>
    public class QueryFormatterOptions
    {
        /// <summary>
        /// Indicates if errors parsing the query or loading display names should be thrown rather than ignored
        /// </summary>
        public bool ThrowOnError { get; set; }

        /// <summary>
        /// Indicates if column aliases should be added using the display names of the underlying attributes
        /// </summary>
        public bool AddDisplayNames { get; set; }

        /// <summary>
        /// The connection to use to identify the source of each column when adding display names
        /// </summary>
        public Sql4CdsConnection Connection { get; set; }

        /// <summary>
        /// The data sources to load metadata from when adding display names, keyed by instance name
        /// </summary>
        public IDictionary<string, DataSource> DataSources { get; set; }
    }
}
