using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace MarkMpn.Sql4Cds.XTB
{
    public static class Formatter
    {
        public static string Format(string sql)
        {
            var dom = new TSql160Parser(true);
            var fragment = dom.Parse(new StringReader(sql), out var errors);

            if (errors.Count != 0)
                return sql;

            var tokens = new Sql160ScriptGenerator().GenerateTokens(fragment);

            // Remove any trailing whitespace tokens
            while (tokens.Count > 0 && tokens[tokens.Count - 1].TokenType == TSqlTokenType.WhiteSpace)
                tokens.RemoveAt(tokens.Count - 1);

            // Insert any comments from the original tokens. Ignore whitespace tokens.
            for (int srcIndex = 0, dstIndex = -1; srcIndex < fragment.ScriptTokenStream.Count; srcIndex++)
            {
                var token = fragment.ScriptTokenStream[srcIndex];

                if (token.TokenType == TSqlTokenType.WhiteSpace)
                    continue;

                if (token.TokenType == TSqlTokenType.MultilineComment || token.TokenType == TSqlTokenType.SingleLineComment)
                {
                    // dstIndex currently points to the previously matched token. Move forward one so we insert the comment after that token
                    dstIndex++;

                    // We may well have added a semicolon at the end of the statement - move after that too
                    if ((srcIndex == fragment.ScriptTokenStream.Count - 1 || fragment.ScriptTokenStream[srcIndex + 1].TokenType != TSqlTokenType.Semicolon) &&
                        dstIndex <= tokens.Count - 1 &&
                        tokens[dstIndex].TokenType == TSqlTokenType.Semicolon)
                        dstIndex++;

                    // Also skip over any matching whitespace
                    var whitespaceCount = 0;

                    while (dstIndex + whitespaceCount < tokens.Count &&
                        srcIndex - whitespaceCount - 1 >= 0 &&
                        IsMatchingWhitespace(tokens[dstIndex + whitespaceCount], fragment.ScriptTokenStream[srcIndex - whitespaceCount - 1]))
                        whitespaceCount++;

                    dstIndex += CopyComment(fragment.ScriptTokenStream, srcIndex, tokens, dstIndex + whitespaceCount);
                }
                else
                {
                    dstIndex++;

                    while (dstIndex < tokens.Count && !IsSameType(token, tokens[dstIndex]))
                        dstIndex++;
                }
            }

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

        private static int CopyComment(IList<TSqlParserToken> src, int srcIndex, IList<TSqlParserToken> dst, int dstIndex)
        {
            var insertedTokenCount = 0;

            if (dstIndex >= dst.Count)
                dst.Add(src[srcIndex]);
            else
                dst.Insert(dstIndex, src[srcIndex]);

            insertedTokenCount++;

            // Also add any leading or trailing whitespace
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

            var trailingSrcIndex = srcIndex + 1;
            var trailingDstIndex = dstIndex + 1;

            while (trailingSrcIndex < src.Count && src[trailingSrcIndex].TokenType == TSqlTokenType.WhiteSpace)
            {
                if (trailingDstIndex < dst.Count && IsMatchingWhitespace(dst[trailingDstIndex], src[trailingSrcIndex]))
                    break;

                if (trailingDstIndex >= dst.Count)
                    dst.Add(src[trailingSrcIndex]);
                else
                    dst.Insert(trailingDstIndex, src[trailingSrcIndex]);

                trailingSrcIndex++;
                trailingDstIndex++;
                insertedTokenCount++;
            }

            return insertedTokenCount;
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
}
