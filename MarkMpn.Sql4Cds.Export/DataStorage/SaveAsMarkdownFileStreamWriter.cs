//
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Web;
using MarkMpn.Sql4Cds.Engine;
using MarkMpn.Sql4Cds.Export.Contracts;

namespace MarkMpn.Sql4Cds.Export.DataStorage
{
    /// <summary>
    /// Writer for exporting results to a Markdown table.
    /// </summary>
    public partial class SaveAsMarkdownFileStreamWriter : SaveAsStreamWriter
    {
        private const string Delimiter = "|";
        private static Regex _newLineRegex = new Regex("(\\r\\n|\\n|\\r)", RegexOptions.Compiled, TimeSpan.FromSeconds(1));

        private static Regex GetNewLineRegex() => _newLineRegex;

        private readonly Encoding _encoding;
        private readonly string _lineSeparator;
        private readonly Func<SqlEntityReference, string> _urlGenerator;

        public SaveAsMarkdownFileStreamWriter(
            Stream stream,
            SaveResultsAsMarkdownRequestParams requestParams,
            IReadOnlyList<DbColumnWrapper> columns,
            Func<SqlEntityReference, string> urlGenerator)
            : base(stream, requestParams, columns)
        {
            // Parse the request params
            this._lineSeparator = string.IsNullOrEmpty(requestParams.LineSeparator)
                ? Environment.NewLine
                : requestParams.LineSeparator;
            this._encoding = ParseEncoding(requestParams.Encoding, Encoding.UTF8);
            this._urlGenerator = urlGenerator;

            // Output the header if requested
            if (requestParams.IncludeHeaders)
            {
                // Write the column header
                IEnumerable<string> selectedColumnNames = columns.Skip(this.ColumnStartIndex)
                    .Take(this.ColumnCount)
                    .Select(c => EncodeMarkdownField(c.ColumnName));
                string headerLine = string.Join(Delimiter, selectedColumnNames);

                this.WriteLine($"{Delimiter}{headerLine}{Delimiter}");

                // Write the separator row
                var separatorBuilder = new StringBuilder(Delimiter);
                for (int i = 0; i < this.ColumnCount; i++)
                {
                    separatorBuilder.Append($"---{Delimiter}");
                }

                this.WriteLine(separatorBuilder.ToString());
            }
        }

        /// <inheritdoc />
        public override void WriteRow(IList<DbCellValue> row, IReadOnlyList<DbColumnWrapper> columns)
        {
            IEnumerable<string> selectedCells = row.Skip(this.ColumnStartIndex)
                .Take(this.ColumnCount)
                .Select(c => EncodeMarkdownField(c));
            string rowLine = string.Join(Delimiter, selectedCells);

            this.WriteLine($"{Delimiter}{rowLine}{Delimiter}");
        }

        internal static string EncodeMarkdownField(string field)
        {
            // Special case for nulls
            if (field == null)
            {
                return "NULL";
            }

            // Escape HTML entities, since Markdown supports inline HTML
            field = HttpUtility.HtmlEncode(field);

            // Escape pipe delimiters
            field = field.Replace(@"|", @"\|");

            // @TODO: Allow option to encode multiple whitespace characters as &nbsp;

            // Replace newlines with br tags, since cell values must be single line
            field = GetNewLineRegex().Replace(field, @"<br />");

            return field;
        }

        private string EncodeMarkdownField(DbCellValue value)
        {
            var encoded = EncodeMarkdownField(value.IsNull ? null : value.DisplayValue);

            if (!value.IsNull && value.RawObject is SqlEntityReference er)
                encoded = $"[{encoded}]({_urlGenerator(er)})";

            return encoded;
        }

        private void WriteLine(string line)
        {
            byte[] bytes = this._encoding.GetBytes(line + this._lineSeparator);
            this.FileStream.Write(bytes, 0, bytes.Length);
        }
    }
}