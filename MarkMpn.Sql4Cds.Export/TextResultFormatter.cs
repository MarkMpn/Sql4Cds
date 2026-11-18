using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace MarkMpn.Sql4Cds.Export
{
    /// <summary>
    /// Formats a result set as fixed-width text, following the same layout as the "Results to Text" option in SSMS
    /// </summary>
    public class TextResultFormatter
    {
        // Limit text columns to 256 characters to follow SSMS defaults
        private const int MaxColumnWidth = 256;

        private readonly string[] _columnNames;
        private readonly string[] _dataTypes;
        private readonly short[] _scales;
        private readonly int[] _columnWidths;
        private readonly bool _localFormatDates;

        /// <summary>
        /// Creates a new <see cref="TextResultFormatter"/>
        /// </summary>
        /// <param name="schemaTable">The schema table describing the columns in the result set, as returned by <see cref="IDataReader.GetSchemaTable"/></param>
        /// <param name="localFormatDates">Indicates if dates should be formatted using the local format rather than ISO format</param>
        public TextResultFormatter(DataTable schemaTable, bool localFormatDates)
        {
            if (schemaTable == null)
                throw new ArgumentNullException(nameof(schemaTable));

            var count = schemaTable.Rows.Count;
            _columnNames = new string[count];
            _dataTypes = new string[count];
            _scales = new short[count];
            _columnWidths = new int[count];
            _localFormatDates = localFormatDates;

            for (var i = 0; i < count; i++)
            {
                var schema = schemaTable.Rows[i];
                _columnNames[i] = schema["ColumnName"] as string;
                _dataTypes[i] = schema["DataTypeName"] as string;
                _scales[i] = schema["NumericScale"] as short? ?? 0;

                var width = GetWidth(_dataTypes[i], schema["ColumnSize"] as int?, schema["NumericPrecision"] as short?, _scales[i]);

                if (_columnNames[i] != null)
                    width = Math.Max(width, _columnNames[i].Length);

                _columnWidths[i] = width;
            }
        }

        /// <summary>
        /// The number of characters used to display each column
        /// </summary>
        public IReadOnlyList<int> ColumnWidths => _columnWidths;

        /// <summary>
        /// Gets the lines of text to display before the first row: the column names and an underline for each column
        /// </summary>
        public IEnumerable<string> GetHeaderLines()
        {
            yield return FormatLine(_columnNames);
            yield return FormatLine(_columnWidths.Select(w => new string('-', w)));
        }

        /// <summary>
        /// Formats a single row of values
        /// </summary>
        /// <param name="values">The values in the row, as returned by <see cref="IDataRecord.GetValues(object[])"/></param>
        /// <returns>The line of text to display the row</returns>
        public string FormatRow(object[] values)
        {
            return FormatLine(values.Select((v, i) => ValueFormatter.Format(v, _dataTypes[i], _scales[i], _localFormatDates).DisplayValue));
        }

        private string FormatLine(IEnumerable<string> values)
        {
            return String.Join(" ", values.Zip(_columnWidths, (v, w) => (v ?? String.Empty).PadRight(w).Substring(0, w)));
        }

        private static int GetWidth(string type, int? maxLength, short? precision, short scale)
        {
            switch (type)
            {
                case "nvarchar":
                case "varchar":
                case "nchar":
                case "char":
                    return maxLength > 0 ? Math.Min(MaxColumnWidth, maxLength.Value) : MaxColumnWidth;

                case "binary":
                case "varbinary":
                    // Displayed as 0x followed by two hex digits per byte
                    return maxLength > 0 && maxLength < (MaxColumnWidth - 2) / 2 ? 2 + maxLength.Value * 2 : MaxColumnWidth;

                case "bit":
                    return 1;

                case "tinyint":
                    return 3;

                case "smallint":
                    return 6;

                case "int":
                    return 11;

                case "bigint":
                    return 20;

                case "real":
                    return 14;

                case "float":
                    return 22;

                case "money":
                case "smallmoney":
                    return 21;

                case "numeric":
                case "decimal":
                    return (precision ?? 38) + (scale > 0 ? 1 : 0);

                case "date":
                    return 10;

                case "smalldatetime":
                case "datetime":
                    return 23;

                case "datetime2":
                    return 19 + (scale > 0 ? 1 + scale : 0);

                case "datetimeoffset":
                    return 19 + (scale > 0 ? 1 + scale : 0) + 7;

                case "time":
                    return 8 + (scale > 0 ? 1 + scale : 0);

                case "uniqueidentifier":
                    return 36;

                case "rowversion":
                case "timestamp":
                    return 18;

                default:
                    return MaxColumnWidth;
            }
        }
    }
}
