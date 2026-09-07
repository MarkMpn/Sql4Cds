using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Globalization;
using System.Linq;
using MarkMpn.Sql4Cds.Export.Contracts;
using MarkMpn.Sql4Cds.LanguageServer.QueryExecution.Contracts;

namespace MarkMpn.Sql4Cds.LanguageServer.QueryExecution
{
    /// <summary>
    /// Applies a results-grid view specification without changing the retained query results.
    /// Row indexes are retained as a final tie breaker so sorting is stable.
    /// </summary>
    internal static class ResultSetViewTransformer
    {
        internal static IReadOnlyList<object[]> Transform(
            IReadOnlyList<object[]> rows,
            DbColumnWrapper[] columns,
            SubsetParams request,
            Func<object, DbColumnWrapper, string> formatValue)
        {
            var indexedRows = rows.Select((row, index) => new IndexedRow(row, index));

            if (!String.IsNullOrWhiteSpace(request.SearchText))
            {
                var searchText = request.SearchText.Trim();
                indexedRows = indexedRows.Where(row => Enumerable.Range(0, row.Values.Length).Any(columnIndex =>
                    Contains(formatValue(row.Values[columnIndex], columns[columnIndex]), searchText)));
            }

            if (request.Filters != null)
            {
                foreach (var filter in request.Filters.Where(filter => filter != null && filter.ColumnIndex >= 0 && filter.ColumnIndex < columns.Length))
                {
                    var currentFilter = filter;
                    indexedRows = indexedRows.Where(row => MatchesFilter(
                        row.Values[currentFilter.ColumnIndex],
                        formatValue(row.Values[currentFilter.ColumnIndex], columns[currentFilter.ColumnIndex]),
                        currentFilter));
                }
            }

            var materialized = indexedRows.ToList();

            if (request.Sort != null &&
                request.Sort.ColumnIndex >= 0 &&
                request.Sort.ColumnIndex < columns.Length &&
                (String.Equals(request.Sort.Direction, "asc", StringComparison.OrdinalIgnoreCase) ||
                 String.Equals(request.Sort.Direction, "desc", StringComparison.OrdinalIgnoreCase)))
            {
                var direction = String.Equals(request.Sort.Direction, "desc", StringComparison.OrdinalIgnoreCase) ? -1 : 1;
                var columnIndex = request.Sort.ColumnIndex;
                materialized.Sort((left, right) =>
                {
                    var comparison = CompareValues(
                        left.Values[columnIndex],
                        right.Values[columnIndex],
                        formatValue(left.Values[columnIndex], columns[columnIndex]),
                        formatValue(right.Values[columnIndex], columns[columnIndex]));

                    if (comparison == 0)
                        return left.OriginalIndex.CompareTo(right.OriginalIndex);

                    // Nulls remain last in both directions.
                    if (IsNull(left.Values[columnIndex]) || IsNull(right.Values[columnIndex]))
                        return comparison;

                    return comparison * direction;
                });
            }

            return materialized.Select(row => row.Values).ToArray();
        }

        private static bool MatchesFilter(object rawValue, string displayValue, ResultSetFilter filter)
        {
            var op = filter.Operator ?? String.Empty;
            var isEmpty = IsNull(rawValue) || String.IsNullOrEmpty(displayValue);

            if (op.Equals("isEmpty", StringComparison.OrdinalIgnoreCase))
                return isEmpty;

            if (op.Equals("isNotEmpty", StringComparison.OrdinalIgnoreCase))
                return !isEmpty;

            var filterValue = filter.Value ?? String.Empty;

            if (op.Equals("contains", StringComparison.OrdinalIgnoreCase))
                return Contains(displayValue, filterValue);

            if (op.Equals("notContains", StringComparison.OrdinalIgnoreCase))
                return !Contains(displayValue, filterValue);

            if (op.Equals("startsWith", StringComparison.OrdinalIgnoreCase))
                return (displayValue ?? String.Empty).StartsWith(filterValue, StringComparison.OrdinalIgnoreCase);

            if (op.Equals("endsWith", StringComparison.OrdinalIgnoreCase))
                return (displayValue ?? String.Empty).EndsWith(filterValue, StringComparison.OrdinalIgnoreCase);

            if (op.Equals("equals", StringComparison.OrdinalIgnoreCase))
                return String.Equals(displayValue ?? String.Empty, filterValue, StringComparison.OrdinalIgnoreCase);

            if (op.Equals("notEquals", StringComparison.OrdinalIgnoreCase))
                return !String.Equals(displayValue ?? String.Empty, filterValue, StringComparison.OrdinalIgnoreCase);

            if (isEmpty || !TryCompareToFilter(rawValue, displayValue, filterValue, out var comparison))
                return false;

            if (op.Equals("greaterThan", StringComparison.OrdinalIgnoreCase) || op.Equals("gt", StringComparison.OrdinalIgnoreCase))
                return comparison > 0;
            if (op.Equals("greaterThanOrEqual", StringComparison.OrdinalIgnoreCase) || op.Equals("gte", StringComparison.OrdinalIgnoreCase))
                return comparison >= 0;
            if (op.Equals("lessThan", StringComparison.OrdinalIgnoreCase) || op.Equals("lt", StringComparison.OrdinalIgnoreCase))
                return comparison < 0;
            if (op.Equals("lessThanOrEqual", StringComparison.OrdinalIgnoreCase) || op.Equals("lte", StringComparison.OrdinalIgnoreCase))
                return comparison <= 0;

            // Unknown operators should not unexpectedly hide every row from older/newer clients.
            return true;
        }

        private static bool TryCompareToFilter(object rawValue, string displayValue, string filterValue, out int comparison)
        {
            rawValue = UnwrapSqlValue(rawValue);

            if (IsNumeric(rawValue) && TryParseDecimal(filterValue, out var numericFilter))
            {
                try
                {
                    comparison = Convert.ToDecimal(rawValue, CultureInfo.InvariantCulture).CompareTo(numericFilter);
                    return true;
                }
                catch (OverflowException)
                {
                    comparison = 0;
                    return false;
                }
            }

            if (rawValue is DateTime dateTime && TryParseDate(filterValue, out var dateFilter))
            {
                comparison = new DateTimeOffset(dateTime).CompareTo(dateFilter);
                return true;
            }

            if (rawValue is DateTimeOffset dateTimeOffset && TryParseDate(filterValue, out var offsetFilter))
            {
                comparison = dateTimeOffset.CompareTo(offsetFilter);
                return true;
            }

            if (rawValue is TimeSpan timeSpan && TimeSpan.TryParse(filterValue, CultureInfo.CurrentCulture, out var timeFilter))
            {
                comparison = timeSpan.CompareTo(timeFilter);
                return true;
            }

            comparison = StringComparer.OrdinalIgnoreCase.Compare(displayValue ?? String.Empty, filterValue);
            return true;
        }

        private static int CompareValues(object left, object right, string leftDisplay, string rightDisplay)
        {
            var leftNull = IsNull(left);
            var rightNull = IsNull(right);

            if (leftNull || rightNull)
                return leftNull == rightNull ? 0 : leftNull ? 1 : -1;

            left = UnwrapSqlValue(left);
            right = UnwrapSqlValue(right);

            if (IsNumeric(left) && IsNumeric(right))
            {
                if (left.GetType() == right.GetType() && left is IComparable numericComparable)
                    return numericComparable.CompareTo(right);

                try
                {
                    return Convert.ToDecimal(left, CultureInfo.InvariantCulture).CompareTo(Convert.ToDecimal(right, CultureInfo.InvariantCulture));
                }
                catch (OverflowException)
                {
                    return Convert.ToDouble(left, CultureInfo.InvariantCulture).CompareTo(Convert.ToDouble(right, CultureInfo.InvariantCulture));
                }
            }

            if (left is DateTime leftDate && right is DateTime rightDate)
                return leftDate.CompareTo(rightDate);

            if (left is DateTimeOffset leftOffset && right is DateTimeOffset rightOffset)
                return leftOffset.CompareTo(rightOffset);

            if (left is string leftString && right is string rightString)
                return StringComparer.OrdinalIgnoreCase.Compare(leftString, rightString);

            if (left.GetType() == right.GetType() && left is IComparable comparable)
                return comparable.CompareTo(right);

            return StringComparer.OrdinalIgnoreCase.Compare(leftDisplay ?? String.Empty, rightDisplay ?? String.Empty);
        }

        private static bool IsNull(object value) =>
            value == null ||
            value == DBNull.Value ||
            (value is INullable nullable && nullable.IsNull);

        private static object UnwrapSqlValue(object value) => value is INullable && value.GetType().GetProperty("Value") != null
            ? value.GetType().GetProperty("Value").GetValue(value)
            : value;

        private static bool IsNumeric(object value)
        {
            if (value == null)
                return false;

            switch (Type.GetTypeCode(value.GetType()))
            {
                case TypeCode.Byte:
                case TypeCode.SByte:
                case TypeCode.UInt16:
                case TypeCode.UInt32:
                case TypeCode.UInt64:
                case TypeCode.Int16:
                case TypeCode.Int32:
                case TypeCode.Int64:
                case TypeCode.Decimal:
                case TypeCode.Double:
                case TypeCode.Single:
                    return true;
                default:
                    return false;
            }
        }

        private static bool TryParseDecimal(string value, out decimal result) =>
            Decimal.TryParse(value, NumberStyles.Number, CultureInfo.InvariantCulture, out result) ||
            Decimal.TryParse(value, NumberStyles.Number, CultureInfo.CurrentCulture, out result);

        private static bool TryParseDate(string value, out DateTimeOffset result) =>
            DateTimeOffset.TryParse(value, CultureInfo.CurrentCulture, DateTimeStyles.AllowWhiteSpaces, out result) ||
            DateTimeOffset.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.AllowWhiteSpaces, out result);

        private static bool Contains(string value, string search) =>
            (value ?? String.Empty).IndexOf(search ?? String.Empty, StringComparison.OrdinalIgnoreCase) >= 0;

        private sealed class IndexedRow
        {
            public IndexedRow(object[] values, int originalIndex)
            {
                Values = values;
                OriginalIndex = originalIndex;
            }

            public object[] Values { get; }

            public int OriginalIndex { get; }
        }
    }
}
