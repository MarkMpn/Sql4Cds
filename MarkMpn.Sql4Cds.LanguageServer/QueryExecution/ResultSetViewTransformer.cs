using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Linq;
using System.Text;
using MarkMpn.Sql4Cds.Engine;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using MarkMpn.Sql4Cds.Export.Contracts;
using MarkMpn.Sql4Cds.LanguageServer.QueryExecution.Contracts;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.LanguageServer.QueryExecution
{
    /// <summary>
    /// Applies a results-grid view specification without changing the retained query results.
    /// Filters are converted to SQL comparisons in ScriptDom, then compiled and executed against
    /// the provider-specific (SQL typed) rows using the same logic as the engine's internal
    /// FilterNode and SortNode. Row indexes are retained as a final tie breaker so sorting is stable.
    /// </summary>
    internal static class ResultSetViewTransformer
    {
        internal static IReadOnlyList<object[]> Transform(
            IReadOnlyList<object[]> rows,
            IReadOnlyList<object[]> providerSpecificRows,
            DbColumnWrapper[] columns,
            SubsetParams request,
            SessionContext session,
            IQueryExecutionOptions options,
            Func<object, DbColumnWrapper, string> formatValue)
        {
            var indexedRows = rows.Select((row, index) => new IndexedRow(row, providerSpecificRows[index], index));

            if (!String.IsNullOrWhiteSpace(request.SearchText))
            {
                var searchText = request.SearchText.Trim();
                indexedRows = indexedRows.Where(row => Enumerable.Range(0, row.Values.Length).Any(columnIndex =>
                    Contains(formatValue(row.Values[columnIndex], columns[columnIndex]), searchText)));
            }

            var filterExpression = BuildFilterExpression(request.Filters, columns);

            if (filterExpression != null)
            {
                var compilationContext = CreateCompilationContext(columns, session, options);
                var filter = filterExpression.Compile(compilationContext);
                var executionContext = new ExpressionExecutionContext(compilationContext);

                indexedRows = indexedRows.Where(row =>
                {
                    executionContext.Entity = ToEntity(row.ProviderSpecificValues);
                    return filter(executionContext);
                });
            }

            var materialized = indexedRows.ToList();

            if (request.Sort != null &&
                request.Sort.ColumnIndex >= 0 &&
                request.Sort.ColumnIndex < columns.Length)
            {
                Sort(materialized, columns, request.Sort, session, options);
            }

            return materialized.Select(row => row.Values).ToArray();
        }

        private static void Sort(
            List<IndexedRow> rows,
            DbColumnWrapper[] columns,
            ResultSetSort sort,
            SessionContext session,
            IQueryExecutionOptions options)
        {
            // Simple case if there's no need to do any sorting
            if (rows.Count <= 1)
                return;

            // Build the same ScriptDom sort description as a SQL ORDER BY clause would produce
            // and compile it using the same logic as the internal SortNode
            var sortExpression = new ExpressionWithSortOrder
            {
                Expression = GetColumnName(sort.ColumnIndex).ToColumnReference(),
                SortOrder = sort.Direction == ResultSetSortDirection.Desc
                    ? SortOrder.Descending
                    : SortOrder.Ascending
            };

            var compilationContext = CreateCompilationContext(columns, session, options);
            var executionContext = new ExpressionExecutionContext(compilationContext);
            var expression = sortExpression.Expression.Compile(compilationContext);

            // Precalculate the sort keys, as SortNode.SortSubset does
            var sortKeys = rows
                .ToDictionary(
                    row => row,
                    row =>
                    {
                        executionContext.Entity = ToEntity(row.ProviderSpecificValues);
                        return expression(executionContext);
                    });

            // Sort the list according to these sort keys, retaining the original row index
            // as a final tie breaker so sorting is stable
            rows.Sort((x, y) =>
            {
                var comparison = ((IComparable)sortKeys[x]).CompareTo(sortKeys[y]);

                if (comparison == 0)
                    return x.OriginalIndex.CompareTo(y.OriginalIndex);

                if (sortExpression.SortOrder == SortOrder.Descending)
                    return -comparison;

                return comparison;
            });
        }

        private static ExpressionCompilationContext CreateCompilationContext(
            DbColumnWrapper[] columns,
            SessionContext session,
            IQueryExecutionOptions options)
        {
            var schema = new ColumnList();
            var dataSource = session.DataSources[options.PrimaryDataSource];

            for (var i = 0; i < columns.Length; i++)
            {
                var providerType = columns[i].ProviderSpecificDataType;

                var type = providerType != null
                    ? providerType.ToSqlType(dataSource)
                    : DataTypeHelpers.NVarChar(Int32.MaxValue, dataSource.DefaultCollation, CollationLabel.CoercibleDefault);

                schema.Add(GetColumnName(i), new Engine.ExecutionPlan.ColumnDefinition(type, isNullable: true, isCalculated: false));
            }

            return new ExpressionCompilationContext(session, options, null, new NodeSchema(schema, null, null, null), null);
        }

        private static Entity ToEntity(object[] providerSpecificValues)
        {
            var entity = new Entity();

            for (var i = 0; i < providerSpecificValues.Length; i++)
                entity[GetColumnName(i)] = providerSpecificValues[i];

            return entity;
        }

        private static string GetColumnName(int columnIndex) => $"col{columnIndex}";

        private static BooleanExpression BuildFilterExpression(ResultSetFilter[] filters, DbColumnWrapper[] columns)
        {
            if (filters == null)
                return null;

            BooleanExpression result = null;

            foreach (var filter in filters.Where(f => f != null && f.ColumnIndex >= 0 && f.ColumnIndex < columns.Length))
                result = result.And(BuildFilterExpression(filter));

            return result;
        }

        private static BooleanExpression BuildFilterExpression(ResultSetFilter filter)
        {
            var column = GetColumnName(filter.ColumnIndex).ToColumnReference();
            var value = new StringLiteral { Value = filter.Value ?? String.Empty };

            switch (filter.Operator)
            {
                case ResultSetFilterOperator.IsEmpty:
                    return IsEmptyExpression(column, isNot: false);

                case ResultSetFilterOperator.IsNotEmpty:
                    return IsEmptyExpression(column, isNot: true);

                case ResultSetFilterOperator.Contains:
                    return LikeExpression(column, "%" + EscapeLikePattern(filter.Value) + "%", notDefined: false);

                case ResultSetFilterOperator.NotContains:
                    return LikeExpression(column, "%" + EscapeLikePattern(filter.Value) + "%", notDefined: true);

                case ResultSetFilterOperator.StartsWith:
                    return LikeExpression(column, EscapeLikePattern(filter.Value) + "%", notDefined: false);

                case ResultSetFilterOperator.EndsWith:
                    return LikeExpression(column, "%" + EscapeLikePattern(filter.Value), notDefined: false);

                case ResultSetFilterOperator.Equals:
                    return ComparisonExpression(column, value, BooleanComparisonType.Equals);

                case ResultSetFilterOperator.NotEquals:
                    return ComparisonExpression(column, value, BooleanComparisonType.NotEqualToBrackets);

                case ResultSetFilterOperator.GreaterThan:
                    return ComparisonExpression(column, value, BooleanComparisonType.GreaterThan);

                case ResultSetFilterOperator.GreaterThanOrEqual:
                    return ComparisonExpression(column, value, BooleanComparisonType.GreaterThanOrEqualTo);

                case ResultSetFilterOperator.LessThan:
                    return ComparisonExpression(column, value, BooleanComparisonType.LessThan);

                case ResultSetFilterOperator.LessThanOrEqual:
                    return ComparisonExpression(column, value, BooleanComparisonType.LessThanOrEqualTo);

                default:
                    throw new ArgumentOutOfRangeException(nameof(filter), $"Unknown filter operator '{filter.Operator}'");
            }
        }

        private static BooleanExpression ComparisonExpression(ColumnReferenceExpression column, ScalarExpression value, BooleanComparisonType comparisonType)
        {
            return new BooleanComparisonExpression
            {
                FirstExpression = column,
                ComparisonType = comparisonType,
                SecondExpression = value
            };
        }

        private static BooleanExpression LikeExpression(ColumnReferenceExpression column, string pattern, bool notDefined)
        {
            // col [NOT] LIKE 'pattern' ESCAPE '\'
            var like = new LikePredicate
            {
                FirstExpression = new ConvertCall
                {
                    DataType = new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.NVarChar, Parameters = { new MaxLiteral() } },
                    Parameter = column
                },
                SecondExpression = new StringLiteral { Value = pattern },
                EscapeExpression = new StringLiteral { Value = "\\" },
                NotDefined = notDefined
            };

            if (!notDefined)
                return like;

            // NOT LIKE also excludes NULLs; the grid semantics expect NULL rows to be kept for
            // negated matches only when the display value genuinely doesn't contain the text, so
            // treat NULL as an empty string via ISNULL semantics: (col IS NULL OR col NOT LIKE ...)
            return new BooleanParenthesisExpression
            {
                Expression = new BooleanBinaryExpression
                {
                    FirstExpression = new BooleanIsNullExpression { Expression = column },
                    BinaryExpressionType = BooleanBinaryExpressionType.Or,
                    SecondExpression = like
                }
            };
        }

        private static BooleanExpression IsEmptyExpression(ColumnReferenceExpression column, bool isNot)
        {
            // col IS NULL OR CONVERT(nvarchar(max), col) = ''
            var isEmpty = new BooleanBinaryExpression
            {
                FirstExpression = new BooleanIsNullExpression { Expression = column },
                BinaryExpressionType = BooleanBinaryExpressionType.Or,
                SecondExpression = new BooleanComparisonExpression
                {
                    FirstExpression = new ConvertCall
                    {
                        DataType = new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.NVarChar, Parameters = { new MaxLiteral() } },
                        Parameter = column
                    },
                    ComparisonType = BooleanComparisonType.Equals,
                    SecondExpression = new StringLiteral { Value = "" }
                }
            };

            if (!isNot)
                return new BooleanParenthesisExpression { Expression = isEmpty };

            return new BooleanNotExpression
            {
                Expression = new BooleanParenthesisExpression { Expression = isEmpty }
            };
        }

        private static string EscapeLikePattern(string value)
        {
            if (String.IsNullOrEmpty(value))
                return String.Empty;

            var sb = new StringBuilder(value.Length);

            foreach (var ch in value)
            {
                if (ch == '%' || ch == '_' || ch == '[' || ch == ']' || ch == '\\')
                    sb.Append('\\');

                sb.Append(ch);
            }

            return sb.ToString();
        }

        private static bool Contains(string value, string search) =>
            (value ?? String.Empty).IndexOf(search ?? String.Empty, StringComparison.OrdinalIgnoreCase) >= 0;

        private sealed class IndexedRow
        {
            public IndexedRow(object[] values, object[] providerSpecificValues, int originalIndex)
            {
                Values = values;
                ProviderSpecificValues = providerSpecificValues;
                OriginalIndex = originalIndex;
            }

            public object[] Values { get; }

            public object[] ProviderSpecificValues { get; }

            public int OriginalIndex { get; }
        }
    }
}
