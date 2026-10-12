using System.Text.Json.Serialization;

namespace MarkMpn.Sql4Cds.LanguageServer.QueryExecution.Contracts
{
    /// <summary>
    /// Parameters for a query result subset retrieval request
    /// </summary>
    public class SubsetParams
    {
        /// <summary>
        /// URI for the file that owns the query to look up the results for
        /// </summary>
        public string OwnerUri { get; set; }

        /// <summary>
        /// Index of the batch to get the results from
        /// </summary>
        public int BatchIndex { get; set; }

        /// <summary>
        /// Index of the result set to get the results from
        /// </summary>
        public int ResultSetIndex { get; set; }

        /// <summary>
        /// Beginning index of the rows to return from the selected resultset. This index will be
        /// included in the results.
        /// </summary>
        public long RowsStartIndex { get; set; }

        /// <summary>
        /// Number of rows to include in the result of this request. If the number of the rows 
        /// exceeds the number of rows available after the start index, all available rows after
        /// the start index will be returned.
        /// </summary>
        public int RowsCount { get; set; }

        /// <summary>
        /// Optional text to find in any column. Matching is case-insensitive and is applied to
        /// the formatted values displayed by the client.
        /// </summary>
        public string SearchText { get; set; }

        /// <summary>
        /// Optional per-column filters. All filters are combined using AND.
        /// </summary>
        public ResultSetFilter[] Filters { get; set; }

        /// <summary>
        /// Optional single-column sort. Omit this value to restore the original result order.
        /// </summary>
        public ResultSetSort Sort { get; set; }

        /// <summary>
        /// Opaque version assigned by the client to this view specification. It is echoed in the
        /// response so clients can discard responses for superseded searches, filters or sorts.
        /// </summary>
        public long ViewVersion { get; set; }
    }

    /// <summary>
    /// The comparison to apply for a <see cref="ResultSetFilter"/>
    /// </summary>
    [JsonConverter(typeof(JsonStringEnumConverter))]
    public enum ResultSetFilterOperator
    {
        Contains,
        NotContains,
        Equals,
        NotEquals,
        StartsWith,
        EndsWith,
        IsEmpty,
        IsNotEmpty,
        GreaterThan,
        GreaterThanOrEqual,
        LessThan,
        LessThanOrEqual
    }

    public class ResultSetFilter
    {
        public int ColumnIndex { get; set; }

        public ResultSetFilterOperator Operator { get; set; }

        public string Value { get; set; }
    }

    /// <summary>
    /// The direction to sort a column in for a <see cref="ResultSetSort"/>
    /// </summary>
    [JsonConverter(typeof(JsonStringEnumConverter))]
    public enum ResultSetSortDirection
    {
        Asc,
        Desc
    }

    public class ResultSetSort
    {
        public int ColumnIndex { get; set; }

        /// <summary>
        /// The direction to sort in. Omit the entire sort object to use original result order.
        /// </summary>
        public ResultSetSortDirection Direction { get; set; }
    }
}
