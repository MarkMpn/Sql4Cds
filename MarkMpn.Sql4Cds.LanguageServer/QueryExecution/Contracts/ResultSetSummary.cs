using System.Collections.Generic;
using MarkMpn.Sql4Cds.Export.Contracts;

namespace MarkMpn.Sql4Cds.LanguageServer.QueryExecution.Contracts
{
    /// <summary>
    /// Represents a summary of information about a result without returning any cells of the results
    /// </summary>
    public class ResultSetSummary
    {
        /// <summary>
        /// The ID of the result set within the batch results
        /// </summary>
        public int Id { get; set; }

        /// <summary>
        /// The ID of the batch set within the query
        /// </summary>
        public int BatchId { get; set; }

        /// <summary>
        /// The number of rows that are available for the resultset thus far
        /// </summary>
        public long RowCount { get; set; }

        /// <summary>
        /// If true it indicates that all rows have been fetched and the RowCount being sent across is final for this ResultSet
        /// </summary>
        public bool Complete { get; set; }

        /// <summary>
        /// Details about the columns that are provided as solutions
        /// </summary>
        public DbColumnWrapper[] ColumnInfo { get; set; }

        /// <summary>
        /// The special action definition of the result set 
        /// </summary>
        public SpecialAction SpecialAction { get; set; }

        /// <summary>
        /// The visualization options for the client to render charts.
        /// </summary>
        public VisualizationOptions Visualization { get; set; }

        internal List<object[]> Values { get; } = new List<object[]>();

        /// <summary>
        /// The provider-specific (SQL type) values for each row, captured via
        /// <see cref="System.Data.Common.DbDataReader.GetProviderSpecificValues"/>. These are used
        /// when filtering/sorting the results so no CLR-to-SQL type conversion is required.
        /// </summary>
        internal List<object[]> ProviderSpecificValues { get; } = new List<object[]>();

        /// <summary>
        /// Returns a string represents the current object.
        /// </summary>
        public override string ToString() => $"Result Summary Id:{Id}, Batch Id:'{BatchId}', RowCount:'{RowCount}', Complete:'{Complete}', SpecialAction:'{SpecialAction}', Visualization:'{Visualization}'";
    }
}
