using Microsoft.SqlTools.ServiceLayer.QueryExecution.Contracts;

namespace MarkMpn.Sql4Cds.LanguageServer.QueryExecution.Contracts
{
    public class ResultSetSubset
    {
        /// <summary>
        /// The number of rows returned from result set, useful for determining if less rows were
        /// returned than requested.
        /// </summary>
        public int RowCount { get; set; }

        /// <summary>
        /// 2D array of the cell values requested from result set
        /// </summary>
        public DbCellValue[][] Rows { get; set; }
    }
}
