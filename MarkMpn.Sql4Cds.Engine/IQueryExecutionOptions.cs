using Microsoft.Xrm.Sdk.Metadata;

namespace MarkMpn.Sql4Cds.Engine
{
    /// <summary>
    /// The options to apply to control the execution of a query
    /// </summary>
    public interface IQueryExecutionOptions
    {
        /// <summary>
        /// Indicates that the query should be cancelled
        /// </summary>
        bool Cancelled { get; }

        /// <summary>
        /// Allows the query execution to report progress
        /// </summary>
        /// <param name="message">The message to report back to the caller</param>
        void Progress(string message);

        /// <summary>
        /// Checks if the query should continue to retrieve more records
        /// </summary>
        /// <param name="count">The number of records retrieved so far</param>
        /// <returns><c>true</c> if the query should continue retrieving more records, or <c>false</c> otherwise</returns>
        bool ContinueRetrieve(int count);

        /// <summary>
        /// Indicates if an UPDATE statement cannot be executed unless it has a WHERE clause
        /// </summary>
        bool BlockUpdateWithoutWhere { get; }

        /// <summary>
        /// Indicates if a DELETE statement cannot be execyted unless it has a WHERE clause
        /// </summary>
        bool BlockDeleteWithoutWhere { get; }

        /// <summary>
        /// Indicates if DELETE queries should be executed with a bulk delete job
        /// </summary>
        bool UseBulkDelete { get; }

        /// <summary>
        /// Checks if an UPDATE query should be executed
        /// </summary>
        /// <param name="count">The number of records that the UPDATE will apply to</param>
        /// <param name="meta">The metadata of the entity type that will be affected</param>
        /// <returns><c>true</c> if the entities should be updated, or <c>false</c> otherwise</returns>
        bool ConfirmUpdate(int count, EntityMetadata meta);

        /// <summary>
        /// Checks if a DELETE query should be executed
        /// </summary>
        /// <param name="count">The number of records that the DELETE will apply to</param>
        /// <param name="meta">The metadata of the entity type that will be affected</param>
        /// <returns><c>true</c> if the entities should be deleted, or <c>false</c> otherwise</returns>
        bool ConfirmDelete(int count, EntityMetadata meta);

        /// <summary>
        /// The number of records that should be inserted, updated or deleted in a single batch
        /// </summary>
        int BatchSize { get; }

        /// <summary>
        /// Indicates if the TDS Endpoint should be used for query execution where possible
        /// </summary>
        bool UseTSQLEndpoint { get; }

        /// <summary>
        /// Indicates if a <see cref="Microsoft.Crm.Sdk.Messages.RetrieveTotalRecordCountRequest"/> should be used for simple SELECT count(*) FROM table queries
        /// </summary>
        bool UseRetrieveTotalRecordCount { get; }

        /// <summary>
        /// The language code to retrieve results in
        /// </summary>
        int LocaleId { get; }

        /// <summary>
        /// The maximum number of worker threads to use to execute DML queries
        /// </summary>
        int MaxDegreeOfParallelism { get; }
    }
}
