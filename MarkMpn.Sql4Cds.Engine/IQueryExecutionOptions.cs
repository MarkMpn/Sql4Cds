using System;
using System.Collections.Generic;
using Microsoft.Xrm.Sdk.Metadata;
using Microsoft.Xrm.Sdk.Query;

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
        /// <param name="progress">The progress (0-1) to report back to the caller</param>
        /// <param name="message">The message to report back to the caller</param>
        void Progress(double? progress, string message);

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
        /// Checks if an INSERT query should be executed
        /// </summary>
        /// <param name="count">The number of records that the INSERT will apply to</param>
        /// <param name="meta">The metadata of the entity type that will be affected</param>
        /// <returns><c>true</c> if the entities should be inserted, or <c>false</c> otherwise</returns>
        bool ConfirmInsert(int count, EntityMetadata meta);

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
        bool UseTDSEndpoint { get; }

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

        /// <summary>
        /// Indicates if the server supports column comparison conditions in FetchXML
        /// </summary>
        bool ColumnComparisonAvailable { get; }

        /// <summary>
        /// Indicates if date/time values should be interpreted in the local timezone or in UTC
        /// </summary>
        bool UseLocalTimeZone { get; }

        /// <summary>
        /// Returns a list of join operators that are supported by the server
        /// </summary>
        List<JoinOperator> JoinOperatorsAvailable { get; }

        /// <summary>
        /// Indicates if plugins should be bypassed when executing DML operations
        /// </summary>
        bool BypassCustomPlugins { get; }

        /// <summary>
        /// A notification that the query is about to retrieve another page of data
        /// </summary>
        void RetrievingNextPage();

        /// <summary>
        /// Returns the name of the primary data source the query is being executed in
        /// </summary>
        string PrimaryDataSource { get; }

        /// <summary>
        /// Returns the unique identifier of the current user
        /// </summary>
        Guid UserId { get; }
    }
}
