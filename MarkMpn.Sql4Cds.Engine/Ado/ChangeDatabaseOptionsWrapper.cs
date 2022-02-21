using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using Microsoft.Xrm.Sdk.Metadata;
using Microsoft.Xrm.Sdk.Query;

namespace MarkMpn.Sql4Cds.Engine
{
    class ChangeDatabaseOptionsWrapper : IQueryExecutionOptions
    {
        private readonly Sql4CdsConnection _connection;
        private readonly IQueryExecutionOptions _options;
        
        public ChangeDatabaseOptionsWrapper(Sql4CdsConnection connection, IQueryExecutionOptions options)
        {
            _connection = connection;
            _options = options;

            PrimaryDataSource = options.PrimaryDataSource;
            UseTDSEndpoint = options.UseTDSEndpoint;
            BlockUpdateWithoutWhere = options.BlockUpdateWithoutWhere;
            BlockDeleteWithoutWhere = options.BlockDeleteWithoutWhere;
            UseBulkDelete = options.UseBulkDelete;
            BatchSize = options.BatchSize;
            UseRetrieveTotalRecordCount = options.UseRetrieveTotalRecordCount;
            MaxDegreeOfParallelism = options.MaxDegreeOfParallelism;
            ColumnComparisonAvailable = options.ColumnComparisonAvailable;
            UseLocalTimeZone = options.UseLocalTimeZone;
            BypassCustomPlugins = options.BypassCustomPlugins;
            QuotedIdentifiers = options.QuotedIdentifiers;
        }

        public CancellationToken CancellationToken => _options.CancellationToken;

        public bool BlockUpdateWithoutWhere { get; set; }

        public bool BlockDeleteWithoutWhere { get; set; }

        public bool UseBulkDelete { get; set; }

        public int BatchSize { get; set; }

        public bool UseTDSEndpoint { get; set; }

        public bool UseRetrieveTotalRecordCount { get; set; }

        public int MaxDegreeOfParallelism { get; set; }

        public bool ColumnComparisonAvailable { get; }

        public bool UseLocalTimeZone { get; set; }

        public List<JoinOperator> JoinOperatorsAvailable => _options.JoinOperatorsAvailable;

        public bool BypassCustomPlugins { get; set; }

        public string PrimaryDataSource { get; set; } // TODO: Update UserId when changing data source

        public Guid UserId => _options.UserId;

        public bool QuotedIdentifiers { get; set; }

        public bool ConfirmDelete(int count, EntityMetadata meta)
        {
            var cancelled = !_options.ConfirmDelete(count, meta);

            if (!cancelled)
            {
                var args = new ConfirmDmlStatementEventArgs(count, meta);
                _connection.OnPreDelete(args);

                cancelled = args.Cancel;
            }

            return !cancelled;
        }

        public bool ConfirmInsert(int count, EntityMetadata meta)
        {
            var cancelled = !_options.ConfirmInsert(count, meta);

            if (!cancelled)
            {
                var args = new ConfirmDmlStatementEventArgs(count, meta);
                _connection.OnPreInsert(args);

                cancelled = args.Cancel;
            }

            return !cancelled;
        }

        public bool ConfirmUpdate(int count, EntityMetadata meta)
        {
            var cancelled = !_options.ConfirmUpdate(count, meta);

            if (!cancelled)
            {
                var args = new ConfirmDmlStatementEventArgs(count, meta);
                _connection.OnPreUpdate(args);

                cancelled = args.Cancel;
            }

            return !cancelled;
        }

        public bool ContinueRetrieve(int count)
        {
            var cancelled = !_options.ContinueRetrieve(count);

            if (!cancelled)
            {
                var args = new ConfirmRetrieveEventArgs(count);
                _connection.OnPreRetrieve(args);

                cancelled = args.Cancel;
            }

            return !cancelled;
        }

        public void Progress(double? progress, string message)
        {
            _options.Progress(progress, message);
            _connection.OnProgress(new ProgressEventArgs(progress, message));
        }
    }
}
