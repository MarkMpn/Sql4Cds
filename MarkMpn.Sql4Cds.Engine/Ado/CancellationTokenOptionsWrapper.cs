using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using Microsoft.Xrm.Sdk.Metadata;
using Microsoft.Xrm.Sdk.Query;

namespace MarkMpn.Sql4Cds.Engine
{
    class CancellationTokenOptionsWrapper : IQueryExecutionOptions
    {
        private readonly IQueryExecutionOptions _options;

        public CancellationTokenOptionsWrapper(IQueryExecutionOptions options, CancellationTokenSource cts)
        {
            _options = options;
            CancellationTokenSource = cts;
        }

        public CancellationTokenSource CancellationTokenSource { get; }

        public CancellationToken CancellationToken => CancellationTokenSource.Token;

        public bool BlockUpdateWithoutWhere => _options.BlockUpdateWithoutWhere;

        public bool BlockDeleteWithoutWhere => _options.BlockDeleteWithoutWhere;

        public bool UseBulkDelete => _options.UseBulkDelete;

        public int BatchSize => _options.BatchSize;

        public bool UseTDSEndpoint => _options.UseTDSEndpoint;

        public bool UseRetrieveTotalRecordCount => _options.UseRetrieveTotalRecordCount;

        public int MaxDegreeOfParallelism => _options.MaxDegreeOfParallelism;

        public bool ColumnComparisonAvailable => _options.ColumnComparisonAvailable;

        public bool UseLocalTimeZone => _options.UseLocalTimeZone;

        public List<JoinOperator> JoinOperatorsAvailable => _options.JoinOperatorsAvailable;

        public bool BypassCustomPlugins => _options.BypassCustomPlugins;

        public string PrimaryDataSource => _options.PrimaryDataSource;

        public Guid UserId => _options.UserId;

        public bool QuotedIdentifiers => _options.QuotedIdentifiers;

        public bool ConfirmDelete(int count, EntityMetadata meta)
        {
            return _options.ConfirmDelete(count, meta);
        }

        public bool ConfirmInsert(int count, EntityMetadata meta)
        {
            return _options.ConfirmInsert(count, meta);
        }

        public bool ConfirmUpdate(int count, EntityMetadata meta)
        {
            return _options.ConfirmUpdate(count, meta);
        }

        public bool ContinueRetrieve(int count)
        {
            return _options.ContinueRetrieve(count);
        }

        public void Progress(double? progress, string message)
        {
            _options.Progress(progress, message);
        }
    }
}
