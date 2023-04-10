using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using Microsoft.Xrm.Sdk.Metadata;
using Microsoft.Xrm.Sdk.Query;

namespace MarkMpn.Sql4Cds.Engine
{
    class CancellationTokenOptionsWrapper : IQueryExecutionOptions, IDisposable
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

        public int MaxDegreeOfParallelism => _options.MaxDegreeOfParallelism;

        public bool ColumnComparisonAvailable => _options.ColumnComparisonAvailable;

        public bool UseLocalTimeZone => _options.UseLocalTimeZone;

        public List<JoinOperator> JoinOperatorsAvailable => _options.JoinOperatorsAvailable;

        public bool BypassCustomPlugins => _options.BypassCustomPlugins;

        public string PrimaryDataSource => _options.PrimaryDataSource;

        public Guid UserId => _options.UserId;

        public bool QuotedIdentifiers => _options.QuotedIdentifiers;

        public ColumnOrdering ColumnOrdering => _options.ColumnOrdering;

        public void ConfirmDelete(ConfirmDmlStatementEventArgs e)
        {
            _options.ConfirmDelete(e);
        }

        public void ConfirmInsert(ConfirmDmlStatementEventArgs e)
        {
            _options.ConfirmInsert(e);
        }

        public void ConfirmUpdate(ConfirmDmlStatementEventArgs e)
        {
            _options.ConfirmUpdate(e);
        }

        public bool ContinueRetrieve(int count)
        {
            return _options.ContinueRetrieve(count);
        }

        public void Progress(double? progress, string message)
        {
            _options.Progress(progress, message);
        }

        void IDisposable.Dispose()
        {
            CancellationTokenSource.Dispose();
        }
    }
}
