using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Metadata;
using Microsoft.Xrm.Sdk.Query;

namespace MarkMpn.Sql4Cds.Engine.Tests
{
    class OptionsWrapper : IQueryExecutionOptions
    {
        private readonly IQueryExecutionOptions _options;

        public OptionsWrapper(IQueryExecutionOptions options)
        {
            _options = options;

            CancellationToken = options.CancellationToken;
            BlockUpdateWithoutWhere = options.BlockUpdateWithoutWhere;
            BlockDeleteWithoutWhere = options.BlockDeleteWithoutWhere;
            UseBulkDelete = options.UseBulkDelete;
            BatchSize = options.BatchSize;
            UseTDSEndpoint = options.UseTDSEndpoint;
            MaxDegreeOfParallelism = options.MaxDegreeOfParallelism;
            UseLocalTimeZone = options.UseLocalTimeZone;
            BypassCustomPlugins = options.BypassCustomPlugins;
            PrimaryDataSource = options.PrimaryDataSource;
            UserId = options.UserId;
            QuotedIdentifiers = options.QuotedIdentifiers;
            ColumnOrdering = options.ColumnOrdering;
        }

        public CancellationToken CancellationToken { get; set; }

        public bool BlockUpdateWithoutWhere { get; set; }

        public bool BlockDeleteWithoutWhere { get; set; }

        public bool UseBulkDelete { get; set; }

        public int BatchSize { get; set; }

        public bool UseTDSEndpoint { get; set; }

        public bool UseRetrieveTotalRecordCount { get; set; }

        public int MaxDegreeOfParallelism { get; set; }

        public bool UseLocalTimeZone { get; set; }

        public bool BypassCustomPlugins { get; set; }

        public string PrimaryDataSource { get; set; }

        public Guid UserId { get; set; }

        public bool QuotedIdentifiers { get; set; }

        public ColumnOrdering ColumnOrdering { get; set; }

        public event EventHandler PrimaryDataSourceChanged
        {
            add
            {
                _options.PrimaryDataSourceChanged += value;
            }

            remove
            {
                _options.PrimaryDataSourceChanged -= value;
            }
        }

        public void ConfirmDelete(ConfirmDmlStatementEventArgs e)
        {
        }

        public void ConfirmInsert(ConfirmDmlStatementEventArgs e)
        {
        }

        public void ConfirmUpdate(ConfirmDmlStatementEventArgs e)
        {
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
