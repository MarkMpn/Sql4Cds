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
            UseRetrieveTotalRecordCount = options.UseRetrieveTotalRecordCount;
            MaxDegreeOfParallelism = options.MaxDegreeOfParallelism;
            ColumnComparisonAvailable = options.ColumnComparisonAvailable;
            UseLocalTimeZone = options.UseLocalTimeZone;
            JoinOperatorsAvailable = new List<JoinOperator>(options.JoinOperatorsAvailable);
            BypassCustomPlugins = options.BypassCustomPlugins;
            PrimaryDataSource = options.PrimaryDataSource;
            UserId = options.UserId;
            QuotedIdentifiers = options.QuotedIdentifiers;
        }

        public CancellationToken CancellationToken { get; set; }

        public bool BlockUpdateWithoutWhere { get; set; }

        public bool BlockDeleteWithoutWhere { get; set; }

        public bool UseBulkDelete { get; set; }

        public int BatchSize { get; set; }

        public bool UseTDSEndpoint { get; set; }

        public bool UseRetrieveTotalRecordCount { get; set; }

        public int MaxDegreeOfParallelism { get; set; }

        public bool ColumnComparisonAvailable { get; set; }

        public bool UseLocalTimeZone { get; set; }

        public List<JoinOperator> JoinOperatorsAvailable { get; set; }

        public bool BypassCustomPlugins { get; set; }

        public string PrimaryDataSource { get; set; }

        public Guid UserId { get; set; }

        public bool QuotedIdentifiers { get; set; }

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
