using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.Engine;
using Microsoft.Xrm.Sdk.Metadata;
using Microsoft.Xrm.Sdk.Query;

namespace MarkMpn.Sql4Cds.SSMS
{
    internal class QueryExecutionOptions : IQueryExecutionOptions
    {
        private readonly SqlScriptEditorControlWrapper _sqlScriptEditorControl;
        private readonly OptionsPage _options;

        public QueryExecutionOptions(SqlScriptEditorControlWrapper sqlScriptEditorControl, OptionsPage options, bool useTds)
        {
            _sqlScriptEditorControl = sqlScriptEditorControl;
            _options = options;
            UseTDSEndpoint = useTds;
        }

        public bool QuotedIdentifiers => _sqlScriptEditorControl.QuotedIdentifiers;

        public bool Cancelled { get; private set; }

        public bool BlockUpdateWithoutWhere => _options.BlockUpdateWithoutWhere;

        public bool BlockDeleteWithoutWhere => _options.BlockDeleteWithoutWhere;

        public bool UseBulkDelete => false;

        public int BatchSize => _options.BatchSize;

        public bool UseTDSEndpoint { get; }

        public bool UseRetrieveTotalRecordCount => false;

        public int MaxDegreeOfParallelism => _options.MaxDegreeOfParallelism;

        public bool ColumnComparisonAvailable => true;

        public bool UseLocalTimeZone => false;

        public List<JoinOperator> JoinOperatorsAvailable => new List<JoinOperator>();

        public bool BypassCustomPlugins => _options.BypassCustomPlugins;

        public string PrimaryDataSource => new SqlConnectionStringBuilder(_sqlScriptEditorControl.ConnectionString).DataSource.Split('.')[0];

        public Guid UserId => Guid.Empty;

        public bool ConfirmInsert(int count, EntityMetadata meta)
        {
            if (count == 1)
                _sqlScriptEditorControl.Results.AddStringToMessages($"Inserting 1 {meta.DisplayName?.UserLocalizedLabel?.Label ?? meta.LogicalName}...\r\n");
            else
                _sqlScriptEditorControl.Results.AddStringToMessages($"Inserting {count:N0} {meta.DisplayCollectionName?.UserLocalizedLabel?.Label ?? meta.LogicalCollectionName ?? meta.LogicalName}...\r\n");

            return true;
        }

        public bool ConfirmDelete(int count, EntityMetadata meta)
        {
            if (count == 1)
                _sqlScriptEditorControl.Results.AddStringToMessages($"Deleting 1 {meta.DisplayName?.UserLocalizedLabel?.Label ?? meta.LogicalName}...\r\n");
            else
                _sqlScriptEditorControl.Results.AddStringToMessages($"Deleting {count:N0} {meta.DisplayCollectionName?.UserLocalizedLabel?.Label ?? meta.LogicalCollectionName ?? meta.LogicalName}...\r\n");

            return true;
        }

        public bool ConfirmUpdate(int count, EntityMetadata meta)
        {
            if (count == 1)
                _sqlScriptEditorControl.Results.AddStringToMessages($"Updating 1 {meta.DisplayName?.UserLocalizedLabel?.Label ?? meta.LogicalName}...\r\n");
            else
                _sqlScriptEditorControl.Results.AddStringToMessages($"Updating {count:N0} {meta.DisplayCollectionName?.UserLocalizedLabel?.Label ?? meta.LogicalCollectionName ?? meta.LogicalName}...\r\n");

            return true;
        }

        public bool ContinueRetrieve(int count)
        {
            return true;
        }

        public void Progress(double? progress, string message)
        {
            if (progress != null)
                _sqlScriptEditorControl.Results.OnQueryProgressUpdateEstimate(progress.Value);
        }

        public Task Task { get; set; }

        public void Cancel()
        {
            _sqlScriptEditorControl.Cancelling();
            Task.ContinueWith(t => _sqlScriptEditorControl.DoCancelExec());
            Cancelled = true;
        }

        public void RetrievingNextPage()
        {
        }
    }
}