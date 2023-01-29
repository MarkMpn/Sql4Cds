using System;
using System.Collections.Generic;
#if System_Data_SqlClient
using System.Data.SqlClient;
#else
using Microsoft.Data.SqlClient;
#endif
using System.Threading;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.Engine;
using Microsoft.Xrm.Sdk.Metadata;
using Microsoft.Xrm.Sdk.Query;

namespace MarkMpn.Sql4Cds.SSMS
{
    class QueryExecutionOptions
    {
        private readonly SqlScriptEditorControlWrapper _sqlScriptEditorControl;
        private readonly OptionsPage _options;
        private readonly bool _useTds;
        private readonly Sql4CdsCommand _cmd;

        public QueryExecutionOptions(SqlScriptEditorControlWrapper sqlScriptEditorControl, OptionsPage options, bool useTds, Sql4CdsCommand cmd)
        {
            _sqlScriptEditorControl = sqlScriptEditorControl;
            _options = options;
            _useTds = useTds;
            _cmd = cmd;
        }

        public void ApplySettings(Sql4CdsConnection con)
        {
            con.QuotedIdentifiers = _sqlScriptEditorControl.QuotedIdentifiers;
            con.BlockUpdateWithoutWhere = _options.BlockUpdateWithoutWhere;
            con.BlockDeleteWithoutWhere = _options.BlockDeleteWithoutWhere;
            con.UseBulkDelete = false;
            con.BatchSize = _options.BatchSize;
            con.UseTDSEndpoint = _useTds;
            con.MaxDegreeOfParallelism = _options.MaxDegreeOfParallelism;
            con.UseLocalTimeZone = false;
            con.BypassCustomPlugins = _options.BypassCustomPlugins;

            con.PreInsert += ConfirmInsert;
            con.PreDelete += ConfirmDelete;
            con.PreUpdate += ConfirmUpdate;
            con.Progress += Progress;
        }

        private void ConfirmInsert(object sender, ConfirmDmlStatementEventArgs e)
        {
            ConfirmInsert(e.Count, e.Metadata);
        }

        private void ConfirmInsert(int count, EntityMetadata meta)
        {
            if (count == 1)
                _sqlScriptEditorControl.Results.AddStringToMessages($"Inserting 1 {meta.DisplayName?.UserLocalizedLabel?.Label ?? meta.LogicalName}...\r\n");
            else
                _sqlScriptEditorControl.Results.AddStringToMessages($"Inserting {count:N0} {meta.DisplayCollectionName?.UserLocalizedLabel?.Label ?? meta.LogicalCollectionName ?? meta.LogicalName}...\r\n");
        }

        private void ConfirmDelete(object sender, ConfirmDmlStatementEventArgs e)
        {
            ConfirmDelete(e.Count, e.Metadata);
        }

        private void ConfirmDelete(int count, EntityMetadata meta)
        {
            if (count == 1)
                _sqlScriptEditorControl.Results.AddStringToMessages($"Deleting 1 {meta.DisplayName?.UserLocalizedLabel?.Label ?? meta.LogicalName}...\r\n");
            else
                _sqlScriptEditorControl.Results.AddStringToMessages($"Deleting {count:N0} {meta.DisplayCollectionName?.UserLocalizedLabel?.Label ?? meta.LogicalCollectionName ?? meta.LogicalName}...\r\n");
        }

        private void ConfirmUpdate(object sender, ConfirmDmlStatementEventArgs e)
        {
            ConfirmUpdate(e.Count, e.Metadata);
        }

        private void ConfirmUpdate(int count, EntityMetadata meta)
        {
            if (count == 1)
                _sqlScriptEditorControl.Results.AddStringToMessages($"Updating 1 {meta.DisplayName?.UserLocalizedLabel?.Label ?? meta.LogicalName}...\r\n");
            else
                _sqlScriptEditorControl.Results.AddStringToMessages($"Updating {count:N0} {meta.DisplayCollectionName?.UserLocalizedLabel?.Label ?? meta.LogicalCollectionName ?? meta.LogicalName}...\r\n");
        }

        private void Progress(object sender, ProgressEventArgs e)
        {
            Progress(e.Progress, e.Message);
        }

        private void Progress(double? progress, string message)
        {
            if (progress != null)
                _sqlScriptEditorControl.Results.OnQueryProgressUpdateEstimate(progress.Value);
        }

        public Task Task { get; set; }

        public void Cancel()
        {
            IsCancelled = true;
            _sqlScriptEditorControl.Cancelling();
            _ = Task.ContinueWith(t => _sqlScriptEditorControl.DoCancelExec(), TaskScheduler.Default);
            _cmd.Cancel();
        }

        public bool IsCancelled { get; private set; }
    }
}