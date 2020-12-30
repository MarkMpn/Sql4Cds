using System;
using System.Reflection;
using MarkMpn.Sql4Cds.Engine;
using Microsoft.Xrm.Sdk.Metadata;

namespace MarkMpn.Sql4Cds.SSMS
{
    internal class QueryExecutionOptions : IQueryExecutionOptions
    {
        private readonly object _sqlScriptEditorControl;
        private readonly object _sqlResultsControl;

        public QueryExecutionOptions(object sqlScriptEditorControl, object sqlResultsControl)
        {
            _sqlScriptEditorControl = sqlScriptEditorControl;
            _sqlResultsControl = sqlResultsControl;
        }

        public bool Cancelled { get; private set; }

        public bool BlockUpdateWithoutWhere => false;

        public bool BlockDeleteWithoutWhere => false;

        public bool UseBulkDelete => false;

        public int BatchSize => 1;

        public bool UseTDSEndpoint => true;

        public bool UseRetrieveTotalRecordCount => false;

        public int LocaleId => 1033;

        public int MaxDegreeOfParallelism => 1;

        public bool ConfirmDelete(int count, EntityMetadata meta)
        {
            return true;
        }

        public bool ConfirmUpdate(int count, EntityMetadata meta)
        {
            return true;
        }

        public bool ContinueRetrieve(int count)
        {
            return true;
        }

        public void Progress(double? progress, string message)
        {
            if (progress != null)
                _sqlResultsControl.InvokeMethod("OnQueryProgressUpdateEstimate", progress.Value);
        }

        public void Cancel()
        {
            _sqlScriptEditorControl.InvokeMethod("DoCancelExec");
            _sqlResultsControl.InvokeMethod("AddStringToErrors", "Query cancelled by user", true);
            Cancelled = true;
        }
    }
}