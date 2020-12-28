using MarkMpn.Sql4Cds.Engine;
using Microsoft.Xrm.Sdk.Metadata;

namespace MarkMpn.Sql4Cds.SSMS
{
    internal class QueryExecutionOptions : IQueryExecutionOptions
    {
        public bool Cancelled => false;

        public bool BlockUpdateWithoutWhere => false;

        public bool BlockDeleteWithoutWhere => false;

        public bool UseBulkDelete => false;

        public int BatchSize => 100;

        public bool UseTDSEndpoint => true;

        public bool UseRetrieveTotalRecordCount => false;

        public int LocaleId => 1033;

        public int MaxDegreeOfParallelism => 10;

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

        public void Progress(string message)
        {
        }
    }
}