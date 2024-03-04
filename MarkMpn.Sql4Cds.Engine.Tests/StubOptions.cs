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
    class StubOptions : IQueryExecutionOptions
    {
        CancellationToken IQueryExecutionOptions.CancellationToken => CancellationToken.None;

        bool IQueryExecutionOptions.BlockUpdateWithoutWhere => false;

        bool IQueryExecutionOptions.BlockDeleteWithoutWhere => false;

        bool IQueryExecutionOptions.UseBulkDelete => false;

        int IQueryExecutionOptions.BatchSize => 1;

        bool IQueryExecutionOptions.UseTDSEndpoint => false;

        int IQueryExecutionOptions.MaxDegreeOfParallelism => 10;

        bool IQueryExecutionOptions.ColumnComparisonAvailable => true;

        bool IQueryExecutionOptions.OrderByEntityNameAvailable => false;

        bool IQueryExecutionOptions.UseLocalTimeZone => false;

        List<JoinOperator> IQueryExecutionOptions.JoinOperatorsAvailable => new List<JoinOperator> { JoinOperator.Inner, JoinOperator.LeftOuter };

        bool IQueryExecutionOptions.BypassCustomPlugins => false;

        ColumnOrdering IQueryExecutionOptions.ColumnOrdering => ColumnOrdering.Alphabetical;

        void IQueryExecutionOptions.ConfirmInsert(ConfirmDmlStatementEventArgs e)
        {
        }

        void IQueryExecutionOptions.ConfirmDelete(ConfirmDmlStatementEventArgs e)
        {
        }

        void IQueryExecutionOptions.ConfirmUpdate(ConfirmDmlStatementEventArgs e)
        {
        }

        bool IQueryExecutionOptions.ContinueRetrieve(int count)
        {
            return true;
        }

        void IQueryExecutionOptions.Progress(double? progress, string message)
        {
        }

        string IQueryExecutionOptions.PrimaryDataSource => "local";

        Guid IQueryExecutionOptions.UserId => Guid.NewGuid();

        bool IQueryExecutionOptions.QuotedIdentifiers => true;
    }
}
