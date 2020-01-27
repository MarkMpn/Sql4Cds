using Microsoft.Xrm.Sdk.Metadata;
using System;
using System.Collections.Generic;
using System.Text;

namespace MarkMpn.Sql4Cds.Engine
{
    public interface IQueryExecutionOptions
    {
        bool Cancelled { get; }

        void Progress(string message);

        bool ContinueRetrieve(int count);

        bool BlockUpdateWithoutWhere { get; }

        bool BlockDeleteWithoutWhere { get; }

        bool UseBulkDelete { get; }

        bool ConfirmUpdate(int count, EntityMetadata meta);

        bool ConfirmDelete(int count, EntityMetadata meta);

        int BatchSize { get; }
    }
}
