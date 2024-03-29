﻿using Microsoft.VisualStudio.LanguageServer.Protocol;

namespace MarkMpn.Sql4Cds.LanguageServer.QueryExecution.Contracts
{
    /// <summary>
    /// Parameters to be sent back as part of a batch start or complete event to indicate that a
    /// batch of a query started or completed.
    /// </summary>
    public class BatchEventParams
    {
        /// <summary>
        /// Summary of the batch that just completed
        /// </summary>
        public BatchSummary BatchSummary { get; set; }

        /// <summary>
        /// URI for the editor that owns the query
        /// </summary>
        public string OwnerUri { get; set; }
    }


    public class BatchCompleteEvent
    {
        public static readonly LspNotification<BatchEventParams> Type = new LspNotification<BatchEventParams>("query/batchComplete");
    }

    public class BatchStartEvent
    {
        public static readonly LspNotification<BatchEventParams> Type = new LspNotification<BatchEventParams>("query/batchStart");
    }
}
