﻿using Microsoft.VisualStudio.LanguageServer.Protocol;

namespace MarkMpn.Sql4Cds.LanguageServer.QueryExecution.Contracts
{
    public class QueryCompleteParams
    {
        /// <summary>
        /// URI for the editor that owns the query
        /// </summary>
        public string OwnerUri { get; set; }

        /// <summary>
        /// Summaries of the result sets that were returned with the query
        /// </summary>
        public BatchSummary[] BatchSummaries { get; set; }
    }

    public class QueryCompleteEvent
    {
        public static readonly LspNotification<QueryCompleteParams> Type = new LspNotification<QueryCompleteParams>("query/complete");
    }
}
