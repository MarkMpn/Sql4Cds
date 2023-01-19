using System.Collections.Generic;
using Microsoft.SqlTools.ServiceLayer.ExecutionPlan.Contracts;
using Microsoft.VisualStudio.LanguageServer.Protocol;

namespace MarkMpn.Sql4Cds.LanguageServer.QueryExecution.Contracts
{
    /// <summary>
    /// Parameters to return when a result set is updated
    /// </summary>
    public class ResultSetUpdatedEventParams : ResultSetEventParams
    {
        /// <summary>
        /// Execution plans for statements in the current batch.
        /// </summary>
        public List<ExecutionPlanGraph> ExecutionPlans { get; set; }
        /// <summary>
        /// Error message for exception raised while generating execution plan.
        /// </summary>
        public string ExecutionPlanErrorMessage { get; set; }
    }

    public class ResultSetUpdatedEvent
    {
        public static string MethodName { get; } = "query/resultSetUpdated";

        public static readonly LspNotification<ResultSetUpdatedEventParams> Type = new LspNotification<ResultSetUpdatedEventParams>(MethodName);
    }
}
