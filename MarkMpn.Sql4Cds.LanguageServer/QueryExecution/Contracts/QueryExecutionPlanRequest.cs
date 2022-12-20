using Microsoft.VisualStudio.LanguageServer.Protocol;

namespace MarkMpn.Sql4Cds.LanguageServer.QueryExecution.Contracts
{
    public class QueryExecutionPlanRequest
    {
        public const string MessageName = "query/executionPlan";

        public static readonly LspRequest<QueryExecutionPlanParams, QueryExecutionPlanResult> Type = new LspRequest<QueryExecutionPlanParams, QueryExecutionPlanResult>(MessageName);
    }
}
