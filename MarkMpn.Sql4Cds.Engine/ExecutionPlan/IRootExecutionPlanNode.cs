using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// An execution plan node that can be at the root of a query
    /// </summary>
    public interface IRootExecutionPlanNode : IExecutionPlanNode
    {
        /// <summary>
        /// The SQL that the query was converted from
        /// </summary>
        string Sql { get; set; }

        /// <summary>
        /// The location within the full query text that the query was parsed from
        /// </summary>
        int Index { get; set; }

        /// <summary>
        /// The length of the query that was converted
        /// </summary>
        int Length { get; set; }
    }

    internal interface IRootExecutionPlanNodeInternal : IRootExecutionPlanNode, IExecutionPlanNodeInternal
    {
        /// <summary>
        /// Attempts to fold this node into its source to simplify the query
        /// </summary>
        /// <param name="context">The context in which the node is being built</param>
        /// <param name="hints">Any optimizer hints to apply</param>
        /// <returns>The node that should be used in place of this node</returns>
        IRootExecutionPlanNodeInternal[] FoldQuery(NodeCompilationContext context, IList<OptimizerHint> hints);
    }
}
