using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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

        /// <summary>
        /// Attempts to fold this node into its source to simplify the query
        /// </summary>
        /// <param name="metadata">The <see cref="IAttributeMetadataCache"/> to use to get metadata</param>
        /// <param name="options"><see cref="IQueryExpressionVisitor"/> to indicate how the query can be executed</param>
        /// <param name="parameterTypes">A mapping of parameter names to their related types</param>
        /// <returns>The node that should be used in place of this node</returns>
        IRootExecutionPlanNode FoldQuery(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes);
    }
}
