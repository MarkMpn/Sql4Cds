using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// A <see cref="IExecutionPlanNode"/> that produces a stream of data
    /// </summary>
    public interface IDataExecutionPlanNode : IExecutionPlanNode
    {
        /// <summary>
        /// Estimates the number of rows that will be returned by this node
        /// </summary>
        int EstimatedRowsOut { get; }

        /// <summary>
        /// Returns the total number of rows returned by this node
        /// </summary>
        int RowsOut { get; }
    }

    internal interface IDataExecutionPlanNodeInternal : IDataExecutionPlanNode, IExecutionPlanNodeInternal
    {
        /// <summary>
        /// Populates <see cref="IDataExecutionPlanNode.EstimatedRowsOut"/> with an estimate of the number of rows that will be returned by this node
        /// </summary>
        /// <param name="context">The context in which the node is being built</param>
        /// <returns>An estimate of how many rows will be returned by the node</returns>
        RowCountEstimate EstimateRowsOut(NodeCompilationContext context);

        /// <summary>
        /// Executes the execution plan
        /// </summary>
        /// <param name="context">The context in which the node is being executed</param>
        /// <returns>A sequence of entities matched by the query</returns>
        IEnumerable<Entity> Execute(NodeExecutionContext context);

        /// <summary>
        /// Attempts to fold the query operator down into its source
        /// </summary>
        /// <param name="context">The context in which the node is being built</param>
        /// <param name="hints">Any optimizer hints which may affect how the query is folded</param>
        /// <returns>The final execution plan node to execute</returns>
        IDataExecutionPlanNodeInternal FoldQuery(NodeCompilationContext context, IList<OptimizerHint> hints);

        /// <summary>
        /// Gets the schema of the dataset returned by the node
        /// </summary>
        /// <param name="context">The context in which the node is being built</param>
        /// <returns>The schema of data that will be produced by the node</returns>
        INodeSchema GetSchema(NodeCompilationContext context);

        /// <summary>
        /// Gets the variables that are in use by this node and optionally its sources
        /// </summary>
        /// <param name="recurse">Indicates if the returned list should include the variables used by the sources of this node</param>
        /// <returns>A sequence of variables names that are in use by this node</returns>
        IEnumerable<string> GetVariables(bool recurse);
    }
}
