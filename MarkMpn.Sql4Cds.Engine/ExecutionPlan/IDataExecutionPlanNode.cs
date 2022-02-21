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
        void EstimateRowsOut(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, DataTypeReference> parameterTypes);

        /// <summary>
        /// Executes the execution plan
        /// </summary>
        /// <param name="org">The <see cref="IOrganizationService"/> to use to execute the plan</param>
        /// <returns>A sequence of entities matched by the query</returns>
        IEnumerable<Entity> Execute(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, DataTypeReference> parameterTypes, IDictionary<string, object> parameterValues);

        /// <summary>
        /// Attempts to fold the query operator down into its source
        /// </summary>
        /// <returns>The final execution plan node to execute</returns>
        IDataExecutionPlanNodeInternal FoldQuery(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, DataTypeReference> parameterTypes, IList<OptimizerHint> hints);

        /// <summary>
        /// Gets the schema of the dataset returned by the node
        /// </summary>
        /// <returns></returns>
        INodeSchema GetSchema(IDictionary<string, DataSource> dataSources, IDictionary<string, DataTypeReference> parameterTypes);
    }
}
