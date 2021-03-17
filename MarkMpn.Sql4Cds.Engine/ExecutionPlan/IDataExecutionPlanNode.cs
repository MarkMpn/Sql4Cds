using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    public interface IDataExecutionPlanNode : IExecutionPlanNode
    {
        /// <summary>
        /// Executes the execution plan
        /// </summary>
        /// <param name="org">The <see cref="IOrganizationService"/> to use to execute the plan</param>
        /// <returns>A sequence of entities matched by the query</returns>
        IEnumerable<Entity> Execute(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues);

        /// <summary>
        /// Attempts to fold the query operator down into its source
        /// </summary>
        /// <returns>The final execution plan node to execute</returns>
        IDataExecutionPlanNode FoldQuery(IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes);

        /// <summary>
        /// Gets the schema of the dataset returned by the node
        /// </summary>
        /// <returns></returns>
        NodeSchema GetSchema(IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes);

        /// <summary>
        /// Estimates the number of rows that will be returned by this node
        /// </summary>
        int EstimateRowsOut(IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes, ITableSizeCache tableSize);

        /// <summary>
        /// Returns the total number of rows returned by this node
        /// </summary>
        int RowsOut { get; }
    }
}
