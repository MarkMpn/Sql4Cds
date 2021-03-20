using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    public interface IDataSetExecutionPlanNode : IRootExecutionPlanNode
    {
        /// <summary>
        /// Executes the execution plan
        /// </summary>
        /// <param name="org">The <see cref="IOrganizationService"/> to use to execute the plan</param>
        /// <returns>A status message for the results of the query</returns>
        DataTable Execute(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues);
    }
}
