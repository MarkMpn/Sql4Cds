using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// Returns a constant data set
    /// </summary>
    class ConstantScanNode : IExecutionPlanNode
    {
        /// <summary>
        /// The list of values to be returned
        /// </summary>
        public List<Entity> Values { get; } = new List<Entity>();

        public IEnumerable<Entity> Execute(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options)
        {
            foreach (var value in Values)
                yield return value;
        }
    }
}
