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
    public class ConstantScanNode : BaseNode
    {
        /// <summary>
        /// The list of values to be returned
        /// </summary>
        public List<Entity> Values { get; } = new List<Entity>();

        /// <summary>
        /// The types of values to be returned
        /// </summary>
        public Dictionary<string, Type> Schema { get; } = new Dictionary<string, Type>();

        public override IEnumerable<Entity> Execute(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options)
        {
            foreach (var value in Values)
                yield return value;
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            return Array.Empty<IExecutionPlanNode>();
        }

        public override NodeSchema GetSchema(IAttributeMetadataCache metadata)
        {
            return new NodeSchema
            {
                Schema = Schema
            };
        }

        public override IEnumerable<string> GetRequiredColumns()
        {
            return Array.Empty<string>();
        }

        public override IExecutionPlanNode MergeNodeDown(IAttributeMetadataCache metadata, IQueryExecutionOptions options)
        {
            return this;
        }
    }
}
