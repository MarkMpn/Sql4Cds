using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// Returns a constant data set
    /// </summary>
    public class ConstantScanNode : BaseDataNode
    {
        /// <summary>
        /// The list of values to be returned
        /// </summary>
        [Browsable(false)]
        public List<Entity> Values { get; } = new List<Entity>();

        /// <summary>
        /// The alias for the data source
        /// </summary>
        public string Alias { get; set; }

        /// <summary>
        /// The types of values to be returned
        /// </summary>
        [Browsable(false)]
        public Dictionary<string, Type> Schema { get; } = new Dictionary<string, Type>();

        protected override IEnumerable<Entity> ExecuteInternal(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues)
        {
            foreach (var value in Values)
            {
                foreach (var col in Schema.Keys)
                    value[$"{Alias}.{col}"] = value[col];
                
                yield return value;
            }
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            return Array.Empty<IDataExecutionPlanNode>();
        }

        public override NodeSchema GetSchema(IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes)
        {
            return new NodeSchema
            {
                Schema = Schema.ToDictionary(kvp => $"{Alias}.{kvp.Key}", kvp => kvp.Value),
                Aliases = Schema.ToDictionary(kvp => kvp.Key, kvp => new List<string> { $"{Alias}.{kvp.Key}" })
            };
        }

        public override IDataExecutionPlanNode FoldQuery(IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes)
        {
            return this;
        }

        public override void AddRequiredColumns(IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes, IList<string> requiredColumns)
        {
        }

        public override int EstimateRowsOut(IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes, ITableSizeCache tableSize)
        {
            return Values.Count;
        }
    }
}
