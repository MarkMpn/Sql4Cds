using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Crm.Sdk.Messages;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// Gets the total number of records in an entity using <see cref="RetrieveTotalRecordCountRequest"/>
    /// </summary>
    public class RetrieveTotalRecordCountNode : BaseDataNode
    {
        /// <summary>
        /// The logical name of the entity to get the record count for
        /// </summary>
        public string EntityName { get; set; }

        protected override IEnumerable<Entity> ExecuteInternal(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues)
        {
            var count = ((RetrieveTotalRecordCountResponse)org.Execute(new RetrieveTotalRecordCountRequest { EntityNames = new[] { EntityName } })).EntityRecordCountCollection[EntityName];

            var resultEntity = new Entity(EntityName)
            {
                [$"{EntityName}_count"] = new AliasedValue(EntityName, $"{EntityName}_count", count)
            };

            yield return resultEntity;
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            return Array.Empty<IDataExecutionPlanNode>();
        }

        public override NodeSchema GetSchema(IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes)
        {
            return new NodeSchema
            {
                Schema =
                {
                    [$"{EntityName}_count"] = typeof(long)
                },
                Aliases =
                {
                    [$"{EntityName}_count"] = new List<string> { $"{EntityName}_count" }
                }
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
            return 1;
        }
    }
}
