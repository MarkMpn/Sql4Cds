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
    public class RetrieveTotalRecordCountNode : BaseNode
    {
        /// <summary>
        /// The logical name of the entity to get the record count for
        /// </summary>
        public string EntityName { get; set; }

        public override IEnumerable<Entity> Execute(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string, object> parameterValues)
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
            return Array.Empty<IExecutionPlanNode>();
        }

        public override NodeSchema GetSchema(IAttributeMetadataCache metadata)
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
