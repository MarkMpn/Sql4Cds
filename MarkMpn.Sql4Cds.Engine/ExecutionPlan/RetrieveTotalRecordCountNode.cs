using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlTypes;
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
    class RetrieveTotalRecordCountNode : BaseDataNode
    {
        [Category("Data Source")]
        [Description("The data source this query is executed against")]
        public string DataSource { get; set; }

        /// <summary>
        /// The logical name of the entity to get the record count for
        /// </summary>
        [Category("Retrieve Total Record Count")]
        [Description("The logical name of the entity to get the record count for")]
        public string EntityName { get; set; }

        protected override IEnumerable<Entity> ExecuteInternal(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues)
        {

            if (!dataSources.TryGetValue(DataSource, out var dataSource))
                throw new QueryExecutionException("Missing datasource " + DataSource);

            var count = ((RetrieveTotalRecordCountResponse)dataSource.Connection.Execute(new RetrieveTotalRecordCountRequest { EntityNames = new[] { EntityName } })).EntityRecordCountCollection[EntityName];

            var resultEntity = new Entity(EntityName)
            {
                [$"{EntityName}_count"] = new SqlInt64(count)
            };

            yield return resultEntity;
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            return Array.Empty<IDataExecutionPlanNode>();
        }

        public override NodeSchema GetSchema(IDictionary<string, DataSource> dataSources, IDictionary<string, Type> parameterTypes)
        {
            return new NodeSchema
            {
                Schema =
                {
                    [$"{EntityName}_count"] = typeof(SqlInt64)
                },
                Aliases =
                {
                    [$"{EntityName}_count"] = new List<string> { $"{EntityName}_count" }
                }
            };
        }

        public override IDataExecutionPlanNode FoldQuery(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes)
        {
            return this;
        }

        public override void AddRequiredColumns(IDictionary<string, DataSource> dataSources, IDictionary<string, Type> parameterTypes, IList<string> requiredColumns)
        {
        }

        public override int EstimateRowsOut(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes)
        {
            return 1;
        }
    }
}
