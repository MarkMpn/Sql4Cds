using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;
#if NETCOREAPP
using Microsoft.PowerPlatform.Dataverse.Client;
#else
using Microsoft.Xrm.Tooling.Connector;
#endif

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    class RevertNode : BaseNode, IRootExecutionPlanNodeInternal, IImpersonateRevertExecutionPlanNode
    {
        private int _executionCount;
        private readonly Timer _timer = new Timer();

        /// <summary>
        /// The instance that this node will be executed against
        /// </summary>
        [Category("Data Source")]
        [Description("The data source this query is executed against")]
        public string DataSource { get; set; }

        /// <summary>
        /// The SQL string that the query was converted from
        /// </summary>
        [Browsable(false)]
        public string Sql { get; set; }

        /// <summary>
        /// The position of the SQL query within the entire query text
        /// </summary>
        [Browsable(false)]
        public int Index { get; set; }

        /// <summary>
        /// The length of the SQL query within the entire query text
        /// </summary>
        [Browsable(false)]
        public int Length { get; set; }

        public override int ExecutionCount => _executionCount;

        public override TimeSpan Duration => _timer.Duration;

        public override void AddRequiredColumns(IDictionary<string, DataSource> dataSources, IDictionary<string, DataTypeReference> parameterTypes, IList<string> requiredColumns)
        {
        }

        public string Execute(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, DataTypeReference> parameterTypes, IDictionary<string, object> parameterValues, out int recordsAffected)
        {
            _executionCount++;

            try
            {
                using (_timer.Run())
                {
                    if (!dataSources.TryGetValue(DataSource, out var dataSource))
                        throw new QueryExecutionException("Missing datasource " + DataSource);

#if NETCOREAPP
                    if (dataSource.Connection is ServiceClient svc)
                        svc.CallerId = Guid.Empty;
#else
                    if (dataSource.Connection is Microsoft.Xrm.Sdk.Client.OrganizationServiceProxy svcProxy)
                        svcProxy.CallerId = Guid.Empty;
                    else if (dataSource.Connection is Microsoft.Xrm.Sdk.WebServiceClient.OrganizationWebProxyClient webProxy)
                        webProxy.CallerId = Guid.Empty;
                    else if (dataSource.Connection is CrmServiceClient svc)
                        svc.CallerId = Guid.Empty;
#endif
                    else
                        throw new QueryExecutionException("Unexpected organization service type") { Node = this };

                    recordsAffected = -1;
                    return "Reverted impersonation";
                }
            }
            catch (QueryExecutionException ex)
            {
                if (ex.Node == null)
                    ex.Node = this;

                throw;
            }
            catch (Exception ex)
            {
                throw new QueryExecutionException(ex.Message, ex) { Node = this };
            }
        }

        public IRootExecutionPlanNodeInternal FoldQuery(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, DataTypeReference> parameterTypes, IList<OptimizerHint> hints)
        {
            return this;
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            return Array.Empty<IExecutionPlanNode>();
        }

        public override string ToString()
        {
            return "REVERT";
        }

        public object Clone()
        {
            return new RevertNode
            {
                DataSource = DataSource,
                Sql = Sql,
                Index = Index,
                Length = Length
            };
        }
    }
}
