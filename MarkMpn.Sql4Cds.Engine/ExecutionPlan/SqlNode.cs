using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
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
    /// <summary>
    /// Executes SQL using the TDS endpoint
    /// </summary>
    class SqlNode : BaseNode, IDataReaderExecutionPlanNode
    {
        private readonly Timer _timer = new Timer();
        private int _executionCount;

        public SqlNode() { }

        public override int ExecutionCount => _executionCount;

        public override TimeSpan Duration => _timer.Duration;

        [Category("Data Source")]
        [Description("The data source this query is executed against")]
        public string DataSource { get; set; }

        [Category("TDS Endpoint")]
        [Description("The SQL query to execute")]
        public string Sql { get; set; }

        [Browsable(false)]
        public int Index { get; set; }

        [Browsable(false)]
        public int Length { get; set; }

        [Browsable(false)]
        public HashSet<string> Parameters { get; private set; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        public override void AddRequiredColumns(IDictionary<string, DataSource> dataSources, IDictionary<string, DataTypeReference> parameterTypes, IList<string> requiredColumns)
        {
        }

        public IDataReader Execute(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, DataTypeReference> parameterTypes, IDictionary<string, object> parameterValues)
        {
            _executionCount++;

            using (_timer.Run())
            {
                try
                {
                    if (!dataSources.TryGetValue(DataSource, out var dataSource))
                        throw new QueryExecutionException("Missing datasource " + DataSource);

                    if (options.UseLocalTimeZone)
                        throw new QueryExecutionException("Cannot use automatic local time zone conversion with the TDS Endpoint");

#if NETCOREAPP
                    if (!(dataSource.Connection is ServiceClient svc))
                        throw new QueryExecutionException($"IOrganizationService implementation needs to be ServiceClient for use with the TDS Endpoint, got {dataSource.Connection.GetType()}");
#else
                    if (!(dataSource.Connection is CrmServiceClient svc))
                        throw new QueryExecutionException($"IOrganizationService implementation needs to be CrmServiceClient for use with the TDS Endpoint, got {dataSource.Connection.GetType()}");
#endif

                    if (svc.CallerId != Guid.Empty)
                        throw new QueryExecutionException("Cannot use impersonation with the TDS Endpoint");

                    if (String.IsNullOrEmpty(svc.CurrentAccessToken))
                        throw new QueryExecutionException("OAuth must be used to authenticate with the TDS Endpoint");

                    var con = TDSEndpoint.Connect(svc);

                    var cmd = con.CreateCommand();
                    cmd.CommandTimeout = (int)TimeSpan.FromMinutes(2).TotalSeconds;
                    cmd.CommandText = Sql;

                    foreach (var paramValue in parameterValues)
                    {
                        if (paramValue.Key.StartsWith("@@"))
                            continue;

                        if (!Parameters.Contains(paramValue.Key))
                            continue;

                        var param = cmd.CreateParameter();
                        param.ParameterName = paramValue.Key;

                        if (paramValue.Value is SqlEntityReference er)
                            param.Value = (SqlGuid)er;
                        else
                            param.Value = paramValue.Value;

                        cmd.Parameters.Add(param);
                    }

                    options.CancellationToken.Register(() => cmd.Cancel());
                    var reader = new SqlDataReaderWrapper(cmd.ExecuteReader(), cmd, con, Parent == null ? parameterValues : null);

                    if (Parent != null)
                        reader.ConvertToSqlTypes = true;

                    return reader;
                }
                catch (QueryExecutionException ex)
                {
                    if (ex.Node == null)
                        ex.Node = this;

                    throw;
                }
                catch (Exception ex)
                {
                    throw new QueryExecutionException(ex.Message, ex)
                    {
                        Node = this
                    };
                }
            }
        }

        public IRootExecutionPlanNodeInternal[] FoldQuery(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, DataTypeReference> parameterTypes, IList<OptimizerHint> hints)
        {
            return new[] { this };
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            return Array.Empty<IExecutionPlanNode>();
        }

        public override string ToString()
        {
            return "TDS Endpoint";
        }

        public object Clone()
        {
            return new SqlNode
            {
                DataSource = DataSource,
                Sql = Sql,
                Index = Index,
                Length = Length,
                Parameters = Parameters
            };
        }
    }
}
