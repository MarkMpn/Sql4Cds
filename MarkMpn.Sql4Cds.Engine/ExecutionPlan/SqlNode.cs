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
    class SqlNode : BaseNode, IDataSetExecutionPlanNode
    {
        private int _executionCount;
        private TimeSpan _duration;

        public override int ExecutionCount => _executionCount;

        public override TimeSpan Duration => _duration;

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

        public override void AddRequiredColumns(IDictionary<string, DataSource> dataSources, IDictionary<string, DataTypeReference> parameterTypes, IList<string> requiredColumns)
        {
        }

        public DataTable Execute(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, DataTypeReference> parameterTypes, IDictionary<string, object> parameterValues)
        {
            _executionCount++;
            var startTime = DateTime.Now;

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

#if NETCOREAPP
                using (var con = new SqlConnection("server=" + svc.ConnectedOrgUriActual.Host))
#else
                using (var con = new SqlConnection("server=" + svc.CrmConnectOrgUriActual.Host))
#endif
                {
                    con.AccessToken = svc.CurrentAccessToken;
                    con.Open();

                    using (var cmd = con.CreateCommand())
                    {
                        cmd.CommandTimeout = (int)TimeSpan.FromMinutes(2).TotalSeconds;
                        cmd.CommandText = Sql;
                        var result = new DataTable();

                        using (var adapter = new SqlDataAdapter(cmd))
                        {
                            adapter.Fill(result);
                        }

                        // SQL doesn't know the data type of NULL, so SELECT NULL will be returned with a schema type
                        // of SqlInt32. This causes problems trying to convert it to other types for updates/inserts,
                        // so change all-null columns to object
                        // https://github.com/MarkMpn/Sql4Cds/issues/122
                        var nullColumns = result.Columns
                            .Cast<DataColumn>()
                            .Select((col, colIndex) => result.Rows
                                .Cast<DataRow>()
                                .Select(row => DBNull.Value.Equals(row[colIndex]))
                                .All(isNull => isNull)
                                )
                            .ToArray();

                        var columnSqlTypes = result.Columns
                            .Cast<DataColumn>()
                            .Select((col, colIndex) => nullColumns[colIndex] ? typeof(object) : SqlTypeConverter.NetToSqlType(col.DataType))
                            .ToArray();
                        var columnNullValues = columnSqlTypes
                            .Select(type => SqlTypeConverter.GetNullValue(type))
                            .ToArray();

                        // Values will be stored as BCL types, convert them to SqlXxx types for consistency with IDataExecutionPlanNodes
                        var sqlTable = new DataTable();

                        for (var i = 0; i < result.Columns.Count; i++)
                            sqlTable.Columns.Add(result.Columns[i].ColumnName, columnSqlTypes[i]);

                        foreach (DataRow row in result.Rows)
                        {
                            var sqlRow = sqlTable.Rows.Add();

                            for (var i = 0; i < result.Columns.Count; i++)
                            {
                                var sqlValue = DBNull.Value.Equals(row[i]) ? columnNullValues[i] : SqlTypeConverter.NetToSqlType(DataSource, row[i]);
                                sqlRow[i] = sqlValue;
                            }
                        }

                        if (Parent == null)
                            parameterValues["@@ROWCOUNT"] = (SqlInt32)sqlTable.Rows.Count;

                        return sqlTable;
                    }
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
                throw new QueryExecutionException(ex.Message, ex)
                {
                    Node = this
                };
            }
            finally
            {
                var endTime = DateTime.Now;
                _duration += (endTime - startTime);
            }
        }

        public IRootExecutionPlanNode FoldQuery(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, DataTypeReference> parameterTypes, IList<OptimizerHint> hints)
        {
            return this;
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            return Array.Empty<IExecutionPlanNode>();
        }

        public override string ToString()
        {
            return "TDS Endpoint";
        }
    }
}
