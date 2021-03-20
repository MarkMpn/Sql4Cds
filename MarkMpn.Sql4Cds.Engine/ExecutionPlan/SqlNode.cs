using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Tooling.Connector;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    public class SqlNode : BaseNode, IDataSetExecutionPlanNode
    {
        private int _executionCount;
        private TimeSpan _duration;

        public override int ExecutionCount => _executionCount;

        public override TimeSpan Duration => _duration;

        public string Sql { get; set; }

        [Browsable(false)]
        public int Index { get; set; }

        [Browsable(false)]
        public int Length { get; set; }

        public override void AddRequiredColumns(IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes, IList<string> requiredColumns)
        {
        }

        public DataTable Execute(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues)
        {
            _executionCount++;
            var startTime = DateTime.Now;

            try
            {
                var svc = (CrmServiceClient) org;
                using (var con = new SqlConnection("server=" + svc.CrmConnectOrgUriActual.Host))
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

                        return result;
                    }
                }
            }
            finally
            {
                var endTime = DateTime.Now;
                _duration += (endTime - startTime);
            }
        }

        public IRootExecutionPlanNode FoldQuery(IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes)
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
