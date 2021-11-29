using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Crm.Sdk.Messages;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// Starts a bulk delete job
    /// </summary>
    class BulkDeleteJobNode : BaseNode, IDmlQueryExecutionPlanNode, IFetchXmlExecutionPlanNode
    {
        private int _executionCount;
        private readonly Timer _timer = new Timer();

        [Category("Data Source")]
        [Description("The data source this query is executed against")]
        public string DataSource { get; set; }

        [Browsable(false)]
        public string Sql { get; set; }

        [Browsable(false)]
        public int Index { get; set; }

        [Browsable(false)]
        public int Length { get; set; }

        public override int ExecutionCount => _executionCount;

        public override TimeSpan Duration => _timer.Duration;

        /// <summary>
        /// The query that identifies the records to delete
        /// </summary>
        [Category("Bulk Delete")]
        [Description("The FetchXML query that identifies the records to delete")]
        [DisplayName("FetchXML")]
        public string FetchXmlString { get; set; }

        public override void AddRequiredColumns(IDictionary<string, DataSource> dataSources, IDictionary<string, Type> parameterTypes, IList<string> requiredColumns)
        {
        }

        public string Execute(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues)
        {
            _executionCount++;

            try
            {
                if (!dataSources.TryGetValue(DataSource, out var dataSource))
                    throw new QueryExecutionException("Missing datasource " + DataSource);

                using (_timer.Run())
                {
                    var query = ((FetchXmlToQueryExpressionResponse)dataSource.Connection.Execute(new FetchXmlToQueryExpressionRequest { FetchXml = FetchXmlString })).Query;
                    var meta = dataSource.Metadata[query.EntityName];

                    var req = new BulkDeleteRequest
                    {
                        JobName = $"SQL 4 CDS {GetDisplayName(0, meta)} Bulk Delete Job",
                        QuerySet = new[] { query },
                        StartDateTime = DateTime.Now,
                        RecurrencePattern = String.Empty,
                        SendEmailNotification = false,
                        ToRecipients = Array.Empty<Guid>(),
                        CCRecipients = Array.Empty<Guid>()
                    };

                    var resp = (BulkDeleteResponse)dataSource.Connection.Execute(req);

                    return $"Bulk delete job started: {resp.JobId}";
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

        public IRootExecutionPlanNode FoldQuery(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes)
        {
            return this;
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            return Array.Empty<IExecutionPlanNode>();
        }

        public override string ToString()
        {
            return "BULK DELETE";
        }
    }
}
