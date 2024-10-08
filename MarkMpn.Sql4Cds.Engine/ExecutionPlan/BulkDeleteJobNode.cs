﻿using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Crm.Sdk.Messages;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Metadata;
using Microsoft.Xrm.Sdk.Query;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// Starts a bulk delete job
    /// </summary>
    class BulkDeleteJobNode : BaseNode, IDmlQueryExecutionPlanNode
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

        [Browsable(false)]
        public int LineNumber { get; set; }

        public override int ExecutionCount => _executionCount;

        public override TimeSpan Duration => _timer.Duration;

        [Browsable(false)]
        public FetchXmlScan Source { get; set; }

        public override void AddRequiredColumns(NodeCompilationContext context, IList<string> requiredColumns)
        {
        }

        public void Execute(NodeExecutionContext context, out int recordsAffected, out string message)
        {
            _executionCount++;

            try
            {
                if (!context.Session.DataSources.TryGetValue(DataSource, out var dataSource))
                    throw new QueryExecutionException("Missing datasource " + DataSource);

                Microsoft.Xrm.Sdk.Query.QueryExpression query;
                EntityMetadata meta;

                using (_timer.Run())
                {
                    Source.ApplyParameterValues(context);
                    query = ((FetchXmlToQueryExpressionResponse)dataSource.Connection.Execute(new FetchXmlToQueryExpressionRequest { FetchXml = Source.FetchXmlString })).Query;
                    meta = dataSource.Metadata[query.EntityName];
                }

                // Check again that the update is allowed. Don't count any UI interaction in the execution time
                var confirmArgs = new ConfirmDmlStatementEventArgs(Int32.MaxValue, meta, false);
                if (context.Options.CancellationToken.IsCancellationRequested)
                    confirmArgs.Cancel = true;
                context.Options.ConfirmDelete(confirmArgs);
                if (confirmArgs.Cancel)
                    throw new QueryExecutionException(new Sql4CdsError(11, 0, 0, null, null, 0, "DELETE cancelled by user", null));

                using (_timer.Run())
                {
                    var req = new BulkDeleteRequest
                    {
                        JobName = $"SQL 4 CDS {GetDisplayName(0, meta)} Bulk Delete Job",
                        QuerySet = new[] { query },
                        StartDateTime = DateTime.UtcNow,
                        RecurrencePattern = String.Empty,
                        SendEmailNotification = false,
                        ToRecipients = Array.Empty<Guid>(),
                        CCRecipients = Array.Empty<Guid>()
                    };

                    var resp = (BulkDeleteResponse)dataSource.Connection.Execute(req);

                    recordsAffected = 1;
                    message = "Bulk delete job started";
                    context.ParameterValues["@@IDENTITY"] = new SqlEntityReference(DataSource, "asyncoperation", resp.JobId);
                }
            }
            catch (QueryExecutionException ex)
            {
                if (ex.Node == null)
                    ex.Node = this;

                throw;
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new QueryExecutionException(ex.Message, ex) { Node = this };
            }
        }

        public IRootExecutionPlanNodeInternal[] FoldQuery(NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            return new[] { this };
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            return new[] { Source };
        }

        public override string ToString()
        {
            return "BULK DELETE";
        }

        public object Clone()
        {
            return new BulkDeleteJobNode
            {
                DataSource = DataSource,
                Sql = Sql,
                Index = Index,
                Length = Length,
                Source = Source,
                LineNumber = LineNumber,
            };
        }
    }
}
