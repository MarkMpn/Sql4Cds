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
    class WaitForNode : BaseNode, IDmlQueryExecutionPlanNode
    {
        private int _executionCount;
        private Func<ExpressionExecutionContext, object> _timeExpr;
        private readonly Timer _timer = new Timer();

        [Category("Wait")]
        [Description("The time to wait for")]
        public ScalarExpression Time { get; set; }

        [Category("Wait")]
        [DisplayName("Wait Type")]
        [Description("Indicates if the wait is until a set time or for a duration")]
        public WaitForOption WaitType { get; set; }

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

        public override void AddRequiredColumns(NodeCompilationContext context, IList<string> requiredColumns)
        {
        }

        public void Execute(NodeExecutionContext context, out int recordsAffected)
        {
            _executionCount++;

            try
            {
                using (_timer.Run())
                {
                    if (_timeExpr == null)
                        _timeExpr = Time.Compile(new ExpressionCompilationContext(context, null, null));

                    var time = (SqlTime) _timeExpr(new ExpressionExecutionContext(context));

                    if (time.IsNull)
                    {
                        ; // TODO ?
                    }
                    else
                    {
                        var delay = time.Value;

                        if (WaitType == WaitForOption.Time)
                        {
                            if (delay > DateTime.Now.TimeOfDay)
                                delay = delay - DateTime.Now.TimeOfDay;
                            else
                                delay = delay + TimeSpan.FromDays(1) - DateTime.Now.TimeOfDay;
                        }

                        context.Options.Progress(null, $"Waiting for {delay}...");
                        context.Options.CancellationToken.WaitHandle.WaitOne(delay);
                    }

                    recordsAffected = -1;
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

        public IRootExecutionPlanNodeInternal[] FoldQuery(NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            return new[] { this };
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            return Array.Empty<IExecutionPlanNode>();
        }

        public override string ToString()
        {
            return "WAITFOR";
        }

        public object Clone()
        {
            return new WaitForNode
            {
                Time = Time,
                _timeExpr = _timeExpr,
                WaitType = WaitType,
                Sql = Sql,
                Index = Index,
                Length = Length
            };
        }
    }
}
