using System;
using System.Collections.Generic;
using System.ComponentModel;
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

        /// <summary>
        /// The number of the first line of the statement
        /// </summary>
        [Browsable(false)]
        public int LineNumber { get; set; }

        public override int ExecutionCount => _executionCount;

        public override TimeSpan Duration => _timer.Duration;

        public override void AddRequiredColumns(NodeCompilationContext context, IList<string> requiredColumns)
        {
        }

        public void Execute(NodeExecutionContext context, out int recordsAffected, out string message)
        {
            _executionCount++;

            try
            {
                using (_timer.Run())
                {
                    if (_timeExpr == null)
                        _timeExpr = Time.Compile(new ExpressionCompilationContext(context, null, null));

                    var time = (INullable) _timeExpr(new ExpressionExecutionContext(context));

                    if (time.IsNull)
                    {
                        ; // TODO ?
                    }
                    else
                    {
                        TimeSpan delay;

                        if (time is SqlInt32 sqlInt32)
                        {
                            delay = TimeSpan.FromSeconds(sqlInt32.Value);
                        }
                        else if (time is SqlInt16 sqlInt16)
                        {
                            delay = TimeSpan.FromSeconds(sqlInt16.Value);
                        }
                        else if (time is SqlDateTime sqlDateTime)
                        {
                            delay = sqlDateTime.Value.TimeOfDay;
                        }
                        else if (time is SqlDateTime2 sqlDateTime2)
                        {
                            delay = sqlDateTime2.Value.TimeOfDay;
                        }
                        else if (time is SqlDateTimeOffset sqlDateTimeOffset)
                        {
                            delay = sqlDateTimeOffset.Value.ToLocalTime().TimeOfDay;
                        }
                        else if (time is SqlString sqlString)
                        {
                            if (!TimeSpan.TryParseExact(sqlString.Value, new[] { @"hh\:mm", @"hh\:mm\:ss", @"hh\:mm\:ss.fff", @"hh\:mm.fff" }, null, out delay))
                                throw new QueryExecutionException(new Sql4CdsError(15, 148, $"Incorrect time syntax in time string '{sqlString.Value}' used with WAITFOR"));
                        }
                        else
                        {
                            Time.GetType(new ExpressionCompilationContext(context, null, null), out var type);
                            throw new QueryExecutionException(new Sql4CdsError(16, 9815, $"Waitfor delay and waitfor time cannot be of type {type.ToSql()}"));
                        }

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
                    message = null;
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
                Length = Length,
                LineNumber = LineNumber,
            };
        }
    }
}
