using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    abstract class BaseCursorNode : BaseNode, IRootExecutionPlanNodeInternal
    {
        private int _executionCount;
        private readonly Timer _timer = new Timer();

        [Browsable(false)]
        public string Sql { get; set; }

        [Browsable(false)]
        public int Index { get; set; }

        [Browsable(false)]
        public int Length { get; set; }

        [Browsable(false)]
        public int LineNumber { get; set; }

        public override TimeSpan Duration => _timer.Duration;

        public override int ExecutionCount => _executionCount;

        public string CursorName { get; set; }

        public override void AddRequiredColumns(NodeCompilationContext context, IList<string> requiredColumns)
        {
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            return Enumerable.Empty<IExecutionPlanNode>();
        }

        public virtual IRootExecutionPlanNodeInternal[] FoldQuery(NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            return new[] { this };
        }

        protected CursorDeclarationBaseNode GetCursor(NodeExecutionContext context)
        {
            if (!context.Cursors.TryGetValue(CursorName, out var cursor) &&
                !context.Session.Cursors.TryGetValue(CursorName, out cursor))
                throw new QueryExecutionException(Sql4CdsError.InvalidCursorName(CursorName));

            return cursor;
        }

        protected void Execute(Action action)
        {
            Execute<object>(() => { action(); return null; });
        }

        protected T Execute<T>(Func<T> action)
        {
            _executionCount++;

            using (_timer.Run())
            {
                try
                {
                    return action();
                }
                catch (QueryExecutionException ex)
                {
                    if (ex.Node == null)
                        ex.Node = this;

                    throw;
                }
                catch (Exception ex)
                {
                    throw new QueryExecutionException(Sql4CdsError.InternalError(ex.Message), ex) { Node = this };
                }
            }
        }

        public abstract object Clone();

        public override string ToString()
        {
            return base.ToString().ToUpperInvariant();
        }
    }
}
