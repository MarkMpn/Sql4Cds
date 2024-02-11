using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlTypes;
using System.Linq;
using System.Text;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    class GoToNode : BaseNode, IGoToNode
    {
        private int _executionCount;
        private readonly Timer _timer = new Timer();
        private Func<ExpressionExecutionContext, bool> _condition;

        public override int ExecutionCount => _executionCount;

        public override TimeSpan Duration => _timer.Duration;

        [Browsable(false)]
        public string Sql { get; set; }

        [Browsable(false)]
        public int Index { get; set; }

        [Browsable(false)]
        public int Length { get; set; }

        [Browsable(false)]
        public int LineNumber { get; set; }

        [Category("GoTo")]
        public string Label { get; set; }

        [Category("Conditional")]
        [Description("The condition that must be true for execution to continue")]
        public BooleanExpression Condition { get; set; }

        [Browsable(false)]
        public IDataExecutionPlanNodeInternal Source { get; set; }

        [Browsable(false)]
        public string SourceColumn { get; set; }

        [Browsable(false)]
        public TSqlStatement Statement { get; set; }

        public override void AddRequiredColumns(NodeCompilationContext context, IList<string> requiredColumns)
        {
            if (Source != null)
                Source.AddRequiredColumns(context, new List<string>(requiredColumns));
        }

        public object Clone()
        {
            return new GoToNode
            {
                _condition = _condition,
                Sql = Sql,
                Index = Index,
                Length = Length,
                LineNumber = LineNumber,
                Condition = Condition,
                Source = (IDataExecutionPlanNodeInternal)Source?.Clone(),
                SourceColumn = SourceColumn,
                Label = Label,
                Statement = Statement
            };
        }

        public string Execute(NodeExecutionContext context)
        {
            using (_timer.Run())
            {
                try
                {
                    _executionCount++;

                    bool result;

                    if (_condition != null)
                    {
                        result = _condition(new ExpressionExecutionContext(context));
                    }
                    else if (Source != null)
                    {
                        var record = Source.Execute(context).First();
                        result = ((SqlInt32)record[SourceColumn]).Value == 1;
                    }
                    else
                    {
                        result = true;
                    }

                    if (result)
                        return Label;

                    return null;
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
        }

        public IRootExecutionPlanNodeInternal[] FoldQuery(NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            if (Source != null)
                Source = Source.FoldQuery(context, hints);

            _condition = Condition?.Compile(new ExpressionCompilationContext(context, null, null));

            return new[] { this };
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            if (Source == null)
                return Array.Empty<IExecutionPlanNode>();

            return new[] { Source };
        }

        public override string ToString()
        {
            return "GOTO" + ((Source != null || Condition != null) ? " (Conditional)" : "");
        }
    }
}
