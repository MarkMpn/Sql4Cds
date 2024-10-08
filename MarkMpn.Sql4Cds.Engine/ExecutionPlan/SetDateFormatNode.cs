using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlTypes;
using System.Linq;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    internal class SetDateFormatNode : BaseNode, IDmlQueryExecutionPlanNode
    {
        private Func<ExpressionExecutionContext, object> _dateFormat;
        private int _executionCount;
        private readonly Timer _timer = new Timer();

        public SetDateFormatNode(ScalarExpression expression)
        {
            DateFormat = expression;
        }

        [Category("Settings")]
        [Description("The date format to use when parsing dates")]
        public ScalarExpression DateFormat { get; set; }

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

        public override void AddRequiredColumns(NodeCompilationContext context, IList<string> requiredColumns)
        {
        }

        public IRootExecutionPlanNodeInternal[] FoldQuery(NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            _dateFormat = DateFormat.Compile(new ExpressionCompilationContext(context, null, null));
            return new[] { this };
        }

        public void Execute(NodeExecutionContext context, out int recordsAffected, out string message)
        {
            _executionCount++;

            using (_timer.Run())
            {
                var formatString = ((SqlString)_dateFormat(new ExpressionExecutionContext(context))).Value;

                if (!Enum.TryParse<DateFormat>(formatString, out var dateFormat))
                    throw new QueryExecutionException(Sql4CdsError.InvalidDateFormat(DateFormat, formatString));

                context.Session.DateFormat = dateFormat;
            }

            recordsAffected = -1;
            message = null;
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            return Enumerable.Empty<IExecutionPlanNode>();
        }

        public object Clone()
        {
            return new SetDateFormatNode(DateFormat)
            {
                _dateFormat = _dateFormat,
            };
        }
    }
}
