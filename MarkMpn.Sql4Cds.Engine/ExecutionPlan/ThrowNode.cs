using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlTypes;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// Throws an exception
    /// </summary>
    class ThrowNode : BaseNode, IDmlQueryExecutionPlanNode
    {
        private int _executionCount;
        private readonly Timer _timer = new Timer();

        [Browsable(false)]
        public ThrowStatement Statement { get; set; }

        [Category("Throw")]
        [Description("The error message that is generated")]
        [DisplayName("Error Message")]
        public ValueExpression ErrorMessage { get; set; }

        [Category("Throw")]
        [Description("The state associated with the error")]
        public ValueExpression State { get; set; }

        [Category("Throw")]
        [Description("The error number that is generated")]
        [DisplayName("Error Number")]
        public ValueExpression ErrorNumber { get; set; }

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

        public object Clone()
        {
            return new ThrowNode
            {
                Statement = Statement,
                ErrorMessage = ErrorMessage.Clone(),
                State = State.Clone(),
                ErrorNumber = ErrorNumber.Clone(),
                Sql = Sql,
                Index = Index,
                Length = Length,
                LineNumber = LineNumber,
            };
        }

        public IDmlQueryExecutionPlanNode[] FoldQuery(NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            return new[] { this };
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            return Array.Empty<IExecutionPlanNode>();
        }

        public void Execute(NodeExecutionContext context, out int recordsAffected, out string message)
        {
            _executionCount++;

            if (ErrorNumber == null)
                context.Log(context.Error);

            var ecc = new ExpressionCompilationContext(context, null, null);
            var eec = new ExpressionExecutionContext(context);
            var num = Execute<SqlInt32>(ErrorNumber, ecc, eec, DataTypeHelpers.Int);
            var msg = Execute<SqlString>(ErrorMessage, ecc, eec, DataTypeHelpers.NVarChar(2048, context.PrimaryDataSource.DefaultCollation, CollationLabel.CoercibleDefault));
            var state = Execute<SqlByte>(State, ecc, eec, DataTypeHelpers.TinyInt);

            if (num.IsNull)
                num = 0;

            if (num < 50000)
            {
                msg = $"Error number {num} in the THROW statement is outside the valid range. Specify an error number in the valid range of 50000 to 2147483647";
                num = 35100;
            }

            if (state.IsNull)
                state = 0;

            context.Log(new Sql4CdsError(16, -1, num.Value, null, null, state.Value, msg.IsNull ? null : msg.Value));
            recordsAffected = 0;
            message = null;
        }

        private T Execute<T>(ScalarExpression expression, ExpressionCompilationContext ecc, ExpressionExecutionContext eec, DataTypeReference dataType)
        {
            expression.GetType(ecc, out var type);

            if (!type.IsSameAs(dataType))
                expression = new ConvertCall { Parameter = expression, DataType = dataType };

            return (T)expression.Compile(ecc)(eec);
        }

        IRootExecutionPlanNodeInternal[] IRootExecutionPlanNodeInternal.FoldQuery(NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            return new[] { this };
        }
    }
}
