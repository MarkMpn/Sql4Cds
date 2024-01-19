using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlTypes;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// Throws an exception
    /// </summary>
    class RaiseErrorNode : BaseNode, IDmlQueryExecutionPlanNode
    {
        private int _executionCount;
        private readonly Timer _timer = new Timer();

        [Category("Raise Error")]
        [Description("The error message that is generated")]
        [DisplayName("Error Message")]
        public ScalarExpression ErrorMessage { get; set; }

        [Category("Raise Error")]
        [Description("The severity of the error")]
        public ScalarExpression Severity { get; set; }

        [Category("Raise Error")]
        [Description("The state associated with the error")]
        public ScalarExpression State { get; set; }

        [Category("Raise Error")]
        [Description("Parameters to use to construct the final error message")]
        public ScalarExpression[] Parameters { get; set; }

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
            return new RaiseErrorNode
            {
                ErrorMessage = ErrorMessage.Clone(),
                Severity = Severity.Clone(),
                State = State.Clone(),
                Parameters = Parameters.Select(p => p.Clone()).ToArray(),
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
            var ecc = new ExpressionCompilationContext(context, null, null);
            var eec = new ExpressionExecutionContext(context);

            var msg = Execute<SqlString>(ErrorMessage, ecc, eec, DataTypeHelpers.NVarChar(Int32.MaxValue, context.PrimaryDataSource.DefaultCollation, CollationLabel.CoercibleDefault));
            var severity = Execute<SqlInt32>(Severity, ecc, eec, DataTypeHelpers.Int);
            var state = Execute<SqlInt32>(State, ecc, eec, DataTypeHelpers.Int);

            if (severity.Value > 18)
                throw new QueryExecutionException(new Sql4CdsError(16, 2754, "Error severity levels greater than 18 can only be specified by members of the sysadmin role, using the WITH LOG option"));

            if (severity.Value < 0)
                severity = 0;

            if (state.Value > 255)
                state = 255;
            else if (state.Value < 0)
                state = 1;

            msg = ExpressionFunctions.FormatMessage(msg, eec, Parameters.Select(p => (INullable)p.Compile(ecc)(eec)).ToArray());

            context.Log(new Sql4CdsError((byte)severity.Value, -1, 50000, null, null, (byte)state.Value, msg.IsNull ? null : msg.Value));
            recordsAffected = 0;
            message = null;
        }

        private T GetValue<T>(ExpressionCompilationContext ecc, ExpressionExecutionContext eec, SqlDataTypeReference expectedType, ref int paramIndex)
        {
            if (paramIndex >= Parameters.Length)
                throw new QueryExecutionException(new Sql4CdsError(16, 2786, $"The data type of substitution parameter {paramIndex + 1} does not match the expected type of the format specification"));

            var param = Parameters[paramIndex];
            param.GetType(ecc, out var type);

            if (expectedType.SqlDataTypeOption.IsStringType() && (!(type is SqlDataTypeReference sqlType) || !sqlType.SqlDataTypeOption.IsStringType()) ||
                !expectedType.SqlDataTypeOption.IsStringType() && !expectedType.IsSameAs(type))
                throw new QueryExecutionException(new Sql4CdsError(16, 2786, $"The data type of substitution parameter {paramIndex + 1} does not match the expected type of the format specification"));

            var value = Execute<T>(param, ecc, eec, type);

            paramIndex++;
            return value;
        }

        private string GetParam(ExpressionCompilationContext ecc, ExpressionExecutionContext eec, ref int paramIndex)
        {
            if (paramIndex >= Parameters.Length)
                throw new QueryExecutionException(new Sql4CdsError(16, 2786, $"The data type of substitution parameter {paramIndex + 1} does not match the expected type of the format specification"));

            var param = Parameters[paramIndex];
            param.GetType(ecc, out var widthType);
            if (!(widthType is SqlDataTypeReference widthSqlType) || !widthSqlType.SqlDataTypeOption.IsExactNumeric())
                throw new QueryExecutionException(new Sql4CdsError(16, 2786, $"The data type of substitution parameter {paramIndex + 1} does not match the expected type of the format specification"));

            var value = Execute<SqlInt32>(param, ecc, eec, widthType);

            if (value.IsNull)
                throw new QueryExecutionException(new Sql4CdsError(16, 2786, $"The data type of substitution parameter {paramIndex + 1} does not match the expected type of the format specification"));

            paramIndex++;
            return value.Value.ToString();
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
