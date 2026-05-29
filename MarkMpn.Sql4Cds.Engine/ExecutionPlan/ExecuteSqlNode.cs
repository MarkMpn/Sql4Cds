using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    internal class ExecuteSqlNode : IRootExecutionPlanNodeInternal
    {
        public ExecuteStatement Statement { get; set; }

        public string Sql { get; set; }
        
        public int Index { get; set; }
        
        public int Length { get; set; }
        
        public int LineNumber { get; set; }
        
        public IExecutionPlanNode Parent { get; set; }

        public int ExecutionCount => 0;

        public TimeSpan Duration => TimeSpan.Zero;

        public void AddRequiredColumns(NodeCompilationContext context, IList<string> requiredColumns)
        {
        }

        public void FinishedFolding(NodeCompilationContext context)
        {
        }

        public IRootExecutionPlanNodeInternal[] FoldQuery(NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            // Check that we have at least one parameter
            var sproc = (ExecutableProcedureReference)Statement.ExecuteSpecification.ExecutableEntity;
            if (sproc.Parameters.Count == 0)
                throw new NotSupportedQueryFragmentException(Sql4CdsError.MissingParameter(sproc.ProcedureReference.ProcedureReference.Name, "@statement", true));

            // Check the SQL to execute is a unicode string
            var sql = sproc.Parameters[0].ParameterValue;
            var ecc = new ExpressionCompilationContext(context, null, null);
            sql.GetType(ecc, out var sqlType);

            if (!(sqlType is SqlDataTypeReference sqlDataType) ||
                (sqlDataType.SqlDataTypeOption != SqlDataTypeOption.NText && sqlDataType.SqlDataTypeOption != SqlDataTypeOption.NChar && sqlDataType.SqlDataTypeOption != SqlDataTypeOption.NVarChar))
                throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidProcedureParameterType(sql, "@statement", "ntext/nchar/nvarchar"));

            return new[] { this };
        }

        public IEnumerable<IExecutionPlanNode> GetSources()
        {
            return Enumerable.Empty<IExecutionPlanNode>();
        }

        public object Clone()
        {
            return new ExecuteSqlNode
            {
                Statement = Statement.Clone(),
                Sql = Sql,
                Index = Index,
                Length = Length,
                LineNumber = LineNumber
            };
        }
    }
}
