using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlTypes;
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
            // Check that we have at least one parameter that contains the SQL to execute
            var sproc = (ExecutableProcedureReference)Statement.ExecuteSpecification.ExecutableEntity;
            if (sproc.Parameters.Count == 0)
                throw new NotSupportedQueryFragmentException(Sql4CdsError.MissingParameter(sproc.ProcedureReference.ProcedureReference.Name, "@stmt", true));

            CheckUnicodeParameter(0, sproc, context, "@stmt");

            // A second parameter can contain the parameter definitions
            if (sproc.Parameters.Count > 1)
                CheckUnicodeParameter(1, sproc, context, "@params");

            // The list of other parameters is variable, and must be validated at runtime. We can validate that they must be named parameters,
            // but we can't validate their names or types until runtime.
            var paramNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            for (var i = 2; i < sproc.Parameters.Count; i++)
            {
                var param = sproc.Parameters[i];
                if (param.Variable == null)
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.NamedParametersRequiredAfter(param, 3));

                if (!paramNames.Add(param.Variable.Name))
                    throw new NotSupportedQueryFragmentException(Sql4CdsError.TooManyArguments(param, ""));
            }

            return new[] { this };
        }

        private void CheckUnicodeParameter(int parameterIndex, ExecutableProcedureReference sproc, NodeCompilationContext context, string expectedParameterName)
        {
            // Check the SQL to execute is a unicode string
            var value = sproc.Parameters[parameterIndex].ParameterValue;
            var ecc = new ExpressionCompilationContext(context, null, null);
            value.GetType(ecc, out var sqlType);

            if (!(sqlType is SqlDataTypeReference sqlDataType) ||
                (sqlDataType.SqlDataTypeOption != SqlDataTypeOption.NText && sqlDataType.SqlDataTypeOption != SqlDataTypeOption.NChar && sqlDataType.SqlDataTypeOption != SqlDataTypeOption.NVarChar))
                throw new NotSupportedQueryFragmentException(Sql4CdsError.InvalidProcedureParameterType(value, expectedParameterName, "ntext/nchar/nvarchar"));
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

        internal void SetupCommand(Sql4CdsCommand cmd, NodeExecutionContext context)
        {
            var ecc = new ExpressionCompilationContext(context, null, null);
            var eec = new ExpressionExecutionContext(context);
            var execSql = (SqlString)Statement.ExecuteSpecification.ExecutableEntity.Parameters[0].ParameterValue.Compile(ecc)(eec);
            cmd.CommandText = execSql.Value;

            // Parse the parameters
            if (Statement.ExecuteSpecification.ExecutableEntity.Parameters.Count > 1)
            {
                var paramParam = Statement.ExecuteSpecification.ExecutableEntity.Parameters[1];
                var paramSpec = (SqlString)paramParam.ParameterValue.Compile(ecc)(eec);

                if (!paramSpec.IsNull)
                {
                    // Create a CREATE PROC statement to parse the parameters
                    var createProcSql = $"CREATE PROC #temp {paramSpec.Value} AS SELECT 1";
                    var parser = new TSql160Parser(false);
                    var createProcFragment = parser.Parse(new System.IO.StringReader(createProcSql), out var errors);

                    if (errors.Count > 0)
                        throw new NotSupportedQueryFragmentException(Sql4CdsError.SyntaxError(paramParam));

                    if (!(createProcFragment is TSqlScript script) || script.Batches.Count != 1 || script.Batches[0].Statements.Count != 1 || !(script.Batches[0].Statements[0] is CreateProcedureStatement createProc))
                        throw new NotSupportedQueryFragmentException(Sql4CdsError.SyntaxError(paramParam));

                    // Check each parameter name is unique
                    var duplicateParam = createProc.Parameters
                        .GroupBy(p => p.VariableName.Value, StringComparer.OrdinalIgnoreCase)
                        .Where(g => g.Count() > 1)
                        .FirstOrDefault();

                    if (duplicateParam != null)
                        throw new NotSupportedQueryFragmentException(Sql4CdsError.DuplicateVariable(paramParam, duplicateParam.Key));

                    for (var i = 2; i < Statement.ExecuteSpecification.ExecutableEntity.Parameters.Count; i++)
                    {
                        var param = Statement.ExecuteSpecification.ExecutableEntity.Parameters[i];
                        var paramName = param.Variable.Name;

                        // Check the parameter is defined
                        var paramDef = createProc.Parameters.FirstOrDefault(p => p.VariableName.Value.Equals(paramName, StringComparison.OrdinalIgnoreCase));

                        if (paramDef == null)
                            throw new NotSupportedQueryFragmentException(Sql4CdsError.TooManyArguments(param, ""));

                        // Convert the parameter value to the correct type
                        var conversion = new ConvertCall
                        {
                            Parameter = param.ParameterValue,
                            DataType = paramDef.DataType
                        };

                        var paramValue = conversion.Compile(ecc)(eec);

                        var sqlParam = cmd.CreateParameter();
                        sqlParam.ParameterName = paramDef.VariableName.Value;
                        sqlParam.Value = paramValue;
                        cmd.Parameters.Add(sqlParam);
                    }

                    // Check all parameters are provided
                    var missingParam = createProc.Parameters
                        .FirstOrDefault(p => !cmd.Parameters.Cast<IDbDataParameter>().Any(sp => sp.ParameterName.Equals(p.VariableName.Value, StringComparison.OrdinalIgnoreCase)));

                    if (missingParam != null)
                        throw new NotSupportedQueryFragmentException(Sql4CdsError.ParameterizedQueryMissingParameter(Statement, $"({paramSpec.Value}){execSql.Value}", missingParam.VariableName.Value));
                }
            }
        }
    }
}
