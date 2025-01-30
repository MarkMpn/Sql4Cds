using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlTypes;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    class AssignVariablesNode : BaseDmlNode
    {
        private int _executionCount;
        private readonly Timer _timer = new Timer();

        [Browsable(false)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override string DataSource { get => base.DataSource; set => base.DataSource = value; }

        [Category("Assign Variables")]
        public List<VariableAssignment> Variables { get; } = new List<VariableAssignment>();

        public override int ExecutionCount => _executionCount;

        public override TimeSpan Duration => _timer.Duration;

        [Browsable(false)]
        public override int MaxDOP { get; set; }

        [Browsable(false)]
        public override int BatchSize { get; set; }

        [Browsable(false)]
        public override bool BypassCustomPluginExecution { get; set; }

        [Browsable(false)]
        public override bool ContinueOnError { get; set; }

        public override void Execute(NodeExecutionContext context, out int recordsAffected, out string message)
        {
            _executionCount++;

            using (_timer.Run())
            {
                var count = 0;

                var entities = GetDmlSourceEntities(context, out var schema);
                var valueAccessors = CompileValueAccessors(schema, entities, context.ParameterTypes);
                var eec = new ExpressionExecutionContext(context);

                foreach (var entity in entities)
                {
                    eec.Entity = entity;

                    foreach (var variable in Variables)
                        context.ParameterValues[variable.VariableName] = (INullable)valueAccessors[variable.VariableName](eec);

                    count++;
                }

                context.ParameterValues["@@ROWCOUNT"] = (SqlInt32)count;
            }

            recordsAffected = -1;
            message = null;
        }

        /// <summary>
        /// Compiles methods to access the data required for the DML operation
        /// </summary>
        /// <param name="mappings">The mappings of attribute name to source column</param>
        /// <param name="schema">The schema of data source</param>
        /// <param name="attributes">The attributes in the target metadata</param>
        /// <param name="dateTimeKind">The time zone that datetime values are supplied in</param>
        /// <returns></returns>
        protected Dictionary<string, Func<ExpressionExecutionContext, object>> CompileValueAccessors(INodeSchema schema, List<Entity> entities, IDictionary<string, DataTypeReference> variableTypes)
        {
            var valueAccessors = new Dictionary<string, Func<ExpressionExecutionContext, object>>();
            var contextParam = Expression.Parameter(typeof(ExpressionExecutionContext));

            foreach (var mapping in Variables)
            {
                var sourceColumnName = mapping.SourceColumn;
                var destVariableName = mapping.VariableName;

                if (!schema.ContainsColumn(sourceColumnName, out sourceColumnName))
                    throw new QueryExecutionException($"Missing source column {mapping.SourceColumn}") { Node = this };

                var sourceSqlType = schema.Schema[sourceColumnName].Type;

                if (!variableTypes.TryGetValue(destVariableName, out var destSqlType))
                    throw new QueryExecutionException($"Unknown variable {mapping.VariableName}") { Node = this };

                var destNetType = destSqlType.ToNetType(out _);

                var entity = Expression.Property(contextParam, nameof(ExpressionExecutionContext.Entity));
                var expr = (Expression)Expression.Property(entity, typeof(Entity).GetCustomAttribute<DefaultMemberAttribute>().MemberName, Expression.Constant(sourceColumnName));
                var originalExpr = expr;

                if (sourceSqlType.IsSameAs(DataTypeHelpers.Int) && !SqlTypeConverter.CanChangeTypeExplicit(sourceSqlType, destSqlType) && entities.All(e => ((SqlInt32)e[sourceColumnName]).IsNull))
                {
                    // null literal is typed as int
                    expr = Expression.Constant(SqlTypeConverter.GetNullValue(destNetType));
                }
                else
                {
                    // Unbox value as source SQL type
                    expr = Expression.Convert(expr, sourceSqlType.ToNetType(out _));

                    // Convert to destination SQL type
                    expr = SqlTypeConverter.Convert(expr, contextParam, sourceSqlType, destSqlType);
                }

                expr = Expr.Box(expr);

                valueAccessors[destVariableName] = Expression.Lambda<Func<ExpressionExecutionContext, object>>(expr, contextParam).Compile();
            }

            return valueAccessors;
        }

        public override void AddRequiredColumns(NodeCompilationContext context, IList<string> requiredColumns)
        {
            foreach (var variable in Variables)
            {
                if (!requiredColumns.Contains(variable.SourceColumn, StringComparer.OrdinalIgnoreCase))
                    requiredColumns.Add(variable.SourceColumn);
            }

            Source.AddRequiredColumns(context, requiredColumns);
        }

        public override object Clone()
        {
            var clone = new AssignVariablesNode
            {
                BatchSize = BatchSize,
                BypassCustomPluginExecution = BypassCustomPluginExecution,
                ContinueOnError = ContinueOnError,
                DataSource = DataSource,
                Index = Index,
                Length = Length,
                MaxDOP = MaxDOP,
                Source = (IDataExecutionPlanNodeInternal)Source.Clone(),
                Sql = Sql
            };

            clone.Source.Parent = clone;
            clone.Variables.AddRange(Variables);

            return clone;
        }
    }

    class VariableAssignment
    {
        public string VariableName { get; set; }

        public string SourceColumn { get; set; }
    }
}
