using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.Engine.Visitors;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// Calculates the value of scalar expressions
    /// </summary>
    class ComputeScalarNode : BaseDataNode, ISingleSourceExecutionPlanNode
    {
        /// <summary>
        /// The names and associated expressions for the columns to calculate
        /// </summary>
        [Category("Compute Scalar")]
        [Description("The names and associated expressions for the columns to calculate")]
        public Dictionary<string, ScalarExpression> Columns { get; } = new Dictionary<string, ScalarExpression>();

        /// <summary>
        /// The data source to use for the calculations
        /// </summary>
        [Browsable(false)]
        public IDataExecutionPlanNode Source { get; set; }

        protected override IEnumerable<Entity> ExecuteInternal(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues)
        {
            var schema = Source.GetSchema(dataSources, parameterTypes);
            var columns = Columns
                .Select(kvp => new { Name = kvp.Key, Expression = kvp.Value.Compile(schema, parameterTypes) })
                .ToList();

            foreach (var entity in Source.Execute(dataSources, options, parameterTypes, parameterValues))
            {
                foreach (var col in columns)
                    entity[col.Name] = col.Expression(entity, parameterValues, options);

                yield return entity;
            }
        }

        public override NodeSchema GetSchema(IDictionary<string, DataSource> dataSources, IDictionary<string, Type> parameterTypes)
        {
            // Copy the source schema and add in the additional computed columns
            var sourceSchema = Source.GetSchema(dataSources, parameterTypes);
            var schema = new NodeSchema { PrimaryKey = sourceSchema.PrimaryKey };

            foreach (var col in sourceSchema.Schema)
                schema.Schema.Add(col);

            foreach (var alias in sourceSchema.Aliases)
                schema.Aliases.Add(alias);

            foreach (var calc in Columns)
                schema.Schema[calc.Key] = calc.Value.GetType(sourceSchema, null, parameterTypes);

            return schema;
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            yield return Source;
        }

        public override IDataExecutionPlanNode FoldQuery(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes)
        {
            Source = Source.FoldQuery(dataSources, options, parameterTypes);

            // Combine multiple ComputeScalar nodes. Calculations in this node might be dependent on those in the previous node, so rewrite any references
            // to the earlier computed columns
            if (Source is ComputeScalarNode computeScalar)
            {
                var rewrites = new Dictionary<ScalarExpression, ScalarExpression>();

                foreach (var prevCalc in computeScalar.Columns)
                    rewrites[prevCalc.Key.ToColumnReference()] = prevCalc.Value;

                var rewrite = new RewriteVisitor(rewrites);

                foreach (var calc in Columns)
                    computeScalar.Columns.Add(calc.Key, rewrite.ReplaceExpression(calc.Value));

                return computeScalar;
            }

            Source.Parent = this;
            return this;
        }

        public override void AddRequiredColumns(IDictionary<string, DataSource> dataSources, IDictionary<string, Type> parameterTypes, IList<string> requiredColumns)
        {
            var schema = Source.GetSchema(dataSources, parameterTypes);

            var calcSourceColumns = Columns.Values
                .SelectMany(expr => expr.GetColumns());

            foreach (var col in calcSourceColumns)
            {
                if (!schema.ContainsColumn(col, out var normalized))
                    continue;

                if (!requiredColumns.Contains(normalized, StringComparer.OrdinalIgnoreCase))
                    requiredColumns.Add(normalized);
            }

            Source.AddRequiredColumns(dataSources, parameterTypes, requiredColumns);
        }

        public override int EstimateRowsOut(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes)
        {
            return Source.EstimateRowsOut(dataSources, options, parameterTypes);
        }
    }
}
