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
        public IDataExecutionPlanNodeInternal Source { get; set; }

        protected override IEnumerable<Entity> ExecuteInternal(NodeExecutionContext context)
        {
            var schema = Source.GetSchema(context);
            var expressionCompilationContext = new ExpressionCompilationContext(context, schema, null);
            var columns = Columns
                .Select(kvp => new { Name = kvp.Key, Expression = kvp.Value.Compile(expressionCompilationContext) })
                .ToList();

            var expressionContext = new ExpressionExecutionContext(context);

            foreach (var entity in Source.Execute(context))
            {
                expressionContext.Entity = entity;

                foreach (var col in columns)
                    entity[col.Name] = col.Expression(expressionContext);

                yield return entity;
            }
        }

        public override INodeSchema GetSchema(NodeCompilationContext context)
        {
            // Copy the source schema and add in the additional computed columns
            var sourceSchema = Source.GetSchema(context);
            var expressionCompilationContext = new ExpressionCompilationContext(context, sourceSchema, null);
            var schema = new ColumnList();

            foreach (var col in sourceSchema.Schema)
                schema[col.Key] = col.Value;

            foreach (var calc in Columns)
            {
                calc.Value.GetType(expressionCompilationContext, out var calcType);
                schema[calc.Key] = new ColumnDefinition(calcType, true, true);
            }

            return new NodeSchema(
                primaryKey: sourceSchema.PrimaryKey,
                schema: schema,
                aliases: sourceSchema.Aliases,
                sortOrder: sourceSchema.SortOrder);
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            yield return Source;
        }

        public override IDataExecutionPlanNodeInternal FoldQuery(NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            Source = Source.FoldQuery(context, hints);

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
            
            // Move literal expressions to the parent ConstantScan node for queries like SELECT 1
            if (Source is ConstantScanNode constant && String.IsNullOrEmpty(constant.Alias))
            {
                var folded = new List<string>();
                var expressionContext = new ExpressionCompilationContext(context, null, null);

                foreach (var calc in Columns)
                {
                    if (calc.Value is ConvertCall c1 && c1.Parameter is Literal ||
                        calc.Value is CastCall c2 && c2.Parameter is Literal ||
                        calc.Value is Literal)
                    {
                        foreach (var row in constant.Values)
                            row[calc.Key] = calc.Value;

                        folded.Add(calc.Key);

                        if (calc.Value is ConvertCall convert)
                        {
                            constant.Schema[calc.Key] = new ColumnDefinition(convert.DataType, true, true);
                        }
                        else if (calc.Value is CastCall cast)
                        {
                            constant.Schema[calc.Key] = new ColumnDefinition(cast.DataType, true, true);
                        }
                        else
                        {
                            calc.Value.GetType(expressionContext, out var calcType);
                            constant.Schema[calc.Key] = new ColumnDefinition(calcType, true, true);
                        }
                    }
                }

                if (folded.Count == Columns.Count)
                    return constant;

                foreach (var col in folded)
                    Columns.Remove(col);
            }

            // If we don't have any calculations, this node is not needed
            if (Columns.Count == 0)
                return Source;
            
            Source.Parent = this;
            return this;
        }

        public override void AddRequiredColumns(NodeCompilationContext context, IList<string> requiredColumns)
        {
            var schema = Source.GetSchema(context);

            var calcSourceColumns = Columns.Values
                .SelectMany(expr => expr.GetColumns());

            foreach (var col in calcSourceColumns)
            {
                if (!schema.ContainsColumn(col, out var normalized))
                    continue;

                if (!requiredColumns.Contains(normalized, StringComparer.OrdinalIgnoreCase))
                    requiredColumns.Add(normalized);
            }

            Source.AddRequiredColumns(context, requiredColumns);
        }

        protected override RowCountEstimate EstimateRowsOutInternal(NodeCompilationContext context)
        {
            return Source.EstimateRowsOut(context);
        }

        protected override IEnumerable<string> GetVariablesInternal()
        {
            return Columns
                .Values
                .SelectMany(expr => expr.GetVariables())
                .Distinct();
        }

        public override object Clone()
        {
            var clone = new ComputeScalarNode
            {
                Source = (IDataExecutionPlanNodeInternal)Source.Clone()
            };

            clone.Source.Parent = clone;

            foreach (var kvp in Columns)
                clone.Columns.Add(kvp.Key, kvp.Value);

            return clone;
        }
    }
}
