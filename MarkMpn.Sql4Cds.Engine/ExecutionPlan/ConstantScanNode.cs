using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// Returns a constant data set
    /// </summary>
    class ConstantScanNode : BaseDataNode
    {
        /// <summary>
        /// The list of values to be returned
        /// </summary>
        [Browsable(false)]
        public List<IDictionary<string, ScalarExpression>> Values { get; } = new List<IDictionary<string, ScalarExpression>>();

        /// <summary>
        /// The alias for the data source
        /// </summary>
        [Category("Constant Scan")]
        [Description("The alias for the data source")]
        public string Alias { get; set; }

        /// <summary>
        /// The types of values to be returned
        /// </summary>
        [Browsable(false)]
        public Dictionary<string, DataTypeReference> Schema { get; private set; } = new Dictionary<string, DataTypeReference>();

        protected override IEnumerable<Entity> ExecuteInternal(NodeExecutionContext context)
        {
            var compilationContext = new ExpressionCompilationContext(context, null, null);
            var executionContext = new ExpressionExecutionContext(context);

            foreach (var row in Values)
            {
                var value = new Entity();

                foreach (var col in Schema)
                    value[PrefixWithAlias(col.Key)] = row[col.Key].Compile(compilationContext)(executionContext);

                yield return value;
            }
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            return Array.Empty<IDataExecutionPlanNode>();
        }

        public override INodeSchema GetSchema(NodeCompilationContext context)
        {
            return new NodeSchema(
                primaryKey: null,
                schema: Schema.ToDictionary(kvp => PrefixWithAlias(kvp.Key), kvp => kvp.Value, StringComparer.OrdinalIgnoreCase),
                aliases: Schema.ToDictionary(kvp => kvp.Key, kvp => (IReadOnlyList<string>) new List<string> { PrefixWithAlias(kvp.Key) }, StringComparer.OrdinalIgnoreCase),
                notNullColumns: null,
                sortOrder: null);
        }

        private string PrefixWithAlias(string columnName)
        {
            if (String.IsNullOrEmpty(Alias))
                return columnName;

            return Alias + "." + columnName;
        }

        public override IDataExecutionPlanNodeInternal FoldQuery(NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            return this;
        }

        public override void AddRequiredColumns(NodeCompilationContext context, IList<string> requiredColumns)
        {
        }

        protected override RowCountEstimate EstimateRowsOutInternal(NodeCompilationContext context)
        {
            return new RowCountEstimateDefiniteRange(Values.Count, Values.Count);
        }

        protected override IEnumerable<string> GetVariablesInternal()
        {
            return Values
                .SelectMany(row => row.Values)
                .SelectMany(expr => expr.GetVariables())
                .Distinct();
        }

        public override object Clone()
        {
            var clone = new ConstantScanNode
            {
                Alias = Alias,
                Schema = Schema
            };

            clone.Values.AddRange(Values);

            return clone;
        }
    }
}
