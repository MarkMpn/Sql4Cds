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
    class SequenceProjectNode : BaseDataNode, ISingleSourceExecutionPlanNode
    {
        class AggregateFunctionState
        {
            public AggregateFunction AggregateFunction { get; set; }

            public object State { get; set; }
        }

        /// <summary>
        /// The list of aggregate values to produce
        /// </summary>
        [Category("Sequence Project")]
        [Description("The list of values to produce")]
        public Dictionary<string, Aggregate> DefinedValues { get; } = new Dictionary<string, Aggregate>();

        [Category("Sequence Project")]
        public string SegmentColumn { get; set; }

        [Category("Sequence Project")]
        public string SegmentColumn2 { get; set; }

        [Browsable(false)]
        public IDataExecutionPlanNodeInternal Source { get; set; }

        public override void AddRequiredColumns(NodeCompilationContext context, IList<string> requiredColumns)
        {
            Source.AddRequiredColumns(context, requiredColumns);
        }

        public override IDataExecutionPlanNodeInternal FoldQuery(NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            Source = Source.FoldQuery(context, hints);

            // TODO: Fold multiple sequence project nodes together if they use the same window definition
            return this;
        }

        public override INodeSchema GetSchema(NodeCompilationContext context)
        {
            // Copy the source schema and add in the additional computed columns
            var sourceSchema = Source.GetSchema(context);
            var schema = new ColumnList();

            foreach (var col in sourceSchema.Schema)
                schema[col.Key] = col.Value;

            foreach (var calc in DefinedValues)
                schema[calc.Key] = new ColumnDefinition(calc.Value.ReturnType, true, true);

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

        protected override RowCountEstimate EstimateRowsOutInternal(NodeCompilationContext context)
        {
            return Source.EstimateRowsOut(context);
        }

        protected override IEnumerable<Entity> ExecuteInternal(NodeExecutionContext context)
        {
            var schema = Source.GetSchema(context);
            var ecc = new ExpressionCompilationContext(context, schema, null);
            var eec = new ExpressionExecutionContext(ecc);
            var values = new Dictionary<string, AggregateFunctionState>();

            foreach (var calc in DefinedValues)
            {
                var state = new AggregateFunctionState();

                switch (calc.Value.AggregateType)
                {
                    case AggregateType.RowNumber:
                        state.AggregateFunction = new RowNumber();
                        break;

                    case AggregateType.Rank:
                        state.AggregateFunction = new Rank(() => eec.Entity[SegmentColumn2]);
                        break;

                    case AggregateType.DenseRank:
                        state.AggregateFunction = new DenseRank(() => eec.Entity[SegmentColumn2]);
                        break;

                    default:
                        throw new QueryExecutionException("Unknown aggregate type");
                }

                values[calc.Key] = state;
            }

            foreach (var entity in Source.Execute(context))
            {
                eec.Entity = entity;

                if (entity[SegmentColumn].Equals(SqlBoolean.True))
                {
                    foreach (var calc in values)
                        calc.Value.State = calc.Value.AggregateFunction.Reset();
                }

                foreach (var calc in values)
                {
                    calc.Value.AggregateFunction.NextRecord(calc.Value.State, eec);
                    entity[calc.Key] = calc.Value.AggregateFunction.GetValue(calc.Value.State);
                }

                yield return entity;
            }
        }

        public override object Clone()
        {
            var clone = new SequenceProjectNode
            {
                Source = (IDataExecutionPlanNodeInternal)Source.Clone(),
                SegmentColumn = SegmentColumn,
                SegmentColumn2 = SegmentColumn2
            };

            foreach (var calc in DefinedValues)
                clone.DefinedValues.Add(calc.Key, calc.Value);

            return clone;
        }

        public override string ToString()
        {
            return "Sequence Project\r\n(Compute Scalar)";
        }
    }
}
