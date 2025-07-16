using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlTypes;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    class SegmentNode : BaseDataNode, ISingleSourceExecutionPlanNode
    {
        /// <summary>
        /// The column name that indicates the start of each segment
        /// </summary>
        [Category("Segment")]
        public string SegmentColumn { get; set; }

        /// <summary>
        /// The columns that identify the key for each segment
        /// </summary>
        [Category("Segment")]
        public List<string> GroupBy { get; set; }

        /// <summary>
        /// The data source to sort
        /// </summary>
        [Browsable(false)]
        public IDataExecutionPlanNodeInternal Source { get; set; }

        public override void AddRequiredColumns(NodeCompilationContext context, IList<string> requiredColumns)
        {
            foreach (var col in GroupBy)
            {
                if (!requiredColumns.Contains(col))
                    requiredColumns.Add(col);
            }

            Source.AddRequiredColumns(context, requiredColumns);
        }

        public override IDataExecutionPlanNodeInternal FoldQuery(NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            Source = Source.FoldQuery(context, hints);
            return this;
        }

        public override INodeSchema GetSchema(NodeCompilationContext context)
        {
            var schema = new NodeSchema(Source.GetSchema(context));
            ((ColumnList)schema.Schema).Add(SegmentColumn, new ColumnDefinition(DataTypeHelpers.Bit, isNullable: false, isCalculated: true, isVisible: false));
            return schema;
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
            var comparer = new DistinctEqualityComparer(GroupBy);
            Entity prev = null;

            foreach (var entity in Source.Execute(context))
            {
                if (prev == null || !comparer.Equals(prev, entity))
                {
                    entity[SegmentColumn] = SqlBoolean.True;
                    prev = entity;
                }
                else
                {
                    entity[SegmentColumn] = SqlBoolean.False;
                }

                yield return entity;
            }
        }

        public override object Clone()
        {
            return new SegmentNode
            {
                Source = (IDataExecutionPlanNodeInternal)Source.Clone(),
                GroupBy = GroupBy,
                SegmentColumn = SegmentColumn,
            };
        }
    }
}
