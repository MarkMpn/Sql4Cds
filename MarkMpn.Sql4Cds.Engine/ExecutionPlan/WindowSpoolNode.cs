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
    class WindowSpoolNode : BaseDataNode, ISingleSourceExecutionPlanNode
    {
        class RowWithWindowFrame
        {
            public Entity Row { get; set; }
            public long RowNumber { get; set; }
            public long BottomRowNumber { get; set; }
            public long TopRowNumber { get; set; }
        }

        [Browsable(false)]
        public IDataExecutionPlanNodeInternal Source { get; set; }

        [Category("Window Spool")]
        [Description("The name of the column to store the window number in")]
        public string WindowCountColumn { get; set; }

        [Category("Window Spool")]
        [Description("The name of the column to store the segment flag in")]
        public string SegmentColumn { get; set; }

        [Category("Window Spool")]
        [Description("The name of the column to store the row number in")]
        public string RowNumberColumn { get; set; }

        [Category("Window Spool")]
        [Description("The name of the column to store the bottom row number for the window in")]
        public string BottomRowNumberColumn { get; set; }

        [Category("Window Spool")]
        [Description("The name of the column to store the top row number for the window in")]
        public string TopRowNumberColumn { get; set; }

        [Category("Window Spool")]
        [Description("Indicates whether the fast track optimization should be used")]
        public bool UseFastTrackOptimization { get; set; }

        public override void AddRequiredColumns(NodeCompilationContext context, IList<string> requiredColumns)
        {
            Source.AddRequiredColumns(context, requiredColumns);
        }

        public override IDataExecutionPlanNodeInternal FoldQuery(NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            Source = Source.FoldQuery(context, hints);
            return this;
        }

        public override INodeSchema GetSchema(NodeCompilationContext context)
        {
            var schema = Source.GetSchema(context);

            // Add the window count column
            var cols = new ColumnList();

            foreach (var col in schema.Schema)
                cols.Add(col);

            cols.Add(WindowCountColumn, new ColumnDefinition(DataTypeHelpers.BigInt, false, true, false));

            return new NodeSchema(
                schema: cols,
                aliases: schema.Aliases,
                primaryKey: schema.PrimaryKey,
                sortOrder: schema.SortOrder);
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            yield return Source;
        }

        protected override RowCountEstimate EstimateRowsOutInternal(NodeCompilationContext context)
        {
            var estimate = Source.EstimateRowsOut(context);

            // Window spool will repeat each row for each window it is in, so multiply the row count by 10
            return new RowCountEstimate(estimate.Value * 10);
        }

        protected override IEnumerable<Entity> ExecuteInternal(NodeExecutionContext context)
        {
            var windowRowOffset = 0L;
            var windowFrame = new List<RowWithWindowFrame>();
            var windowCount = (SqlInt64)0L;

            foreach (var row in Source.Execute(context))
            {
                var rowNumber = (SqlInt64)row[RowNumberColumn];
                var bottomRowNumber = (SqlInt64)row[BottomRowNumberColumn];
                var topRowNumber = (SqlInt64)row[TopRowNumberColumn];

                if (((SqlBoolean)row[SegmentColumn]).Value)
                {
                    // Finished the previous segment - replay the rows and the associated rows in the same frame
                    foreach (var windowRow in windowFrame)
                    {
                        windowCount = windowCount + 1;

                        windowRow.Row[WindowCountColumn] = windowCount;
                        yield return windowRow.Row;

                        var topRow = Math.Max(0, windowRow.TopRowNumber - windowRowOffset);
                        var bottomRow = Math.Min(windowRow.BottomRowNumber - windowRowOffset, windowFrame.Count - 1);

                        // If we're using the fast track optimization we only need to output the additional row for this window,
                        // which will be the bottom row
                        if (UseFastTrackOptimization)
                            topRow = bottomRow;

                        for (var i = topRow; i <= bottomRow; i++)
                        {
                            windowFrame[(int)i].Row[WindowCountColumn] = windowCount;
                            yield return windowFrame[(int)i].Row;
                        }
                    }

                    // Start of a new segment - flush the old window details
                    windowRowOffset = rowNumber.Value;
                    windowFrame.Clear();
                }

                // Add the row to the window frame
                windowFrame.Add(new RowWithWindowFrame
                {
                    Row = row,
                    RowNumber = rowNumber.Value,
                    BottomRowNumber = bottomRowNumber.Value,
                    TopRowNumber = topRowNumber.Value
                });
            }

            // Replay the rows in the final segment
            foreach (var windowRow in windowFrame)
            {
                windowCount = windowCount + 1;

                windowRow.Row[WindowCountColumn] = windowCount;
                yield return windowRow.Row;

                var topRow = Math.Max(0, windowRow.TopRowNumber - windowRowOffset);
                var bottomRow = Math.Min(windowRow.BottomRowNumber - windowRowOffset, windowFrame.Count - 1);

                // If we're using the fast track optimization we only need to output the additional row for this window,
                // which will be the bottom row
                if (UseFastTrackOptimization)
                    topRow = bottomRow;

                for (var i = topRow; i <= bottomRow; i++)
                {
                    windowFrame[(int)i].Row[WindowCountColumn] = windowCount;
                    yield return windowFrame[(int)i].Row;
                }
            }
        }

        public override object Clone()
        {
            return new WindowSpoolNode
            {
                Source = (IDataExecutionPlanNodeInternal)Source.Clone(),
                WindowCountColumn = WindowCountColumn,
                SegmentColumn = SegmentColumn,
                RowNumberColumn = RowNumberColumn,
                BottomRowNumberColumn = BottomRowNumberColumn,
                TopRowNumberColumn = TopRowNumberColumn,
                UseFastTrackOptimization = UseFastTrackOptimization
            };
        }
    }
}
