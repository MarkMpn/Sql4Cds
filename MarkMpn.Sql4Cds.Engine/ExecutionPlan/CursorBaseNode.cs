using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    abstract class CursorBaseNode : BaseNode, IRootExecutionPlanNodeInternal
    {
        public string Sql { get; set; }
        public int Index { get; set; }
        public int Length { get; set; }
        public int LineNumber { get; set; }
        public override int ExecutionCount => 0;

        public override TimeSpan Duration => TimeSpan.Zero;

        public override void AddRequiredColumns(NodeCompilationContext context, IList<string> requiredColumns)
        {
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            if (PopulationQuery != null)
                yield return PopulationQuery;

            if (FetchQuery != null)
                yield return FetchQuery;
        }

        public IExecutionPlanNodeInternal PopulationQuery { get; set; }

        public IDataReaderExecutionPlanNode FetchQuery { get; set; }

        public abstract object Clone();

        public IRootExecutionPlanNodeInternal[] FoldQuery(NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            if (PopulationQuery is IDataExecutionPlanNodeInternal population)
                PopulationQuery = population.FoldQuery(context, hints);

            if (FetchQuery is IDataExecutionPlanNodeInternal fetch)
                FetchQuery = (IDataReaderExecutionPlanNode)fetch.FoldQuery(context, hints);

            return new[] { this };
        }
    }
}
