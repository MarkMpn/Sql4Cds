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

        public IExecutionPlanNodeInternal PopulationQuery { get; set; }

        public IDataReaderExecutionPlanNode FetchQuery { get; set; }

        public abstract object Clone();

        public IRootExecutionPlanNodeInternal[] FoldQuery(NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            throw new NotImplementedException();
        }
    }
}
