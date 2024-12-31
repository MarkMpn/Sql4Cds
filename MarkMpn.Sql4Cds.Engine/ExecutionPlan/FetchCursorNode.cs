using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    class FetchCursorNode : BaseNode, ISingleSourceExecutionPlanNode, IDataReaderExecutionPlanNode
    {
        public override int ExecutionCount => throw new NotImplementedException();

        public override TimeSpan Duration => throw new NotImplementedException();

        public string Sql { get; set; }

        public int Index { get; set; }

        public int Length { get; set; }

        public int LineNumber { get; set; }

        public IDataExecutionPlanNodeInternal Source { get; set; }

        public string CursorName { get; set; }

        public override void AddRequiredColumns(NodeCompilationContext context, IList<string> requiredColumns)
        {
            throw new NotImplementedException();
        }

        public object Clone()
        {
            throw new NotImplementedException();
        }

        public DbDataReader Execute(NodeExecutionContext context, CommandBehavior behavior)
        {
            throw new NotImplementedException();
        }

        public IRootExecutionPlanNodeInternal[] FoldQuery(NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            throw new NotImplementedException();
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            throw new NotImplementedException();
        }
    }
}
