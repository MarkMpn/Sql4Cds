using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    public interface IRootExecutionPlanNode : IExecutionPlanNode
    {
        string Sql { get; set; }
        int Index { get; set; }
        int Length { get; set; }

        IRootExecutionPlanNode FoldQuery(IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes);
    }
}
