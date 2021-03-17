using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    public class UpdateNode : BaseNode, IDmlQueryExecutionPlanNode, ISingleSourceExecutionPlanNode
    {
        private int _executionCount;
        private TimeSpan _duration;

        public string Sql { get; set; }

        public int Index { get; set; }

        public int Length { get; set; }

        public override int ExecutionCount => _executionCount;

        public override TimeSpan Duration => _duration;

        public IDataExecutionPlanNode Source { get; set; }

        public override void AddRequiredColumns(IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes, IList<string> requiredColumns)
        {
            throw new NotImplementedException();
        }

        public string Execute(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues)
        {
            throw new NotImplementedException();
        }

        public IRootExecutionPlanNode FoldQuery(IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes)
        {
            Source = Source.FoldQuery(metadata, options, parameterTypes);
            return this;
        }

        public override IEnumerable<IDataExecutionPlanNode> GetSources()
        {
            yield return Source;
        }

        public override string ToString()
        {
            return "UPDATE";
        }
    }

    public class ColumnMapping
    {
        public string SourceColumn { get; set; }
        public string OutputColumn { get; set; }
    }
}
