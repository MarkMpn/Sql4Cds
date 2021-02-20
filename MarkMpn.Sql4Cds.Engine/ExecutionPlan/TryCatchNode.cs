using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    public class TryCatchNode : BaseNode
    {
        public IExecutionPlanNode TrySource { get; set; }
        public IExecutionPlanNode CatchSource { get; set; }
        public Func<Exception,bool> ExceptionFilter { get; set; }

        public override IEnumerable<Entity> Execute(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string, object> parameterValues)
        {
            try
            {
                return TrySource.Execute(org, metadata, options, parameterValues);
            }
            catch (Exception ex)
            {
                if (ExceptionFilter != null && !ExceptionFilter(ex))
                    throw;

                return CatchSource.Execute(org, metadata, options, parameterValues);
            }
        }

        public override IEnumerable<string> GetRequiredColumns()
        {
            return TrySource.GetRequiredColumns();
        }

        public override NodeSchema GetSchema(IAttributeMetadataCache metadata)
        {
            return TrySource.GetSchema(metadata);
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            yield return TrySource;
            yield return CatchSource;
        }

        public override IExecutionPlanNode MergeNodeDown(IAttributeMetadataCache metadata, IQueryExecutionOptions options)
        {
            TrySource = TrySource.MergeNodeDown(metadata, options);
            CatchSource = CatchSource.MergeNodeDown(metadata, options);
            return this;
        }
    }
}
